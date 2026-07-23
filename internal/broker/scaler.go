package broker

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/actions/scaleset"
	"github.com/google/uuid"

	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/slots"
)

// scaler implements listener.Scaler by converging live runner containers
// toward GitHub's assigned-job count (plus configured warm capacity), bounded
// by free slots.
type scaler struct{ b *Broker }

func (s *scaler) HandleDesiredRunnerCount(ctx context.Context, assigned int) (int, error) {
	b := s.b
	b.retryErroredSlots()

	b.mu.Lock()
	pausedOrDraining := b.paused || b.draining
	dockerDown := b.dockerDown
	b.mu.Unlock()

	// Jobs GitHub already assigned to this scale set are OURS — no other
	// runner will take them, so we run them even while paused/draining
	// (stranding acquired jobs is worse than finishing them). Pause/drain
	// only stop NEW capacity: no warm runners, no new acquisition (cap 0 via
	// effectiveCap). Docker-down blocks spawning outright.
	desired := assigned + b.cfg.Slots.Warm
	if pausedOrDraining {
		desired = assigned
	}
	if dockerDown {
		desired = 0
	}

	live := b.slots.Live()
	for live < desired {
		if !b.spawnOne(ctx) {
			break
		}
		live = b.slots.Live()
	}
	// Excess jobless runners (warm reduced, pause, scale-down) are culled;
	// running jobs are never touched — they finish and free naturally.
	if excess := live - desired; excess > 0 {
		b.cullIdle(ctx, excess)
	}
	// Scale-down convergence: jobless runners on draining slots go now, not
	// whenever GitHub happens to route them a job.
	b.cullSlots(ctx, b.slots.DrainingReady(), "drained")
	b.poke()
	return b.slots.Live(), nil
}

func (s *scaler) HandleJobStarted(ctx context.Context, job *scaleset.JobStarted) error {
	b := s.b
	info := &slots.Job{
		RequestID:   job.RunnerRequestID,
		Repo:        bus.Sanitize(job.OwnerName + "/" + job.RepositoryName),
		Workflow:    bus.Sanitize(job.JobWorkflowRef),
		DisplayName: bus.Sanitize(job.JobDisplayName),
		RunURL:      fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d", job.OwnerName, job.RepositoryName, job.WorkflowRunID),
		StartedAt:   time.Now(),
	}
	found := b.slots.Mutate(job.RunnerName, func(sl *slots.Slot) {
		sl.State = slots.Running
		sl.Job = info
		sl.Since = time.Now()
	})
	if found {
		b.event(bus.Info, b.slotIndexOf(job.RunnerName), fmt.Sprintf("job started: %s (%s)", info.DisplayName, info.Repo))
	}
	return nil
}

func (s *scaler) HandleJobCompleted(ctx context.Context, job *scaleset.JobCompleted) error {
	b := s.b
	slot := b.slotIndexOf(job.RunnerName)
	var containerID string
	found := b.slots.Mutate(job.RunnerName, func(sl *slots.Slot) {
		sl.State = slots.Reaping
		containerID = sl.ContainerID
	})
	// Count only when we still owned the slot: if the container-exit watcher
	// already reaped (and counted) this runner, a late JobCompleted must not
	// count the same job twice.
	if found {
		if strings.EqualFold(job.Result, "succeeded") {
			b.counters.completed.Add(1)
		} else {
			b.counters.failed.Add(1)
		}
		b.event(bus.Info, slot, fmt.Sprintf("job %s: %s (%s)",
			bus.Sanitize(job.Result), bus.Sanitize(job.JobDisplayName), bus.Sanitize(job.OwnerName+"/"+job.RepositoryName)))
	}
	if containerID != "" {
		if err := b.provider.Remove(ctx, containerID); err != nil {
			b.event(bus.Warn, slot, fmt.Sprintf("container remove failed: %v", err))
		}
	}
	b.slots.Free(slot, job.RunnerName)
	b.poke()
	return nil
}

func (b *Broker) slotIndexOf(runnerName string) int {
	if runnerName == "" {
		return -1
	}
	for _, s := range b.slots.Snapshot() {
		if s.RunnerName == runnerName {
			return s.Index
		}
	}
	return -1
}

// spawnOne reserves a slot, mints a JIT config and starts a runner container.
// Returns false when no capacity or on failure (the failed slot goes to
// Errored and is retried after a cooldown).
func (b *Broker) spawnOne(ctx context.Context) bool {
	if b.isDockerDown() {
		return false
	}
	index, cpuset, ok := b.slots.Acquire()
	if !ok {
		return false
	}
	// The runner name embeds the slot index for at-a-glance mapping in
	// GitHub's UI and docker ps, so it can only be minted after Acquire.
	name := fmt.Sprintf("%s-s%d-%s", b.cfg.ScaleSet.Name, index, uuid.NewString()[:8])
	b.slots.SetRunnerName(index, name)

	fail := func(err error) bool {
		b.counters.spawnErrors.Add(1)
		b.slots.MutateIndex(index, func(sl *slots.Slot) {
			sl.State = slots.Errored
			sl.RunnerName = ""
			sl.ContainerID = ""
			sl.Err = err.Error()
			sl.Since = time.Now()
		})
		b.event(bus.Error, index, fmt.Sprintf("spawn failed: %v", err))
		return false
	}

	jit, err := b.gh.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       name,
		WorkFolder: "/home/runner/_work",
	}, b.scaleSetID())
	if err != nil {
		return fail(fmt.Errorf("jit config: %w", err))
	}

	if err := b.provider.EnsureImage(ctx); err != nil {
		b.removeRunnerRegistration(ctx, name) // don't leak the JIT registration
		return fail(err)
	}
	containerID, err := b.provider.Spawn(ctx, index, name, cpuset, jit.EncodedJITConfig)
	if err != nil {
		b.removeRunnerRegistration(ctx, name)
		b.setDockerDown(b.provider.Ping(ctx) != nil)
		return fail(err)
	}
	b.setDockerDown(false)
	b.slots.MutateIndex(index, func(sl *slots.Slot) {
		sl.State = slots.Ready
		sl.ContainerID = containerID
		sl.Since = time.Now()
	})
	b.event(bus.Info, index, fmt.Sprintf("runner %s up (waiting for a job)", name))
	b.armWatcher(index, name, containerID)
	return true
}

// removeRunnerRegistration best-effort deletes a runner's GitHub registration
// by name. Tolerates the (nil, nil) not-found contract of GetRunnerByName.
func (b *Broker) removeRunnerRegistration(ctx context.Context, runnerName string) {
	ref, err := b.gh.GetRunnerByName(ctx, runnerName)
	if err != nil || ref == nil {
		return
	}
	_ = b.gh.RemoveRunner(ctx, int64(ref.ID))
}

// retryErroredSlots returns slots that failed a spawn back to idle after a
// cooldown, so capacity self-heals instead of pinning at error forever.
func (b *Broker) retryErroredSlots() {
	for _, s := range b.slots.Snapshot() {
		if s.State == slots.Errored && time.Since(s.Since) > b.tm.erroredCooldown {
			b.slots.FreeErrored(s.Index)
		}
	}
}

// cullIdle removes up to n jobless runners, oldest first.
func (b *Broker) cullIdle(ctx context.Context, n int) {
	idle := b.slots.IdleRunners()
	if len(idle) > n {
		idle = idle[:n]
	}
	b.cullSlots(ctx, idle, "excess idle")
}

// cullSlots removes the given jobless runners: container stopped and the
// still-unused GitHub registration deleted. A runner that turns out to be
// mid-job — or whose worker probe FAILS — is skipped: an unanswerable probe
// never licenses killing a container.
func (b *Broker) cullSlots(ctx context.Context, list []slots.Slot, reason string) {
	for _, s := range list {
		hasWorker, err := b.provider.HasWorker(ctx, s.ContainerID)
		if hasWorker || err != nil {
			continue
		}
		if err := b.provider.Remove(ctx, s.ContainerID); err != nil {
			b.event(bus.Warn, s.Index, fmt.Sprintf("cull failed: %v", err))
			continue
		}
		b.removeRunnerRegistration(ctx, s.RunnerName)
		if b.slots.Free(s.Index, s.RunnerName) {
			b.event(bus.Info, s.Index, fmt.Sprintf("culled %s runner %s", reason, s.RunnerName))
		}
	}
}

// armWatcher registers and starts the container-exit watcher exactly once per
// container.
func (b *Broker) armWatcher(index int, runnerName, containerID string) {
	b.mu.Lock()
	if _, exists := b.watched[containerID]; exists {
		b.mu.Unlock()
		return
	}
	b.watched[containerID] = struct{}{}
	ctx := b.runCtx
	b.mu.Unlock()
	go b.watchContainer(ctx, index, runnerName, containerID)
}

func (b *Broker) unwatch(containerID string) {
	b.mu.Lock()
	delete(b.watched, containerID)
	b.mu.Unlock()
}

// watchContainer notices a runner container exiting. A clean ephemeral exit
// is normally followed by a JobCompleted message that does the accounting; if
// none arrives within a grace window (daemon missed it, runner died early, or
// it crashed mid-job) the watcher cleans up and surfaces the evidence. A Wait
// failure (docker outage) just ends the watcher — reconcile re-arms or frees
// the slot once docker answers again.
func (b *Broker) watchContainer(ctx context.Context, index int, runnerName, containerID string) {
	defer b.unwatch(containerID)
	exitCode, err := b.provider.Wait(ctx, containerID)
	if err != nil {
		return
	}

	deadline := time.NewTimer(b.tm.watchGrace)
	defer deadline.Stop()
	poll := time.NewTicker(b.tm.watchPoll)
	defer poll.Stop()
	for {
		if !b.ownsSlot(index, runnerName) {
			return // JobCompleted already accounted for it
		}
		select {
		case <-ctx.Done():
			return
		case <-deadline.C:
			b.reapUnreported(ctx, index, runnerName, containerID, exitCode)
			return
		case <-poll.C:
		}
	}
}

func (b *Broker) reapUnreported(ctx context.Context, index int, runnerName, containerID string, exitCode int64) {
	logs, _ := b.provider.LogsTail(ctx, containerID, 50)
	_ = b.provider.Remove(ctx, containerID)
	if b.slots.Free(index, runnerName) {
		b.counters.failed.Add(1)
		msg := fmt.Sprintf("runner %s exited (code %d) without a job-completed report", runnerName, exitCode)
		if exitCode == 137 {
			msg += " — killed (OOM?)"
		}
		b.event(bus.Error, index, msg)
		if logs != "" {
			b.event(bus.Error, index, "last output: "+tailLines(logs, 5))
		}
		b.poke()
	}
}

func (b *Broker) ownsSlot(index int, runnerName string) bool {
	s, ok := b.slots.Get(index)
	return ok && runnerName != "" && s.RunnerName == runnerName
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return bus.Sanitize(strings.Join(lines, " | "))
}
