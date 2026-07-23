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

	desired := assigned + b.cfg.Slots.Warm

	b.mu.Lock()
	blocked := b.paused || b.draining || b.dockerDown
	b.mu.Unlock()
	if blocked {
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
	b.poke()
	return b.slots.Live(), nil
}

func (s *scaler) HandleJobStarted(ctx context.Context, job *scaleset.JobStarted) error {
	b := s.b
	info := &slots.Job{
		RequestID:   job.RunnerRequestID,
		Repo:        job.OwnerName + "/" + job.RepositoryName,
		Workflow:    job.JobWorkflowRef,
		DisplayName: job.JobDisplayName,
		RunURL:      fmt.Sprintf("https://github.com/%s/%s/actions/runs/%d", job.OwnerName, job.RepositoryName, job.WorkflowRunID),
		StartedAt:   time.Now(),
	}
	found := b.slots.Mutate(job.RunnerName, func(sl *slots.Slot) {
		sl.State = slots.Running
		sl.Job = info
		sl.Since = time.Now()
	})
	slot := b.slotIndexOf(job.RunnerName)
	if found {
		b.event(bus.Info, slot, fmt.Sprintf("job started: %s (%s)", job.JobDisplayName, info.Repo))
	}
	return nil
}

func (s *scaler) HandleJobCompleted(ctx context.Context, job *scaleset.JobCompleted) error {
	b := s.b
	slot := b.slotIndexOf(job.RunnerName)
	var containerID string
	b.slots.Mutate(job.RunnerName, func(sl *slots.Slot) {
		sl.State = slots.Reaping
		containerID = sl.ContainerID
	})
	if strings.EqualFold(job.Result, "succeeded") {
		b.counters.Completed.Add(1)
	} else {
		b.counters.Failed.Add(1)
	}
	b.event(bus.Info, slot, fmt.Sprintf("job %s: %s (%s)", job.Result, job.JobDisplayName, job.OwnerName+"/"+job.RepositoryName))
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
	for _, s := range b.slots.Snapshot() {
		if s.RunnerName == runnerName {
			return s.Index
		}
	}
	return -1
}

// spawnOne reserves a slot, mints a JIT config and starts a runner container.
// Returns false when no capacity or on failure (the failed slot goes to
// Errored and is retried by a later HandleDesiredRunnerCount tick).
func (b *Broker) spawnOne(ctx context.Context) bool {
	name := fmt.Sprintf("%s-s%d-%s", b.cfg.ScaleSet.Name, 0, uuid.NewString()[:8])
	index, cpuset, ok := b.slots.Acquire(name)
	if !ok {
		return false
	}
	// The slot index is part of the runner name for at-a-glance mapping in
	// GitHub's UI and docker ps; rewrite it now that we know the slot.
	name = fmt.Sprintf("%s-s%d-%s", b.cfg.ScaleSet.Name, index, uuid.NewString()[:8])
	b.slots.MutateIndex(index, func(sl *slots.Slot) { sl.RunnerName = name })

	fail := func(err error) bool {
		b.counters.SpawnErrors.Add(1)
		b.slots.MutateIndex(index, func(sl *slots.Slot) {
			sl.State = slots.Errored
			sl.Err = err.Error()
			sl.Since = time.Now()
		})
		b.event(bus.Error, index, fmt.Sprintf("spawn failed: %v", err))
		return false
	}

	jit, err := b.client.GenerateJitRunnerConfig(ctx, &scaleset.RunnerScaleSetJitRunnerSetting{
		Name:       name,
		WorkFolder: "/home/runner/_work",
	}, b.scaleSet.ID)
	if err != nil {
		return fail(fmt.Errorf("jit config: %w", err))
	}

	if err := b.provider.EnsureImage(ctx); err != nil {
		return fail(err)
	}
	containerID, err := b.provider.Spawn(ctx, index, name, cpuset, jit.EncodedJITConfig)
	if err != nil {
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
	go b.watchContainer(index, name, containerID)
	return true
}

// retryErroredSlots returns slots that failed a spawn back to idle after a
// cooldown, so capacity self-heals instead of pinning at error forever.
func (b *Broker) retryErroredSlots() {
	for _, s := range b.slots.Snapshot() {
		if s.State == slots.Errored && time.Since(s.Since) > 30*time.Second {
			b.slots.Free(s.Index, s.RunnerName)
		}
	}
}

// cullIdle removes up to n jobless runners (oldest first): their containers
// are stopped and their still-unused registrations removed from GitHub.
func (b *Broker) cullIdle(ctx context.Context, n int) {
	for _, s := range b.slots.IdleRunners() {
		if n <= 0 {
			return
		}
		if b.provider.HasWorker(ctx, s.ContainerID) {
			continue // a job just landed; not idle after all
		}
		if err := b.provider.Remove(ctx, s.ContainerID); err != nil {
			b.event(bus.Warn, s.Index, fmt.Sprintf("cull failed: %v", err))
			continue
		}
		if ref, err := b.client.GetRunnerByName(ctx, s.RunnerName); err == nil {
			_ = b.client.RemoveRunner(ctx, int64(ref.ID))
		}
		b.slots.Free(s.Index, s.RunnerName)
		b.event(bus.Info, s.Index, fmt.Sprintf("culled idle runner %s", s.RunnerName))
		n--
	}
}

// watchContainer notices a runner container exiting. A clean ephemeral exit
// is normally followed by a JobCompleted message that does the accounting; if
// none arrives within a grace window (daemon missed it, runner died early, or
// it crashed mid-job) the watcher cleans up and surfaces the evidence.
func (b *Broker) watchContainer(index int, runnerName, containerID string) {
	ctx := context.Background()
	exitCode, err := b.provider.Wait(ctx, containerID)
	if err != nil {
		return // daemon shutting down or docker gone; adoption will reconcile
	}

	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if !b.ownsSlot(index, runnerName) {
			return // JobCompleted already accounted for it
		}
		time.Sleep(2 * time.Second)
	}

	logs, _ := b.provider.LogsTail(ctx, containerID, 50)
	_ = b.provider.Remove(ctx, containerID)
	if b.slots.Free(index, runnerName) {
		b.counters.Failed.Add(1)
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
	owns := false
	b.slots.MutateIndex(index, func(sl *slots.Slot) {
		owns = sl.RunnerName == runnerName
	})
	return owns
}

func tailLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " | ")
}
