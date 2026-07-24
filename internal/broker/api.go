package broker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/config"
	"github.com/roark-dev/deckhand/internal/slots"
)

// Status is the control-API view of the daemon; it drives the TUI and
// `deckhand status`.
type Status struct {
	Broker    Info          `json:"broker"`
	Docker    DockerStatus  `json:"docker"`
	Slots     []slots.Slot  `json:"slots"`
	Counters  CounterValues `json:"counters"`
	Resources ResourceUsage `json:"resources"`
}

// ResourceUsage is CPU/memory summed across running slots against the host
// totals, sampled off a background ticker. OK is false until the first sample.
type ResourceUsage struct {
	OK            bool    `json:"ok"`
	CPUCoresUsed  float64 `json:"cpu_cores_used"`
	CPUCores      int     `json:"cpu_cores"`
	MemUsedBytes  int64   `json:"mem_used_bytes"`
	MemTotalBytes int64   `json:"mem_total_bytes"`
}

type Info struct {
	State         State  `json:"state"`
	Paused        bool   `json:"paused"`
	Draining      bool   `json:"draining"`
	ScaleSetName  string `json:"scale_set_name"`
	ScaleSetID    int    `json:"scale_set_id"`
	GitHubURL     string `json:"github_url"`
	Target        int    `json:"target"`
	Warm          int    `json:"warm"`
	SessionAgeSec int    `json:"session_age_sec"`
}

type DockerStatus struct {
	OK bool `json:"ok"`
}

type CounterValues struct {
	Completed        int64 `json:"completed"`
	Failed           int64 `json:"failed"`
	ZombiesReclaimed int64 `json:"zombies_reclaimed"`
	SpawnErrors      int64 `json:"spawn_errors"`
	Reconnects       int64 `json:"reconnects"`
	// Latency accounting (ms) from GitHub's job timestamps; consumers derive
	// averages from sum/count, and min vs max duration exposes the
	// contention-variance health signal.
	QueueMsSum    int64 `json:"queue_ms_sum"`
	QueueCount    int64 `json:"queue_count"`
	DurationMsSum int64 `json:"duration_ms_sum"`
	DurationCount int64 `json:"duration_count"`
	DurationMsMin int64 `json:"duration_ms_min"`
	DurationMsMax int64 `json:"duration_ms_max"`
}

func (b *Broker) Status() Status {
	b.mu.Lock()
	st := Info{
		State:        b.state,
		Paused:       b.paused,
		Draining:     b.draining,
		ScaleSetName: b.cfg.ScaleSet.Name,
		GitHubURL:    b.cfg.GitHub.URL,
		Target:       b.slots.Target(),
		Warm:         b.cfg.Slots.Warm,
	}
	if b.scaleSet != nil {
		st.ScaleSetID = b.scaleSet.ID
	}
	if !b.sessionAt.IsZero() && b.state == Active {
		st.SessionAgeSec = int(time.Since(b.sessionAt).Seconds())
	}
	dockerOK := !b.dockerDown
	b.mu.Unlock()
	b.resMu.Lock()
	res := b.res
	b.resMu.Unlock()
	return Status{
		Broker: st,
		Docker: DockerStatus{OK: dockerOK},
		Slots:  b.slots.Snapshot(),
		Resources: ResourceUsage{
			OK:            res.ok,
			CPUCoresUsed:  res.cpuCoresUsed,
			CPUCores:      res.ncpu,
			MemUsedBytes:  res.memUsedBytes,
			MemTotalBytes: res.memTotalBytes,
		},
		Counters: CounterValues{
			Completed:        b.counters.completed.Load(),
			Failed:           b.counters.failed.Load(),
			ZombiesReclaimed: b.counters.zombiesReclaimed.Load(),
			SpawnErrors:      b.counters.spawnErrors.Load(),
			Reconnects:       b.counters.reconnects.Load(),
			QueueMsSum:       b.counters.queueMsSum.Load(),
			QueueCount:       b.counters.queueCount.Load(),
			DurationMsSum:    b.counters.durMsSum.Load(),
			DurationCount:    b.counters.durCount.Load(),
			DurationMsMin:    b.counters.durMsMin.Load(),
			DurationMsMax:    b.counters.durMsMax.Load(),
		},
	}
}

// Scale sets the slot target. 0 is allowed at runtime (an operator "take
// nothing new" that persists), unlike the config default which must be >= 1.
func (b *Broker) Scale(n int) error {
	if n < 0 || n > config.MaxSlots {
		return fmt.Errorf("slot count %d out of range 0-%d", n, config.MaxSlots)
	}
	b.slots.SetTarget(n)
	b.applyAutoPin(context.Background())
	b.saveState()
	b.poke()
	b.event(bus.Action, -1, fmt.Sprintf("slot target set to %d", n))
	return nil
}

func (b *Broker) Pause() {
	b.mu.Lock()
	b.paused = true
	b.mu.Unlock()
	b.poke()
	b.event(bus.Action, -1, "paused — taking no new jobs (already-accepted jobs still run; queued jobs wait on GitHub)")
}

// Resume clears pause AND drain — including a pending drain-stop, which is
// thereby cancelled.
func (b *Broker) Resume() {
	b.mu.Lock()
	b.paused = false
	b.draining = false
	stopWasPending := b.stopPending
	b.stopPending = false
	b.mu.Unlock()
	b.poke()
	if stopWasPending {
		b.event(bus.Action, -1, "resumed — pending stop cancelled")
	} else {
		b.event(bus.Action, -1, "resumed")
	}
}

// Drain stops acquiring work and lets running jobs finish; the daemon stays up.
func (b *Broker) Drain() {
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
	b.poke()
	b.event(bus.Action, -1, "draining — finishing running jobs, taking nothing new")
}

// StopMode selects how Stop shuts the daemon down.
type StopMode string

const (
	// StopDrain waits for running jobs to finish first.
	StopDrain StopMode = "drain"
	// StopNow stops immediately; refuses mid-job runners unless forced.
	StopNow StopMode = "now"
)

// ParseStopMode validates a wire-format stop mode. Empty means drain; any
// other unknown string is an error (a typo must not silently pick a mode).
func ParseStopMode(s string) (StopMode, error) {
	switch StopMode(s) {
	case StopDrain, "":
		return StopDrain, nil
	case StopNow:
		return StopNow, nil
	default:
		return "", fmt.Errorf("unknown stop mode %q (want %q or %q)", s, StopDrain, StopNow)
	}
}

// ErrBusy is returned when a stop/reclaim would kill live jobs without force.
var ErrBusy = errors.New("jobs are running")

// ErrNotFound is returned for operations on a slot that doesn't exist or has
// no container.
var ErrNotFound = errors.New("not found")

// Stop shuts the daemon down. StopNow refuses if any container has (or might
// have — probe failures count) a live worker, unless forced. StopDrain waits
// for running jobs and is cancelled by Resume.
func (b *Broker) Stop(ctx context.Context, mode StopMode, force bool, shutdown func(cause string)) error {
	switch mode {
	case StopNow:
		if !force {
			if busy := b.busyRunnerNames(ctx); len(busy) > 0 {
				return fmt.Errorf("%w: mid-flight on %s — drain first or pass force (their runs would FAIL on GitHub)",
					ErrBusy, strings.Join(busy, ", "))
			}
		}
		b.event(bus.Action, -1, "stopping now")
		b.removeAllRunners(ctx, force)
		shutdown("stop requested")
		return nil
	default: // StopDrain
		b.mu.Lock()
		if b.stopPending {
			b.mu.Unlock()
			return nil // already stopping; don't stack watchers
		}
		b.stopPending = true
		runCtx := b.runCtx
		b.mu.Unlock()
		b.Drain()
		b.mu.Lock()
		b.stopPending = true // Drain doesn't touch it, but keep the invariant obvious
		b.mu.Unlock()
		go b.awaitDrainedThenStop(runCtx, shutdown)
		return nil
	}
}

// awaitDrainedThenStop completes a drain-mode stop. It aborts if Resume
// cancelled the drain, so an operator's change of heart wins over a stale
// stop request.
func (b *Broker) awaitDrainedThenStop(ctx context.Context, shutdown func(cause string)) {
	tick := time.NewTicker(b.tm.stopPollEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		b.mu.Lock()
		cancelled := !b.stopPending || !b.draining
		b.mu.Unlock()
		if cancelled {
			return
		}
		if b.slots.Live() == 0 {
			b.event(bus.Action, -1, "drained — stopping")
			shutdown("drained")
			return
		}
	}
}

// busyRunnerNames lists runners that have — or, on probe failure, MIGHT have
// — a job mid-flight. The fail-safe direction matters: an unanswerable probe
// must block an unforced stop, never permit it.
func (b *Broker) busyRunnerNames(ctx context.Context) []string {
	var busy []string
	for _, s := range b.slots.Snapshot() {
		if s.ContainerID == "" {
			continue
		}
		if s.State == slots.Running {
			busy = append(busy, s.RunnerName)
			continue
		}
		hasWorker, err := b.provider.HasWorker(ctx, s.ContainerID)
		if hasWorker || err != nil {
			busy = append(busy, s.RunnerName)
		}
	}
	return busy
}

// removeAllRunners tears the fleet down. Unless forced, each container is
// re-checked for a live worker at removal time (jobs can start between the
// caller's check and here) and skipped if busy or unprovable.
func (b *Broker) removeAllRunners(ctx context.Context, force bool) {
	for _, s := range b.slots.Snapshot() {
		if s.ContainerID == "" {
			continue
		}
		if !force {
			hasWorker, err := b.provider.HasWorker(ctx, s.ContainerID)
			if hasWorker || err != nil {
				b.event(bus.Warn, s.Index, fmt.Sprintf("leaving %s: job mid-flight (or unprovable) — it keeps running", s.RunnerName))
				continue
			}
		}
		_ = b.provider.Remove(ctx, s.ContainerID)
		b.removeRunnerRegistration(ctx, s.RunnerName)
		b.slots.Free(s.Index, s.RunnerName)
	}
}

// Reclaim force-frees one slot (the manual zombie kill). Refuses a slot whose
// worker is alive — or whose worker state cannot be proven — unless forced.
func (b *Broker) Reclaim(ctx context.Context, index int, force bool) error {
	target, ok := b.slots.Get(index)
	if !ok || target.ContainerID == "" {
		return fmt.Errorf("%w: slot %d has no container", ErrNotFound, index)
	}
	if !force {
		hasWorker, err := b.provider.HasWorker(ctx, target.ContainerID)
		if hasWorker || err != nil {
			return fmt.Errorf("%w: slot %d is (or might be) mid-job — pass force to kill it (the run will FAIL on GitHub)", ErrBusy, index)
		}
	}
	if err := b.provider.Remove(ctx, target.ContainerID); err != nil {
		return err
	}
	b.removeRunnerRegistration(ctx, target.RunnerName)
	if b.slots.Free(index, target.RunnerName) {
		b.counters.zombiesReclaimed.Add(1)
		b.event(bus.Action, index, "slot reclaimed by operator")
	}
	b.poke()
	return nil
}

// SlotLogs returns the last n log lines from a slot's container.
func (b *Broker) SlotLogs(ctx context.Context, index, n int) (string, error) {
	s, ok := b.slots.Get(index)
	if !ok {
		return "", fmt.Errorf("%w: no slot %d", ErrNotFound, index)
	}
	if s.ContainerID == "" {
		return "", fmt.Errorf("%w: slot %d has no container", ErrNotFound, index)
	}
	return b.provider.LogsTail(ctx, s.ContainerID, n)
}
