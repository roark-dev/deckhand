package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/slots"
)

// Status is the control-API view of the daemon; it drives the TUI and
// `deckhand status`.
type Status struct {
	Broker   BrokerStatus  `json:"broker"`
	Docker   DockerStatus  `json:"docker"`
	Slots    []slots.Slot  `json:"slots"`
	Counters CounterValues `json:"counters"`
}

type BrokerStatus struct {
	State         BrokerState `json:"state"`
	Paused        bool        `json:"paused"`
	Draining      bool        `json:"draining"`
	ScaleSetName  string      `json:"scale_set_name"`
	ScaleSetID    int         `json:"scale_set_id"`
	GitHubURL     string      `json:"github_url"`
	Target        int         `json:"target"`
	Warm          int         `json:"warm"`
	SessionAgeSec int         `json:"session_age_sec"`
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
}

func (b *Broker) Status() Status {
	b.mu.Lock()
	st := BrokerStatus{
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
	return Status{
		Broker: st,
		Docker: DockerStatus{OK: dockerOK},
		Slots:  b.slots.Snapshot(),
		Counters: CounterValues{
			Completed:        b.counters.Completed.Load(),
			Failed:           b.counters.Failed.Load(),
			ZombiesReclaimed: b.counters.ZombiesReclaimed.Load(),
			SpawnErrors:      b.counters.SpawnErrors.Load(),
			Reconnects:       b.counters.Reconnects.Load(),
		},
	}
}

func (b *Broker) Scale(n int) error {
	if n < 0 || n > 64 {
		return fmt.Errorf("slot count %d out of range 0-64", n)
	}
	b.slots.SetTarget(n)
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
	b.event(bus.Action, -1, "paused — no new jobs will be taken (queued jobs wait on GitHub)")
}

func (b *Broker) Resume() {
	b.mu.Lock()
	b.paused = false
	b.draining = false
	b.mu.Unlock()
	b.poke()
	b.event(bus.Action, -1, "resumed")
}

// Drain stops acquiring work and lets running jobs finish; the daemon stays up.
func (b *Broker) Drain() {
	b.mu.Lock()
	b.draining = true
	b.mu.Unlock()
	b.setState(Draining)
	b.poke()
	b.event(bus.Action, -1, "draining — finishing running jobs, taking nothing new")
}

// ErrBusy is returned when a stop/reclaim would kill live jobs without force.
var ErrBusy = errors.New("jobs are running")

// Stop shuts the daemon down. mode "drain" waits for running jobs; mode "now"
// refuses if jobs are mid-flight unless force is set (their runs would fail on
// GitHub as vanished runners).
func (b *Broker) Stop(ctx context.Context, mode string, force bool, shutdown func(cause string)) error {
	busy := b.slots.BusyCount()
	switch mode {
	case "now":
		if busy > 0 && !force {
			return fmt.Errorf("%w: %d job(s) mid-flight — drain first or pass force", ErrBusy, busy)
		}
		b.event(bus.Action, -1, "stopping now")
		b.removeAllRunners(ctx)
		shutdown("stop requested")
		return nil
	default: // drain
		b.Drain()
		go func() {
			for b.slots.Live() > 0 {
				time.Sleep(2 * time.Second)
			}
			b.event(bus.Action, -1, "drained — stopping")
			shutdown("drained")
		}()
		return nil
	}
}

func (b *Broker) removeAllRunners(ctx context.Context) {
	for _, s := range b.slots.Snapshot() {
		if s.ContainerID == "" {
			continue
		}
		_ = b.provider.Remove(ctx, s.ContainerID)
		if ref, err := b.client.GetRunnerByName(ctx, s.RunnerName); err == nil {
			_ = b.client.RemoveRunner(ctx, int64(ref.ID))
		}
		b.slots.Free(s.Index, s.RunnerName)
	}
}

// Reclaim force-frees one slot (the manual zombie kill). Refuses a slot with
// a live worker unless forced.
func (b *Broker) Reclaim(ctx context.Context, index int, force bool) error {
	var target slots.Slot
	found := false
	for _, s := range b.slots.Snapshot() {
		if s.Index == index {
			target, found = s, true
		}
	}
	if !found || target.ContainerID == "" {
		return fmt.Errorf("slot %d has no container", index)
	}
	if !force && b.provider.HasWorker(ctx, target.ContainerID) {
		return fmt.Errorf("%w: slot %d is mid-job — pass force to kill it (the run will fail on GitHub)", ErrBusy, index)
	}
	if err := b.provider.Remove(ctx, target.ContainerID); err != nil {
		return err
	}
	if ref, err := b.client.GetRunnerByName(ctx, target.RunnerName); err == nil {
		_ = b.client.RemoveRunner(ctx, int64(ref.ID))
	}
	b.slots.Free(index, target.RunnerName)
	b.counters.ZombiesReclaimed.Add(1)
	b.event(bus.Action, index, "slot reclaimed by operator")
	b.poke()
	return nil
}

// SlotLogs returns the last n log lines from a slot's container.
func (b *Broker) SlotLogs(ctx context.Context, index, n int) (string, error) {
	for _, s := range b.slots.Snapshot() {
		if s.Index == index {
			if s.ContainerID == "" {
				return "", fmt.Errorf("slot %d has no container", index)
			}
			return b.provider.LogsTail(ctx, s.ContainerID, n)
		}
	}
	return "", fmt.Errorf("no slot %d", index)
}
