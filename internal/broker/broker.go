// Package broker is deckhand's daemon core: it holds the single runner scale
// set registration, runs the scale-set listener with reconnect/backoff, and
// bridges desired-runner-count decisions onto slot-bound ephemeral runner
// containers.
package broker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/actions/scaleset"
	"github.com/actions/scaleset/listener"
	"github.com/google/uuid"

	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/config"
	"github.com/roark-dev/deckhand/internal/runner"
	"github.com/roark-dev/deckhand/internal/slots"
)

type BrokerState string

const (
	Starting BrokerState = "starting"
	Active   BrokerState = "active"
	Degraded BrokerState = "degraded"
	Draining BrokerState = "draining"
	Stopped  BrokerState = "stopped"
)

// persistedState survives daemon restarts (scale target changed at runtime
// takes precedence over the config default; the scale set ID proves ownership
// of an existing scale set entity).
type persistedState struct {
	ScaleSetID int `json:"scale_set_id"`
	SlotTarget int `json:"slot_target"`
}

type Broker struct {
	cfg    *config.Config
	paths  config.Paths
	logger *slog.Logger
	bus    *bus.Bus

	slots    *slots.Manager
	provider *runner.Provider
	client   *scaleset.Client
	scaleSet *scaleset.RunnerScaleSet

	mu         sync.Mutex
	state      BrokerState
	paused     bool
	draining   bool
	dockerDown bool
	sessionAt  time.Time
	lastMsgAt  time.Time
	listener   atomic.Pointer[listener.Listener]
	stopCause  string

	counters Counters

	// takeover permits attaching to an existing scale set whose ID we did not
	// persist (i.e. possibly another broker's).
	takeover bool
}

type Counters struct {
	Completed        atomic.Int64
	Failed           atomic.Int64
	ZombiesReclaimed atomic.Int64
	SpawnErrors      atomic.Int64
	Reconnects       atomic.Int64
}

func New(cfg *config.Config, paths config.Paths, logger *slog.Logger, eventBus *bus.Bus, takeover bool) (*Broker, error) {
	client, err := cfg.ScalesetClient()
	if err != nil {
		return nil, err
	}
	provider, err := runner.New(runner.Options{
		Image:              cfg.Runner.Image,
		ScaleSetName:       cfg.ScaleSet.Name,
		ExposeDockerSocket: cfg.Runner.ExposeDockerSocket,
		Env:                cfg.Runner.Env,
	})
	if err != nil {
		return nil, err
	}
	b := &Broker{
		cfg:      cfg,
		paths:    paths,
		logger:   logger,
		bus:      eventBus,
		provider: provider,
		client:   client,
		state:    Starting,
		takeover: takeover,
	}
	target := cfg.Slots.Count
	if st, err := b.loadState(); err == nil && st.SlotTarget > 0 {
		target = st.SlotTarget
	}
	b.slots = slots.NewManager(target, cfg.Slots.CPUsPerSlot)
	return b, nil
}

func (b *Broker) loadState() (persistedState, error) {
	var st persistedState
	raw, err := os.ReadFile(b.paths.StateFile)
	if err != nil {
		return st, err
	}
	err = json.Unmarshal(raw, &st)
	return st, err
}

func (b *Broker) saveState() {
	st := persistedState{SlotTarget: b.slots.Target()}
	if ss := b.scaleSet; ss != nil {
		st.ScaleSetID = ss.ID
	}
	raw, _ := json.MarshalIndent(st, "", "  ")
	if err := os.WriteFile(b.paths.StateFile, raw, 0o600); err != nil {
		b.logger.Warn("persist state failed", "error", err)
	}
}

// Run is the daemon main loop; it returns when ctx is cancelled (or on a
// fatal, non-retryable startup error).
func (b *Broker) Run(ctx context.Context) error {
	if err := b.provider.Ping(ctx); err != nil {
		// Not fatal: broker starts degraded and waits for docker, mirroring
		// the never-crash-loop posture. But surface it loudly.
		b.setDockerDown(true)
		b.event(bus.Warn, -1, fmt.Sprintf("docker unreachable at startup: %v (waiting — is Colima/Docker running?)", err))
	} else if err := b.provider.EnsureImage(ctx); err != nil {
		b.event(bus.Warn, -1, fmt.Sprintf("runner image pull failed: %v (will retry per spawn)", err))
	}

	if err := b.ensureScaleSet(ctx); err != nil {
		return err
	}
	b.saveState()
	b.adopt(ctx)

	go b.sweeper(ctx)

	// Session/listener loop with jittered backoff and sleep/wake detection.
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			b.setState(Stopped)
			return nil
		}
		started := time.Now()
		err := b.runSession(ctx)
		if ctx.Err() != nil {
			b.setState(Stopped)
			return nil
		}
		b.counters.Reconnects.Add(1)
		b.setState(Degraded)
		if time.Since(started) > 5*time.Minute {
			backoff = time.Second // the session was healthy for a while; reset
		}
		b.event(bus.Warn, -1, fmt.Sprintf("session ended: %v — reconnecting in %s", err, backoff.Round(time.Second)))

		wallBefore := time.Now()
		select {
		case <-ctx.Done():
			b.setState(Stopped)
			return nil
		case <-time.After(jitter(backoff)):
		}
		// A wall-clock jump across the sleep means the machine suspended:
		// reconnect immediately with fresh backoff rather than compounding.
		if time.Since(wallBefore) > backoff+30*time.Second {
			b.event(bus.Info, -1, "woke from sleep — resyncing")
			backoff = time.Second
			b.adopt(ctx)
		} else if backoff < time.Minute {
			backoff *= 2
		}
	}
}

func jitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int63n(int64(d)/2+1))
}

func (b *Broker) runSession(ctx context.Context) error {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = uuid.NewString()
	}
	session, err := b.client.MessageSessionClient(ctx, b.scaleSet.ID, hostname)
	if err != nil {
		return fmt.Errorf("create message session: %w", err)
	}
	defer session.Close(context.WithoutCancel(ctx))

	lst, err := listener.New(session, listener.Config{
		ScaleSetID: b.scaleSet.ID,
		MaxRunners: b.effectiveCap(),
		Logger:     b.logger.WithGroup("listener"),
	})
	if err != nil {
		return err
	}
	b.listener.Store(lst)
	b.mu.Lock()
	b.sessionAt = time.Now()
	b.mu.Unlock()
	b.setState(Active)
	b.event(bus.Info, -1, fmt.Sprintf("connected to GitHub (scale set %q id=%d)", b.scaleSet.Name, b.scaleSet.ID))

	err = lst.Run(ctx, &scaler{b: b})
	b.listener.Store(nil)
	return err
}

// ensureScaleSet finds or creates the one scale set entity deckhand owns.
func (b *Broker) ensureScaleSet(ctx context.Context) error {
	groupID := 1 // scaleset.DefaultRunnerGroup
	if b.cfg.ScaleSet.RunnerGroup != scaleset.DefaultRunnerGroup {
		group, err := b.client.GetRunnerGroupByName(ctx, b.cfg.ScaleSet.RunnerGroup)
		if err != nil {
			return fmt.Errorf("runner group %q: %w", b.cfg.ScaleSet.RunnerGroup, err)
		}
		groupID = group.ID
	}

	existing, err := b.client.GetRunnerScaleSet(ctx, groupID, b.cfg.ScaleSet.Name)
	if err == nil && existing != nil {
		st, _ := b.loadState()
		if st.ScaleSetID != existing.ID && !b.takeover {
			return fmt.Errorf(
				"scale set %q already exists (id=%d) but this deckhand did not create it — another daemon may own it; rerun with --takeover to adopt it",
				existing.Name, existing.ID)
		}
		b.scaleSet = existing
		return nil
	}

	labels := make([]scaleset.Label, 0, len(b.cfg.ScaleSet.Labels)+1)
	labels = append(labels, scaleset.Label{Name: b.cfg.ScaleSet.Name, Type: "System"})
	for _, l := range b.cfg.ScaleSet.Labels {
		if l == b.cfg.ScaleSet.Name {
			continue
		}
		labels = append(labels, scaleset.Label{Name: l, Type: "User"})
	}
	created, err := b.client.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          b.cfg.ScaleSet.Name,
		RunnerGroupID: groupID,
		Labels:        labels,
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	})
	if err != nil {
		return fmt.Errorf("create scale set %q: %w", b.cfg.ScaleSet.Name, err)
	}
	b.scaleSet = created
	b.event(bus.Info, -1, fmt.Sprintf("registered scale set %q (id=%d) — use `runs-on: %s` in workflows", created.Name, created.ID, created.Name))
	return nil
}

// adopt re-learns the container fleet from docker labels after a (re)start.
func (b *Broker) adopt(ctx context.Context) {
	managed, err := b.provider.ListManaged(ctx)
	if err != nil {
		b.setDockerDown(true)
		return
	}
	b.setDockerDown(false)
	for _, m := range managed {
		if !m.Running {
			_ = b.provider.Remove(ctx, m.ContainerID)
			continue
		}
		b.slots.AdoptAt(m.Slot, m.RunnerName, m.ContainerID, m.HasWorker)
		state := "ready"
		if m.HasWorker {
			state = "mid-job"
			b.slots.Mutate(m.RunnerName, func(s *slots.Slot) {
				s.Job = &slots.Job{DisplayName: "(adopted — details unknown)", StartedAt: time.Now()}
			})
		}
		b.event(bus.Info, m.Slot, fmt.Sprintf("adopted running container %s (%s)", m.RunnerName, state))
		go b.watchContainer(m.Slot, m.RunnerName, m.ContainerID)
	}
}

// sweeper handles zombie reclaim and exited-container pruning.
func (b *Broker) sweeper(ctx context.Context) {
	misses := map[string]int{} // runnerName -> consecutive not-found probes
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if _, err := b.provider.PruneExited(ctx, 10*time.Minute); err != nil {
			continue // docker down; the spawn/status paths already surface it
		}
		// Zombie shape: runner container alive and jobless well past spawn,
		// but its registration is gone from GitHub — it will never be
		// assigned work and never exit. Debounced 3 probes, and a probe
		// error never counts (probe failure ≠ fleet failure).
		for _, s := range b.slots.Snapshot() {
			if s.State != slots.Ready || time.Since(s.Since) < 5*time.Minute {
				delete(misses, s.RunnerName)
				continue
			}
			_, err := b.client.GetRunnerByName(ctx, s.RunnerName)
			switch {
			case err == nil:
				delete(misses, s.RunnerName)
			case errors.Is(err, scaleset.RunnerNotFoundError):
				misses[s.RunnerName]++
				if misses[s.RunnerName] >= 3 {
					delete(misses, s.RunnerName)
					b.reclaimZombie(ctx, s)
				}
			default:
				// transient probe failure: no state change
			}
		}
	}
}

func (b *Broker) reclaimZombie(ctx context.Context, s slots.Slot) {
	if b.provider.HasWorker(ctx, s.ContainerID) {
		return // a job snuck in; never touch a live worker
	}
	if err := b.provider.Remove(ctx, s.ContainerID); err != nil {
		b.event(bus.Warn, s.Index, fmt.Sprintf("zombie reclaim failed: %v", err))
		return
	}
	b.slots.Free(s.Index, s.RunnerName)
	b.counters.ZombiesReclaimed.Add(1)
	b.event(bus.Action, s.Index, fmt.Sprintf("reclaimed zombie runner %s (gone from GitHub, jobless locally)", s.RunnerName))
	b.poke()
}

func (b *Broker) setState(s BrokerState) {
	b.mu.Lock()
	b.state = s
	b.mu.Unlock()
}

func (b *Broker) setDockerDown(down bool) {
	b.mu.Lock()
	changed := b.dockerDown != down
	b.dockerDown = down
	b.mu.Unlock()
	if changed {
		if down {
			b.event(bus.Error, -1, "docker unreachable — not acquiring jobs (Colima/Docker stopped?)")
		} else {
			b.event(bus.Info, -1, "docker back — resuming")
		}
	}
}

// effectiveCap is what we advertise to GitHub as capacity: 0 while paused,
// draining, or docker-down (never take jobs you can't run), else free+live.
func (b *Broker) effectiveCap() int {
	b.mu.Lock()
	blocked := b.paused || b.draining || b.dockerDown
	b.mu.Unlock()
	if blocked {
		return 0
	}
	return b.slots.Live() + b.slots.FreeCount()
}

// poke pushes the current capacity to the live listener.
func (b *Broker) poke() {
	if lst := b.listener.Load(); lst != nil {
		lst.SetMaxRunners(b.effectiveCap())
	}
}

func (b *Broker) event(level bus.Level, slot int, msg string) {
	b.bus.Publish(level, slot, msg)
	b.logger.Info(msg, "slot", slot, "level", string(level))
}
