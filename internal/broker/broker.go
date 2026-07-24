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
	"strings"
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

// State is the broker's connection state with GitHub. Pause/drain are
// orthogonal flags (see Info), not states — a draining broker can be active
// or degraded at the same time.
type State string

const (
	Starting State = "starting"
	Active   State = "active"
	Degraded State = "degraded"
	Stopped  State = "stopped"
)

// ErrScaleSetConflict means the configured scale set name exists on GitHub
// but this deckhand has no record of creating it. Fatal and actionable
// (--takeover), never retried.
var ErrScaleSetConflict = errors.New("scale set exists but is not owned by this deckhand")

// ghAPI is the slice of the scaleset client the broker uses outside message
// sessions. *scaleset.Client satisfies it; tests inject a fake.
// Contract quirk (verified against scaleset v0.4.0): GetRunnerByName and
// GetRunnerScaleSet return (nil, nil) when nothing matches — nil results MUST
// be handled, and only a non-nil error means the probe itself failed.
type ghAPI interface {
	GenerateJitRunnerConfig(ctx context.Context, setting *scaleset.RunnerScaleSetJitRunnerSetting, scaleSetID int) (*scaleset.RunnerScaleSetJitRunnerConfig, error)
	GetRunnerByName(ctx context.Context, runnerName string) (*scaleset.RunnerReference, error)
	RemoveRunner(ctx context.Context, runnerID int64) error
	GetRunnerGroupByName(ctx context.Context, runnerGroup string) (*scaleset.RunnerGroup, error)
	GetRunnerScaleSet(ctx context.Context, runnerGroupID int, name string) (*scaleset.RunnerScaleSet, error)
	CreateRunnerScaleSet(ctx context.Context, ss *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error)
}

// containerProvider is the slice of the docker provider the broker uses.
// *runner.Provider satisfies it; tests inject a fake.
type containerProvider interface {
	Ping(ctx context.Context) error
	EnsureImage(ctx context.Context) error
	Spawn(ctx context.Context, slot int, runnerName, cpuset, jitConfig string) (string, error)
	Wait(ctx context.Context, containerID string) (int64, error)
	Remove(ctx context.Context, containerID string) error
	Exists(ctx context.Context, containerID string) (bool, error)
	HasWorker(ctx context.Context, containerID string) (bool, error)
	LogsTail(ctx context.Context, containerID string, n int) (string, error)
	ListManaged(ctx context.Context) ([]runner.Managed, error)
	PruneExited(ctx context.Context, olderThan time.Duration) (int, error)
	NCPU(ctx context.Context) (int, error)
	SampleStats(ctx context.Context, containerID string) (runner.Stats, error)
	HostMem(ctx context.Context) (int64, error)
}

// timings collects every interval/threshold so tests can compress time.
type timings struct {
	watchGrace      time.Duration // JobCompleted grace after container exit
	watchPoll       time.Duration // ownership re-check cadence in the watcher
	sweepEvery      time.Duration // sweeper cadence (prune/reconcile/zombie)
	dockerPingEvery time.Duration // docker recovery probe cadence while down
	erroredCooldown time.Duration // errored slot retry cooldown
	zombieMinAge    time.Duration // min Ready age before zombie probing
	zombieMisses    int           // consecutive gone-from-GitHub probes to reclaim
	pruneAge        time.Duration // exited-container prune age
	wakeSlack       time.Duration // extra wall-clock beyond planned sleep = suspend
	stopPollEvery   time.Duration // drain-stop completion poll cadence
	resourceEvery   time.Duration // CPU/memory usage sampling cadence
}

func defaultTimings() timings {
	return timings{
		watchGrace:      90 * time.Second,
		watchPoll:       2 * time.Second,
		sweepEvery:      time.Minute,
		dockerPingEvery: 5 * time.Second,
		erroredCooldown: 30 * time.Second,
		zombieMinAge:    5 * time.Minute,
		zombieMisses:    3,
		pruneAge:        10 * time.Minute,
		wakeSlack:       30 * time.Second,
		stopPollEvery:   2 * time.Second,
		resourceEvery:   2 * time.Second,
	}
}

// persistedState survives daemon restarts (a scale target changed at runtime
// takes precedence over the config default; the scale set ID proves ownership
// of an existing scale set entity).
type persistedState struct {
	ScaleSetID int `json:"scale_set_id"`
	SlotTarget int `json:"slot_target"`
}

// resourceSnapshot is the latest CPU/memory usage summed across running slots,
// alongside the host totals. ok stays false until the first successful sample.
type resourceSnapshot struct {
	ok            bool
	cpuCoresUsed  float64
	memUsedBytes  int64
	memTotalBytes int64
	ncpu          int
}

type Broker struct {
	cfg    *config.Config
	paths  config.Paths
	logger *slog.Logger
	bus    *bus.Bus

	slots    *slots.Manager
	provider containerProvider
	gh       ghAPI
	// sessions mints message-session clients; nil in unit tests (which never
	// run sessions).
	sessions *scaleset.Client

	tm timings

	mu          sync.Mutex
	state       State
	paused      bool
	draining    bool
	dockerDown  bool
	stopPending bool
	sessionAt   time.Time
	scaleSet    *scaleset.RunnerScaleSet
	watched     map[string]struct{} // container IDs with a live watcher
	runCtx      context.Context     // daemon lifecycle ctx, set by Run

	// reconcileMu serializes reconcile passes (startup, sweeper, docker
	// monitor and wake path may overlap).
	reconcileMu sync.Mutex

	listener atomic.Pointer[listener.Listener]
	counters counters

	// resMu guards res, the latest CPU/memory usage sampled off a background
	// ticker so Status() never blocks on docker stats calls.
	resMu sync.Mutex
	res   resourceSnapshot

	// takeover permits attaching to an existing scale set whose ID we did not
	// persist (i.e. possibly another broker's).
	takeover bool
}

type counters struct {
	completed        atomic.Int64
	failed           atomic.Int64
	zombiesReclaimed atomic.Int64
	spawnErrors      atomic.Int64
	reconnects       atomic.Int64
	// Latency accounting from GitHub's own job timestamps. Queue = job
	// queued → assigned to a runner; duration = assigned → finished.
	// Duration VARIANCE across identical jobs is the oversubscription
	// health signal (contention amplifies fixed overhead), so min/max are
	// tracked alongside the sums.
	queueMsSum atomic.Int64
	queueCount atomic.Int64
	durMsSum   atomic.Int64
	durCount   atomic.Int64
	durMsMin   atomic.Int64
	durMsMax   atomic.Int64
}

func New(cfg *config.Config, paths config.Paths, logger *slog.Logger, eventBus *bus.Bus, takeover bool) (*Broker, error) {
	client, err := cfg.ScalesetClient()
	if err != nil {
		return nil, err
	}
	provider, err := runner.New(runner.Options{
		Image:                    cfg.Runner.Image,
		ScaleSetName:             cfg.ScaleSet.Name,
		ExposeDockerSocket:       cfg.Runner.ExposeDockerSocket,
		Env:                      cfg.Runner.Env,
		MemoryBytes:              int64(cfg.Runner.MemoryMB) << 20,
		PidsLimit:                int64(cfg.Runner.PidsLimit),
		ToolCache:                cfg.Runner.ToolCacheEnabled(),
		CachePaths:               cfg.Runner.CachePaths,
		AllowPrivilegeEscalation: !cfg.Runner.NoNewPrivilegesEnabled(),
	})
	if err != nil {
		return nil, err
	}
	b := newBroker(cfg, paths, logger, eventBus, provider, client, takeover)
	b.sessions = client
	return b, nil
}

// newBroker wires a Broker without touching docker or GitHub — the seam unit
// tests use with fake gh/provider implementations.
func newBroker(cfg *config.Config, paths config.Paths, logger *slog.Logger, eventBus *bus.Bus, provider containerProvider, gh ghAPI, takeover bool) *Broker {
	b := &Broker{
		cfg:      cfg,
		paths:    paths,
		logger:   logger,
		bus:      eventBus,
		provider: provider,
		gh:       gh,
		tm:       defaultTimings(),
		state:    Starting,
		watched:  make(map[string]struct{}),
		runCtx:   context.Background(),
		takeover: takeover,
	}
	target := cfg.Slots.Count
	if st, err := b.loadState(); err == nil && st.SlotTarget > 0 {
		target = st.SlotTarget
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		logger.Warn("state file unreadable; using configured slot count", "error", err)
	}
	// -1 (no pinning) and 0 (auto — resolved once docker answers) both start
	// unpinned; auto is applied by applyAutoPin.
	perSlot := cfg.Slots.CPUsPerSlot
	if perSlot < 0 {
		perSlot = 0
	}
	b.slots = slots.NewManager(target, perSlot)
	return b
}

// applyAutoPin divides the docker host's CPUs across the slot target when
// cpus_per_slot is 0 (auto) — out-of-the-box contention bounding with no
// config. Fewer CPUs than slots means pinning can't help; slots stay
// unpinned. Called at startup, on docker recovery (VM resizes change NCPU)
// and on scale changes.
func (b *Broker) applyAutoPin(ctx context.Context) {
	if b.cfg.Slots.CPUsPerSlot != 0 {
		return
	}
	ncpu, err := b.provider.NCPU(ctx)
	if err != nil || ncpu <= 0 {
		return
	}
	target := b.slots.Target()
	if target <= 0 {
		return
	}
	b.slots.SetCPUsPerSlot(ncpu / target) // 0 when ncpu < target = unpinned
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

// saveState persists the slot target and scale set ID. When the scale set is
// not yet known (early Scale call during startup) the previously persisted ID
// is preserved rather than clobbered with zero.
func (b *Broker) saveState() {
	st := persistedState{SlotTarget: b.slots.Target()}
	b.mu.Lock()
	ss := b.scaleSet
	b.mu.Unlock()
	if ss != nil {
		st.ScaleSetID = ss.ID
	} else if prev, err := b.loadState(); err == nil {
		st.ScaleSetID = prev.ScaleSetID
	}
	raw, _ := json.MarshalIndent(st, "", "  ")
	if err := os.WriteFile(b.paths.StateFile, raw, 0o600); err != nil {
		b.logger.Warn("persist state failed", "error", err)
	}
}

// Run is the daemon main loop; it returns when ctx is cancelled or on a
// fatal, non-retryable startup error (scale set ownership conflict).
func (b *Broker) Run(ctx context.Context) error {
	b.mu.Lock()
	b.runCtx = ctx
	b.mu.Unlock()

	if err := b.provider.Ping(ctx); err != nil {
		b.setDockerDown(true)
		b.event(bus.Warn, -1, fmt.Sprintf("docker unreachable at startup: %v (waiting — is Colima/Docker running?)", err))
	} else if err := b.provider.EnsureImage(ctx); err != nil {
		b.event(bus.Warn, -1, fmt.Sprintf("runner image pull failed: %v (will retry per spawn)", err))
	}
	if !isDigestPinned(b.cfg.Runner.Image) {
		b.event(bus.Warn, -1, fmt.Sprintf("runner.image %q is a mutable tag — pin a digest (image@sha256:...) for supply-chain safety", b.cfg.Runner.Image))
	}

	// A transient GitHub error at startup must not crash-loop the daemon
	// (only an ownership conflict is fatal — that needs a human decision).
	for backoff := time.Second; ; {
		err := b.ensureScaleSet(ctx)
		if err == nil {
			break
		}
		if errors.Is(err, ErrScaleSetConflict) {
			return err
		}
		if ctx.Err() != nil {
			return nil
		}
		b.setState(Degraded)
		b.event(bus.Warn, -1, fmt.Sprintf("scale set setup failed: %v — retrying in %s", err, backoff.Round(time.Second)))
		if !sleepCtx(ctx, backoff) {
			return nil
		}
		if backoff < time.Minute {
			backoff *= 2
		}
	}
	b.saveState()
	b.reconcile(ctx)

	go b.sweeper(ctx)
	go b.dockerMonitor(ctx)
	go b.resourceSampler(ctx)

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
		b.counters.reconnects.Add(1)
		b.setState(Degraded)
		if time.Since(started) > 5*time.Minute {
			backoff = time.Second // the session was healthy for a while; reset
		}
		b.event(bus.Warn, -1, fmt.Sprintf("session ended: %v — reconnecting in %s", err, backoff.Round(time.Second)))

		planned := jitter(backoff)
		wallBefore := time.Now()
		if !sleepCtx(ctx, planned) {
			b.setState(Stopped)
			return nil
		}
		// Only a wall-clock overshoot beyond the PLANNED (jittered) sleep
		// plus slack indicates a suspend; comparing against the pre-jitter
		// backoff would misread large jitters as sleep and defeat backoff.
		if time.Since(wallBefore) > planned+b.tm.wakeSlack {
			b.event(bus.Info, -1, "woke from sleep — resyncing")
			backoff = time.Second
			b.reconcile(ctx)
		} else if backoff < time.Minute {
			backoff *= 2
		}
	}
}

func jitter(d time.Duration) time.Duration {
	return d + time.Duration(rand.Int63n(int64(d)/2+1))
}

// sleepCtx sleeps for d; returns false if ctx ended first.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func isDigestPinned(image string) bool {
	return strings.Contains(image, "@sha256:")
}

func (b *Broker) runSession(ctx context.Context) error {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = uuid.NewString()
	}
	session, err := b.sessions.MessageSessionClient(ctx, b.scaleSetID(), hostname)
	if err != nil {
		return fmt.Errorf("create message session: %w", err)
	}
	defer session.Close(context.WithoutCancel(ctx))

	lst, err := listener.New(session, listener.Config{
		ScaleSetID: b.scaleSetID(),
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
	b.event(bus.Info, -1, fmt.Sprintf("connected to GitHub (scale set %q id=%d)", b.cfg.ScaleSet.Name, b.scaleSetID()))

	err = lst.Run(ctx, &scaler{b: b})
	b.listener.Store(nil)
	return err
}

// ensureScaleSet finds or creates the one scale set entity deckhand owns.
// Only a definitive "exists but not ours" is fatal; transient errors are
// returned for the caller to retry.
func (b *Broker) ensureScaleSet(ctx context.Context) error {
	groupID := 1 // GitHub's ID for the "default" runner group
	if b.cfg.ScaleSet.RunnerGroup != scaleset.DefaultRunnerGroup {
		group, err := b.gh.GetRunnerGroupByName(ctx, b.cfg.ScaleSet.RunnerGroup)
		if err != nil {
			return fmt.Errorf("runner group %q: %w", b.cfg.ScaleSet.RunnerGroup, err)
		}
		if group == nil {
			return fmt.Errorf("runner group %q not found", b.cfg.ScaleSet.RunnerGroup)
		}
		groupID = group.ID
	}

	existing, err := b.gh.GetRunnerScaleSet(ctx, groupID, b.cfg.ScaleSet.Name)
	if err != nil {
		// Transient lookup failure: NEVER fall through to create — the
		// duplicate-name failure would look fatal and mask the real cause.
		return fmt.Errorf("look up scale set %q: %w", b.cfg.ScaleSet.Name, err)
	}
	if existing != nil {
		st, _ := b.loadState()
		if st.ScaleSetID != existing.ID && !b.takeover {
			return fmt.Errorf(
				"%w: %q (id=%d) — another daemon may own it; rerun with --takeover to adopt it",
				ErrScaleSetConflict, existing.Name, existing.ID)
		}
		b.setScaleSet(existing)
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
	created, err := b.gh.CreateRunnerScaleSet(ctx, &scaleset.RunnerScaleSet{
		Name:          b.cfg.ScaleSet.Name,
		RunnerGroupID: groupID,
		Labels:        labels,
		RunnerSetting: scaleset.RunnerSetting{DisableUpdate: true},
	})
	if err != nil {
		return fmt.Errorf("create scale set %q: %w", b.cfg.ScaleSet.Name, err)
	}
	b.setScaleSet(created)
	// Persist immediately: a crash before the caller's saveState would
	// otherwise orphan the scale set and trigger the ownership refusal on
	// the very daemon that created it.
	b.saveState()
	b.event(bus.Info, -1, fmt.Sprintf("registered scale set %q (id=%d) — use `runs-on: %s` in workflows", created.Name, created.ID, created.Name))
	return nil
}

func (b *Broker) setScaleSet(ss *scaleset.RunnerScaleSet) {
	b.mu.Lock()
	b.scaleSet = ss
	b.mu.Unlock()
}

func (b *Broker) scaleSetID() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.scaleSet == nil {
		return 0
	}
	return b.scaleSet.ID
}

// reconcile is the single idempotent recovery pass: it re-learns the
// container fleet from docker labels and repairs tracking. Called at startup,
// on wake from sleep, on docker recovery, and every sweep. It never touches a
// container that might be running a job.
func (b *Broker) reconcile(ctx context.Context) {
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()

	managed, err := b.provider.ListManaged(ctx)
	if err != nil {
		b.setDockerDown(true)
		return
	}
	b.setDockerDown(false)
	b.applyAutoPin(ctx)

	byID := make(map[string]runner.Managed, len(managed))
	for _, m := range managed {
		byID[m.ContainerID] = m
	}

	// Pass 1: repair tracked slots.
	for _, s := range b.slots.Snapshot() {
		if s.ContainerID == "" {
			continue
		}
		m, found := byID[s.ContainerID]
		switch {
		case !found:
			// The container is gone (docker restart, manual rm). Its job, if
			// any, dies with it on GitHub's side.
			if b.slots.Free(s.Index, s.RunnerName) {
				if s.State == slots.Running {
					b.counters.failed.Add(1)
				}
				b.event(bus.Warn, s.Index, fmt.Sprintf("container for %s vanished — slot freed", s.RunnerName))
			}
		case !m.Running:
			// Exited. A live watcher owns the JobCompleted grace window; a
			// dead one (daemon restarted, docker outage) means we reap here.
			if !b.isWatched(s.ContainerID) {
				_ = b.provider.Remove(ctx, s.ContainerID)
				if b.slots.Free(s.Index, s.RunnerName) {
					if s.State == slots.Running {
						b.counters.failed.Add(1)
					}
					b.event(bus.Warn, s.Index, fmt.Sprintf("runner %s had exited while unwatched — slot freed", s.RunnerName))
				}
			}
		default:
			if !b.isWatched(s.ContainerID) {
				b.armWatcher(s.Index, s.RunnerName, s.ContainerID)
			}
		}
	}

	// Pass 2: adopt untracked containers (fresh start, or spawned before a
	// crash). Exited ones are just debris.
	tracked := make(map[string]bool)
	for _, s := range b.slots.Snapshot() {
		if s.ContainerID != "" {
			tracked[s.ContainerID] = true
		}
	}
	for _, m := range managed {
		if tracked[m.ContainerID] {
			continue
		}
		if !m.Running {
			_ = b.provider.Remove(ctx, m.ContainerID)
			continue
		}
		hasWorker, werr := b.provider.HasWorker(ctx, m.ContainerID)
		// Fail-safe: an unanswerable probe is treated as mid-job. The worst
		// case of guessing "busy" is a slot that frees on container exit; the
		// worst case of guessing "idle" would be culling a live job.
		busy := hasWorker || werr != nil
		idx := b.slots.Adopt(m.Slot, m.RunnerName, m.ContainerID, busy)
		if busy {
			b.slots.Mutate(m.RunnerName, func(sl *slots.Slot) {
				sl.Job = &slots.Job{DisplayName: "(adopted job — details unknown)", StartedAt: time.Now()}
			})
		}
		b.event(bus.Info, idx, fmt.Sprintf("adopted running container %s", m.RunnerName))
		b.armWatcher(idx, m.RunnerName, m.ContainerID)
	}
	b.poke()
}

// dockerMonitor probes for docker recovery while it is down, so the daemon
// notices a restarted Colima/Docker without waiting for operator action.
func (b *Broker) dockerMonitor(ctx context.Context) {
	tick := time.NewTicker(b.tm.dockerPingEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if !b.isDockerDown() {
			continue
		}
		if b.provider.Ping(ctx) == nil {
			b.reconcile(ctx) // clears dockerDown on success
		}
	}
}

// resourceSampler refreshes the cached CPU/memory usage on a ticker. It is
// kept off the Status path because a stats read blocks ~1s per container (see
// runner.SampleStats) and Status() must stay instant.
func (b *Broker) resourceSampler(ctx context.Context) {
	tick := time.NewTicker(b.tm.resourceEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		b.sampleResourcesOnce(ctx)
	}
}

// sampleResourcesOnce sums CPU/memory across the running slots' containers and
// refreshes the host totals (fetched once, then reused — they don't change).
func (b *Broker) sampleResourcesOnce(ctx context.Context) {
	if b.isDockerDown() {
		return // keep the last good sample rather than flapping to zero
	}
	var ids []string
	for _, s := range b.slots.Snapshot() {
		if s.State == slots.Running && s.ContainerID != "" {
			ids = append(ids, s.ContainerID)
		}
	}

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		cores float64
		mem   int64
	)
	for _, id := range ids {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			st, err := b.provider.SampleStats(cctx, id)
			if err != nil {
				return // a container that exited mid-sample just drops out of the sum
			}
			mu.Lock()
			cores += st.CPUCores
			mem += st.MemBytes
			mu.Unlock()
		}(id)
	}
	wg.Wait()

	b.resMu.Lock()
	memTotal, ncpu := b.res.memTotalBytes, b.res.ncpu
	b.resMu.Unlock()
	if memTotal == 0 {
		if v, err := b.provider.HostMem(ctx); err == nil {
			memTotal = v
		}
	}
	if ncpu == 0 {
		if v, err := b.provider.NCPU(ctx); err == nil {
			ncpu = v
		}
	}

	b.resMu.Lock()
	b.res = resourceSnapshot{ok: true, cpuCoresUsed: cores, memUsedBytes: mem, memTotalBytes: memTotal, ncpu: ncpu}
	b.resMu.Unlock()
}

// sweeper is the periodic maintenance pass: prune debris, reconcile tracking,
// retry errored slots, converge draining capacity, and reclaim zombies.
func (b *Broker) sweeper(ctx context.Context) {
	misses := map[string]int{} // runnerName -> consecutive gone-from-GitHub probes
	tick := time.NewTicker(b.tm.sweepEvery)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		b.sweepOnce(ctx, misses)
	}
}

func (b *Broker) sweepOnce(ctx context.Context, misses map[string]int) {
	_, _ = b.provider.PruneExited(ctx, b.tm.pruneAge)
	b.reconcile(ctx)
	b.retryErroredSlots()
	// Converge drains even when no listener message is flowing (session
	// outage, or nothing queued): jobless drained runners can go now.
	b.cullSlots(ctx, b.slots.DrainingReady(), "drained")
	if b.isDraining() {
		b.cullSlots(ctx, b.slots.IdleRunners(), "draining")
	}

	// Zombie shape: runner container alive and jobless well past spawn, but
	// its registration is gone from GitHub — it will never be assigned work
	// and never exit. GetRunnerByName returns (nil, nil) for "gone" (verified
	// v0.4.0 contract); a non-nil error is a probe failure and NEVER counts
	// toward reclaim (probe failure ≠ fleet failure). Debounced.
	live := map[string]bool{}
	for _, s := range b.slots.Snapshot() {
		if s.RunnerName != "" {
			live[s.RunnerName] = true
		}
		if s.State != slots.Ready || time.Since(s.Since) < b.tm.zombieMinAge {
			delete(misses, s.RunnerName)
			continue
		}
		ref, err := b.gh.GetRunnerByName(ctx, s.RunnerName)
		switch {
		case err != nil:
			// transient probe failure: no state change
		case ref != nil:
			delete(misses, s.RunnerName)
		default: // (nil, nil): registration is gone
			misses[s.RunnerName]++
			if misses[s.RunnerName] >= b.tm.zombieMisses {
				delete(misses, s.RunnerName)
				b.reclaimZombie(ctx, s)
			}
		}
	}
	// Drop counters for runners that no longer exist so the map stays bounded.
	for name := range misses {
		if !live[name] {
			delete(misses, name)
		}
	}
}

func (b *Broker) reclaimZombie(ctx context.Context, s slots.Slot) {
	hasWorker, err := b.provider.HasWorker(ctx, s.ContainerID)
	if hasWorker || err != nil {
		return // a job snuck in, or we cannot prove it didn't: never touch it
	}
	if err := b.provider.Remove(ctx, s.ContainerID); err != nil {
		b.event(bus.Warn, s.Index, fmt.Sprintf("zombie reclaim failed: %v", err))
		return
	}
	if b.slots.Free(s.Index, s.RunnerName) {
		b.counters.zombiesReclaimed.Add(1)
		b.event(bus.Action, s.Index, fmt.Sprintf("reclaimed zombie runner %s (gone from GitHub, jobless locally)", s.RunnerName))
	}
	b.poke()
}

func (b *Broker) setState(s State) {
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
			b.event(bus.Error, -1, "docker unreachable — not taking jobs (Colima/Docker stopped?)")
		} else {
			b.event(bus.Info, -1, "docker reachable — resuming")
		}
		b.poke()
	}
}

func (b *Broker) isDockerDown() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dockerDown
}

func (b *Broker) isDraining() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.draining
}

// effectiveCap is what we advertise to GitHub as capacity: 0 while paused,
// draining, or docker-down (never take jobs you can't run), else the count of
// usable non-draining slots — a drain-marked runner may still be finishing a
// job, but advertising it invites new work onto capacity that is leaving.
func (b *Broker) effectiveCap() int {
	b.mu.Lock()
	blocked := b.paused || b.draining || b.dockerDown
	b.mu.Unlock()
	if blocked {
		return 0
	}
	return b.slots.Capacity()
}

// poke pushes the current capacity to the live listener.
func (b *Broker) poke() {
	if lst := b.listener.Load(); lst != nil {
		lst.SetMaxRunners(b.effectiveCap())
	}
}

func (b *Broker) isWatched(containerID string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	_, ok := b.watched[containerID]
	return ok
}

func (b *Broker) event(level bus.Level, slot int, msg string) {
	b.bus.Publish(level, slot, msg)
	switch level {
	case bus.Error:
		b.logger.Error(msg, "slot", slot)
	case bus.Warn:
		b.logger.Warn(msg, "slot", slot)
	default:
		b.logger.Info(msg, "slot", slot)
	}
}
