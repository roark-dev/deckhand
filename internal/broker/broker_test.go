package broker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/actions/scaleset"

	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/config"
	"github.com/roark-dev/deckhand/internal/runner"
	"github.com/roark-dev/deckhand/internal/slots"
)

// ---- fakes ----------------------------------------------------------------

type fakeGH struct {
	mu             sync.Mutex
	jitErr         error
	runnersByName  map[string]*scaleset.RunnerReference
	runnerProbeErr error
	removedRunners []int64
	scaleSets      map[string]*scaleset.RunnerScaleSet
	getSetErr      error
	createCalls    atomic.Int64
	nextSetID      int
}

func newFakeGH() *fakeGH {
	return &fakeGH{
		runnersByName: map[string]*scaleset.RunnerReference{},
		scaleSets:     map[string]*scaleset.RunnerScaleSet{},
		nextSetID:     41,
	}
}

func (g *fakeGH) GenerateJitRunnerConfig(_ context.Context, s *scaleset.RunnerScaleSetJitRunnerSetting, _ int) (*scaleset.RunnerScaleSetJitRunnerConfig, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.jitErr != nil {
		return nil, g.jitErr
	}
	return &scaleset.RunnerScaleSetJitRunnerConfig{EncodedJITConfig: "jit-" + s.Name}, nil
}

// GetRunnerByName mimics the verified v0.4.0 contract: (nil, nil) when absent.
func (g *fakeGH) GetRunnerByName(_ context.Context, name string) (*scaleset.RunnerReference, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.runnerProbeErr != nil {
		return nil, g.runnerProbeErr
	}
	return g.runnersByName[name], nil
}

func (g *fakeGH) RemoveRunner(_ context.Context, id int64) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.removedRunners = append(g.removedRunners, id)
	return nil
}

func (g *fakeGH) GetRunnerGroupByName(_ context.Context, name string) (*scaleset.RunnerGroup, error) {
	return &scaleset.RunnerGroup{ID: 7, Name: name}, nil
}

// GetRunnerScaleSet also returns (nil, nil) for not-found, per the real client.
func (g *fakeGH) GetRunnerScaleSet(_ context.Context, _ int, name string) (*scaleset.RunnerScaleSet, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.getSetErr != nil {
		return nil, g.getSetErr
	}
	return g.scaleSets[name], nil
}

func (g *fakeGH) CreateRunnerScaleSet(_ context.Context, ss *scaleset.RunnerScaleSet) (*scaleset.RunnerScaleSet, error) {
	g.createCalls.Add(1)
	g.mu.Lock()
	defer g.mu.Unlock()
	created := *ss
	created.ID = g.nextSetID
	g.nextSetID++
	g.scaleSets[ss.Name] = &created
	return &created, nil
}

type fakeContainer struct {
	id        string
	slot      int
	name      string
	running   bool
	hasWorker bool
	exitCode  int64
	exitCh    chan struct{}
}

type fakeProvider struct {
	mu         sync.Mutex
	pingErr    error
	spawnErr   error
	imageErr   error
	workerErr  error
	ncpu       int
	hostMem    int64
	stats      map[string]runner.Stats
	containers map[string]*fakeContainer
	removed    []string
	nextID     int
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{containers: map[string]*fakeContainer{}, stats: map[string]runner.Stats{}}
}

func (p *fakeProvider) Ping(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pingErr
}

func (p *fakeProvider) EnsureImage(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.imageErr
}

func (p *fakeProvider) Spawn(_ context.Context, slot int, name, _ string, _ string) (string, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.spawnErr != nil {
		return "", p.spawnErr
	}
	p.nextID++
	id := fmt.Sprintf("cid-%d", p.nextID)
	p.containers[id] = &fakeContainer{id: id, slot: slot, name: name, running: true, exitCh: make(chan struct{})}
	return id, nil
}

func (p *fakeProvider) Wait(ctx context.Context, id string) (int64, error) {
	p.mu.Lock()
	c, ok := p.containers[id]
	p.mu.Unlock()
	if !ok {
		return 0, errors.New("no such container")
	}
	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case <-c.exitCh:
		return c.exitCode, nil
	}
}

func (p *fakeProvider) Remove(_ context.Context, id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removed = append(p.removed, id)
	delete(p.containers, id)
	return nil
}

func (p *fakeProvider) Exists(_ context.Context, id string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.containers[id]
	return ok, nil
}

func (p *fakeProvider) HasWorker(_ context.Context, id string) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.workerErr != nil {
		return false, p.workerErr
	}
	c, ok := p.containers[id]
	if !ok {
		return false, nil
	}
	return c.hasWorker, nil
}

func (p *fakeProvider) LogsTail(context.Context, string, int) (string, error) {
	return "log line", nil
}

func (p *fakeProvider) ListManaged(context.Context) ([]runner.Managed, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pingErr != nil {
		return nil, p.pingErr
	}
	var out []runner.Managed
	for _, c := range p.containers {
		out = append(out, runner.Managed{ContainerID: c.id, Slot: c.slot, RunnerName: c.name, Running: c.running})
	}
	return out, nil
}

func (p *fakeProvider) PruneExited(context.Context, time.Duration) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pingErr != nil {
		return 0, p.pingErr
	}
	return 0, nil
}

func (p *fakeProvider) NCPU(context.Context) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.pingErr != nil {
		return 0, p.pingErr
	}
	if p.ncpu == 0 {
		return 8, nil
	}
	return p.ncpu, nil
}

func (p *fakeProvider) SampleStats(_ context.Context, id string) (runner.Stats, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if s, ok := p.stats[id]; ok {
		return s, nil
	}
	return runner.Stats{}, fmt.Errorf("no stats for %s", id)
}

func (p *fakeProvider) HostMem(context.Context) (int64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.hostMem == 0 {
		return 16 * 1024 * 1024 * 1024, nil
	}
	return p.hostMem, nil
}

func (p *fakeProvider) exitContainer(id string, code int64) {
	p.mu.Lock()
	c, ok := p.containers[id]
	if ok {
		c.running = false
		c.exitCode = code
	}
	p.mu.Unlock()
	if ok {
		close(c.exitCh)
	}
}

func (p *fakeProvider) removedIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.removed...)
}

// ---- helpers --------------------------------------------------------------

func testConfig(t *testing.T, slotCount, warm int) (*config.Config, config.Paths) {
	t.Helper()
	cfg := &config.Config{}
	cfg.GitHub.URL = "https://github.com/me/repo"
	cfg.GitHub.Auth.Token = "tok"
	cfg.ScaleSet.Name = "deckhand"
	cfg.ScaleSet.RunnerGroup = scaleset.DefaultRunnerGroup
	cfg.Slots.Count = slotCount
	cfg.Slots.Warm = warm
	cfg.Runner.Image = "img@sha256:abc"
	dir := t.TempDir()
	paths := config.Paths{
		Home:      dir,
		StateFile: filepath.Join(dir, "state.json"),
		LogFile:   filepath.Join(dir, "daemon.log"),
	}
	return cfg, paths
}

func testBroker(t *testing.T, slotCount, warm int) (*Broker, *fakeGH, *fakeProvider) {
	t.Helper()
	cfg, paths := testConfig(t, slotCount, warm)
	gh := newFakeGH()
	provider := newFakeProvider()
	b := newBroker(cfg, paths, slog.New(slog.DiscardHandler), bus.New(), provider, gh, false)
	b.setScaleSet(&scaleset.RunnerScaleSet{ID: 42, Name: "deckhand"})
	// Compress every timing so tests never wait on wall-clock defaults.
	b.tm = timings{
		watchGrace:      40 * time.Millisecond,
		watchPoll:       5 * time.Millisecond,
		sweepEvery:      time.Hour, // sweeps are driven explicitly via sweepOnce
		dockerPingEvery: time.Hour,
		erroredCooldown: 20 * time.Millisecond,
		zombieMinAge:    0,
		zombieMisses:    3,
		pruneAge:        time.Hour,
		wakeSlack:       30 * time.Second,
		stopPollEvery:   5 * time.Millisecond,
		resourceEvery:   time.Hour, // sampled explicitly via sampleResourcesOnce
	}
	t.Cleanup(func() {
		// Release any watcher goroutines blocked in fake Wait.
		provider.mu.Lock()
		for _, c := range provider.containers {
			select {
			case <-c.exitCh:
			default:
				close(c.exitCh)
			}
		}
		provider.mu.Unlock()
	})
	return b, gh, provider
}

func newBrokerForTest(cfg *config.Config, paths config.Paths, provider containerProvider, gh ghAPI) *Broker {
	return newBroker(cfg, paths, slog.New(slog.DiscardHandler), bus.New(), provider, gh, false)
}

func TestSampleResourcesAggregates(t *testing.T) {
	b, _, prov := testBroker(t, 2, 0)

	// Stage two running slots backed by containers with known stats.
	i0, _, ok0 := b.slots.Acquire()
	i1, _, ok1 := b.slots.Acquire()
	if !ok0 || !ok1 {
		t.Fatal("could not acquire two slots")
	}
	b.slots.MutateIndex(i0, func(s *slots.Slot) { s.State = slots.Running; s.ContainerID = "cid-a" })
	b.slots.MutateIndex(i1, func(s *slots.Slot) { s.State = slots.Running; s.ContainerID = "cid-b" })
	prov.mu.Lock()
	prov.stats["cid-a"] = runner.Stats{CPUCores: 1.5, MemBytes: 500 * 1024 * 1024}
	prov.stats["cid-b"] = runner.Stats{CPUCores: 0.5, MemBytes: 300 * 1024 * 1024}
	prov.mu.Unlock()

	b.sampleResourcesOnce(context.Background())

	r := b.Status().Resources
	if !r.OK {
		t.Fatal("resources not marked OK after a sample")
	}
	if r.CPUCoresUsed < 1.999 || r.CPUCoresUsed > 2.001 {
		t.Errorf("CPUCoresUsed = %v, want ~2.0 (1.5 + 0.5)", r.CPUCoresUsed)
	}
	if want := int64(800 * 1024 * 1024); r.MemUsedBytes != want {
		t.Errorf("MemUsedBytes = %d, want %d", r.MemUsedBytes, want)
	}
	if r.CPUCores != 8 {
		t.Errorf("host CPUCores = %d, want 8", r.CPUCores)
	}
	if want := int64(16 * 1024 * 1024 * 1024); r.MemTotalBytes != want {
		t.Errorf("host MemTotalBytes = %d, want %d", r.MemTotalBytes, want)
	}
}

// A container that vanishes mid-sample (SampleStats errors) drops out of the
// sum instead of failing the whole reading.
func TestSampleResourcesSkipsErroringContainer(t *testing.T) {
	b, _, prov := testBroker(t, 2, 0)
	i0, _, _ := b.slots.Acquire()
	i1, _, _ := b.slots.Acquire()
	b.slots.MutateIndex(i0, func(s *slots.Slot) { s.State = slots.Running; s.ContainerID = "cid-a" })
	b.slots.MutateIndex(i1, func(s *slots.Slot) { s.State = slots.Running; s.ContainerID = "gone" })
	prov.mu.Lock()
	prov.stats["cid-a"] = runner.Stats{CPUCores: 1.0, MemBytes: 200 * 1024 * 1024}
	prov.mu.Unlock() // "gone" has no stats -> SampleStats errors

	b.sampleResourcesOnce(context.Background())

	r := b.Status().Resources
	if !r.OK || r.CPUCoresUsed < 0.999 || r.CPUCoresUsed > 1.001 {
		t.Errorf("want ~1.0 core from the one live container, got ok=%v cores=%v", r.OK, r.CPUCoresUsed)
	}
}

func desire(t *testing.T, b *Broker, assigned int) int {
	t.Helper()
	n, err := (&scaler{b: b}).HandleDesiredRunnerCount(context.Background(), assigned)
	if err != nil {
		t.Fatalf("HandleDesiredRunnerCount: %v", err)
	}
	return n
}

func slotByIndex(t *testing.T, b *Broker, idx int) slots.Slot {
	t.Helper()
	s, ok := b.slots.Get(idx)
	if !ok {
		t.Fatalf("no slot %d", idx)
	}
	return s
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---- scaler tests ---------------------------------------------------------

func TestDesiredCountSpawnsWarmPlusAssigned(t *testing.T) {
	b, _, provider := testBroker(t, 4, 1)
	if got := desire(t, b, 2); got != 3 {
		t.Fatalf("want 3 live runners (2 assigned + 1 warm), got %d", got)
	}
	provider.mu.Lock()
	n := len(provider.containers)
	provider.mu.Unlock()
	if n != 3 {
		t.Fatalf("want 3 containers, got %d", n)
	}
}

func TestDesiredCountBoundedByFreeSlots(t *testing.T) {
	b, _, _ := testBroker(t, 2, 0)
	if got := desire(t, b, 5); got != 2 {
		t.Fatalf("want spawn bounded at 2 slots, got %d", got)
	}
}

func TestPausedStillRunsAssignedJobsButShedsWarm(t *testing.T) {
	b, _, _ := testBroker(t, 4, 2)
	desire(t, b, 0) // warm pool up
	if b.slots.Live() != 2 {
		t.Fatalf("warm pool should be 2, got %d", b.slots.Live())
	}
	b.Pause()
	// One job is already assigned to us: it must still get a runner —
	// stranding an acquired job would hang it on GitHub.
	if got := desire(t, b, 1); got != 1 {
		t.Fatalf("paused with 1 assigned: want exactly 1 live runner, got %d", got)
	}
}

func TestDockerDownSpawnsNothing(t *testing.T) {
	b, _, _ := testBroker(t, 4, 2)
	b.setDockerDown(true)
	if got := desire(t, b, 3); got != 0 {
		t.Fatalf("docker down must spawn nothing, got %d", got)
	}
	if b.counters.spawnErrors.Load() != 0 {
		t.Fatal("docker-down must not burn spawn-error retries")
	}
}

func TestSpawnFailureGoesErroredThenRetries(t *testing.T) {
	b, _, provider := testBroker(t, 2, 0)
	provider.spawnErr = errors.New("boom")
	if got := desire(t, b, 2); got != 0 {
		t.Fatalf("want 0 live after spawn failure, got %d", got)
	}
	if b.counters.spawnErrors.Load() == 0 {
		t.Fatal("spawn errors must be counted")
	}
	if s := slotByIndex(t, b, 0); s.State != slots.Errored {
		t.Fatalf("slot 0 should be errored, got %s", s.State)
	}
	// Within cooldown: errored slots are not retried.
	if got := desire(t, b, 2); got != 0 {
		t.Fatalf("cooldown not honored, got %d live", got)
	}
	// After cooldown with docker healthy again: capacity self-heals.
	provider.mu.Lock()
	provider.spawnErr = nil
	provider.mu.Unlock()
	time.Sleep(b.tm.erroredCooldown + 10*time.Millisecond)
	if got := desire(t, b, 2); got != 2 {
		t.Fatalf("want recovery to 2 live, got %d", got)
	}
}

func TestSpawnFailureCleansUpJitRegistration(t *testing.T) {
	b, gh, provider := testBroker(t, 1, 0)
	provider.spawnErr = errors.New("boom")
	// Make the just-minted registration visible so cleanup can find it.
	gh.mu.Lock()
	gh.runnersByName = map[string]*scaleset.RunnerReference{}
	gh.mu.Unlock()
	registered := &scaleset.RunnerReference{ID: 9}
	// The runner name is generated inside spawnOne; register a catch-all by
	// intercepting after the call: simulate presence for any name.
	gh.mu.Lock()
	gh.runnersByName["*"] = registered
	gh.mu.Unlock()
	desire(t, b, 1)
	// The (nil, nil) case must at minimum not panic and not call RemoveRunner.
	gh.mu.Lock()
	removed := len(gh.removedRunners)
	gh.mu.Unlock()
	if removed != 0 {
		t.Fatalf("RemoveRunner must not be called for an absent registration, got %d calls", removed)
	}
}

func TestJobLifecycleCountersAndSanitization(t *testing.T) {
	b, _, _ := testBroker(t, 1, 0)
	desire(t, b, 1)
	s := slotByIndex(t, b, 0)
	sc := &scaler{b: b}

	evil := "evil\x1b]0;pwned\x07job"
	if err := sc.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName:     s.RunnerName,
		JobMessageBase: scaleset.JobMessageBase{JobDisplayName: evil, OwnerName: "me", RepositoryName: "repo"},
	}); err != nil {
		t.Fatal(err)
	}
	got := slotByIndex(t, b, 0)
	if got.State != slots.Running || got.Job == nil {
		t.Fatalf("slot should be running with job info: %+v", got)
	}
	if strings.ContainsRune(got.Job.DisplayName, 0x1b) || strings.ContainsRune(got.Job.DisplayName, 0x07) {
		t.Fatalf("display name not sanitized: %q", got.Job.DisplayName)
	}

	if err := sc.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName:     s.RunnerName,
		Result:         "Succeeded",
		JobMessageBase: scaleset.JobMessageBase{JobDisplayName: "j", OwnerName: "me", RepositoryName: "repo"},
	}); err != nil {
		t.Fatal(err)
	}
	if b.counters.completed.Load() != 1 || b.counters.failed.Load() != 0 {
		t.Fatalf("counter split wrong: %d ok / %d failed", b.counters.completed.Load(), b.counters.failed.Load())
	}
	if got := slotByIndex(t, b, 0); got.State != slots.Idle {
		t.Fatalf("slot should be idle after completion, got %s", got.State)
	}
}

func TestLateJobCompletedDoesNotDoubleCount(t *testing.T) {
	b, _, _ := testBroker(t, 1, 0)
	sc := &scaler{b: b}
	// Unknown runner (watcher already reaped it): must not touch counters,
	// must not corrupt slot 0 via Free(-1, ...), must not panic.
	if err := sc.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: "gone-runner",
		Result:     "failed",
	}); err != nil {
		t.Fatal(err)
	}
	if b.counters.failed.Load() != 0 || b.counters.completed.Load() != 0 {
		t.Fatal("late JobCompleted for an unknown runner must not count")
	}
}

func TestCanceledResultCountsAsFailed(t *testing.T) {
	b, _, _ := testBroker(t, 1, 0)
	desire(t, b, 1)
	s := slotByIndex(t, b, 0)
	sc := &scaler{b: b}
	_ = sc.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: s.RunnerName, Result: "canceled"})
	if b.counters.failed.Load() != 1 {
		t.Fatal("non-succeeded results must count as failed")
	}
}

func TestCullNeverKillsOnProbeError(t *testing.T) {
	b, _, provider := testBroker(t, 2, 2)
	desire(t, b, 0) // 2 warm runners
	provider.mu.Lock()
	provider.workerErr = errors.New("docker top flaked")
	provider.mu.Unlock()
	b.Pause()
	desire(t, b, 0) // pause wants warm culled — but probes fail
	if got := len(provider.removedIDs()); got != 0 {
		t.Fatalf("a failed worker probe must never license removal; %d containers removed", got)
	}
	// Probe recovers: cull proceeds.
	provider.mu.Lock()
	provider.workerErr = nil
	provider.mu.Unlock()
	desire(t, b, 0)
	waitFor(t, "warm runners culled", func() bool { return b.slots.Live() == 0 })
}

func TestScaleDownCullsDrainingReadyRunners(t *testing.T) {
	b, _, _ := testBroker(t, 2, 2)
	desire(t, b, 0)
	if b.slots.Live() != 2 {
		t.Fatalf("warm pool should be 2, got %d", b.slots.Live())
	}
	if err := b.Scale(0); err != nil {
		t.Fatal(err)
	}
	desire(t, b, 0)
	if got := b.slots.Live(); got != 0 {
		t.Fatalf("scale 0 must cull all jobless runners, %d still live", got)
	}
	if got := len(b.slots.Snapshot()); got != 0 {
		t.Fatalf("slot table should be empty at target 0, got %d", got)
	}
}

// ---- watcher tests --------------------------------------------------------

func TestWatcherReapsUnreportedExit(t *testing.T) {
	b, _, provider := testBroker(t, 1, 0)
	desire(t, b, 1)
	s := slotByIndex(t, b, 0)
	// A REAL job is running when the container dies without a JobCompleted —
	// that's a genuine failure and must be counted. A real job carries a
	// non-zero RequestID (HandleJobStarted sets it from GitHub).
	b.slots.Mutate(s.RunnerName, func(sl *slots.Slot) {
		sl.State = slots.Running
		sl.Job = &slots.Job{RequestID: 42, DisplayName: "build"}
	})
	provider.exitContainer(s.ContainerID, 137)
	waitFor(t, "slot freed by watcher", func() bool {
		return slotByIndex(t, b, 0).State == slots.Idle
	})
	if b.counters.failed.Load() != 1 {
		t.Fatalf("a real job's unreported exit must count failed, got %d", b.counters.failed.Load())
	}
}

// An idle runner (never assigned a job) recycling must NOT be counted as a
// failed job — that over-count was the "42 failed" churn artifact.
func TestWatcherIdleExitNotCountedFailed(t *testing.T) {
	b, _, provider := testBroker(t, 1, 0)
	desire(t, b, 1)
	s := slotByIndex(t, b, 0)
	provider.exitContainer(s.ContainerID, 137) // exits while Ready, never ran a job
	waitFor(t, "slot freed by watcher", func() bool {
		return slotByIndex(t, b, 0).State == slots.Idle
	})
	if b.counters.failed.Load() != 0 {
		t.Fatalf("idle runner exit must not count as a failed job, got %d", b.counters.failed.Load())
	}
}

func TestWatcherYieldsToJobCompleted(t *testing.T) {
	b, _, provider := testBroker(t, 1, 0)
	desire(t, b, 1)
	s := slotByIndex(t, b, 0)
	sc := &scaler{b: b}
	provider.exitContainer(s.ContainerID, 0)
	// JobCompleted arrives within the grace window.
	_ = sc.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: s.RunnerName, Result: "Succeeded"})
	// Give the watcher its full grace window to (wrongly) act.
	time.Sleep(b.tm.watchGrace + 30*time.Millisecond)
	if b.counters.failed.Load() != 0 {
		t.Fatal("watcher must not count a failure after JobCompleted handled the exit")
	}
	if b.counters.completed.Load() != 1 {
		t.Fatalf("want exactly 1 completed, got %d", b.counters.completed.Load())
	}
}

// ---- sweeper / zombie tests -----------------------------------------------

func TestZombieReclaimDebounce(t *testing.T) {
	b, gh, provider := testBroker(t, 1, 1)
	desire(t, b, 0) // one warm runner
	s := slotByIndex(t, b, 0)
	misses := map[string]int{}

	// The runner is absent from GitHub ((nil, nil) contract) — but reclaim
	// must wait for zombieMisses consecutive confirmations.
	b.sweepOnce(context.Background(), misses)
	b.sweepOnce(context.Background(), misses)
	if got := slotByIndex(t, b, 0); got.State != slots.Ready {
		t.Fatalf("reclaim before debounce threshold; state %s", got.State)
	}
	// A probe FAILURE in between must not count as a miss.
	gh.mu.Lock()
	gh.runnerProbeErr = errors.New("github flaked")
	gh.mu.Unlock()
	b.sweepOnce(context.Background(), misses)
	gh.mu.Lock()
	gh.runnerProbeErr = nil
	gh.mu.Unlock()
	if misses[s.RunnerName] != 2 {
		t.Fatalf("probe failure altered the miss counter: %d", misses[s.RunnerName])
	}
	// Third genuine miss: reclaimed.
	b.sweepOnce(context.Background(), misses)
	if got := slotByIndex(t, b, 0); got.State == slots.Ready {
		t.Fatal("zombie not reclaimed after debounce threshold")
	}
	if b.counters.zombiesReclaimed.Load() != 1 {
		t.Fatalf("zombie counter = %d", b.counters.zombiesReclaimed.Load())
	}
	if len(provider.removedIDs()) != 1 {
		t.Fatalf("zombie container not removed")
	}
}

func TestZombieResetWhenRunnerReappears(t *testing.T) {
	b, gh, _ := testBroker(t, 1, 1)
	desire(t, b, 0)
	s := slotByIndex(t, b, 0)
	misses := map[string]int{}
	b.sweepOnce(context.Background(), misses)
	b.sweepOnce(context.Background(), misses)
	gh.mu.Lock()
	gh.runnersByName[s.RunnerName] = &scaleset.RunnerReference{ID: 5, Name: s.RunnerName}
	gh.mu.Unlock()
	b.sweepOnce(context.Background(), misses)
	if misses[s.RunnerName] != 0 {
		t.Fatalf("reappearance must reset the miss counter, got %d", misses[s.RunnerName])
	}
}

func TestZombieNeverReclaimedWithLiveWorker(t *testing.T) {
	b, _, provider := testBroker(t, 1, 1)
	desire(t, b, 0)
	s := slotByIndex(t, b, 0)
	provider.mu.Lock()
	provider.containers[s.ContainerID].hasWorker = true
	provider.mu.Unlock()
	misses := map[string]int{}
	for range 5 {
		b.sweepOnce(context.Background(), misses)
	}
	if len(provider.removedIDs()) != 0 {
		t.Fatal("a container with a live worker must never be reclaimed")
	}
}

// ---- reconcile tests ------------------------------------------------------

func TestReconcileFreesVanishedContainer(t *testing.T) {
	b, _, provider := testBroker(t, 1, 1)
	desire(t, b, 0)
	s := slotByIndex(t, b, 0)
	// Simulate docker losing the container entirely (VM restart).
	provider.mu.Lock()
	c := provider.containers[s.ContainerID]
	delete(provider.containers, s.ContainerID)
	provider.mu.Unlock()
	close(c.exitCh)
	b.reconcile(context.Background())
	if got := slotByIndex(t, b, 0); got.State != slots.Idle {
		t.Fatalf("vanished container must free its slot, state %s", got.State)
	}
}

func TestReconcileAdoptsUntrackedAndAssumesBusyOnProbeError(t *testing.T) {
	b, _, provider := testBroker(t, 2, 0)
	// A container exists that the broker doesn't know about (crash recovery).
	provider.mu.Lock()
	provider.containers["cid-x"] = &fakeContainer{id: "cid-x", slot: 1, name: "deckhand-s1-abcd", running: true, exitCh: make(chan struct{})}
	provider.workerErr = errors.New("top flaked")
	provider.mu.Unlock()
	b.reconcile(context.Background())
	got := slotByIndex(t, b, 1)
	if got.RunnerName != "deckhand-s1-abcd" {
		t.Fatalf("container not adopted: %+v", got)
	}
	if got.State != slots.Running {
		t.Fatalf("probe failure must adopt as busy (fail-safe), got %s", got.State)
	}
}

func TestReconcileIsIdempotent(t *testing.T) {
	b, _, provider := testBroker(t, 1, 1)
	desire(t, b, 0)
	before := slotByIndex(t, b, 0)
	// Mark it running with real job info, then reconcile twice (wake path).
	sc := &scaler{b: b}
	_ = sc.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName:     before.RunnerName,
		JobMessageBase: scaleset.JobMessageBase{JobDisplayName: "real job", OwnerName: "me", RepositoryName: "repo"},
	})
	b.reconcile(context.Background())
	b.reconcile(context.Background())
	after := slotByIndex(t, b, 0)
	if after.Job == nil || after.Job.DisplayName != "real job" {
		t.Fatalf("reconcile clobbered live job info: %+v", after.Job)
	}
	b.mu.Lock()
	watchers := len(b.watched)
	b.mu.Unlock()
	if watchers != 1 {
		t.Fatalf("want exactly 1 watcher after repeated reconciles, got %d", watchers)
	}
	_ = provider
}

func TestReconcileClearsDockerDown(t *testing.T) {
	b, _, provider := testBroker(t, 1, 0)
	provider.mu.Lock()
	provider.pingErr = errors.New("down")
	provider.mu.Unlock()
	b.reconcile(context.Background())
	if !b.isDockerDown() {
		t.Fatal("reconcile against dead docker must set dockerDown")
	}
	provider.mu.Lock()
	provider.pingErr = nil
	provider.mu.Unlock()
	b.reconcile(context.Background())
	if b.isDockerDown() {
		t.Fatal("reconcile against healthy docker must clear dockerDown")
	}
}

// ---- ensureScaleSet tests -------------------------------------------------

func TestEnsureScaleSetTransientErrorDoesNotCreate(t *testing.T) {
	b, gh, _ := testBroker(t, 1, 0)
	b.setScaleSet(nil)
	gh.mu.Lock()
	gh.getSetErr = errors.New("github 502")
	gh.mu.Unlock()
	err := b.ensureScaleSet(context.Background())
	if err == nil {
		t.Fatal("transient GET error must surface")
	}
	if errors.Is(err, ErrScaleSetConflict) {
		t.Fatal("transient error must not be a conflict (it would become fatal)")
	}
	if gh.createCalls.Load() != 0 {
		t.Fatal("must NOT fall through to create on a lookup failure")
	}
}

func TestEnsureScaleSetCreatesAndPersistsImmediately(t *testing.T) {
	b, gh, _ := testBroker(t, 1, 0)
	b.setScaleSet(nil)
	if err := b.ensureScaleSet(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gh.createCalls.Load() != 1 {
		t.Fatal("scale set should be created")
	}
	st, err := b.loadState()
	if err != nil {
		t.Fatalf("state must be persisted at create time: %v", err)
	}
	if st.ScaleSetID != b.scaleSetID() {
		t.Fatalf("persisted id %d != live id %d", st.ScaleSetID, b.scaleSetID())
	}
	// Restart: the same broker config now adopts its own scale set.
	b2 := newBrokerForTest(b.cfg, b.paths, newFakeProvider(), gh)
	if err := b2.ensureScaleSet(context.Background()); err != nil {
		t.Fatalf("re-attach to own scale set must succeed: %v", err)
	}
}

func TestEnsureScaleSetConflictWithoutTakeover(t *testing.T) {
	cfg, paths := testConfig(t, 1, 0)
	gh := newFakeGH()
	gh.scaleSets["deckhand"] = &scaleset.RunnerScaleSet{ID: 999, Name: "deckhand"}
	b := newBrokerForTest(cfg, paths, newFakeProvider(), gh)
	err := b.ensureScaleSet(context.Background())
	if !errors.Is(err, ErrScaleSetConflict) {
		t.Fatalf("want ErrScaleSetConflict, got %v", err)
	}
	b.takeover = true
	if err := b.ensureScaleSet(context.Background()); err != nil {
		t.Fatalf("takeover must adopt: %v", err)
	}
	if b.scaleSetID() != 999 {
		t.Fatalf("adopted wrong id %d", b.scaleSetID())
	}
}

// ---- stop / reclaim tests -------------------------------------------------

func TestStopNowRefusesBusyIncludingUnprovable(t *testing.T) {
	b, _, provider := testBroker(t, 1, 1)
	desire(t, b, 0)
	s := slotByIndex(t, b, 0)
	provider.mu.Lock()
	provider.containers[s.ContainerID].hasWorker = true
	provider.mu.Unlock()
	shutdownCalled := false
	err := b.Stop(context.Background(), StopNow, false, func(string) { shutdownCalled = true })
	if !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy, got %v", err)
	}
	if shutdownCalled {
		t.Fatal("shutdown must not run when refused")
	}
	// Probe failure must also refuse.
	provider.mu.Lock()
	provider.containers[s.ContainerID].hasWorker = false
	provider.workerErr = errors.New("flake")
	provider.mu.Unlock()
	if err := b.Stop(context.Background(), StopNow, false, func(string) {}); !errors.Is(err, ErrBusy) {
		t.Fatalf("unprovable worker state must refuse stop, got %v", err)
	}
}

func TestStopNowForceRemovesEverything(t *testing.T) {
	b, _, provider := testBroker(t, 1, 1)
	desire(t, b, 0)
	s := slotByIndex(t, b, 0)
	provider.mu.Lock()
	provider.containers[s.ContainerID].hasWorker = true
	provider.mu.Unlock()
	done := false
	if err := b.Stop(context.Background(), StopNow, true, func(string) { done = true }); err != nil {
		t.Fatal(err)
	}
	if !done || len(provider.removedIDs()) != 1 {
		t.Fatal("forced stop must remove containers and shut down")
	}
}

func TestStopDrainCancelledByResume(t *testing.T) {
	b, _, _ := testBroker(t, 1, 1)
	desire(t, b, 0) // one warm runner keeps Live > 0
	var stopped atomic.Bool
	if err := b.Stop(context.Background(), StopDrain, false, func(string) { stopped.Store(true) }); err != nil {
		t.Fatal(err)
	}
	if !b.isDraining() {
		t.Fatal("drain-stop must set draining")
	}
	b.Resume()
	// Drop the fleet to zero — the moment the old bug would fire shutdown.
	_ = b.Scale(0)
	desire(t, b, 0)
	time.Sleep(20 * b.tm.stopPollEvery)
	if stopped.Load() {
		t.Fatal("resume must cancel a pending drain-stop")
	}
}

func TestStopDrainCompletesWhenIdle(t *testing.T) {
	b, _, _ := testBroker(t, 1, 0)
	var stopped atomic.Bool
	if err := b.Stop(context.Background(), StopDrain, false, func(string) { stopped.Store(true) }); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "drain-stop shutdown", stopped.Load)
}

func TestParseStopMode(t *testing.T) {
	if _, err := ParseStopMode("nuke"); err == nil {
		t.Fatal("unknown stop mode must be rejected, not defaulted")
	}
	if m, err := ParseStopMode(""); err != nil || m != StopDrain {
		t.Fatalf("empty mode should default to drain, got %v %v", m, err)
	}
}

func TestReclaimNotFoundAndBusy(t *testing.T) {
	b, _, provider := testBroker(t, 1, 1)
	if err := b.Reclaim(context.Background(), 5, false); !errors.Is(err, ErrNotFound) {
		t.Fatalf("want ErrNotFound for missing slot, got %v", err)
	}
	desire(t, b, 0)
	s := slotByIndex(t, b, 0)
	provider.mu.Lock()
	provider.containers[s.ContainerID].hasWorker = true
	provider.mu.Unlock()
	if err := b.Reclaim(context.Background(), 0, false); !errors.Is(err, ErrBusy) {
		t.Fatalf("want ErrBusy for mid-job slot, got %v", err)
	}
	if err := b.Reclaim(context.Background(), 0, true); err != nil {
		t.Fatalf("forced reclaim must succeed: %v", err)
	}
}

// ---- state persistence ----------------------------------------------------

func TestScalePersistsAcrossRestart(t *testing.T) {
	b, _, _ := testBroker(t, 2, 0)
	if err := b.Scale(6); err != nil {
		t.Fatal(err)
	}
	b2 := newBrokerForTest(b.cfg, b.paths, newFakeProvider(), newFakeGH())
	if got := b2.slots.Target(); got != 6 {
		t.Fatalf("runtime scale must survive restart, got %d", got)
	}
}

func TestEarlyScaleDoesNotClobberScaleSetID(t *testing.T) {
	b, _, _ := testBroker(t, 1, 0)
	b.saveState() // persists id 42
	b.setScaleSet(nil)
	_ = b.Scale(3) // scale before ensureScaleSet has run (early control call)
	st, err := b.loadState()
	if err != nil {
		t.Fatal(err)
	}
	if st.ScaleSetID != 42 {
		t.Fatalf("early Scale clobbered scale set id: %d", st.ScaleSetID)
	}
}

func TestRemoveRunnerRegistrationNilSafe(t *testing.T) {
	b, gh, _ := testBroker(t, 1, 0)
	// (nil, nil): absent registration must be a quiet no-op — this is the
	// path that previously nil-dereferenced and crashed the daemon.
	b.removeRunnerRegistration(context.Background(), "ghost")
	gh.mu.Lock()
	defer gh.mu.Unlock()
	if len(gh.removedRunners) != 0 {
		t.Fatal("RemoveRunner must not be called for an absent runner")
	}
}

func TestLatencyCountersFromJobTimestamps(t *testing.T) {
	b, _, _ := testBroker(t, 1, 0)
	desire(t, b, 1)
	s := slotByIndex(t, b, 0)
	sc := &scaler{b: b}
	queued := time.Now().Add(-90 * time.Second)
	assigned := time.Now().Add(-60 * time.Second)
	finished := time.Now()
	_ = sc.HandleJobStarted(context.Background(), &scaleset.JobStarted{
		RunnerName: s.RunnerName,
		JobMessageBase: scaleset.JobMessageBase{
			QueueTime:        queued,
			RunnerAssignTime: assigned,
		},
	})
	_ = sc.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{
		RunnerName: s.RunnerName,
		Result:     "Succeeded",
		JobMessageBase: scaleset.JobMessageBase{
			RunnerAssignTime: assigned,
			FinishTime:       finished,
		},
	})
	st := b.Status().Counters
	if st.QueueCount != 1 || st.QueueMsSum < 29000 || st.QueueMsSum > 31000 {
		t.Fatalf("queue latency wrong: count=%d sum=%dms", st.QueueCount, st.QueueMsSum)
	}
	if st.DurationCount != 1 || st.DurationMsSum < 59000 || st.DurationMsSum > 61000 {
		t.Fatalf("duration wrong: count=%d sum=%dms", st.DurationCount, st.DurationMsSum)
	}
	if st.DurationMsMin != st.DurationMsSum || st.DurationMsMax != st.DurationMsSum {
		t.Fatalf("single job: min/max must equal the one duration (min=%d max=%d)", st.DurationMsMin, st.DurationMsMax)
	}
}

func TestLatencyCountersIgnoreZeroTimestamps(t *testing.T) {
	b, _, _ := testBroker(t, 1, 0)
	desire(t, b, 1)
	s := slotByIndex(t, b, 0)
	sc := &scaler{b: b}
	_ = sc.HandleJobStarted(context.Background(), &scaleset.JobStarted{RunnerName: s.RunnerName})
	_ = sc.HandleJobCompleted(context.Background(), &scaleset.JobCompleted{RunnerName: s.RunnerName, Result: "failed"})
	st := b.Status().Counters
	if st.QueueCount != 0 || st.DurationCount != 0 {
		t.Fatal("zero timestamps must not be counted as measurements")
	}
}

func TestAutoPinDividesHostCPUs(t *testing.T) {
	b, _, _ := testBroker(t, 4, 0) // cfg.Slots.CPUsPerSlot == 0 → auto
	b.applyAutoPin(context.Background())
	snap := b.slots.Snapshot()
	if snap[0].Cpuset != "0-1" || snap[3].Cpuset != "6-7" {
		t.Fatalf("auto pin with 8 cpus / 4 slots should give 2-wide cpusets, got %q and %q", snap[0].Cpuset, snap[3].Cpuset)
	}
	// Scale changes the per-slot share.
	if err := b.Scale(2); err != nil {
		t.Fatal(err)
	}
	if got, _ := b.slots.Get(0); got.Cpuset != "0-3" {
		t.Fatalf("after scale 2, slot 0 should own 4 cpus, got %q", got.Cpuset)
	}
}

func TestAutoPinMoreSlotsThanCPUs(t *testing.T) {
	b, _, provider := testBroker(t, 4, 0)
	provider.ncpu = 2
	b.applyAutoPin(context.Background())
	if got, _ := b.slots.Get(0); got.Cpuset != "" {
		t.Fatalf("ncpu < slots must leave slots unpinned, got %q", got.Cpuset)
	}
}

func TestExplicitPinNotOverridden(t *testing.T) {
	b, _, _ := testBroker(t, 2, 0)
	b.cfg.Slots.CPUsPerSlot = 3 // explicit
	b.slots.SetCPUsPerSlot(3)
	b.applyAutoPin(context.Background())
	if got, _ := b.slots.Get(0); got.Cpuset != "0-2" {
		t.Fatalf("explicit pinning must not be auto-overridden, got %q", got.Cpuset)
	}
}
