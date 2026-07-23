package slots

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func acquireNamed(t *testing.T, m *Manager, name string) int {
	t.Helper()
	idx, _, ok := m.Acquire()
	if !ok {
		t.Fatalf("expected a free slot for %s", name)
	}
	m.SetRunnerName(idx, name)
	return idx
}

func TestScaleUpAddsIdleSlots(t *testing.T) {
	m := NewManager(2, 0)
	m.SetTarget(5)
	if got := len(m.Snapshot()); got != 5 {
		t.Fatalf("want 5 slots, got %d", got)
	}
	if m.FreeCount() != 5 {
		t.Fatalf("want 5 free, got %d", m.FreeCount())
	}
}

func TestScaleDownRemovesIdleImmediately(t *testing.T) {
	m := NewManager(4, 0)
	m.SetTarget(2)
	if got := len(m.Snapshot()); got != 2 {
		t.Fatalf("want 2 slots after shrink, got %d", got)
	}
}

func TestScaleDownDrainsBusySlots(t *testing.T) {
	m := NewManager(3, 0)
	for i := range 3 {
		idx := acquireNamed(t, m, name(i))
		m.MutateIndex(idx, func(s *Slot) { s.State = Running })
	}
	m.SetTarget(1)
	if got := len(m.Snapshot()); got != 3 {
		t.Fatalf("busy slots must survive scale-down; got %d slots", got)
	}
	m.Free(2, name(2))
	if got := len(m.Snapshot()); got != 2 {
		t.Fatalf("drained slot should be removed after Free; got %d slots", got)
	}
	m.Free(1, name(1))
	if got := len(m.Snapshot()); got != 1 {
		t.Fatalf("want 1 slot at target, got %d", got)
	}
	if m.FreeCount() != 0 {
		t.Fatalf("running slot must not count as free")
	}
}

func TestScaleUpCancelsDraining(t *testing.T) {
	m := NewManager(2, 0)
	for i := range 2 {
		idx := acquireNamed(t, m, name(i))
		m.MutateIndex(idx, func(s *Slot) { s.State = Running })
	}
	m.SetTarget(1) // slot 1 drains
	m.SetTarget(2) // change of heart before it finished
	m.Free(1, name(1))
	snap := m.Snapshot()
	if len(snap) != 2 || snap[1].State != Idle {
		t.Fatalf("un-drained slot should return to idle, got %+v", snap)
	}
}

func TestFreeIsOwnershipChecked(t *testing.T) {
	m := NewManager(1, 0)
	idx := acquireNamed(t, m, "current")
	if m.Free(idx, "stale-runner") {
		t.Fatal("Free with a stale runner name must be a no-op")
	}
	if m.Free(idx, "") {
		t.Fatal("Free with an empty runner name must be a no-op")
	}
	if !m.Free(idx, "current") {
		t.Fatal("Free with the owning runner name must succeed")
	}
	// Double-free (the JobCompleted vs container-exit race) is a no-op.
	if m.Free(idx, "current") {
		t.Fatal("second Free must be a no-op")
	}
}

func TestFreeOutOfRange(t *testing.T) {
	m := NewManager(2, 0)
	// HandleJobCompleted calls Free(-1, name) for unknown runners; both
	// out-of-range directions must be safe no-ops.
	if m.Free(-1, "x") || m.Free(99, "x") {
		t.Fatal("out-of-range Free must return false")
	}
}

func TestAcquireSkipsDrainingSlots(t *testing.T) {
	m := NewManager(2, 0)
	m.SetTarget(1)
	idx, _, ok := m.Acquire()
	if !ok {
		t.Fatal("the non-draining slot must be acquirable")
	}
	if idx != 0 {
		t.Fatalf("acquired slot %d, want 0 (slot 1 is draining)", idx)
	}
	m.SetRunnerName(idx, "x")
	if _, _, ok := m.Acquire(); ok {
		t.Fatal("no second slot should be acquirable")
	}
}

func TestMutateUnknownAndEmptyName(t *testing.T) {
	m := NewManager(2, 0)
	if m.Mutate("nope", func(s *Slot) { t.Fatal("fn must not run") }) {
		t.Fatal("Mutate on unknown runner must return false")
	}
	// An empty name must never match an idle slot's empty RunnerName.
	if m.Mutate("", func(s *Slot) { t.Fatal("fn must not run") }) {
		t.Fatal("Mutate with empty name must return false")
	}
}

func TestAdoptBeyondTargetDrains(t *testing.T) {
	m := NewManager(2, 0)
	idx := m.Adopt(4, "adopted", "cid", true)
	if idx != 4 {
		t.Fatalf("expected adoption at labeled index 4, got %d", idx)
	}
	snap := m.Snapshot()
	if len(snap) != 5 {
		t.Fatalf("adoption should grow the table to 5, got %d", len(snap))
	}
	if snap[4].State != Running {
		t.Fatalf("adopted busy slot should be Running, got %s", snap[4].State)
	}
	m.Free(4, "adopted")
	if got := len(m.Snapshot()); got != 2 {
		t.Fatalf("want table back at target 2, got %d", got)
	}
}

func TestAdoptCollisionAppendsInsteadOfClobbering(t *testing.T) {
	m := NewManager(2, 0)
	idx := acquireNamed(t, m, "tenant")
	m.MutateIndex(idx, func(s *Slot) { s.State = Running; s.ContainerID = "cid-tenant" })
	// A second container labeled with the SAME slot index (stale label after
	// a failed remove + restart) must not evict the live tenant.
	got := m.Adopt(idx, "intruder", "cid-intruder", true)
	if got == idx {
		t.Fatalf("collision adoption must pick a fresh index, reused %d", got)
	}
	s, _ := m.Get(idx)
	if s.RunnerName != "tenant" || s.ContainerID != "cid-tenant" {
		t.Fatalf("tenant clobbered: %+v", s)
	}
	adopted, _ := m.Get(got)
	if adopted.RunnerName != "intruder" || adopted.State != Running {
		t.Fatalf("intruder not tracked: %+v", adopted)
	}
}

func TestAdoptClearsStaleJobAndError(t *testing.T) {
	m := NewManager(1, 0)
	idx := acquireNamed(t, m, "old")
	m.MutateIndex(idx, func(s *Slot) {
		s.State = Running
		s.Job = &Job{DisplayName: "stale"}
		s.Err = "stale error"
	})
	m.Free(idx, "old")
	m.Adopt(idx, "new", "cid", false)
	s, _ := m.Get(idx)
	if s.Job != nil || s.Err != "" {
		t.Fatalf("adoption must clear stale job/error, got %+v", s)
	}
}

func TestCpusetAssignment(t *testing.T) {
	m := NewManager(3, 2)
	snap := m.Snapshot()
	want := []string{"0-1", "2-3", "4-5"}
	for i, w := range want {
		if snap[i].Cpuset != w {
			t.Fatalf("slot %d cpuset = %q, want %q", i, snap[i].Cpuset, w)
		}
	}
	single := NewManager(2, 1)
	if s := single.Snapshot(); s[0].Cpuset != "0" || s[1].Cpuset != "1" {
		t.Fatalf("single-cpu cpusets wrong: %+v", s)
	}
}

func TestIdleRunnersOldestFirst(t *testing.T) {
	m := NewManager(3, 0)
	base := time.Now()
	// Deliberately out of index order: slot 2 is oldest, slot 0 newest.
	ages := map[int]time.Duration{0: 0, 1: -2 * time.Hour, 2: -5 * time.Hour}
	for i := range 3 {
		idx := acquireNamed(t, m, name(i))
		m.MutateIndex(idx, func(s *Slot) {
			s.State = Ready
			s.Since = base.Add(ages[i])
		})
	}
	got := m.IdleRunners()
	if len(got) != 3 {
		t.Fatalf("want 3 idle runners, got %d", len(got))
	}
	if got[0].Index != 2 || got[1].Index != 1 || got[2].Index != 0 {
		t.Fatalf("cull order must be oldest first, got %d,%d,%d", got[0].Index, got[1].Index, got[2].Index)
	}
}

func TestDrainingReady(t *testing.T) {
	m := NewManager(2, 0)
	for i := range 2 {
		idx := acquireNamed(t, m, name(i))
		m.MutateIndex(idx, func(s *Slot) { s.State = Ready })
	}
	m.SetTarget(1)
	dr := m.DrainingReady()
	if len(dr) != 1 || dr[0].Index != 1 {
		t.Fatalf("want slot 1 as draining-ready, got %+v", dr)
	}
}

func TestFreeErrored(t *testing.T) {
	m := NewManager(1, 0)
	idx, _, _ := m.Acquire()
	m.MutateIndex(idx, func(s *Slot) { s.State = Errored; s.Err = "boom" })
	if !m.FreeErrored(idx) {
		t.Fatal("FreeErrored on an errored slot must succeed")
	}
	s, _ := m.Get(idx)
	if s.State != Idle || s.Err != "" {
		t.Fatalf("want clean idle slot, got %+v", s)
	}
	if m.FreeErrored(idx) {
		t.Fatal("FreeErrored on a non-errored slot must be a no-op")
	}
}

func TestSnapshotDeepCopiesJob(t *testing.T) {
	m := NewManager(1, 0)
	idx := acquireNamed(t, m, "r")
	m.MutateIndex(idx, func(s *Slot) {
		s.State = Running
		s.Job = &Job{DisplayName: "original"}
	})
	snap := m.Snapshot()
	snap[0].Job.DisplayName = "mutated-by-consumer"
	s, _ := m.Get(idx)
	if s.Job.DisplayName != "original" {
		t.Fatal("Snapshot must not share Job pointers with the live table")
	}
}

func TestLiveAndBusyCounts(t *testing.T) {
	m := NewManager(3, 0)
	i0 := acquireNamed(t, m, name(0))
	m.MutateIndex(i0, func(s *Slot) { s.State = Ready })
	i1 := acquireNamed(t, m, name(1))
	m.MutateIndex(i1, func(s *Slot) { s.State = Running })
	if m.Live() != 2 {
		t.Fatalf("want live 2, got %d", m.Live())
	}
	if m.BusyCount() != 1 {
		t.Fatalf("want busy 1, got %d", m.BusyCount())
	}
	if m.FreeCount() != 1 {
		t.Fatalf("want free 1, got %d", m.FreeCount())
	}
}

// TestConcurrentAccess hammers every mutating entry point under -race and
// then checks structural invariants. This is what makes -race load-bearing
// for the package.
func TestConcurrentAccess(t *testing.T) {
	m := NewManager(8, 0)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	time.AfterFunc(100*time.Millisecond, func() { close(stop) })

	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			i := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				i++
				runnerName := fmt.Sprintf("w%d-r%d", w, i)
				idx, _, ok := m.Acquire()
				if !ok {
					continue
				}
				m.SetRunnerName(idx, runnerName)
				m.MutateIndex(idx, func(s *Slot) { s.State = Running })
				m.Free(idx, runnerName)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 1
		for {
			select {
			case <-stop:
				return
			default:
			}
			m.SetTarget(1 + n%12)
			m.Snapshot()
			m.IdleRunners()
			m.Live()
			n++
		}
	}()
	wg.Wait()

	// Invariants: no two slots share an owner; indexes are dense and ordered.
	seen := map[string]bool{}
	for i, s := range m.Snapshot() {
		if s.Index != i {
			t.Fatalf("slot table not dense: pos %d has index %d", i, s.Index)
		}
		if s.RunnerName == "" {
			continue
		}
		if seen[s.RunnerName] {
			t.Fatalf("runner %s owns two slots", s.RunnerName)
		}
		seen[s.RunnerName] = true
	}
}

func name(i int) string {
	return string(rune('a' + i))
}

func TestCapacityExcludesDrainingSlots(t *testing.T) {
	m := NewManager(3, 0)
	for i := range 3 {
		idx := acquireNamed(t, m, name(i))
		m.MutateIndex(idx, func(s *Slot) { s.State = Ready })
	}
	m.SetTarget(2) // slot 2 drains but its runner is still Ready
	if got := m.Live(); got != 3 {
		t.Fatalf("Live counts everything running: want 3, got %d", got)
	}
	if got := m.Capacity(); got != 2 {
		t.Fatalf("Capacity must exclude draining slots: want 2, got %d", got)
	}
}
