package slots

import (
	"testing"
)

func mustAcquire(t *testing.T, m *Manager, name string) int {
	t.Helper()
	idx, _, ok := m.Acquire(name)
	if !ok {
		t.Fatalf("expected a free slot for %s", name)
	}
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
	// Occupy all three slots.
	for i := range 3 {
		idx := mustAcquire(t, m, name(i))
		m.MutateIndex(idx, func(s *Slot) { s.State = Running })
	}
	m.SetTarget(1)
	// Busy slots above target survive as draining, nothing was killed.
	if got := len(m.Snapshot()); got != 3 {
		t.Fatalf("busy slots must survive scale-down; got %d slots", got)
	}
	// Job on slot 2 finishes → the slot vanishes rather than going idle.
	m.Free(2, name(2))
	snap := m.Snapshot()
	if got := len(snap); got != 2 {
		t.Fatalf("drained slot should be removed after Free; got %d slots", got)
	}
	// Slot 1 finishes too → also removed; slot 0 remains.
	m.Free(1, name(1))
	if got := len(m.Snapshot()); got != 1 {
		t.Fatalf("want 1 slot at target, got %d", got)
	}
	// The surviving busy slot still isn't free capacity.
	if m.FreeCount() != 0 {
		t.Fatalf("running slot must not count as free")
	}
}

func TestScaleUpCancelsDraining(t *testing.T) {
	m := NewManager(2, 0)
	idx := mustAcquire(t, m, "r")
	m.MutateIndex(idx, func(s *Slot) { s.State = Running })
	_ = mustAcquire(t, m, "r2")
	m.MutateIndex(1, func(s *Slot) { s.State = Running })
	m.SetTarget(1) // slot 1 drains
	m.SetTarget(2) // change of heart before it finished
	m.Free(1, "r2")
	snap := m.Snapshot()
	if len(snap) != 2 || snap[1].State != Idle {
		t.Fatalf("un-drained slot should return to idle, got %+v", snap)
	}
}

func TestFreeIsOwnershipChecked(t *testing.T) {
	m := NewManager(1, 0)
	idx := mustAcquire(t, m, "current")
	if m.Free(idx, "stale-runner") {
		t.Fatal("Free with a stale runner name must be a no-op")
	}
	if !m.Free(idx, "current") {
		t.Fatal("Free with the owning runner name must succeed")
	}
	// Double-free (the JobCompleted vs container-exit race) is a no-op.
	if m.Free(idx, "current") {
		t.Fatal("second Free must be a no-op")
	}
}

func TestAcquireSkipsDrainingSlots(t *testing.T) {
	m := NewManager(2, 0)
	m.SetTarget(1)
	for range 2 {
		if idx, _, ok := m.Acquire("x"); ok && idx >= 1 {
			t.Fatalf("acquired drained slot %d", idx)
		}
		m.Free(0, "x")
	}
}

func TestAdoptBeyondTargetDrains(t *testing.T) {
	m := NewManager(2, 0)
	m.AdoptAt(4, "adopted", "cid", true)
	snap := m.Snapshot()
	if len(snap) != 5 {
		t.Fatalf("adoption should grow the table to 5, got %d", len(snap))
	}
	if snap[4].State != Running {
		t.Fatalf("adopted busy slot should be Running, got %s", snap[4].State)
	}
	// When its job ends it drains away, and the intermediate empty slots
	// (2,3) compact too since they are idle-draining.
	m.Free(4, "adopted")
	if got := len(m.Snapshot()); got != 2 {
		t.Fatalf("want table back at target 2, got %d", got)
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
}

func TestLiveAndBusyCounts(t *testing.T) {
	m := NewManager(3, 0)
	i0 := mustAcquire(t, m, name(0))
	m.MutateIndex(i0, func(s *Slot) { s.State = Ready })
	i1 := mustAcquire(t, m, name(1))
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

func name(i int) string {
	return string(rune('a' + i))
}
