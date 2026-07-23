// Package slots owns deckhand's slot model. A slot is a stable local identity
// (index, optional cpuset, display row); runner containers are per-job and
// disposable — the slot outlives them. The manager is pure state: no docker or
// GitHub calls happen here, which keeps scale/drain semantics unit-testable.
package slots

import (
	"fmt"
	"sync"
	"time"
)

type State string

const (
	// Idle: no container; the slot is free capacity.
	Idle State = "idle"
	// Starting: a runner container is being created for this slot.
	Starting State = "starting"
	// Ready: runner is up and registered, waiting for GitHub to assign a job.
	Ready State = "ready"
	// Running: a job is executing.
	Running State = "running"
	// Reaping: job finished (or container exited); cleanup in progress.
	Reaping State = "reaping"
	// Errored: last spawn/reap attempt failed; retried with backoff.
	Errored State = "error"
	// Draining: slot is above the current target and disappears once idle.
	Draining State = "draining"
)

type Job struct {
	RequestID   int64     `json:"request_id"`
	Repo        string    `json:"repo"`
	Workflow    string    `json:"workflow"`
	DisplayName string    `json:"display_name"`
	RunURL      string    `json:"run_url"`
	StartedAt   time.Time `json:"started_at"`
}

type Slot struct {
	Index       int       `json:"index"`
	State       State     `json:"state"`
	Cpuset      string    `json:"cpuset,omitempty"`
	RunnerName  string    `json:"runner_name,omitempty"`
	ContainerID string    `json:"container_id,omitempty"`
	Job         *Job      `json:"job,omitempty"`
	Err         string    `json:"error,omitempty"`
	Since       time.Time `json:"since"`
	// draining marks a slot above target; it is removed once it returns to
	// idle instead of becoming free capacity again.
	draining bool
}

func (s *Slot) busy() bool {
	switch s.State {
	case Starting, Ready, Running, Reaping:
		return true
	}
	return false
}

// Manager holds the slot table and the target count.
type Manager struct {
	mu          sync.Mutex
	slots       []*Slot
	target      int
	cpusPerSlot int
}

func NewManager(target, cpusPerSlot int) *Manager {
	m := &Manager{cpusPerSlot: cpusPerSlot}
	m.SetTarget(target)
	return m
}

func (m *Manager) cpuset(index int) string {
	if m.cpusPerSlot <= 0 {
		return ""
	}
	lo := index * m.cpusPerSlot
	hi := lo + m.cpusPerSlot - 1
	if m.cpusPerSlot == 1 {
		return fmt.Sprintf("%d", lo)
	}
	return fmt.Sprintf("%d-%d", lo, hi)
}

// SetTarget grows or shrinks the slot table. Growing appends idle slots
// immediately (and un-drains any slot that was on its way out). Shrinking
// removes idle slots at the top immediately and marks busy ones draining —
// they finish their current job and then vanish; a mid-job runner is never
// killed.
func (m *Manager) SetTarget(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.target = n
	for i := len(m.slots); i < n; i++ {
		m.slots = append(m.slots, &Slot{Index: i, State: Idle, Cpuset: m.cpuset(i), Since: time.Now()})
	}
	for _, s := range m.slots {
		if s.Index < n && s.draining {
			s.draining = false
			if s.State == Draining {
				s.State = Idle
			}
		}
		if s.Index >= n {
			s.draining = true
			if s.State == Idle {
				s.State = Draining
			}
		}
	}
	m.compactLocked()
}

// compactLocked drops trailing drained slots that have reached idle.
func (m *Manager) compactLocked() {
	for len(m.slots) > m.target {
		last := m.slots[len(m.slots)-1]
		if last.busy() {
			break
		}
		m.slots = m.slots[:len(m.slots)-1]
	}
}

func (m *Manager) Target() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.target
}

// Acquire reserves the lowest idle, non-draining slot for a spawn and returns
// its index, cpuset and false if none is free.
func (m *Manager) Acquire(runnerName string) (index int, cpuset string, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.slots {
		if s.State == Idle && !s.draining {
			s.State = Starting
			s.RunnerName = runnerName
			s.Job = nil
			s.Err = ""
			s.Since = time.Now()
			return s.Index, s.Cpuset, true
		}
	}
	return 0, "", false
}

// Free returns a slot to idle (or removes it if draining). It is a no-op
// unless runnerName still owns the slot — this is what makes the two cleanup
// paths (JobCompleted message vs container-exit watcher) race-safe.
func (m *Manager) Free(index int, runnerName string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getLocked(index)
	if s == nil || s.RunnerName != runnerName {
		return false
	}
	s.RunnerName = ""
	s.ContainerID = ""
	s.Job = nil
	s.Err = ""
	s.Since = time.Now()
	if s.draining {
		s.State = Draining
	} else {
		s.State = Idle
	}
	m.compactLocked()
	return true
}

// Mutate runs fn against the slot currently owned by runnerName, if any.
func (m *Manager) Mutate(runnerName string, fn func(*Slot)) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range m.slots {
		if s.RunnerName == runnerName && s.RunnerName != "" {
			fn(s)
			return true
		}
	}
	return false
}

// MutateIndex runs fn against a slot by index.
func (m *Manager) MutateIndex(index int, fn func(*Slot)) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	s := m.getLocked(index)
	if s == nil {
		return false
	}
	fn(s)
	return true
}

// AdoptAt places an adopted (pre-existing) container into a specific slot,
// growing the table if the daemon restarted with a smaller target; slots
// beyond the target drain away once their job finishes.
func (m *Manager) AdoptAt(index int, runnerName, containerID string, running bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for len(m.slots) <= index {
		i := len(m.slots)
		s := &Slot{Index: i, State: Idle, Cpuset: m.cpuset(i), Since: time.Now()}
		if i >= m.target {
			s.draining = true
			s.State = Draining
		}
		m.slots = append(m.slots, s)
	}
	s := m.slots[index]
	s.RunnerName = runnerName
	s.ContainerID = containerID
	s.Since = time.Now()
	if running {
		s.State = Running
	} else {
		s.State = Ready
	}
}

func (m *Manager) getLocked(index int) *Slot {
	if index < 0 || index >= len(m.slots) {
		return nil
	}
	return m.slots[index]
}

// Live counts runners that exist or are being created (Starting/Ready/Running).
func (m *Manager) Live() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.slots {
		switch s.State {
		case Starting, Ready, Running:
			n++
		}
	}
	return n
}

// FreeCount is how many more runners could be spawned right now.
func (m *Manager) FreeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.slots {
		if s.State == Idle && !s.draining {
			n++
		}
	}
	return n
}

// BusyCount counts slots with a job actually running.
func (m *Manager) BusyCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, s := range m.slots {
		if s.State == Running {
			n++
		}
	}
	return n
}

// IdleRunners returns slots whose runner is up but jobless (Ready), oldest
// first — the cull order for scale-down of warm capacity.
func (m *Manager) IdleRunners() []Slot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Slot
	for _, s := range m.slots {
		if s.State == Ready {
			out = append(out, *s)
		}
	}
	return out
}

// Snapshot returns a copy of the slot table for display/serialization.
func (m *Manager) Snapshot() []Slot {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]Slot, 0, len(m.slots))
	for _, s := range m.slots {
		out = append(out, *s)
	}
	return out
}
