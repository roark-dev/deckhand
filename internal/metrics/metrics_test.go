package metrics

import (
	"strings"
	"testing"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/slots"
)

// TestRenderGolden pins the metric names, types and values — renaming a
// metric silently breaks every dashboard and alert scraping it.
func TestRenderGolden(t *testing.T) {
	st := broker.Status{
		Broker: broker.Info{State: broker.Active, Target: 4},
		Docker: broker.DockerStatus{OK: true},
		Slots: []slots.Slot{
			{Index: 0, State: slots.Running},
			{Index: 1, State: slots.Ready},
			{Index: 2, State: slots.Idle},
		},
		Counters: broker.CounterValues{
			Completed:        10,
			Failed:           2,
			ZombiesReclaimed: 1,
			SpawnErrors:      3,
			Reconnects:       4,
		},
	}
	out := render(st)
	want := []string{
		"deckhand_up 1",
		"deckhand_docker_up 1",
		"deckhand_slots_target 4",
		"deckhand_slots_busy 1",
		"deckhand_slots_ready 1",
		"deckhand_jobs_completed_total 10",
		"deckhand_jobs_failed_total 2",
		"deckhand_zombies_reclaimed_total 1",
		"deckhand_spawn_errors_total 3",
		"deckhand_session_reconnects_total 4",
	}
	for _, w := range want {
		if !strings.Contains(out, w+"\n") {
			t.Errorf("missing metric line %q in output:\n%s", w, out)
		}
	}
}

func TestRenderDegradedIsDown(t *testing.T) {
	st := broker.Status{Broker: broker.Info{State: broker.Degraded}}
	out := render(st)
	if !strings.Contains(out, "deckhand_up 0\n") {
		t.Fatalf("degraded broker must export deckhand_up 0:\n%s", out)
	}
}
