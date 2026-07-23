package tui

import (
	"strings"
	"testing"
	"time"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/slots"
)

func testStatus() *broker.Status {
	return &broker.Status{
		Broker: broker.Info{
			State:        broker.Active,
			ScaleSetName: "deckhand",
			GitHubURL:    "https://github.com/me/repo",
			Target:       2,
		},
		Docker: broker.DockerStatus{OK: true},
		Slots: []slots.Slot{
			{Index: 0, State: slots.Running, Since: time.Now(), Job: &slots.Job{
				DisplayName: "test-shard-1", Repo: "me/repo", StartedAt: time.Now().Add(-90 * time.Second),
			}},
			{Index: 1, State: slots.Errored, Err: "spawn failed: boom", Since: time.Now()},
		},
		Counters: broker.CounterValues{Completed: 5, Failed: 1},
	}
}

func TestViewRendersSlotsAndCounters(t *testing.T) {
	m := model{status: testStatus(), width: 100}
	out := m.View()
	for _, want := range []string{
		"deckhand", "test-shard-1", "me/repo",
		"busy", "error", "spawn failed: boom",
		"1/2 busy", "5", // completed counter
		"[+/-] slots",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("view missing %q\n%s", want, out)
		}
	}
}

func TestViewStates(t *testing.T) {
	st := testStatus()
	st.Broker.Paused = true
	st.Broker.Draining = true
	st.Docker.OK = false
	m := model{status: st, width: 100}
	out := m.View()
	for _, chip := range []string{"PAUSED", "DRAINING", "DOCKER DOWN"} {
		if !strings.Contains(out, chip) {
			t.Errorf("view missing chip %q", chip)
		}
	}
}

func TestViewNoDaemon(t *testing.T) {
	m := model{err: errFake("connect: no such file")}
	out := m.View()
	if !strings.Contains(out, "cannot reach daemon") || !strings.Contains(out, "deckhand up") {
		t.Fatalf("disconnected view unhelpful:\n%s", out)
	}
	if out := (model{}).View(); !strings.Contains(out, "connecting") {
		t.Fatalf("initial view: %s", out)
	}
}

func TestConfirmPrompt(t *testing.T) {
	m := model{status: testStatus(), confirm: "stop", width: 80}
	if out := m.View(); !strings.Contains(out, "stop the daemon") {
		t.Fatal("confirm prompt not shown")
	}
}

func TestRenderSlotAllStates(t *testing.T) {
	cases := map[slots.State]string{
		slots.Idle:     "idle",
		slots.Starting: "starting",
		slots.Ready:    "ready",
		slots.Running:  "busy",
		slots.Reaping:  "reaping",
		slots.Errored:  "error",
		slots.Draining: "draining",
	}
	for state, want := range cases {
		got, _ := renderSlot(slots.Slot{State: state})
		if !strings.Contains(got, want) {
			t.Errorf("state %s renders %q, want to contain %q", state, got, want)
		}
	}
}

func TestTruncateRuneSafeAndSanitizing(t *testing.T) {
	if got := truncate("日本語のジョブ名まだまだ続く", 8); len([]rune(got)) > 8 {
		t.Fatalf("truncate not rune-bounded: %q", got)
	}
	if got := truncate("evil\x1b[2Jjob", 50); strings.ContainsRune(got, 0x1b) {
		t.Fatalf("truncate must sanitize: %q", got)
	}
	if got := truncate("short", 10); got != "short" {
		t.Fatalf("no-op truncate changed string: %q", got)
	}
}

func TestAgeOrDash(t *testing.T) {
	if ageOrDash(0) != "—" || ageOrDash(-1) != "—" {
		t.Fatal("zero/negative session age must render as dash")
	}
	if ageOrDash(90) != "1m30s" {
		t.Fatalf("ageOrDash(90) = %q", ageOrDash(90))
	}
}

type errFake string

func (e errFake) Error() string { return string(e) }
