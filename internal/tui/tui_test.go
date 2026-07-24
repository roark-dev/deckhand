package tui

import (
	"regexp"
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

func TestViewRendersSlots(t *testing.T) {
	m := model{status: testStatus(), width: 100}
	out := m.View()
	for _, want := range []string{
		"deckhand", "test-shard-1", "me/repo",
		"busy", "error", "spawn failed: boom",
		"serving me/repo", "runs-on: deckhand", // header in workflow-author terms
		"[+/-] slots",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("view missing %q\n%s", want, out)
		}
	}
	// The counters bar was deliberately removed (operator feedback): the
	// table + events pane are the dashboard.
	if strings.Contains(out, "zombies reclaimed") || strings.Contains(out, "busy   jobs") {
		t.Error("counters bar should not be rendered")
	}
}

func TestViewRendersResourceUsage(t *testing.T) {
	st := testStatus()
	st.Resources = broker.ResourceUsage{
		OK: true, CPUCoresUsed: 2.3, CPUCores: 8,
		MemUsedBytes: 1717986918 /*~1.6G*/, MemTotalBytes: 16 * 1024 * 1024 * 1024,
	}
	out := (model{status: st, width: 100}).View()
	for _, want := range []string{"usage:", "2.3 / 8 CPU cores", "1.6G", "16.0G"} {
		if !strings.Contains(out, want) {
			t.Errorf("resource line missing %q\n%s", want, out)
		}
	}
	// No sample yet -> the line is omitted, not rendered with zeros/dashes.
	st.Resources = broker.ResourceUsage{}
	if out := (model{status: st, width: 100}).View(); strings.Contains(out, "usage:") {
		t.Errorf("resource line should be hidden before first sample\n%s", out)
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{
		512:                     "512B",
		1024:                    "1.0K",
		1610612736:              "1.5G",
		16 * 1024 * 1024 * 1024: "16.0G",
		512 * 1024 * 1024:       "512.0M",
	}
	for n, want := range cases {
		if got := humanBytes(n); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

// Styling must never shift columns: cells are padded before ANSI styling, so
// a styled state cell occupies exactly the same display width as a plain one.
func TestTableColumnsAlignAcrossStyledStates(t *testing.T) {
	m := model{status: testStatus(), width: 120}
	out := m.View()
	var elapsedCols []int
	for _, line := range strings.Split(out, "\n") {
		plain := stripANSI(line)
		if !strings.HasPrefix(strings.TrimSpace(plain), "0") && !strings.HasPrefix(strings.TrimSpace(plain), "1") {
			continue
		}
		// Row shape: "  <slot> <state-padded-11> <elapsed> ..."; the elapsed
		// column starts at a fixed offset when padding is display-width.
		if idx := strings.Index(plain, "s "); idx > 0 { // crude: elapsed like "45s"/"1m15s"
			elapsedCols = append(elapsedCols, elapsedStart(plain))
		}
	}
	if len(elapsedCols) < 2 {
		t.Fatalf("expected at least 2 slot rows, got %d\n%s", len(elapsedCols), out)
	}
	for _, c := range elapsedCols[1:] {
		if c != elapsedCols[0] {
			t.Fatalf("elapsed column drifts across styled rows: %v\n%s", elapsedCols, out)
		}
	}
}

// elapsedStart finds the column where the third field begins on a plain row.
// The REPO column has its own header and lines up across rows whose job names
// differ in length (operator feedback: it used to be header-less and ragged).
func TestRepoColumnHasHeaderAndAligns(t *testing.T) {
	st := testStatus()
	st.Slots = []slots.Slot{
		{Index: 0, State: slots.Running, Since: time.Now(), Job: &slots.Job{
			DisplayName: "static-checks", Repo: "me/repo", StartedAt: time.Now()}},
		{Index: 1, State: slots.Running, Since: time.Now(), Job: &slots.Job{
			DisplayName: "test (3)", Repo: "me/repo", StartedAt: time.Now()}},
		{Index: 2, State: slots.Ready, Since: time.Now()},
	}
	out := (model{status: st, width: 120}).View()

	var headerRepoCol int = -1
	var dataRepoCols []int
	for _, line := range strings.Split(out, "\n") {
		plain := stripANSI(line)
		switch {
		case strings.Contains(plain, "JOB") && strings.Contains(plain, "REPO"):
			headerRepoCol = strings.Index(plain, "REPO")
		case strings.Contains(plain, "busy") && strings.Contains(plain, "me/repo"):
			dataRepoCols = append(dataRepoCols, strings.Index(plain, "me/repo"))
		}
	}
	if headerRepoCol < 0 {
		t.Fatalf("no REPO header found\n%s", out)
	}
	if len(dataRepoCols) != 2 {
		t.Fatalf("expected 2 repo rows, got %d\n%s", len(dataRepoCols), out)
	}
	for _, c := range dataRepoCols {
		if c != dataRepoCols[0] {
			t.Fatalf("repo column drifts across rows: %v\n%s", dataRepoCols, out)
		}
	}
	if dataRepoCols[0] != headerRepoCol {
		t.Fatalf("REPO header col %d != data repo col %d\n%s", headerRepoCol, dataRepoCols[0], out)
	}
}

func elapsedStart(plain string) int {
	fields := 0
	inField := false
	for i, r := range plain {
		if r != ' ' && !inField {
			fields++
			inField = true
			if fields == 3 {
				return i
			}
		} else if r == ' ' {
			inField = false
		}
		_ = i
	}
	return -1
}

var ansiRe = regexp.MustCompile("\x1b\\[[0-9;]*m")

func stripANSI(s string) string { return ansiRe.ReplaceAllString(s, "") }

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
		got, _, _ := slotCell(slots.Slot{State: state})
		if !strings.Contains(got, want) {
			t.Errorf("state %s renders %q, want to contain %q", state, got, want)
		}
	}
	// Drain-marked slots carry a visible marker so a ready-but-leaving
	// runner is distinguishable from a plain ready one.
	if got, _, _ := slotCell(slots.Slot{State: slots.Ready, Drain: true}); got != "ready*" {
		t.Errorf("drain-marked ready slot renders %q, want ready*", got)
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
