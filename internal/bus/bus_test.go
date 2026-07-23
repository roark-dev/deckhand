package bus

import (
	"testing"
	"time"
)

func TestSubscribeGetsBacklogAndLive(t *testing.T) {
	b := New()
	b.Publish(Info, -1, "before")
	ch, backlog, cancel := b.Subscribe()
	defer cancel()
	if len(backlog) != 1 || backlog[0].Msg != "before" {
		t.Fatalf("backlog = %+v", backlog)
	}
	b.Publish(Warn, 2, "after")
	select {
	case ev := <-ch:
		if ev.Msg != "after" || ev.Slot != 2 {
			t.Fatalf("got %+v", ev)
		}
	case <-time.After(time.Second):
		t.Fatal("no live event")
	}
}

func TestStalledSubscriberNeverBlocks(t *testing.T) {
	b := New()
	_, _, cancel := b.Subscribe() // never read
	defer cancel()
	done := make(chan struct{})
	go func() {
		for range 200 {
			b.Publish(Info, -1, "spam")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("publish blocked on a stalled subscriber")
	}
}

func TestRingKeepsNewest(t *testing.T) {
	b := New()
	for i := range 500 {
		b.Publish(Info, i, "x") // slot doubles as a sequence number
	}
	recent := b.Recent()
	if got := len(recent); got != ringSize {
		t.Fatalf("ring = %d, want %d", got, ringSize)
	}
	if recent[0].Slot != 500-ringSize || recent[len(recent)-1].Slot != 499 {
		t.Fatalf("ring must keep the NEWEST %d events, got [%d..%d]", ringSize, recent[0].Slot, recent[len(recent)-1].Slot)
	}
}

func TestCancelThenPublishSafe(t *testing.T) {
	b := New()
	ch, _, cancel := b.Subscribe()
	cancel()
	done := make(chan struct{})
	go func() {
		for range 50 {
			b.Publish(Info, -1, "after-cancel")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publish after cancel blocked")
	}
	select {
	case ev, ok := <-ch:
		if ok {
			t.Fatalf("cancelled subscriber received %+v", ev)
		}
	default:
		// channel open but empty — the documented contract (never closed)
	}
}

func TestSanitizeStripsTerminalEscapes(t *testing.T) {
	cases := map[string]string{
		"plain job name": "plain job name",
		// Every control byte (C0, DEL, C1 — including the ESC/CSI/OSC
		// introducers) is removed; printable remnants of a would-be sequence
		// ("[2J") are harmless text once the introducer is gone.
		"esc\x1b[2Jwipe":               "esc[2Jwipe",
		"osc\x1b]0;title\x07done":      "osc]0;titledone",
		"c1-csi\u009bhidden":           "c1-csihidden",
		"tab\tand\nnewline":            "tabandnewline",
		"del\x7fchar":                  "delchar",
		"unicode ✓ stays — 日本語 intact": "unicode ✓ stays — 日本語 intact",
	}
	for in, want := range cases {
		if got := Sanitize(in); got != want {
			t.Errorf("Sanitize(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPublishSanitizes(t *testing.T) {
	b := New()
	b.Publish(Info, -1, "evil\x1b[31mred")
	if got := b.Recent()[0].Msg; got != "evil[31mred" {
		t.Fatalf("publish must sanitize, got %q", got)
	}
}
