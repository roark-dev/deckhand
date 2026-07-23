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

func TestRingBounded(t *testing.T) {
	b := New()
	for range 500 {
		b.Publish(Info, -1, "x")
	}
	if got := len(b.Recent()); got != ringSize {
		t.Fatalf("ring = %d, want %d", got, ringSize)
	}
}
