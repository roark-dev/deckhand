// Package bus is deckhand's in-process event feed: a bounded ring of recent
// events plus fan-out to live subscribers (the TUI's log pane and the control
// API's ndjson stream).
package bus

import (
	"strings"
	"sync"
	"time"
)

type Level string

const (
	Info   Level = "info"
	Warn   Level = "warn"
	Error  Level = "error"
	Action Level = "action"
)

type Event struct {
	Time  time.Time `json:"time"`
	Level Level     `json:"level"`
	Msg   string    `json:"msg"`
	// Slot is the slot index the event concerns, or -1 for broker-wide events.
	Slot int `json:"slot"`
}

// Sanitize strips control characters — including the ESC/CSI/OSC introducers
// used for terminal escape injection — from untrusted text before it reaches
// a terminal, a log file, or the event stream. Workflow-controlled strings
// (job names, repo names, container logs) MUST pass through this before
// display.
func Sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || (r >= 0x80 && r <= 0x9f) {
			return -1
		}
		return r
	}, s)
}

const ringSize = 200

type Bus struct {
	mu   sync.Mutex
	ring []Event
	subs map[chan Event]struct{}
}

func New() *Bus {
	return &Bus{subs: make(map[chan Event]struct{})}
}

func (b *Bus) Publish(level Level, slot int, msg string) {
	// Defense in depth: producers sanitize workflow-controlled fields at the
	// source, but nothing control-character-laden may enter the feed at all.
	ev := Event{Time: time.Now(), Level: level, Msg: Sanitize(msg), Slot: slot}
	b.mu.Lock()
	b.ring = append(b.ring, ev)
	if len(b.ring) > ringSize {
		b.ring = b.ring[len(b.ring)-ringSize:]
	}
	for ch := range b.subs {
		select {
		case ch <- ev:
		default: // a stalled subscriber never blocks the broker
		}
	}
	b.mu.Unlock()
}

// Subscribe returns a channel of future events plus the recent backlog.
// Call the returned cancel func to unsubscribe.
func (b *Bus) Subscribe() (<-chan Event, []Event, func()) {
	ch := make(chan Event, 64)
	b.mu.Lock()
	backlog := make([]Event, len(b.ring))
	copy(backlog, b.ring)
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	cancel := func() {
		b.mu.Lock()
		delete(b.subs, ch)
		b.mu.Unlock()
	}
	return ch, backlog, cancel
}

// Recent returns a copy of the buffered events.
func (b *Bus) Recent() []Event {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Event, len(b.ring))
	copy(out, b.ring)
	return out
}
