// Package bus is deckhand's in-process event feed: a bounded ring of recent
// events plus fan-out to live subscribers (the TUI's log pane and the control
// API's ndjson stream).
package bus

import (
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
	ev := Event{Time: time.Now(), Level: level, Msg: msg, Slot: slot}
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
