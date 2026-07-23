// Package metrics serves a minimal Prometheus text-format endpoint derived
// from broker status — enough for a scrape job and alert rules, without a
// client-library dependency.
package metrics

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/slots"
)

// Serve exposes /metrics on addr until ctx is done.
func Serve(ctx context.Context, addr string, b *broker.Broker) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		_, _ = w.Write([]byte(render(b.Status())))
	})
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
	}()
	err := srv.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func render(st broker.Status) string {
	var sb strings.Builder
	gauge := func(name, help string, v int64) {
		fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s gauge\n%s %d\n", name, help, name, name, v)
	}
	counter := func(name, help string, v int64) {
		fmt.Fprintf(&sb, "# HELP %s %s\n# TYPE %s counter\n%s %d\n", name, help, name, name, v)
	}
	busy, ready := 0, 0
	for _, s := range st.Slots {
		switch s.State {
		case slots.Running:
			busy++
		case slots.Ready:
			ready++
		}
	}
	up := int64(0)
	if st.Broker.State == broker.Active {
		up = 1
	}
	dockerUp := int64(0)
	if st.Docker.OK {
		dockerUp = 1
	}
	gauge("deckhand_up", "1 when the broker session with GitHub is active", up)
	gauge("deckhand_docker_up", "1 when the docker daemon is reachable", dockerUp)
	gauge("deckhand_slots_target", "configured slot target", int64(st.Broker.Target))
	gauge("deckhand_slots_busy", "slots with a running job", int64(busy))
	gauge("deckhand_slots_ready", "idle registered runners waiting for a job", int64(ready))
	counter("deckhand_jobs_completed_total", "jobs that reported success", st.Counters.Completed)
	counter("deckhand_jobs_failed_total", "jobs that failed, were cancelled, or vanished", st.Counters.Failed)
	counter("deckhand_zombies_reclaimed_total", "runner containers reclaimed after losing their GitHub registration", st.Counters.ZombiesReclaimed)
	counter("deckhand_spawn_errors_total", "runner container spawn failures", st.Counters.SpawnErrors)
	counter("deckhand_session_reconnects_total", "GitHub message-session reconnects", st.Counters.Reconnects)
	return sb.String()
}
