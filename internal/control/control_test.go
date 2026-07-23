package control

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/config"
)

// offlineBroker builds a real broker that never dials docker or GitHub (both
// clients construct lazily), so every endpoint that touches only slot/bus
// state is exercisable.
func offlineBroker(t *testing.T) (*broker.Broker, *bus.Bus) {
	t.Helper()
	cfg := &config.Config{}
	cfg.GitHub.URL = "https://github.com/me/repo"
	cfg.GitHub.Auth.Token = "tok"
	cfg.ScaleSet.Name = "deckhand"
	cfg.Slots.Count = 2
	cfg.Runner.Image = "img@sha256:abc"
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	paths := config.Paths{Home: dir, StateFile: filepath.Join(dir, "state.json")}
	eventBus := bus.New()
	b, err := broker.New(cfg, paths, slog.New(slog.DiscardHandler), eventBus, false)
	if err != nil {
		t.Fatal(err)
	}
	return b, eventBus
}

func testServer(t *testing.T) (*httptest.Server, *broker.Broker, *bus.Bus, *string) {
	t.Helper()
	b, eventBus := offlineBroker(t)
	var shutdownCause string
	s := NewServer(b, eventBus, "test", func(cause string) { shutdownCause = cause })
	srv := httptest.NewServer(s.mux())
	t.Cleanup(srv.Close)
	return srv, b, eventBus, &shutdownCause
}

func post(t *testing.T, srv *httptest.Server, path, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(srv.URL+path, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { resp.Body.Close() })
	return resp
}

func TestStatusShape(t *testing.T) {
	srv, _, _, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/v1/status")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var st broker.Status
	if err := jsonDecode(resp, &st); err != nil {
		t.Fatal(err)
	}
	if st.Broker.ScaleSetName != "deckhand" || st.Broker.Target != 2 || len(st.Slots) != 2 {
		t.Fatalf("unexpected status: %+v", st.Broker)
	}
}

func TestScaleValidationAndEffect(t *testing.T) {
	srv, b, _, _ := testServer(t)
	if resp := post(t, srv, "/v1/scale", `{"n":65}`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("scale 65 should 400, got %d", resp.StatusCode)
	}
	if resp := post(t, srv, "/v1/scale", `not json`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad json should 400, got %d", resp.StatusCode)
	}
	if resp := post(t, srv, "/v1/scale", `{"n":5}`); resp.StatusCode != 200 {
		t.Fatalf("scale 5 should 200, got %d", resp.StatusCode)
	}
	if b.Status().Broker.Target != 5 {
		t.Fatal("scale did not take effect")
	}
}

func TestPauseResumeDrainRoundTrip(t *testing.T) {
	srv, b, _, _ := testServer(t)
	post(t, srv, "/v1/pause", "")
	if !b.Status().Broker.Paused {
		t.Fatal("pause did not take effect")
	}
	post(t, srv, "/v1/drain", "")
	if !b.Status().Broker.Draining {
		t.Fatal("drain did not take effect")
	}
	post(t, srv, "/v1/resume", "")
	st := b.Status().Broker
	if st.Paused || st.Draining {
		t.Fatal("resume did not clear pause+drain")
	}
}

func TestStopModeValidation(t *testing.T) {
	srv, _, _, cause := testServer(t)
	if resp := post(t, srv, "/v1/stop", `{"mode":"nuke"}`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown stop mode should 400, got %d", resp.StatusCode)
	}
	if *cause != "" {
		t.Fatal("rejected stop must not shut down")
	}
	// now-mode with an idle fleet succeeds.
	if resp := post(t, srv, "/v1/stop", `{"mode":"now"}`); resp.StatusCode != 200 {
		t.Fatalf("stop now on idle fleet should 200, got %d", resp.StatusCode)
	}
	if *cause == "" {
		t.Fatal("stop now should have shut down")
	}
}

func TestReclaimErrorMapping(t *testing.T) {
	srv, _, _, _ := testServer(t)
	if resp := post(t, srv, "/v1/slots/9/reclaim", `{}`); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("reclaim of empty slot should 404, got %d", resp.StatusCode)
	}
	if resp := post(t, srv, "/v1/slots/x/reclaim", `{}`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("non-numeric slot should 400, got %d", resp.StatusCode)
	}
}

func TestSlotLogsNotFound(t *testing.T) {
	srv, _, _, _ := testServer(t)
	resp, err := http.Get(srv.URL + "/v1/slots/0/logs")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("logs for containerless slot should 404, got %d", resp.StatusCode)
	}
}

func TestOversizeBodyRejected(t *testing.T) {
	srv, _, _, _ := testServer(t)
	big := strings.Repeat("x", maxBody+10)
	if resp := post(t, srv, "/v1/scale", big); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversize body should 400, got %d", resp.StatusCode)
	}
}

// TestUnixSocketRoundTrip runs the real Serve (peer-credential check
// included) and drives it through the Client — the exact production path.
func TestUnixSocketRoundTrip(t *testing.T) {
	b, eventBus := offlineBroker(t)
	s := NewServer(b, eventBus, "test", func(string) {})
	dir, err := os.MkdirTemp("", "dh")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "d.sock")
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = s.Serve(ctx, sock) }()
	waitForSocket(t, sock)

	// Socket must be owner-only from creation (umask-guarded).
	info, err := os.Stat(sock)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm&0o077 != 0 {
		t.Fatalf("socket permissions too loose: %o", perm)
	}

	c := NewClient(sock)
	st, err := c.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.Broker.ScaleSetName != "deckhand" {
		t.Fatalf("round-trip status wrong: %+v", st.Broker)
	}
	if err := c.Scale(context.Background(), 3); err != nil {
		t.Fatal(err)
	}

	// Event stream: backlog then live.
	eventBus.Publish(bus.Info, -1, "backlog-event")
	evCtx, evCancel := context.WithCancel(context.Background())
	defer evCancel()
	ch, err := c.Events(evCtx)
	if err != nil {
		t.Fatal(err)
	}
	if ev := recvEvent(t, ch); !strings.Contains(ev.Msg, "backlog-event") && !strings.Contains(ev.Msg, "slot target") {
		t.Fatalf("expected backlog first, got %q", ev.Msg)
	}
	eventBus.Publish(bus.Warn, 1, "live-event")
	waitFor(t, "live event", func() bool {
		for {
			select {
			case ev, ok := <-ch:
				if !ok {
					return false
				}
				if ev.Msg == "live-event" {
					return true
				}
			default:
				return false
			}
		}
	})
}

func TestClientDaemonNotRunningError(t *testing.T) {
	// Short path: unix socket paths are capped (~104 bytes on darwin) and
	// t.TempDir() can exceed it, turning ENOENT into EINVAL.
	dir, err0 := os.MkdirTemp("", "dh")
	if err0 != nil {
		t.Fatal(err0)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	c := NewClient(filepath.Join(dir, "absent.sock"))
	_, err := c.Status(context.Background())
	if err == nil || !strings.Contains(err.Error(), "daemon not running") {
		t.Fatalf("want daemon-not-running guidance, got %v", err)
	}
}

// ---- helpers ---------------------------------------------------------------

func jsonDecode(resp *http.Response, out any) error {
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}

func waitForSocket(t *testing.T, path string) {
	t.Helper()
	waitFor(t, "socket "+path, func() bool {
		_, err := os.Stat(path)
		return err == nil
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func recvEvent(t *testing.T, ch <-chan bus.Event) bus.Event {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("event channel closed early")
		}
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("no event received")
	}
	return bus.Event{}
}
