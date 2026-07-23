// Package control exposes the daemon over a unix socket (HTTP+JSON) and
// provides the matching client used by the CLI and TUI.
package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/bus"
)

// maxBody bounds control-request bodies; every legitimate payload is tiny.
const maxBody = 1 << 20

// maxEventStreams bounds concurrent /v1/events subscribers.
const maxEventStreams = 32

type Server struct {
	broker       *broker.Broker
	bus          *bus.Bus
	shutdown     func(cause string)
	version      string
	eventStreams atomic.Int64
}

func NewServer(b *broker.Broker, eventBus *bus.Bus, version string, shutdown func(cause string)) *Server {
	return &Server{broker: b, bus: eventBus, shutdown: shutdown, version: version}
}

// Serve listens on the unix socket until ctx is done. The socket is created
// with owner-only permissions from the first instant (umask), and every
// accepted connection is verified to come from this uid (peer credentials) —
// filesystem modes alone are not the only line of defense.
func (s *Server) Serve(ctx context.Context, socketPath string) error {
	_ = os.Remove(socketPath)
	old := unix.Umask(0o177)
	ln, err := net.Listen("unix", socketPath)
	unix.Umask(old)
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	srv := &http.Server{
		Handler:           s.mux(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutCtx)
		_ = os.Remove(socketPath)
	}()
	err = srv.Serve(&peerCheckedListener{Listener: ln, uid: os.Getuid()})
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// peerCheckedListener rejects connections whose peer uid is neither ours nor
// root's.
type peerCheckedListener struct {
	net.Listener
	uid int
}

func (l *peerCheckedListener) Accept() (net.Conn, error) {
	for {
		conn, err := l.Listener.Accept()
		if err != nil {
			return nil, err
		}
		uc, ok := conn.(*net.UnixConn)
		if !ok {
			conn.Close()
			continue
		}
		peer, err := peerUID(uc)
		if err != nil || (peer != l.uid && peer != 0) {
			conn.Close()
			continue
		}
		return conn, nil
	}
}

func (s *Server) mux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("GET /v1/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.broker.Status())
	})
	m.HandleFunc("GET /v1/version", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, map[string]string{"version": s.version})
	})
	m.HandleFunc("GET /v1/events", func(w http.ResponseWriter, r *http.Request) {
		s.serveEvents(w, r)
	})
	m.HandleFunc("POST /v1/scale", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			N int `json:"n"`
		}
		if err := decodeBody(w, r, &req); err != nil {
			httpErr(w, http.StatusBadRequest, err)
			return
		}
		if err := s.broker.Scale(req.N); err != nil {
			httpErr(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, s.broker.Status())
	})
	m.HandleFunc("POST /v1/pause", func(w http.ResponseWriter, r *http.Request) {
		s.broker.Pause()
		writeJSON(w, s.broker.Status())
	})
	m.HandleFunc("POST /v1/resume", func(w http.ResponseWriter, r *http.Request) {
		s.broker.Resume()
		writeJSON(w, s.broker.Status())
	})
	m.HandleFunc("POST /v1/drain", func(w http.ResponseWriter, r *http.Request) {
		s.broker.Drain()
		writeJSON(w, s.broker.Status())
	})
	m.HandleFunc("POST /v1/stop", func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			Mode  string `json:"mode"`
			Force bool   `json:"force"`
		}
		if err := decodeBody(w, r, &req); err != nil {
			httpErr(w, http.StatusBadRequest, err)
			return
		}
		mode, err := broker.ParseStopMode(req.Mode)
		if err != nil {
			httpErr(w, http.StatusBadRequest, err)
			return
		}
		if err := s.broker.Stop(r.Context(), mode, req.Force, s.shutdown); err != nil {
			httpErr(w, statusFor(err), err)
			return
		}
		writeJSON(w, map[string]string{"stopping": string(mode)})
	})
	m.HandleFunc("POST /v1/slots/{id}/reclaim", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			httpErr(w, http.StatusBadRequest, err)
			return
		}
		var req struct {
			Force bool `json:"force"`
		}
		if err := decodeBody(w, r, &req); err != nil {
			httpErr(w, http.StatusBadRequest, err)
			return
		}
		if err := s.broker.Reclaim(r.Context(), id, req.Force); err != nil {
			httpErr(w, statusFor(err), err)
			return
		}
		writeJSON(w, s.broker.Status())
	})
	m.HandleFunc("GET /v1/slots/{id}/logs", func(w http.ResponseWriter, r *http.Request) {
		id, err := strconv.Atoi(r.PathValue("id"))
		if err != nil {
			httpErr(w, http.StatusBadRequest, err)
			return
		}
		tail := 100
		if t := r.URL.Query().Get("tail"); t != "" {
			if tail, err = strconv.Atoi(t); err != nil {
				httpErr(w, http.StatusBadRequest, err)
				return
			}
		}
		logs, err := s.broker.SlotLogs(r.Context(), id, tail)
		if err != nil {
			httpErr(w, statusFor(err), err)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = io.WriteString(w, logs)
	})
	return m
}

// decodeBody parses a size-bounded JSON body; an empty body decodes to the
// zero value.
func decodeBody(w http.ResponseWriter, r *http.Request, out any) error {
	err := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBody)).Decode(out)
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	return nil
}

func statusFor(err error) int {
	switch {
	case errors.Is(err, broker.ErrBusy):
		return http.StatusConflict
	case errors.Is(err, broker.ErrNotFound):
		return http.StatusNotFound
	default:
		return http.StatusInternalServerError
	}
}

// serveEvents streams the event feed as ndjson (backlog first, then live).
func (s *Server) serveEvents(w http.ResponseWriter, r *http.Request) {
	if s.eventStreams.Add(1) > maxEventStreams {
		s.eventStreams.Add(-1)
		httpErr(w, http.StatusTooManyRequests, errors.New("too many event streams"))
		return
	}
	defer s.eventStreams.Add(-1)
	fl, ok := w.(http.Flusher)
	if !ok {
		httpErr(w, http.StatusInternalServerError, errors.New("streaming unsupported"))
		return
	}
	w.Header().Set("Content-Type", "application/x-ndjson")
	ch, backlog, cancel := s.bus.Subscribe()
	defer cancel()
	enc := json.NewEncoder(w)
	for _, ev := range backlog {
		_ = enc.Encode(ev)
	}
	fl.Flush()
	for {
		select {
		case <-r.Context().Done():
			return
		case ev := <-ch:
			if err := enc.Encode(ev); err != nil {
				return
			}
			fl.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func httpErr(w http.ResponseWriter, code int, err error) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
}

// Client talks to a running daemon over the unix socket.
type Client struct {
	http *http.Client
}

func NewClient(socketPath string) *Client {
	return &Client{http: &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", socketPath)
			},
		},
	}}
}

func (c *Client) do(ctx context.Context, method, path string, body io.Reader, out any) error {
	req, err := http.NewRequestWithContext(ctx, method, "http://deckhand"+path, body)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// errors.Is unwraps through *url.Error and *net.OpError.
		if errors.Is(err, fs.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return fmt.Errorf("daemon not running (start it with `deckhand up`): %w", err)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		var e struct {
			Error string `json:"error"`
		}
		raw, _ := io.ReadAll(resp.Body)
		if json.Unmarshal(raw, &e) == nil && e.Error != "" {
			return errors.New(e.Error)
		}
		return fmt.Errorf("%s: %s", resp.Status, string(raw))
	}
	if out == nil {
		return nil
	}
	if s, ok := out.(*string); ok {
		raw, err := io.ReadAll(resp.Body)
		*s = string(raw)
		return err
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) Status(ctx context.Context) (*broker.Status, error) {
	var st broker.Status
	if err := c.do(ctx, http.MethodGet, "/v1/status", nil, &st); err != nil {
		return nil, err
	}
	return &st, nil
}

func (c *Client) Scale(ctx context.Context, n int) error {
	return c.do(ctx, http.MethodPost, "/v1/scale", strings.NewReader(fmt.Sprintf(`{"n":%d}`, n)), nil)
}

func (c *Client) Pause(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/pause", nil, nil)
}

func (c *Client) Resume(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/resume", nil, nil)
}

func (c *Client) Drain(ctx context.Context) error {
	return c.do(ctx, http.MethodPost, "/v1/drain", nil, nil)
}

func (c *Client) Stop(ctx context.Context, mode string, force bool) error {
	body := fmt.Sprintf(`{"mode":%q,"force":%v}`, mode, force)
	return c.do(ctx, http.MethodPost, "/v1/stop", strings.NewReader(body), nil)
}

func (c *Client) Reclaim(ctx context.Context, slot int, force bool) error {
	return c.do(ctx, http.MethodPost, fmt.Sprintf("/v1/slots/%d/reclaim", slot), strings.NewReader(fmt.Sprintf(`{"force":%v}`, force)), nil)
}

func (c *Client) SlotLogs(ctx context.Context, slot, tail int) (string, error) {
	var out string
	err := c.do(ctx, http.MethodGet, fmt.Sprintf("/v1/slots/%d/logs?tail=%d", slot, tail), nil, &out)
	return out, err
}

// Events opens the ndjson event stream; the returned channel closes when the
// stream ends. Cancel ctx to stop.
func (c *Client) Events(ctx context.Context) (<-chan bus.Event, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://deckhand/v1/events", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		resp.Body.Close()
		return nil, fmt.Errorf("events stream: %s", resp.Status)
	}
	ch := make(chan bus.Event, 64)
	go func() {
		defer resp.Body.Close()
		defer close(ch)
		dec := json.NewDecoder(resp.Body)
		for {
			var ev bus.Event
			if err := dec.Decode(&ev); err != nil {
				return
			}
			select {
			case ch <- ev:
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
