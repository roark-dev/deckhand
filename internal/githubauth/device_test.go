package githubauth

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeGitHub serves the two device-flow endpoints, approving after n polls.
func fakeGitHub(t *testing.T, approveAfter int, terminalErr string) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	polls := &atomic.Int64{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /login/device/code", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("client_id") == "" {
			http.Error(w, "no client id", http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(DeviceCode{
			DeviceCode:      "dev-123",
			UserCode:        "ABCD-1234",
			VerificationURI: "https://github.com/login/device",
			ExpiresIn:       60,
			Interval:        0, // Flow clamps to a minimum internally; keep tests fast via short clamp below
		})
	})
	mux.HandleFunc("POST /login/oauth/access_token", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		if r.Form.Get("device_code") != "dev-123" {
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "incorrect_device_code"})
			return
		}
		n := polls.Add(1)
		switch {
		case terminalErr != "" && n >= int64(approveAfter):
			_ = json.NewEncoder(w).Encode(map[string]string{"error": terminalErr, "error_description": "nope"})
		case n >= int64(approveAfter):
			_ = json.NewEncoder(w).Encode(map[string]string{"access_token": "gho_test_token"})
		default:
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "authorization_pending"})
		}
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, polls
}

// testFlow builds a flow with the poll interval clamp shrunk via the server's
// Interval=0 → max(0,5)=5s would be too slow for tests, so we run with a
// context deadline and a hacked interval by pre-seeding.
func runFlow(t *testing.T, srv *httptest.Server) (string, error) {
	t.Helper()
	f := &Flow{
		ClientID: "test-client",
		Scopes:   []string{"repo"},
		BaseURL:  srv.URL,
		Prompt: func(code, uri string) {
			if code != "ABCD-1234" {
				t.Errorf("prompt got code %q", code)
			}
		},
	}
	// Shrink polling for tests: Run uses max(Interval,5)s — instead of
	// waiting, poll the internals directly with a tiny interval.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	dc, err := f.requestDeviceCode(ctx)
	if err != nil {
		return "", err
	}
	f.Prompt(dc.UserCode, dc.VerificationURI)
	interval := 5 * time.Millisecond
	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case <-time.After(interval):
		}
		token, retry, err := f.pollToken(ctx, dc.DeviceCode, &interval)
		if err != nil || !retry {
			return token, err
		}
	}
}

func TestDeviceFlowApproves(t *testing.T) {
	srv, polls := fakeGitHub(t, 3, "")
	token, err := runFlow(t, srv)
	if err != nil {
		t.Fatal(err)
	}
	if token != "gho_test_token" {
		t.Fatalf("token = %q", token)
	}
	if polls.Load() < 3 {
		t.Fatalf("expected pending polls before approval, got %d", polls.Load())
	}
}

func TestDeviceFlowDenied(t *testing.T) {
	srv, _ := fakeGitHub(t, 1, "access_denied")
	_, err := runFlow(t, srv)
	if err == nil || !strings.Contains(err.Error(), "access_denied") {
		t.Fatalf("want access_denied, got %v", err)
	}
}

func TestSlowDownGrowsInterval(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /login/oauth/access_token", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "slow_down"})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	f := &Flow{ClientID: "c", BaseURL: srv.URL, http: srv.Client()}
	interval := time.Second
	_, retry, err := f.pollToken(context.Background(), "x", &interval)
	if err != nil || !retry {
		t.Fatalf("slow_down should retry, got retry=%v err=%v", retry, err)
	}
	if interval != 6*time.Second {
		t.Fatalf("slow_down must grow the interval by 5s, got %s", interval)
	}
}

func TestRunEndToEnd(t *testing.T) {
	srv, polls := fakeGitHub(t, 2, "")
	prompted := false
	f := &Flow{
		ClientID: "test-client",
		Scopes:   []string{"repo"},
		BaseURL:  srv.URL,
		Prompt: func(code, uri string) {
			prompted = true
			if code != "ABCD-1234" || uri == "" {
				t.Errorf("bad prompt: %q %q", code, uri)
			}
		},
		minPoll: 5 * time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	token, err := f.Run(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if token != "gho_test_token" || !prompted {
		t.Fatalf("token=%q prompted=%v", token, prompted)
	}
	if polls.Load() < 2 {
		t.Fatalf("expected a pending poll before approval, got %d", polls.Load())
	}
}

func TestRunCancelled(t *testing.T) {
	srv, _ := fakeGitHub(t, 1000, "") // never approves
	f := &Flow{ClientID: "c", BaseURL: srv.URL, minPoll: 5 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := f.Run(ctx); err == nil {
		t.Fatal("cancelled Run must error")
	}
}

func TestNoClientID(t *testing.T) {
	f := &Flow{}
	if _, err := f.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "client ID") {
		t.Fatalf("want client-ID guidance, got %v", err)
	}
}
