package service

import (
	"strings"
	"testing"
)

var spec = Spec{
	BinaryPath: "/opt/homebrew/bin/deckhand",
	HomeDir:    "/Users/x",
	LogPath:    "/Users/x/.deckhand/service.log",
}

func TestLaunchdPlist(t *testing.T) {
	out := LaunchdPlist(spec)
	for _, want := range []string{
		"<string>com.deckhand.daemon</string>",
		"<string>/opt/homebrew/bin/deckhand</string>",
		"<string>up</string>",
		"<string>/Users/x/.deckhand/service.log</string>",
		"<key>RunAtLoad</key>",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("plist missing %q", want)
		}
	}
	if strings.Contains(out, "EnvironmentVariables") {
		t.Error("generated plist must never carry credentials/env vars")
	}
}

func TestSystemdUnit(t *testing.T) {
	out := SystemdUnit(spec)
	for _, want := range []string{
		"ExecStart=/opt/homebrew/bin/deckhand up",
		"Restart=always",
		"WantedBy=default.target",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("unit missing %q", want)
		}
	}
}

func TestCurrentSpecAndUnitPath(t *testing.T) {
	s, err := currentSpec()
	if err != nil {
		t.Fatal(err)
	}
	if s.BinaryPath == "" || s.HomeDir == "" || !strings.HasSuffix(s.LogPath, "service.log") {
		t.Fatalf("bad spec: %+v", s)
	}
	p := unitPath(s)
	if !strings.HasPrefix(p, s.HomeDir) {
		t.Fatalf("unit path %q not under home", p)
	}
	if !strings.HasSuffix(p, ".plist") && !strings.HasSuffix(p, ".service") {
		t.Fatalf("unexpected unit path %q", p)
	}
}
