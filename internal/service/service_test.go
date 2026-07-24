package service

import (
	"strings"
	"testing"
)

var spec = Spec{
	BinaryPath:   "/opt/homebrew/bin/deckhand",
	HomeDir:      "/Users/x",
	InstanceHome: "/Users/x/.deckhand",
	LogPath:      "/Users/x/.deckhand/service.log",
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
	// PATH is deliberately set (launchd's default misses Homebrew → no gh);
	// anything secret-shaped must never appear.
	if !strings.Contains(out, "<string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin</string>") {
		t.Error("plist must set a PATH that includes Homebrew")
	}
	// The default instance's plist carries no DECKHAND_HOME — it stays
	// byte-compatible with the pre-multi-instance layout.
	if strings.Contains(out, "DECKHAND_HOME") {
		t.Error("default-instance plist must not pin DECKHAND_HOME")
	}
	for _, banned := range []string{"TOKEN", "github_pat_", "SECRET"} {
		if strings.Contains(out, banned) {
			t.Errorf("generated plist must never carry credentials (found %q)", banned)
		}
	}
}

// A named instance gets a distinct launchd label and pins its home, so two
// instances install side by side without colliding.
func TestLaunchdPlistNamedInstance(t *testing.T) {
	out := LaunchdPlist(Spec{
		BinaryPath:   "/opt/homebrew/bin/deckhand",
		HomeDir:      "/Users/x",
		Instance:     "roark-dev-deckhand",
		InstanceHome: "/Users/x/.deckhand/instances/roark-dev-deckhand",
		LogPath:      "/Users/x/.deckhand/instances/roark-dev-deckhand/service.log",
	})
	for _, want := range []string{
		"<string>com.deckhand.roark-dev-deckhand</string>",
		"<key>DECKHAND_HOME</key>",
		"<string>/Users/x/.deckhand/instances/roark-dev-deckhand</string>",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("named-instance plist missing %q", want)
		}
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
	if strings.Contains(out, "DECKHAND_HOME") {
		t.Error("default-instance unit must not pin DECKHAND_HOME")
	}
	named := SystemdUnit(Spec{BinaryPath: "/b/deckhand", Instance: "acme-web", InstanceHome: "/h/instances/acme-web"})
	if !strings.Contains(named, "Environment=DECKHAND_HOME=/h/instances/acme-web") {
		t.Errorf("named-instance unit must pin DECKHAND_HOME:\n%s", named)
	}
}

func TestLabelAndUnitNames(t *testing.T) {
	if labelFor("") != "com.deckhand.daemon" {
		t.Errorf("default label = %q", labelFor(""))
	}
	if labelFor("acme-web") != "com.deckhand.acme-web" {
		t.Errorf("named label = %q", labelFor("acme-web"))
	}
	if systemdName("") != "deckhand" || systemdName("acme-web") != "deckhand-acme-web" {
		t.Errorf("systemd names wrong: %q / %q", systemdName(""), systemdName("acme-web"))
	}
}

func TestCurrentSpecAndUnitPath(t *testing.T) {
	s, err := currentSpec("", "")
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
	// A named instance resolves to a distinct unit path.
	named, _ := currentSpec("acme-web", "/tmp/h/instances/acme-web")
	if unitPath(named) == p {
		t.Fatal("named instance must not share the default unit path")
	}
	if named.InstanceHome != "/tmp/h/instances/acme-web" {
		t.Fatalf("InstanceHome = %q", named.InstanceHome)
	}
}
