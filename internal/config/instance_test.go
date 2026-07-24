package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestSlug(t *testing.T) {
	cases := map[string]string{
		"https://github.com/roark-dev/deckhand":   "roark-dev-deckhand",
		"https://github.com/roark-dev/tradingBot": "roark-dev-tradingbot",
		"https://github.com/roark-dev":            "roark-dev",
		"https://github.com/Acme/My_Repo.git":     "acme-my-repo",
		"https://github.com/a/b/":                 "a-b",
	}
	for in, want := range cases {
		if got := Slug(in); got != want {
			t.Errorf("Slug(%q) = %q, want %q", in, got, want)
		}
	}
}

// A repo whose git remote differs from the legacy flat config resolves to its
// own instance dir instead of clobbering the flat one.
func TestResolvePathsSecondRepoGetsOwnInstance(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)        // userRoot() -> <root>/.deckhand
	t.Setenv("DECKHAND_HOME", "") // ensure the escape hatch is off
	dh := filepath.Join(root, ".deckhand")
	mkConfig(t, filepath.Join(dh, "config.yaml"), "https://github.com/roark-dev/tradingBot")
	repo := gitRepo(t, "https://github.com/roark-dev/deckhand")

	p := ResolvePaths(InstanceOptions{Dir: repo})
	wantHome := filepath.Join(dh, "instances", "roark-dev-deckhand")
	if p.Home != wantHome {
		t.Fatalf("Home = %q, want %q", p.Home, wantHome)
	}
	if p.Instance != "roark-dev-deckhand" {
		t.Fatalf("Instance = %q, want roark-dev-deckhand", p.Instance)
	}
}

// The repo the legacy flat config serves keeps using the flat layout untouched.
func TestResolvePathsFlatConfigStaysDefaultForItsRepo(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("DECKHAND_HOME", "")
	dh := filepath.Join(root, ".deckhand")
	mkConfig(t, filepath.Join(dh, "config.yaml"), "https://github.com/roark-dev/tradingBot")
	repo := gitRepo(t, "https://github.com/roark-dev/tradingBot")

	p := ResolvePaths(InstanceOptions{Dir: repo})
	if p.Home != dh || p.Instance != "" {
		t.Fatalf("expected legacy flat home %q with empty instance, got Home=%q Instance=%q", dh, p.Home, p.Instance)
	}
}

// --instance beats auto-detection; DECKHAND_HOME beats everything.
func TestResolvePathsExplicitOverrides(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)
	t.Setenv("DECKHAND_HOME", "")
	dh := filepath.Join(root, ".deckhand")

	p := ResolvePaths(InstanceOptions{Instance: "acme-web"})
	if want := filepath.Join(dh, "instances", "acme-web"); p.Home != want {
		t.Fatalf("--instance Home = %q, want %q", p.Home, want)
	}

	p = ResolvePaths(InstanceOptions{Home: "/custom/home", Instance: "ignored"})
	if p.Home != "/custom/home" || p.Instance != "" {
		t.Fatalf("DECKHAND_HOME must win: got Home=%q Instance=%q", p.Home, p.Instance)
	}
}

func TestListInstances(t *testing.T) {
	root := t.TempDir()
	t.Setenv("HOME", root)
	dh := filepath.Join(root, ".deckhand")
	mkConfig(t, filepath.Join(dh, "config.yaml"), "https://github.com/roark-dev/tradingBot")
	mkConfig(t, filepath.Join(dh, "instances", "roark-dev-deckhand", "config.yaml"), "https://github.com/roark-dev/deckhand")

	got := ListInstances()
	if len(got) != 2 {
		t.Fatalf("want 2 instances, got %d: %+v", len(got), got)
	}
	var sawDefault bool
	for _, i := range got {
		if i.IsDefault && i.Home == dh && i.URL == "https://github.com/roark-dev/tradingBot" {
			sawDefault = true
		}
	}
	if !sawDefault {
		t.Errorf("legacy flat config not reported as the default instance: %+v", got)
	}
}

func mkConfig(t *testing.T, path, url string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	body := "github:\n  url: " + url + "\n  auth: {token: t}\n"
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// gitRepo makes a throwaway git repo with an origin remote so DetectGitHubURL
// resolves against it.
func gitRepo(t *testing.T, remote string) string {
	t.Helper()
	dir := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q"},
		{"remote", "add", "origin", remote},
	} {
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	return dir
}
