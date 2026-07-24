package config

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

const minimal = `
github:
  url: https://github.com/me/repo
  auth:
    token_env: TEST_TOKEN
`

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ScaleSet.Name != "deckhand" {
		t.Errorf("default scale set name = %q", cfg.ScaleSet.Name)
	}
	if cfg.ScaleSet.RunnerGroup != "default" {
		t.Errorf("default runner group = %q", cfg.ScaleSet.RunnerGroup)
	}
	if cfg.Slots.Count < 1 {
		t.Errorf("default slot count = %d", cfg.Slots.Count)
	}
	if cfg.Runner.Image == "" {
		t.Error("default image empty")
	}
}

func TestAuthExactlyOne(t *testing.T) {
	cases := []string{
		// none
		`
github:
  url: https://github.com/me/repo
  auth: {}
`,
		// two
		`
github:
  url: https://github.com/me/repo
  auth:
    token: abc
    token_env: FOO
`,
	}
	for _, body := range cases {
		if _, err := Load(writeConfig(t, body)); err == nil || !strings.Contains(err.Error(), "exactly one") {
			t.Errorf("want exactly-one auth error, got %v", err)
		}
	}
}

func TestUnknownFieldRejected(t *testing.T) {
	body := minimal + `
slotts:
  count: 2
`
	if _, err := Load(writeConfig(t, body)); err == nil {
		t.Fatal("unknown top-level field should fail to parse (typo protection)")
	}
}

func TestWarmBounded(t *testing.T) {
	body := minimal + `
slots:
  count: 2
  warm: 3
`
	if _, err := Load(writeConfig(t, body)); err == nil {
		t.Fatal("warm > count should be rejected")
	}
}

func TestResolveTokenEnv(t *testing.T) {
	t.Setenv("TEST_TOKEN", "sekrit")
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := cfg.ResolveToken()
	if err != nil || tok != "sekrit" {
		t.Fatalf("token = %q err = %v", tok, err)
	}
}

func TestResolveTokenEnvMissing(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("TEST_TOKEN", "") // isolate from sibling tests, not just unset
	if _, err := cfg.ResolveToken(); err == nil {
		t.Fatal("empty token env should error")
	}
}

func TestResolveTokenFile(t *testing.T) {
	tokenPath := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenPath, []byte("  sekrit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(writeConfig(t, `
github:
  url: https://github.com/me/repo
  auth:
    token_file: `+tokenPath+`
`))
	if err != nil {
		t.Fatal(err)
	}
	tok, err := cfg.ResolveToken()
	if err != nil || tok != "sekrit" {
		t.Fatalf("token = %q err = %v (must be trimmed)", tok, err)
	}
	cfg.GitHub.Auth.TokenFile = filepath.Join(t.TempDir(), "missing")
	if _, err := cfg.ResolveToken(); err == nil {
		t.Fatal("missing token file should error")
	}
}

func TestValidateMatrix(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr string
	}{
		{"url without scheme", `
github:
  url: github.com/me/repo
  auth: {token: t}
`, "must be a full URL"},
		{"partial app auth", `
github:
  url: https://github.com/me/repo
  auth:
    app:
      client_id: abc
`, "client_id, installation_id and private_key_file"},
		{"count too high", minimal + `
slots:
  count: 65
`, "out of range"},
		{"cpus below -1", minimal + `
slots:
  count: 2
  cpus_per_slot: -2
`, "cpus_per_slot must be -1"},
		{"negative memory", minimal + `
runner:
  memory_mb: -5
`, "cannot be negative"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeConfig(t, tc.body))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("want error containing %q, got %v", tc.wantErr, err)
			}
		})
	}
}

func TestCPUsPerSlotSentinels(t *testing.T) {
	// -1 (no pinning) and 0 (auto) are valid sentinels, not errors.
	for _, v := range []int{-1, 0} {
		if _, err := Load(writeConfig(t, minimal+fmt.Sprintf(`
slots:
  count: 2
  cpus_per_slot: %d
`, v))); err != nil {
			t.Fatalf("cpus_per_slot %d should be valid, got %v", v, err)
		}
	}
}

func TestAppAuthValid(t *testing.T) {
	keyPath := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(keyPath, []byte("not-a-real-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(writeConfig(t, `
github:
  url: https://github.com/me/repo
  auth:
    app:
      client_id: Iv1.abc
      installation_id: 123
      private_key_file: `+keyPath+`
`))
	if err != nil {
		t.Fatal(err)
	}
	if tok, err := cfg.ResolveToken(); err != nil || tok != "" {
		t.Fatalf("app auth resolves no token: %q %v", tok, err)
	}
}

func TestPidsLimitDefaultAndUnlimited(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Runner.PidsLimit != DefaultPidsLimit {
		t.Fatalf("default pids limit = %d, want %d", cfg.Runner.PidsLimit, DefaultPidsLimit)
	}
	cfg, err = Load(writeConfig(t, minimal+`
runner:
  pids_limit: -1
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Runner.PidsLimit != 0 {
		t.Fatalf("-1 should mean unlimited (0), got %d", cfg.Runner.PidsLimit)
	}
}

func TestDefaultSlotCountClamped(t *testing.T) {
	n := defaultSlotCount()
	if n < 1 || n > 4 {
		t.Fatalf("defaultSlotCount out of [1,4]: %d", n)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "nope.yaml"))
	if !os.IsNotExist(err) {
		t.Fatalf("want raw not-exist error for caller mapping, got %v", err)
	}
}

func TestAuthGHExactlyOne(t *testing.T) {
	body := `
github:
  url: https://github.com/me/repo
  auth:
    gh: true
`
	cfg, err := Load(writeConfig(t, body))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.GitHub.Auth.GH {
		t.Fatal("gh auth not parsed")
	}
	// gh combined with another method must be rejected.
	if _, err := Load(writeConfig(t, body+"    token: abc\n")); err == nil {
		t.Fatal("gh + token must fail exactly-one validation")
	}
}

func TestDetectGitHubURL(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = dir
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	run("init", "-q")
	run("remote", "add", "origin", "git@github.com:me/my-repo.git")
	if got := DetectGitHubURL(dir); got != "https://github.com/me/my-repo" {
		t.Fatalf("ssh remote: got %q", got)
	}
	run("remote", "set-url", "origin", "https://github.com/me/other.git")
	if got := DetectGitHubURL(dir); got != "https://github.com/me/other" {
		t.Fatalf("https remote: got %q", got)
	}
	run("remote", "set-url", "origin", "https://gitlab.com/me/elsewhere.git")
	if got := DetectGitHubURL(dir); got != "" {
		t.Fatalf("non-github remote must return empty, got %q", got)
	}
	if got := DetectGitHubURL(t.TempDir()); got != "" {
		t.Fatalf("non-repo dir must return empty, got %q", got)
	}
}

func TestCachePathsValidation(t *testing.T) {
	if _, err := Load(writeConfig(t, minimal+`
runner:
  cache_paths: ["relative/.npm"]
`)); err == nil {
		t.Fatal("relative cache path must be rejected")
	}
	cfg, err := Load(writeConfig(t, minimal+`
runner:
  cache_paths: ["/home/runner/.npm"]
  tool_cache: false
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Runner.ToolCacheEnabled() {
		t.Fatal("tool_cache: false must disable the tool cache")
	}
	if len(cfg.Runner.CachePaths) != 1 {
		t.Fatal("cache path not parsed")
	}
	// Default: tool cache on.
	cfg, err = Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Runner.ToolCacheEnabled() {
		t.Fatal("tool cache must default to enabled")
	}
}

func TestNoNewPrivilegesDefaultAndOptOut(t *testing.T) {
	cfg, err := Load(writeConfig(t, minimal))
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Runner.NoNewPrivilegesEnabled() {
		t.Fatal("no-new-privileges must default to enabled")
	}
	cfg, err = Load(writeConfig(t, minimal+`
runner:
  no_new_privileges: false
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Runner.NoNewPrivilegesEnabled() {
		t.Fatal("explicit false must disable no-new-privileges")
	}
}
