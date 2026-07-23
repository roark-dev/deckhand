package config

import (
	"os"
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
	os.Unsetenv("TEST_TOKEN")
	if _, err := cfg.ResolveToken(); err == nil {
		t.Fatal("empty token env should error")
	}
}
