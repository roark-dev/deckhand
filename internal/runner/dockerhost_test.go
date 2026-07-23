package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
)

func writeDockerConfig(t *testing.T, currentContext, endpointHost string) string {
	t.Helper()
	dir := t.TempDir()
	cfg := `{"currentContext": "` + currentContext + `"}`
	if err := os.WriteFile(filepath.Join(dir, "config.json"), []byte(cfg), 0o600); err != nil {
		t.Fatal(err)
	}
	if endpointHost != "" {
		sum := sha256.Sum256([]byte(currentContext))
		metaDir := filepath.Join(dir, "contexts", "meta", hex.EncodeToString(sum[:]))
		if err := os.MkdirAll(metaDir, 0o755); err != nil {
			t.Fatal(err)
		}
		meta := `{"Name":"` + currentContext + `","Endpoints":{"docker":{"Host":"` + endpointHost + `"}}}`
		if err := os.WriteFile(filepath.Join(metaDir, "meta.json"), []byte(meta), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

func TestResolveDockerHostFromContext(t *testing.T) {
	dir := writeDockerConfig(t, "colima", "unix:///Users/x/.colima/default/docker.sock")
	t.Setenv("DOCKER_CONFIG", dir)
	t.Setenv("DOCKER_HOST", "")
	if got := resolveDockerHost(); got != "unix:///Users/x/.colima/default/docker.sock" {
		t.Fatalf("resolveDockerHost = %q", got)
	}
}

func TestResolveDockerHostEnvWins(t *testing.T) {
	dir := writeDockerConfig(t, "colima", "unix:///ctx.sock")
	t.Setenv("DOCKER_CONFIG", dir)
	t.Setenv("DOCKER_HOST", "unix:///env.sock")
	// "" defers to FromEnv, which applies DOCKER_HOST itself.
	if got := resolveDockerHost(); got != "" {
		t.Fatalf("DOCKER_HOST set: resolver must defer to FromEnv, got %q", got)
	}
}

func TestResolveDockerHostDefaultContext(t *testing.T) {
	dir := writeDockerConfig(t, "default", "")
	t.Setenv("DOCKER_CONFIG", dir)
	t.Setenv("DOCKER_HOST", "")
	if got := resolveDockerHost(); got != "" {
		t.Fatalf("default context must use the SDK default, got %q", got)
	}
}

func TestResolveDockerHostMissingConfig(t *testing.T) {
	t.Setenv("DOCKER_CONFIG", t.TempDir())
	t.Setenv("DOCKER_HOST", "")
	if got := resolveDockerHost(); got != "" {
		t.Fatalf("missing config must fall back to SDK default, got %q", got)
	}
}

func TestResolveDockerHostBrokenMeta(t *testing.T) {
	dir := writeDockerConfig(t, "colima", "") // context named but no meta.json
	t.Setenv("DOCKER_CONFIG", dir)
	t.Setenv("DOCKER_HOST", "")
	if got := resolveDockerHost(); got != "" {
		t.Fatalf("missing context meta must fall back, got %q", got)
	}
}
