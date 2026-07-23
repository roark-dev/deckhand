package runner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
)

// resolveDockerHost mirrors the docker CLI's endpoint resolution, which the
// Go SDK's FromEnv does NOT do: FromEnv only honors DOCKER_HOST and otherwise
// assumes /var/run/docker.sock — wrong on hosts using CLI contexts (Colima,
// OrbStack, Docker Desktop with custom contexts, rootless docker).
//
// Order: DOCKER_HOST env wins (return "" so FromEnv applies it); else the
// current CLI context's docker endpoint from ~/.docker; else "" (SDK default).
func resolveDockerHost() string {
	if os.Getenv("DOCKER_HOST") != "" {
		return ""
	}
	configDir := os.Getenv("DOCKER_CONFIG")
	if configDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return ""
		}
		configDir = filepath.Join(home, ".docker")
	}
	raw, err := os.ReadFile(filepath.Join(configDir, "config.json"))
	if err != nil {
		return ""
	}
	var cfg struct {
		CurrentContext string `json:"currentContext"`
	}
	if json.Unmarshal(raw, &cfg) != nil || cfg.CurrentContext == "" || cfg.CurrentContext == "default" {
		return ""
	}
	// Context metadata lives at contexts/meta/<sha256(name)>/meta.json.
	sum := sha256.Sum256([]byte(cfg.CurrentContext))
	metaPath := filepath.Join(configDir, "contexts", "meta", hex.EncodeToString(sum[:]), "meta.json")
	raw, err = os.ReadFile(metaPath)
	if err != nil {
		return ""
	}
	var meta struct {
		Endpoints map[string]struct {
			Host string `json:"Host"`
		} `json:"Endpoints"`
	}
	if json.Unmarshal(raw, &meta) != nil {
		return ""
	}
	return meta.Endpoints["docker"].Host
}
