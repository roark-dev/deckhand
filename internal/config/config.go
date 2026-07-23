// Package config loads and validates deckhand's configuration and owns the
// layout of the state directory (config, lock, socket, persisted runtime
// state).
package config

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/actions/scaleset"
	"gopkg.in/yaml.v3"
)

// DefaultRunnerImage is the official GitHub Actions runner image. Users are
// encouraged to pin a digest in their config; the tag default keeps first-run
// friction low.
const DefaultRunnerImage = "ghcr.io/actions/actions-runner:latest"

type Config struct {
	GitHub   GitHub   `yaml:"github"`
	ScaleSet ScaleSet `yaml:"scale_set"`
	Slots    Slots    `yaml:"slots"`
	Runner   Runner   `yaml:"runner"`
	Metrics  Metrics  `yaml:"metrics"`
}

type GitHub struct {
	// URL of the org or repo the scale set attaches to,
	// e.g. https://github.com/my-org or https://github.com/my-org/my-repo.
	URL  string `yaml:"url"`
	Auth Auth   `yaml:"auth"`
}

type Auth struct {
	// Exactly one of Token / TokenEnv / TokenFile / App.
	Token     string `yaml:"token,omitempty"`
	TokenEnv  string `yaml:"token_env,omitempty"`
	TokenFile string `yaml:"token_file,omitempty"`
	App       *App   `yaml:"app,omitempty"`
}

type App struct {
	ClientID       string `yaml:"client_id"`
	InstallationID int64  `yaml:"installation_id"`
	PrivateKeyFile string `yaml:"private_key_file"`
}

type ScaleSet struct {
	Name        string   `yaml:"name"`
	RunnerGroup string   `yaml:"runner_group"`
	Labels      []string `yaml:"labels,omitempty"`
}

type Slots struct {
	Count int `yaml:"count"`
	// Warm keeps this many runners pre-registered and idle so jobs start
	// without waiting for a container boot. 0 = spawn purely on demand.
	Warm int `yaml:"warm"`
	// CPUsPerSlot pins each slot to a dedicated cpuset of this size
	// (slot i gets cpus [i*n, (i+1)*n)). 0 disables pinning.
	CPUsPerSlot int `yaml:"cpus_per_slot"`
}

type Runner struct {
	Image string `yaml:"image"`
	// ExposeDockerSocket bind-mounts /var/run/docker.sock into job
	// containers. Job code then has root-equivalent control of the docker
	// host — leave off unless workflows genuinely need docker.
	ExposeDockerSocket bool `yaml:"expose_docker_socket"`
	// Env is extra environment for job containers (never put secrets here;
	// anything a job can read, job code can exfiltrate).
	Env map[string]string `yaml:"env,omitempty"`
}

type Metrics struct {
	// Listen enables a plaintext Prometheus endpoint, e.g. "127.0.0.1:9642".
	Listen string `yaml:"listen,omitempty"`
}

// Paths locates everything deckhand keeps on disk.
type Paths struct {
	Home       string // state dir, default ~/.deckhand
	ConfigFile string
	Socket     string
	LockFile   string
	StateFile  string
	LogFile    string
}

func DefaultPaths() Paths {
	home := os.Getenv("DECKHAND_HOME")
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			userHome = "."
		}
		home = filepath.Join(userHome, ".deckhand")
	}
	return Paths{
		Home:       home,
		ConfigFile: filepath.Join(home, "config.yaml"),
		Socket:     filepath.Join(home, "deckhand.sock"),
		LockFile:   filepath.Join(home, "daemon.lock"),
		StateFile:  filepath.Join(home, "state.json"),
		LogFile:    filepath.Join(home, "daemon.log"),
	}
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := &Config{}
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return cfg, nil
}

func (c *Config) applyDefaults() {
	if c.ScaleSet.Name == "" {
		c.ScaleSet.Name = "deckhand"
	}
	if c.ScaleSet.RunnerGroup == "" {
		c.ScaleSet.RunnerGroup = scaleset.DefaultRunnerGroup
	}
	if c.Slots.Count == 0 {
		c.Slots.Count = defaultSlotCount()
	}
	if c.Runner.Image == "" {
		c.Runner.Image = DefaultRunnerImage
	}
}

func defaultSlotCount() int {
	n := runtime.NumCPU() / 2
	if n < 1 {
		return 1
	}
	if n > 4 {
		return 4
	}
	return n
}

func (c *Config) Validate() error {
	if c.GitHub.URL == "" {
		return errors.New("github.url is required (org or repo URL)")
	}
	if !strings.HasPrefix(c.GitHub.URL, "https://") && !strings.HasPrefix(c.GitHub.URL, "http://") {
		return fmt.Errorf("github.url %q must be a full URL", c.GitHub.URL)
	}
	n := 0
	a := c.GitHub.Auth
	for _, set := range []bool{a.Token != "", a.TokenEnv != "", a.TokenFile != "", a.App != nil} {
		if set {
			n++
		}
	}
	if n != 1 {
		return errors.New("github.auth needs exactly one of: token, token_env, token_file, app")
	}
	if a.App != nil && (a.App.ClientID == "" || a.App.InstallationID == 0 || a.App.PrivateKeyFile == "") {
		return errors.New("github.auth.app needs client_id, installation_id and private_key_file")
	}
	if c.Slots.Count < 1 || c.Slots.Count > 64 {
		return fmt.Errorf("slots.count %d out of range 1-64", c.Slots.Count)
	}
	if c.Slots.Warm < 0 || c.Slots.Warm > c.Slots.Count {
		return fmt.Errorf("slots.warm %d must be within 0..slots.count", c.Slots.Warm)
	}
	if c.Slots.CPUsPerSlot < 0 {
		return errors.New("slots.cpus_per_slot cannot be negative")
	}
	return nil
}

// ResolveToken returns the configured PAT, or "" when GitHub App auth is used.
func (c *Config) ResolveToken() (string, error) {
	a := c.GitHub.Auth
	switch {
	case a.Token != "":
		return a.Token, nil
	case a.TokenEnv != "":
		v := os.Getenv(a.TokenEnv)
		if v == "" {
			return "", fmt.Errorf("github.auth.token_env: environment variable %s is empty", a.TokenEnv)
		}
		return v, nil
	case a.TokenFile != "":
		raw, err := os.ReadFile(a.TokenFile)
		if err != nil {
			return "", fmt.Errorf("github.auth.token_file: %w", err)
		}
		return strings.TrimSpace(string(raw)), nil
	}
	return "", nil
}

// ScalesetClient builds the authenticated scaleset API client.
func (c *Config) ScalesetClient() (*scaleset.Client, error) {
	if c.GitHub.Auth.App != nil {
		key, err := os.ReadFile(c.GitHub.Auth.App.PrivateKeyFile)
		if err != nil {
			return nil, fmt.Errorf("github.auth.app.private_key_file: %w", err)
		}
		return scaleset.NewClientWithGitHubApp(scaleset.ClientWithGitHubAppConfig{
			GitHubConfigURL: c.GitHub.URL,
			GitHubAppAuth: scaleset.GitHubAppAuth{
				ClientID:       c.GitHub.Auth.App.ClientID,
				InstallationID: c.GitHub.Auth.App.InstallationID,
				PrivateKey:     string(key),
			},
		})
	}
	token, err := c.ResolveToken()
	if err != nil {
		return nil, err
	}
	return scaleset.NewClientWithPersonalAccessToken(scaleset.NewClientWithPersonalAccessTokenConfig{
		GitHubConfigURL:     c.GitHub.URL,
		PersonalAccessToken: token,
	})
}
