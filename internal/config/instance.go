package config

import (
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Slug turns a GitHub org or org/repo URL into a filesystem- and
// service-label-safe instance name:
//
//	https://github.com/roark-dev/deckhand -> roark-dev-deckhand
func Slug(githubURL string) string {
	s := strings.TrimPrefix(githubURL, "https://github.com/")
	s = strings.TrimPrefix(s, "http://github.com/")
	s = strings.TrimSuffix(strings.TrimSuffix(s, "/"), ".git")
	s = strings.ToLower(s)
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-")
}

// DefaultScaleSetName derives the runs-on label from a GitHub URL — the repo
// name (or the org, for an org-level URL), lowercased. Scale sets are scoped to
// the repo, so keying the name to the repo means users never have to invent
// one, and it can't collide with another repo's.
func DefaultScaleSetName(githubURL string) string {
	path := strings.TrimPrefix(githubURL, "https://github.com/")
	path = strings.TrimPrefix(path, "http://github.com/")
	path = strings.TrimSuffix(strings.Trim(path, "/"), ".git")
	if i := strings.LastIndex(path, "/"); i >= 0 {
		path = path[i+1:] // org/repo -> repo
	}
	path = strings.ToLower(path)
	if path == "" {
		return "deckhand"
	}
	return path
}

// InstanceOptions selects which instance ResolvePaths targets.
type InstanceOptions struct {
	Instance string // explicit --instance / DECKHAND_INSTANCE (beats auto-detect)
	Home     string // explicit DECKHAND_HOME (beats everything)
	Dir      string // working dir whose git remote auto-selects the instance
}

func userRoot() string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "."
	}
	return filepath.Join(home, ".deckhand")
}

func pathsForHome(home, instance string) Paths {
	return Paths{
		Home:       home,
		Instance:   instance,
		ConfigFile: filepath.Join(home, "config.yaml"),
		Socket:     filepath.Join(home, "deckhand.sock"),
		LockFile:   filepath.Join(home, "daemon.lock"),
		StateFile:  filepath.Join(home, "state.json"),
		LogFile:    filepath.Join(home, "daemon.log"),
	}
}

// ResolvePaths locates the instance to operate on so several repos can share one
// machine without juggling DECKHAND_HOME. Precedence:
//
//  1. DECKHAND_HOME  -> that directory verbatim (the pre-existing escape hatch).
//  2. an instance name: explicit (opts.Instance) or auto-detected from the git
//     remote in opts.Dir.
//  3. the legacy flat ~/.deckhand/config.yaml is the DEFAULT instance — used
//     when the resolved name matches the repo it serves, or when there's no
//     name at all (running outside any repo). This keeps a pre-multi-instance
//     setup working untouched.
//  4. every other instance lives at ~/.deckhand/instances/<name>/.
func ResolvePaths(opts InstanceOptions) Paths {
	if opts.Home != "" {
		return pathsForHome(opts.Home, "")
	}
	root := userRoot()

	name := opts.Instance
	if name == "" && opts.Dir != "" {
		if url := DetectGitHubURL(opts.Dir); url != "" {
			name = Slug(url)
		}
	}

	// The legacy flat config is the default instance; match it by the repo it
	// serves so `deckhand` inside that repo keeps using it unchanged.
	if url, ok := peekGitHubURL(filepath.Join(root, "config.yaml")); ok {
		if name == "" || name == Slug(url) {
			return pathsForHome(root, "")
		}
	}
	if name == "" {
		// Outside a repo with no matching flat default: point at the root so the
		// caller's "no config — run init" error lands somewhere sane.
		return pathsForHome(root, "")
	}
	return pathsForHome(filepath.Join(root, "instances", name), name)
}

// peekGitHubURL reads only github.url from a config file, tolerating an
// otherwise-incomplete file so instance resolution never depends on full
// validation.
func peekGitHubURL(path string) (string, bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", false
	}
	var peek struct {
		GitHub struct {
			URL string `yaml:"url"`
		} `yaml:"github"`
	}
	if err := yaml.Unmarshal(raw, &peek); err != nil || peek.GitHub.URL == "" {
		return "", false
	}
	return peek.GitHub.URL, true
}

// InstanceInfo describes one configured instance for `deckhand instances`.
type InstanceInfo struct {
	Name      string // slug; also the value to pass to --instance
	Home      string
	URL       string
	IsDefault bool // the legacy flat ~/.deckhand instance
}

// ListInstances enumerates every configured instance: the legacy flat config
// (if present) plus each ~/.deckhand/instances/<name>/config.yaml.
func ListInstances() []InstanceInfo {
	root := userRoot()
	var out []InstanceInfo
	if url, ok := peekGitHubURL(filepath.Join(root, "config.yaml")); ok {
		out = append(out, InstanceInfo{Name: Slug(url), Home: root, URL: url, IsDefault: true})
	}
	entries, _ := os.ReadDir(filepath.Join(root, "instances"))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		home := filepath.Join(root, "instances", e.Name())
		if url, ok := peekGitHubURL(filepath.Join(home, "config.yaml")); ok {
			out = append(out, InstanceInfo{Name: e.Name(), Home: home, URL: url})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
