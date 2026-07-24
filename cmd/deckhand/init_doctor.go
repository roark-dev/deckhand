package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/roark-dev/deckhand/internal/config"
	"github.com/roark-dev/deckhand/internal/githubauth"
	"github.com/roark-dev/deckhand/internal/runner"
)

var flagOAuthClientID string

func init() {
	initCmd.Flags().StringVar(&flagOAuthClientID, "oauth-client-id", "", "OAuth app client ID for browser device login (also DECKHAND_OAUTH_CLIENT_ID)")
}

var instancesCmd = &cobra.Command{
	Use:   "instances",
	Short: "List the configured deckhand instances (one per org/repo)",
	RunE: func(cmd *cobra.Command, args []string) error {
		list := config.ListInstances()
		if len(list) == 0 {
			fmt.Println("no instances configured yet — run `deckhand init` inside a repo")
			return nil
		}
		for _, in := range list {
			marker := "  "
			if in.Name == paths.Instance || (in.IsDefault && paths.Instance == "") {
				marker = "* " // the instance this directory resolves to
			}
			def := ""
			if in.IsDefault {
				def = "  (default)"
			}
			fmt.Printf("%s%-28s %s%s\n", marker, in.Name, in.URL, def)
		}
		return nil
	},
}

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Interactive setup: write the config for this repo's instance",
	RunE: func(cmd *cobra.Command, args []string) error {
		if existing, err := config.Load(paths.ConfigFile); err == nil {
			// Re-running init must not be an error when there's nothing to
			// do — the whole point is "run two commands in the repo".
			detected := config.DetectGitHubURL(".")
			if detected == "" || detected == existing.GitHub.URL {
				fmt.Printf("already configured for %s (%s)\n", existing.GitHub.URL, paths.ConfigFile)
				fmt.Println("nothing to do — next: `deckhand service install`, then `runs-on: " + existing.ScaleSet.Name + "`")
				return nil
			}
			// With per-repo instances this only happens under an explicit
			// override (--instance / DECKHAND_INSTANCE / DECKHAND_HOME / -c)
			// pointing at another repo's config.
			return fmt.Errorf("%s targets %s, not this repo (%s) — deckhand keeps a separate instance per repo, so run `deckhand init` without an override here, or edit that config",
				paths.ConfigFile, existing.GitHub.URL, detected)
		} else if !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("%s exists but is invalid: %w (fix or delete it, then re-run init)", paths.ConfigFile, err)
		}
		in := bufio.NewReader(os.Stdin)
		var askErr error
		ask := func(prompt, def string) string {
			if askErr != nil {
				return def
			}
			if def != "" {
				fmt.Printf("%s [%s]: ", prompt, def)
			} else {
				fmt.Printf("%s: ", prompt)
			}
			line, err := in.ReadString('\n')
			line = strings.TrimSpace(line)
			if err != nil && line == "" {
				// EOF / closed stdin: silently accepting every default would
				// write a broken config; surface it instead.
				askErr = fmt.Errorf("stdin closed during prompts — run init interactively or write %s by hand", paths.ConfigFile)
				return def
			}
			if line == "" {
				return def
			}
			return line
		}

		fmt.Println("deckhand setup — one runner scale set, load-balanced across local slots.")
		fmt.Println()

		// Pre-fill the target from the current directory's git remote so a
		// `cd my-repo && deckhand init` is press-enter-through.
		urlDefault := config.DetectGitHubURL(".")
		url := ask("GitHub org or repo URL the runners serve", urlDefault)
		if url == "" {
			return errors.New("a GitHub org or repo URL is required")
		}

		// Auth: prefer whatever needs the least of the user.
		ghAvailable := exec.Command("gh", "auth", "token").Run() == nil
		fmt.Println()
		fmt.Println("How should deckhand authenticate to GitHub?")
		authDefault := "2"
		if ghAvailable {
			fmt.Println("  1) gh CLI — reuse your existing `gh` login; deckhand stores no credential (recommended)")
			authDefault = "1"
		} else {
			fmt.Println("  1) gh CLI — not detected (`gh auth status` failed)")
		}
		fmt.Println("  2) browser — sign in with a device code (like `gh auth login`)")
		fmt.Println("  3) token — you provide a PAT via env var or file")
		choice := ask("Auth method", authDefault)

		cfg := config.Config{}
		cfg.GitHub.URL = url
		switch choice {
		case "1":
			if !ghAvailable {
				return errors.New("gh CLI auth not available — install gh and run `gh auth login`, or pick another method")
			}
			cfg.GitHub.Auth.GH = true
		case "2":
			token, err := deviceLogin(cmd.Context(), url)
			if err != nil {
				return err
			}
			if err := os.MkdirAll(paths.Home, 0o700); err != nil {
				return err
			}
			tokenPath := filepath.Join(paths.Home, "token")
			if err := os.WriteFile(tokenPath, []byte(token), 0o600); err != nil {
				return err
			}
			cfg.GitHub.Auth.TokenFile = tokenPath
			fmt.Printf("token saved to %s (0600)\n", tokenPath)
		case "3":
			tokenEnv := ask("Environment variable holding the token", "DECKHAND_GITHUB_TOKEN")
			cfg.GitHub.Auth.TokenEnv = tokenEnv
		default:
			return fmt.Errorf("unknown choice %q", choice)
		}

		// The scale set name (the `runs-on:` label) is derived from the repo —
		// it's scoped to the repo, so there's nothing for the user to invent.
		name := config.DefaultScaleSetName(url)
		fmt.Printf("scale set name: %s  (your `runs-on:` label — edit config.yaml to change)\n", name)
		slotsN := ask("Slots (max concurrent jobs)", strconv.Itoa(4))
		if askErr != nil {
			return askErr
		}
		n, err := strconv.Atoi(slotsN)
		if err != nil {
			return fmt.Errorf("not a number: %s", slotsN)
		}
		cfg.ScaleSet.Name = name
		cfg.Slots.Count = n
		cfg.Runner.Image = config.DefaultRunnerImage
		// Project-type detection: a Node repo gets a persistent npm cache out
		// of the box (the remote `cache: npm` restore is redundant on
		// self-hosted — see README).
		if _, err := os.Stat("package.json"); err == nil {
			cfg.Runner.CachePaths = []string{"/home/runner/.npm"}
			fmt.Println("detected package.json — persisting /home/runner/.npm across jobs (per slot)")
		}

		if err := os.MkdirAll(paths.Home, 0o700); err != nil {
			return err
		}
		raw, err := yaml.Marshal(&cfg)
		if err != nil {
			return err
		}
		header := "# deckhand configuration — https://github.com/roark-dev/deckhand\n" +
			"# Tip: pin runner.image to a digest for reproducible runners.\n"
		if err := os.WriteFile(paths.ConfigFile, []byte(header+string(raw)), 0o600); err != nil {
			return err
		}
		fmt.Printf("\nwrote %s\n\nnext steps:\n", paths.ConfigFile)
		step := 1
		if choice == "3" {
			fmt.Printf("  %d. export %s=<your token>\n", step, cfg.GitHub.Auth.TokenEnv)
			step++
		}
		fmt.Printf("  %d. deckhand doctor            # verify docker + github connectivity\n", step)
		fmt.Printf("  %d. deckhand service install   # run at login (or `deckhand up` in a terminal)\n", step+1)
		fmt.Printf("  %d. use `runs-on: %s` in your workflows\n", step+2, name)
		return nil
	},
}

// deviceLogin runs the browser device-code flow. Repo-level targets need the
// `repo` scope; an org-level target additionally needs admin:org.
func deviceLogin(ctx context.Context, targetURL string) (string, error) {
	scopes := []string{"repo"}
	// An org URL has no repo path segment: https://github.com/my-org
	if parts := strings.Split(strings.TrimPrefix(strings.TrimSuffix(targetURL, "/"), "https://github.com/"), "/"); len(parts) == 1 && parts[0] != "" {
		scopes = append(scopes, "admin:org")
	}
	clientID := flagOAuthClientID
	if clientID == "" {
		clientID = os.Getenv("DECKHAND_OAUTH_CLIENT_ID")
	}
	if clientID == "" {
		clientID = githubauth.DefaultClientID
	}
	flow := &githubauth.Flow{
		ClientID: clientID,
		Scopes:   scopes,
		Prompt: func(code, uri string) {
			fmt.Printf("\n  Open %s and enter code: %s\n  (waiting for approval…)\n\n", uri, code)
		},
	}
	return flow.Run(ctx)
}

var doctorCmd = &cobra.Command{
	Use:   "doctor",
	Short: "Check config, docker and GitHub connectivity",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()
		failed := false
		check := func(name string, err error) {
			if err != nil {
				failed = true
				fmt.Printf("  ✗ %s: %v\n", name, err)
			} else {
				fmt.Printf("  ✓ %s\n", name)
			}
		}

		cfg, err := loadConfig()
		check("config "+paths.ConfigFile, err)
		if err != nil {
			return errors.New("doctor found problems")
		}

		prov, err := runner.New(runner.Options{Image: cfg.Runner.Image, ScaleSetName: cfg.ScaleSet.Name})
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = prov.Ping(pingCtx)
			cancel()
		}
		check("docker daemon reachable", err)

		if app := cfg.GitHub.Auth.App; app != nil {
			_, err = os.ReadFile(app.PrivateKeyFile)
			check("github app private key readable", err)
		} else {
			_, err = cfg.ResolveToken()
			check("credential resolvable", err)
		}

		if strings.Contains(cfg.Runner.Image, "@sha256:") {
			check("runner image pinned by digest", nil)
		} else {
			fmt.Printf("  ! runner image %q is a mutable tag — pin a digest (image@sha256:...) for supply-chain safety\n", cfg.Runner.Image)
		}

		// Oversubscription math: contention amplifies per-job fixed overhead
		// (duration variance across identical jobs is the tell). Warn, don't
		// fail — deliberate oversubscription can be fine for bursty light jobs.
		if ncpu, nerr := prov.NCPU(ctx); nerr == nil && ncpu > 0 {
			switch {
			case cfg.Slots.CPUsPerSlot > 0 && cfg.Slots.Count*cfg.Slots.CPUsPerSlot > ncpu:
				fmt.Printf("  ! oversubscribed: %d slots × %d cpus = %d > %d host CPUs — concurrent jobs will contend (expect duration variance)\n",
					cfg.Slots.Count, cfg.Slots.CPUsPerSlot, cfg.Slots.Count*cfg.Slots.CPUsPerSlot, ncpu)
			case cfg.Slots.CPUsPerSlot == 0 && ncpu/cfg.Slots.Count >= 1:
				check(fmt.Sprintf("cpu pinning: auto (%d host CPUs / %d slots = %d per slot)", ncpu, cfg.Slots.Count, ncpu/cfg.Slots.Count), nil)
			case cfg.Slots.CPUsPerSlot == 0:
				fmt.Printf("  ! %d slots > %d host CPUs — auto pinning disabled, jobs will contend; lower slots.count\n", cfg.Slots.Count, ncpu)
			case cfg.Slots.CPUsPerSlot < 0 && cfg.Slots.Count > 1:
				fmt.Printf("  ! pinning disabled (cpus_per_slot: -1): each of %d concurrent jobs sees all %d CPUs and sizes parallelism accordingly\n",
					cfg.Slots.Count, ncpu)
			default:
				check(fmt.Sprintf("cpu budget (%d slots × %d cpus ≤ %d host CPUs)", cfg.Slots.Count, cfg.Slots.CPUsPerSlot, ncpu), nil)
			}
		}

		ghErr := func() error {
			client, err := cfg.ScalesetClient()
			if err != nil {
				return err
			}
			ghCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			defer cancel()
			// Cheapest authenticated scale-set API call: resolves the runner
			// group, proving URL + credential + permissions in one shot.
			if cfg.ScaleSet.RunnerGroup != "default" {
				_, err = client.GetRunnerGroupByName(ghCtx, cfg.ScaleSet.RunnerGroup)
				return err
			}
			_, err = client.GetRunnerScaleSet(ghCtx, 1, cfg.ScaleSet.Name)
			return err
		}()
		check("github scale-set API access", ghErr)

		colimaPosture(check)

		if failed {
			return errors.New("doctor found problems")
		}
		fmt.Println("all good")
		return nil
	},
}

// colimaPosture warns about Colima's default writable-$HOME mount, which
// hands job code (root-equivalent via any exposed docker socket) a path to
// host credentials. Advisory only — non-Colima hosts skip it silently.
//
// The probes use HOST-home paths (/Users/you/...): mounts appear in the VM
// at their host paths, while the VM user's own $HOME (/home/*.guest) always
// contains a Lima-provisioned .ssh and must not be mistaken for exposure.
func colimaPosture(check func(string, error)) {
	if _, err := exec.LookPath("colima"); err != nil {
		return
	}
	hostHome, err := os.UserHomeDir()
	if err != nil {
		return
	}
	var exposed []string
	probed := false
	for _, dir := range []string{".aws", ".ssh", ".deckhand"} {
		path := hostHome + "/" + dir
		out, err := exec.Command("colima", "ssh", "--", "sh", "-c", fmt.Sprintf("test -e %q && echo yes || echo no", path)).Output()
		if err != nil {
			return // colima not running; nothing to assert
		}
		probed = true
		if strings.TrimSpace(string(out)) == "yes" {
			exposed = append(exposed, "~/"+dir)
		}
	}
	if !probed {
		return
	}
	if len(exposed) > 0 {
		check("colima mount posture", fmt.Errorf("the VM can see %s — with permissive mounts, job code that reaches the docker socket can read host credentials; restrict mounts in ~/.colima/default/colima.yaml (see templates/colima.yaml)", strings.Join(exposed, ", ")))
	} else {
		check("colima mount posture (host credentials hidden)", nil)
	}
}
