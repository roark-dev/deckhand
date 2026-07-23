package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"

	"github.com/roark-dev/deckhand/internal/config"
	"github.com/roark-dev/deckhand/internal/runner"
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Interactive setup: write ~/.deckhand/config.yaml",
	RunE: func(cmd *cobra.Command, args []string) error {
		if _, err := os.Stat(paths.ConfigFile); err == nil {
			return fmt.Errorf("%s already exists — edit it directly, or delete it to re-run init", paths.ConfigFile)
		}
		in := bufio.NewReader(os.Stdin)
		ask := func(prompt, def string) string {
			if def != "" {
				fmt.Printf("%s [%s]: ", prompt, def)
			} else {
				fmt.Printf("%s: ", prompt)
			}
			line, _ := in.ReadString('\n')
			line = strings.TrimSpace(line)
			if line == "" {
				return def
			}
			return line
		}

		fmt.Println("deckhand setup — one runner scale set, load-balanced across local slots.")
		fmt.Println()
		url := ask("GitHub org or repo URL the runners serve (e.g. https://github.com/me/repo)", "")
		fmt.Println()
		fmt.Println("Auth: deckhand needs a fine-grained PAT (repo Administration: read/write for a")
		fmt.Println("repo URL; org Self-hosted runners: read/write for an org URL), or a GitHub App.")
		fmt.Println("The credential stays on this machine; job containers never see it.")
		tokenEnv := ask("Environment variable holding the token (recommended over storing it in the file)", "DECKHAND_GITHUB_TOKEN")
		name := ask("Scale set name (this is what `runs-on:` will reference)", "deckhand")
		slotsN := ask("Slots (max concurrent jobs)", strconv.Itoa(4))
		n, err := strconv.Atoi(slotsN)
		if err != nil {
			return fmt.Errorf("not a number: %s", slotsN)
		}

		cfg := config.Config{}
		cfg.GitHub.URL = url
		cfg.GitHub.Auth.TokenEnv = tokenEnv
		cfg.ScaleSet.Name = name
		cfg.Slots.Count = n
		cfg.Runner.Image = config.DefaultRunnerImage

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
		fmt.Printf("  1. export %s=<your token>\n", tokenEnv)
		fmt.Printf("  2. deckhand doctor   # verify docker + github connectivity\n")
		fmt.Printf("  3. deckhand up       # start the daemon\n")
		fmt.Printf("  4. use `runs-on: %s` in your workflows\n", name)
		return nil
	},
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
			return fmt.Errorf("doctor found problems")
		}

		prov, err := runner.New(runner.Options{Image: cfg.Runner.Image, ScaleSetName: cfg.ScaleSet.Name})
		if err == nil {
			pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			err = prov.Ping(pingCtx)
			cancel()
		}
		check("docker daemon reachable", err)

		_, err = cfg.ResolveToken()
		if cfg.GitHub.Auth.App != nil {
			err = nil
		}
		check("credential resolvable", err)

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
			return fmt.Errorf("doctor found problems")
		}
		fmt.Println("all good")
		return nil
	},
}

// colimaPosture warns about Colima's default writable-$HOME mount, which
// hands job code (root-equivalent via any exposed docker socket) a path to
// host credentials. Advisory only — non-Colima hosts skip it silently.
func colimaPosture(check func(string, error)) {
	if _, err := exec.LookPath("colima"); err != nil {
		return
	}
	out, err := exec.Command("colima", "ssh", "--", "sh", "-c", "test -e \"$HOME/.aws\" || test -e \"$HOME/.ssh\"; echo $?").Output()
	if err != nil {
		return // colima not running; nothing to assert
	}
	if strings.TrimSpace(string(out)) == "0" {
		check("colima mount posture", fmt.Errorf("the VM can see ~/.aws or ~/.ssh — with default mounts, job code that reaches the docker socket can read host credentials; restrict mounts in ~/.colima/default/colima.yaml (see templates/colima.yaml)"))
	} else {
		check("colima mount posture (restricted)", nil)
	}
}
