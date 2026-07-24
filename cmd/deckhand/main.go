// deckhand — one GitHub Actions runner scale set, load-balanced across local
// container slots, with a TUI dashboard.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/roark-dev/deckhand/internal/config"
)

var version = "dev" // set via -ldflags at release time

var (
	flagConfig   string
	flagInstance string
	paths        config.Paths
)

var root = &cobra.Command{
	Use:   "deckhand",
	Short: "Self-hosted GitHub Actions runners with one registration, tunable slots and a TUI",
	Long: `deckhand runs self-hosted GitHub Actions runners on your machine.

It registers ONE runner scale set with GitHub; GitHub queues jobs against it
and deckhand load-balances them across local ephemeral runner containers
("slots"). Slot count is tunable at runtime, and a TUI dashboard shows the
fleet live.`,
	SilenceUsage:  true,
	SilenceErrors: true,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		instance := flagInstance
		if instance == "" {
			instance = os.Getenv("DECKHAND_INSTANCE")
		}
		// Auto-detect the instance from the current repo's git remote; an
		// explicit --instance / DECKHAND_INSTANCE, or DECKHAND_HOME, overrides.
		paths = config.ResolvePaths(config.InstanceOptions{
			Instance: instance,
			Home:     os.Getenv("DECKHAND_HOME"),
			Dir:      ".",
		})
		if flagConfig != "" {
			paths.ConfigFile = flagConfig
		}
	},
}

func main() {
	root.PersistentFlags().StringVarP(&flagConfig, "config", "c", "", "config file (default ~/.deckhand/config.yaml)")
	root.PersistentFlags().StringVar(&flagInstance, "instance", "", "instance to target (default: auto-detected from the current repo; also DECKHAND_INSTANCE)")
	root.AddCommand(upCmd, dashCmd, statusCmd, scaleCmd, pauseCmd, resumeCmd, drainCmd, stopCmd, logsCmd, reclaimCmd, cachesCmd, doctorCmd, initCmd, instancesCmd, serviceCmd, versionCmd)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

var versionCmd = &cobra.Command{
	Use:   "version",
	Short: "Print the deckhand version",
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("deckhand", version)
	},
}

func loadConfig() (*config.Config, error) {
	cfg, err := config.Load(paths.ConfigFile)
	if errors.Is(err, fs.ErrNotExist) {
		// If other instances exist, the user is likely outside the right repo
		// or forgot --instance — point them at the list rather than at init.
		if others := config.ListInstances(); len(others) > 0 {
			names := make([]string, len(others))
			for i, o := range others {
				names[i] = o.Name
			}
			return nil, fmt.Errorf("no config for this instance at %s — run `deckhand init` here, or target an existing instance with --instance <name> (%s); see `deckhand instances`",
				paths.ConfigFile, strings.Join(names, ", "))
		}
		return nil, fmt.Errorf("no config at %s — run `deckhand init` first", paths.ConfigFile)
	}
	return cfg, err
}
