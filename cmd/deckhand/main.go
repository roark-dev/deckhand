// deckhand — one GitHub Actions runner scale set, load-balanced across local
// container slots, with a TUI dashboard.
package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"

	"github.com/spf13/cobra"

	"github.com/roark-dev/deckhand/internal/config"
)

var version = "dev" // set via -ldflags at release time

var (
	flagConfig string
	paths      config.Paths
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
		paths = config.DefaultPaths()
		if flagConfig != "" {
			paths.ConfigFile = flagConfig
		}
	},
}

func main() {
	root.PersistentFlags().StringVarP(&flagConfig, "config", "c", "", "config file (default ~/.deckhand/config.yaml)")
	root.AddCommand(upCmd, dashCmd, statusCmd, scaleCmd, pauseCmd, resumeCmd, drainCmd, stopCmd, logsCmd, reclaimCmd, cachesCmd, doctorCmd, initCmd, serviceCmd, versionCmd)
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
		return nil, fmt.Errorf("no config at %s — run `deckhand init` first", paths.ConfigFile)
	}
	return cfg, err
}
