package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/roark-dev/deckhand/internal/runner"
)

// cachesCmd makes the persist-vs-wipe split explicit and operable: workspaces
// die with every job container; these volumes are the ONLY cross-job state.
var cachesCmd = &cobra.Command{
	Use:   "caches",
	Short: "Inspect or wipe the per-slot persistent caches (tool cache, cache_paths)",
}

func cachesProvider() (*runner.Provider, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	return runner.New(runner.Options{
		Image:        cfg.Runner.Image,
		ScaleSetName: cfg.ScaleSet.Name,
		ToolCache:    cfg.Runner.ToolCacheEnabled(),
		CachePaths:   cfg.Runner.CachePaths,
	})
}

func init() {
	wipeSlot := -1
	wipe := &cobra.Command{
		Use:   "wipe",
		Short: "Delete cache volumes (all slots, or --slot N) — e.g. after a suspected cache poisoning",
		RunE: func(cmd *cobra.Command, args []string) error {
			p, err := cachesProvider()
			if err != nil {
				return err
			}
			n, err := p.WipeCacheVolumes(cmd.Context(), wipeSlot)
			if n > 0 {
				fmt.Printf("removed %d cache volume(s)\n", n)
			}
			if err != nil {
				return err
			}
			if n == 0 {
				fmt.Println("no cache volumes to remove")
			}
			return nil
		},
	}
	wipe.Flags().IntVar(&wipeSlot, "slot", -1, "wipe only this slot's caches")

	cachesCmd.AddCommand(
		&cobra.Command{
			Use:   "list",
			Short: "List persistent cache volumes and what path each serves",
			RunE: func(cmd *cobra.Command, args []string) error {
				p, err := cachesProvider()
				if err != nil {
					return err
				}
				vols, err := p.ListCacheVolumes(cmd.Context())
				if err != nil {
					return err
				}
				if len(vols) == 0 {
					fmt.Println("no cache volumes yet (created on first job per slot)")
					return nil
				}
				fmt.Println("persists across jobs (everything else dies with the job container):")
				for _, v := range vols {
					fmt.Printf("  slot %-3d %-28s -> %s\n", v.Slot, v.Path, v.Name)
				}
				return nil
			},
		},
		wipe,
	)
}
