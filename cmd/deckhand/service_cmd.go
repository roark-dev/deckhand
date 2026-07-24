package main

import (
	"github.com/spf13/cobra"

	"github.com/roark-dev/deckhand/internal/service"
)

var serviceCmd = &cobra.Command{
	Use:   "service",
	Short: "Run deckhand at login (launchd/systemd) — no template editing",
}

func init() {
	serviceCmd.AddCommand(
		&cobra.Command{
			Use:   "install",
			Short: "Generate the service definition from this binary's path and start it",
			RunE: func(cmd *cobra.Command, args []string) error {
				if _, err := loadConfig(); err != nil {
					return err // no point installing a service that can't start
				}
				return service.Install(paths.Instance, paths.Home)
			},
		},
		&cobra.Command{
			Use:   "uninstall",
			Short: "Stop the service and remove its definition",
			RunE:  func(cmd *cobra.Command, args []string) error { return service.Uninstall(paths.Instance, paths.Home) },
		},
		&cobra.Command{
			Use:   "status",
			Short: "Show whether the service is installed and running",
			RunE:  func(cmd *cobra.Command, args []string) error { return service.Status(paths.Instance, paths.Home) },
		},
	)
}
