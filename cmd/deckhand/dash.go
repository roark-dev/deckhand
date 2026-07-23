package main

import (
	"github.com/spf13/cobra"

	"github.com/roark-dev/deckhand/internal/tui"
)

var dashCmd = &cobra.Command{
	Use:     "dash",
	Aliases: []string{"dashboard", "watch"},
	Short:   "Open the TUI dashboard",
	RunE: func(cmd *cobra.Command, args []string) error {
		return tui.Run(paths.Socket)
	},
}
