package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/spf13/cobra"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/control"
	"github.com/roark-dev/deckhand/internal/slots"
)

func client() *control.Client { return control.NewClient(paths.Socket) }

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "One-shot daemon status",
	RunE: func(cmd *cobra.Command, args []string) error {
		st, err := client().Status(cmd.Context())
		if err != nil {
			return err
		}
		printStatus(st)
		return nil
	},
}

func printStatus(st *broker.Status) {
	b := st.Broker
	mode := string(b.State)
	if b.Paused {
		mode += " (paused)"
	}
	docker := "up"
	if !st.Docker.OK {
		docker = "DOWN"
	}
	fmt.Printf("broker: %-18s scale set: %q (id %d)  slots: %d  docker: %s\n", mode, b.ScaleSetName, b.ScaleSetID, b.Target, docker)
	fmt.Printf("github: %s\n\n", b.GitHubURL)
	for _, s := range st.Slots {
		line := string(s.State)
		switch {
		case s.State == slots.Running && s.Job != nil:
			line = fmt.Sprintf("BUSY  %s  (%s, %s)", s.Job.DisplayName, s.Job.Repo, time.Since(s.Job.StartedAt).Round(time.Second))
		case s.State == slots.Errored:
			line = "ERROR " + s.Err
		}
		fmt.Printf("  slot %-2d %s\n", s.Index, line)
	}
	c := st.Counters
	fmt.Printf("\njobs: %d ok / %d failed   spawn errors: %d   zombies reclaimed: %d   reconnects: %d\n",
		c.Completed, c.Failed, c.SpawnErrors, c.ZombiesReclaimed, c.Reconnects)
}

var scaleCmd = &cobra.Command{
	Use:   "scale <n>",
	Short: "Set the slot count (takes effect immediately; busy slots drain)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		n, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("not a number: %s", args[0])
		}
		if err := client().Scale(cmd.Context(), n); err != nil {
			return err
		}
		fmt.Printf("slot target set to %d\n", n)
		return nil
	},
}

var pauseCmd = &cobra.Command{
	Use:   "pause",
	Short: "Stop taking new jobs (queued jobs wait on GitHub)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return client().Pause(cmd.Context())
	},
}

var resumeCmd = &cobra.Command{
	Use:   "resume",
	Short: "Resume taking jobs after pause/drain",
	RunE: func(cmd *cobra.Command, args []string) error {
		return client().Resume(cmd.Context())
	},
}

var drainCmd = &cobra.Command{
	Use:   "drain",
	Short: "Finish running jobs, take nothing new (daemon stays up)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return client().Drain(cmd.Context())
	},
}

var (
	flagStopNow   bool
	flagStopForce bool
)

func init() {
	stopCmd.Flags().BoolVar(&flagStopNow, "now", false, "stop immediately instead of draining first")
	stopCmd.Flags().BoolVar(&flagStopForce, "force", false, "with --now: kill mid-job runners (their runs FAIL on GitHub)")
	logsCmd.Flags().Int("tail", 100, "lines to show")
	reclaimCmd.Flags().Bool("force", false, "reclaim even if a job is running (the run will FAIL)")
}

var stopCmd = &cobra.Command{
	Use:   "stop",
	Short: "Stop the daemon (drains by default)",
	RunE: func(cmd *cobra.Command, args []string) error {
		mode := "drain"
		if flagStopNow {
			mode = "now"
		}
		if err := client().Stop(cmd.Context(), mode, flagStopForce); err != nil {
			return err
		}
		fmt.Println("stopping:", mode)
		return nil
	},
}

var logsCmd = &cobra.Command{
	Use:   "logs <slot>",
	Short: "Show a slot's container logs",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		slot, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("not a slot number: %s", args[0])
		}
		tail, _ := cmd.Flags().GetInt("tail")
		out, err := client().SlotLogs(cmd.Context(), slot, tail)
		if err != nil {
			return err
		}
		fmt.Print(out)
		return nil
	},
}

var reclaimCmd = &cobra.Command{
	Use:   "reclaim <slot>",
	Short: "Force-free a wedged slot (manual zombie kill)",
	Args:  cobra.ExactArgs(1),
	RunE: func(cmd *cobra.Command, args []string) error {
		slot, err := strconv.Atoi(args[0])
		if err != nil {
			return fmt.Errorf("not a slot number: %s", args[0])
		}
		force, _ := cmd.Flags().GetBool("force")
		return client().Reclaim(cmd.Context(), slot, force)
	},
}
