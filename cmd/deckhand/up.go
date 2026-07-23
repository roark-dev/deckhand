package main

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/control"
	"github.com/roark-dev/deckhand/internal/metrics"
)

var (
	flagTakeover bool
	flagLogJSON  bool
)

func init() {
	upCmd.Flags().BoolVar(&flagTakeover, "takeover", false, "adopt an existing scale set with this name even if this deckhand did not create it")
	upCmd.Flags().BoolVar(&flagLogJSON, "log-json", false, "log as JSON")
}

var upCmd = &cobra.Command{
	Use:   "up",
	Short: "Run the deckhand daemon (foreground)",
	Long: `Runs the daemon: registers/attaches the runner scale set, listens for
queued jobs and runs them in ephemeral containers. Run it under launchd or
systemd for background operation (templates/ in the repo).`,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := loadConfig()
		if err != nil {
			return err
		}
		if err := os.MkdirAll(paths.Home, 0o700); err != nil {
			return err
		}

		// Single-instance lock: one daemon per state dir, which also keeps us
		// to GitHub's one-message-session-per-scale-set constraint.
		lock, err := os.OpenFile(paths.LockFile, os.O_CREATE|os.O_RDWR, 0o600)
		if err != nil {
			return err
		}
		defer lock.Close()
		if err := unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
			return fmt.Errorf("another deckhand daemon is already running (lock %s held)", paths.LockFile)
		}

		var logW io.Writer = os.Stderr
		if f, err := os.OpenFile(paths.LogFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600); err == nil {
			defer f.Close()
			logW = io.MultiWriter(os.Stderr, f)
		}
		var handler slog.Handler
		if flagLogJSON {
			handler = slog.NewJSONHandler(logW, nil)
		} else {
			handler = slog.NewTextHandler(logW, nil)
		}
		logger := slog.New(handler)

		eventBus := bus.New()
		b, err := broker.New(cfg, paths, logger, eventBus, flagTakeover)
		if err != nil {
			return err
		}

		ctx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
		defer cancel()
		shutdown := func(cause string) {
			logger.Info("shutdown", "cause", cause)
			cancel()
		}

		ctl := control.NewServer(b, eventBus, version, shutdown)
		go func() {
			if err := ctl.Serve(ctx, paths.Socket); err != nil {
				logger.Error("control server failed", "error", err)
			}
		}()
		if cfg.Metrics.Listen != "" {
			go func() {
				if err := metrics.Serve(ctx, cfg.Metrics.Listen, b); err != nil {
					logger.Error("metrics server failed", "error", err)
				}
			}()
		}

		fmt.Fprintf(os.Stderr, "deckhand %s — scale set %q on %s, %d slot(s)\n", version, cfg.ScaleSet.Name, cfg.GitHub.URL, cfg.Slots.Count)
		fmt.Fprintf(os.Stderr, "dashboard: `deckhand dash` (in another terminal)\n")
		return b.Run(ctx)
	},
}
