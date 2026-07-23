// Package tui is the deckhand dashboard: live slot table, event feed and
// keyboard controls, talking to the daemon over the control socket.
package tui

import (
	"context"
	"fmt"
	"strings"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"

	"github.com/roark-dev/deckhand/internal/broker"
	"github.com/roark-dev/deckhand/internal/bus"
	"github.com/roark-dev/deckhand/internal/control"
	"github.com/roark-dev/deckhand/internal/slots"
)

var (
	titleStyle  = lipgloss.NewStyle().Bold(true)
	dimStyle    = lipgloss.NewStyle().Faint(true)
	okStyle     = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	warnStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	errStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	busyStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("4")).Bold(true)
	actionStyle = lipgloss.NewStyle().Foreground(lipgloss.Color("5"))
	headerRow   = lipgloss.NewStyle().Faint(true).Underline(true)
)

type model struct {
	client  *control.Client
	status  *broker.Status
	err     error
	events  []bus.Event
	eventCh <-chan bus.Event
	width   int
	confirm string // pending destructive action, e.g. "stop"
}

type statusMsg struct {
	st  *broker.Status
	err error
}
type eventMsg bus.Event
type eventsClosedMsg struct{}
type tickMsg time.Time

// Run starts the dashboard; it returns when the user quits.
func Run(socketPath string) error {
	c := control.NewClient(socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := model{client: c}
	if ch, err := c.Events(ctx); err == nil {
		m.eventCh = ch
	}
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.fetchStatus, m.waitEvent, tick())
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m model) fetchStatus() tea.Msg {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	st, err := m.client.Status(ctx)
	return statusMsg{st, err}
}

func (m model) waitEvent() tea.Msg {
	if m.eventCh == nil {
		return eventsClosedMsg{}
	}
	ev, ok := <-m.eventCh
	if !ok {
		return eventsClosedMsg{}
	}
	return eventMsg(ev)
}

func (m model) post(f func(context.Context) error) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := f(ctx); err != nil {
			return statusMsg{m.status, err}
		}
		return m.fetchStatus()
	}
}

func (m model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width = msg.Width
		return m, nil
	case tickMsg:
		return m, tea.Batch(m.fetchStatus, tick())
	case statusMsg:
		m.status, m.err = msg.st, msg.err
		return m, nil
	case eventMsg:
		m.events = append(m.events, bus.Event(msg))
		if len(m.events) > 200 {
			m.events = m.events[len(m.events)-200:]
		}
		return m, m.waitEvent
	case eventsClosedMsg:
		return m, nil
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	if m.confirm != "" {
		defer func() {}()
		confirm := m.confirm
		m.confirm = ""
		if key == "y" {
			switch confirm {
			case "stop":
				return m, m.post(func(ctx context.Context) error { return m.client.Stop(ctx, "drain", false) })
			}
		}
		return m, nil
	}

	switch key {
	case "q", "ctrl+c":
		return m, tea.Quit
	case "+", "=":
		if st := m.status; st != nil {
			n := st.Broker.Target + 1
			return m, m.post(func(ctx context.Context) error { return m.client.Scale(ctx, n) })
		}
	case "-", "_":
		if st := m.status; st != nil && st.Broker.Target > 0 {
			n := st.Broker.Target - 1
			return m, m.post(func(ctx context.Context) error { return m.client.Scale(ctx, n) })
		}
	case "p":
		if st := m.status; st != nil {
			if st.Broker.Paused {
				return m, m.post(m.client.Resume)
			}
			return m, m.post(m.client.Pause)
		}
	case "d":
		return m, m.post(m.client.Drain)
	case "r":
		return m, m.post(m.client.Resume)
	case "s":
		m.confirm = "stop"
		return m, nil
	}
	return m, nil
}

func (m model) View() string {
	var b strings.Builder
	if m.status == nil {
		if m.err != nil {
			return errStyle.Render("cannot reach daemon: "+m.err.Error()) + "\n" + dimStyle.Render("start it with `deckhand up` — q to quit")
		}
		return "connecting…"
	}
	st := m.status
	br := st.Broker

	// Header
	state := okStyle.Render(string(br.State))
	switch br.State {
	case broker.Degraded:
		state = warnStyle.Render(string(br.State))
	case broker.Draining, broker.Stopped:
		state = errStyle.Render(string(br.State))
	}
	chips := []string{state}
	if br.Paused {
		chips = append(chips, warnStyle.Render("PAUSED"))
	}
	if !st.Docker.OK {
		chips = append(chips, errStyle.Render("DOCKER DOWN"))
	}
	if m.err != nil {
		chips = append(chips, errStyle.Render(m.err.Error()))
	}
	fmt.Fprintf(&b, "%s  %s\n", titleStyle.Render("deckhand"), strings.Join(chips, "  "))
	fmt.Fprintf(&b, "%s\n\n", dimStyle.Render(fmt.Sprintf("scale set %q on %s — session %s", br.ScaleSetName, br.GitHubURL, ageOrDash(br.SessionAgeSec))))

	// Slot table
	busy := 0
	fmt.Fprintln(&b, headerRow.Render(fmt.Sprintf("  %-5s %-9s %-8s %s", "SLOT", "STATE", "ELAPSED", "JOB")))
	for _, s := range st.Slots {
		stateStr, detail := renderSlot(s)
		if s.State == slots.Running {
			busy++
		}
		elapsed := time.Since(s.Since).Round(time.Second).String()
		if s.State == slots.Running && s.Job != nil {
			elapsed = time.Since(s.Job.StartedAt).Round(time.Second).String()
		}
		fmt.Fprintf(&b, "  %-5d %-9s %-8s %s\n", s.Index, stateStr, elapsed, detail)
	}
	fmt.Fprintf(&b, "\n  %d/%d busy   jobs %s ok / %s failed   zombies reclaimed %d\n",
		busy, br.Target,
		okStyle.Render(fmt.Sprint(st.Counters.Completed)),
		errStyle.Render(fmt.Sprint(st.Counters.Failed)),
		st.Counters.ZombiesReclaimed)

	// Events
	fmt.Fprintln(&b, "\n"+dimStyle.Render(strings.Repeat("─", max(20, m.width-2))))
	events := m.events
	if len(events) > 8 {
		events = events[len(events)-8:]
	}
	for _, ev := range events {
		style := dimStyle
		switch ev.Level {
		case bus.Warn:
			style = warnStyle
		case bus.Error:
			style = errStyle
		case bus.Action:
			style = actionStyle
		}
		slot := "      "
		if ev.Slot >= 0 {
			slot = fmt.Sprintf("slot %d", ev.Slot)
		}
		fmt.Fprintf(&b, " %s %s %s\n", dimStyle.Render(ev.Time.Format("15:04:05")), style.Render(slot), truncate(ev.Msg, max(20, m.width-20)))
	}

	if m.confirm != "" {
		fmt.Fprintln(&b, "\n"+warnStyle.Render("stop the daemon (drains first)? y/n"))
	} else {
		fmt.Fprintln(&b, "\n"+dimStyle.Render("[+/-] slots  [p] pause/resume  [d] drain  [r] resume  [s] stop daemon  [q] quit"))
	}
	return b.String()
}

func renderSlot(s slots.Slot) (string, string) {
	switch s.State {
	case slots.Running:
		detail := ""
		if s.Job != nil {
			detail = fmt.Sprintf("%s  %s", s.Job.DisplayName, dimStyle.Render(s.Job.Repo))
		}
		return busyStyle.Render("busy"), detail
	case slots.Ready:
		return okStyle.Render("ready"), dimStyle.Render("waiting for a job")
	case slots.Starting:
		return warnStyle.Render("starting"), ""
	case slots.Reaping:
		return dimStyle.Render("reaping"), ""
	case slots.Errored:
		return errStyle.Render("error"), errStyle.Render(truncate(s.Err, 60))
	case slots.Draining:
		return warnStyle.Render("draining"), dimStyle.Render("removed when idle")
	default:
		return dimStyle.Render("idle"), ""
	}
}

func ageOrDash(sec int) string {
	if sec <= 0 {
		return "—"
	}
	return (time.Duration(sec) * time.Second).Round(time.Second).String()
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	if n <= 1 {
		return "…"
	}
	return s[:n-1] + "…"
}
