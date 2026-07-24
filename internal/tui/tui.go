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
	plainStyle  = lipgloss.NewStyle() // no-op: renders text at the default fg, no ANSI
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
	ctx     context.Context
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
type actionErrMsg struct{ err error }
type eventMsg bus.Event
type eventsClosedMsg struct{}
type eventsConnectedMsg struct{ ch <-chan bus.Event }
type tickMsg time.Time

// Run starts the dashboard; it returns when the user quits.
func Run(socketPath string) error {
	c := control.NewClient(socketPath)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := model{client: c, ctx: ctx}
	p := tea.NewProgram(m, tea.WithAltScreen())
	_, err := p.Run()
	return err
}

func (m model) Init() tea.Cmd {
	return tea.Batch(m.fetchStatus, m.connectEvents, tick())
}

func tick() tea.Cmd {
	return tea.Tick(time.Second, func(t time.Time) tea.Msg { return tickMsg(t) })
}

func (m model) fetchStatus() tea.Msg {
	ctx, cancel := context.WithTimeout(m.ctx, 2*time.Second)
	defer cancel()
	st, err := m.client.Status(ctx)
	return statusMsg{st, err}
}

// connectEvents (re)opens the event stream; on failure or closure the Update
// loop schedules a retry, so a daemon restart doesn't leave a dead pane.
func (m model) connectEvents() tea.Msg {
	ch, err := m.client.Events(m.ctx)
	if err != nil {
		return eventsClosedMsg{}
	}
	return eventsConnectedMsg{ch: ch}
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

func retryEventsLater() tea.Cmd {
	return tea.Tick(2*time.Second, func(time.Time) tea.Msg { return retryEventsMsg{} })
}

type retryEventsMsg struct{}

// post runs a control action; a failure surfaces as an error banner without
// touching the currently displayed status (the next tick refreshes it).
func (m model) post(f func(context.Context) error) tea.Cmd {
	return func() tea.Msg {
		ctx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
		defer cancel()
		if err := f(ctx); err != nil {
			return actionErrMsg{err}
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
		if msg.st != nil || msg.err != nil {
			m.status, m.err = msg.st, msg.err
			if msg.st != nil && msg.err == nil {
				m.err = nil
			}
		}
		return m, nil
	case actionErrMsg:
		m.err = msg.err
		return m, nil
	case eventsConnectedMsg:
		m.eventCh = msg.ch
		return m, m.waitEvent
	case eventMsg:
		m.events = append(m.events, bus.Event(msg))
		if len(m.events) > 200 {
			m.events = m.events[len(m.events)-200:]
		}
		return m, m.waitEvent
	case eventsClosedMsg:
		m.eventCh = nil
		return m, retryEventsLater()
	case retryEventsMsg:
		return m, m.connectEvents
	case tea.KeyMsg:
		return m.handleKey(msg)
	}
	return m, nil
}

func (m model) handleKey(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	key := msg.String()

	if m.confirm != "" {
		confirm := m.confirm
		m.confirm = ""
		if key == "y" && confirm == "stop" {
			return m, m.post(func(ctx context.Context) error { return m.client.Stop(ctx, "drain", false) })
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
	case broker.Degraded, broker.Starting:
		state = warnStyle.Render(string(br.State))
	case broker.Stopped:
		state = errStyle.Render(string(br.State))
	}
	chips := []string{state}
	if br.Paused {
		chips = append(chips, warnStyle.Render("PAUSED"))
	}
	if br.Draining {
		chips = append(chips, warnStyle.Render("DRAINING"))
	}
	if !st.Docker.OK {
		chips = append(chips, errStyle.Render("DOCKER DOWN"))
	}
	if m.err != nil {
		chips = append(chips, errStyle.Render(truncate(m.err.Error(), 60)))
	}
	fmt.Fprintf(&b, "%s  %s\n", titleStyle.Render("deckhand"), strings.Join(chips, "  "))
	// Header, in workflow-author terms: which repo/org this daemon serves,
	// what to write in `runs-on:`, and how long the GitHub connection has
	// been up.
	fmt.Fprintf(&b, "%s\n\n", dimStyle.Render(fmt.Sprintf(
		"serving %s  ·  runs-on: %s  ·  connected %s",
		strings.TrimPrefix(br.GitHubURL, "https://github.com/"), br.ScaleSetName, ageOrDash(br.SessionAgeSec))))

	// Slot table. Cells are padded to their column width BEFORE styling —
	// ANSI escape codes have zero display width but count in %-Ns padding,
	// which is exactly the misalignment styling-then-padding causes. The JOB
	// column is sized to its widest cell (first pass) so the REPO column that
	// follows lines up across rows; jobColCap bounds it so one long name or
	// error can't shove REPO off-screen.
	const jobColCap = 24
	type rendered struct {
		index      int
		stateText  string
		stateStyle lipgloss.Style
		elapsed    string
		job        jobCol
	}
	rows := make([]rendered, 0, len(st.Slots))
	jobW := len("JOB")
	for _, s := range st.Slots {
		stateText, stateStyle, job := slotCell(s)
		elapsed := time.Since(s.Since).Round(time.Second).String()
		if s.State == slots.Running && s.Job != nil {
			elapsed = time.Since(s.Job.StartedAt).Round(time.Second).String()
		}
		if w := len(job.text); w <= jobColCap && w > jobW {
			jobW = w
		}
		rows = append(rows, rendered{s.Index, stateText, stateStyle, elapsed, job})
	}
	fmt.Fprintln(&b, headerRow.Render(fmt.Sprintf("  %-6s %-11s %-10s %-*s %s", "SLOT", "STATE", "ELAPSED", jobW, "JOB", "REPO")))
	for _, r := range rows {
		state := r.stateStyle.Render(fmt.Sprintf("%-11s", r.stateText))
		if r.job.repo != "" {
			// Pad (truncating if needed) the JOB cell to jobW so REPO aligns.
			job := r.job.style.Render(fmt.Sprintf("%-*s", jobW, truncate(r.job.text, jobW)))
			fmt.Fprintf(&b, "  %-6d %s %-10s %s %s\n", r.index, state, r.elapsed, job, dimStyle.Render(r.job.repo))
			continue
		}
		fmt.Fprintf(&b, "  %-6d %s %-10s %s\n", r.index, state, r.elapsed, r.job.style.Render(r.job.text))
	}

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

// jobCol is the JOB/REPO portion of a slot row. text and repo are PLAIN
// (unstyled) so the caller can pad them to the column width BEFORE styling —
// ANSI codes have zero display width but count in %-Ns padding. repo is empty
// unless the slot is running a job; text is the job name, a status line, or a
// (truncated) error, rendered with style.
type jobCol struct {
	text  string
	style lipgloss.Style
	repo  string
}

// slotCell returns the STATE cell's plain text (padded by the caller, THEN
// styled — see the table comment), its style, and the JOB/REPO columns.
func slotCell(s slots.Slot) (string, lipgloss.Style, jobCol) {
	stateText := string(s.State)
	if s.Drain && s.State != slots.Draining {
		stateText += "*" // drain-marked: finishes its work, then removed
	}
	switch s.State {
	case slots.Running:
		if s.Job != nil {
			return "busy", busyStyle, jobCol{sanitizeCell(s.Job.DisplayName), plainStyle, sanitizeCell(s.Job.Repo)}
		}
		return "busy", busyStyle, jobCol{}
	case slots.Ready:
		return stateText, okStyle, jobCol{text: "waiting for a job", style: dimStyle}
	case slots.Starting:
		return stateText, warnStyle, jobCol{}
	case slots.Reaping:
		return stateText, dimStyle, jobCol{}
	case slots.Errored:
		return "error", errStyle, jobCol{text: truncate(s.Err, 60), style: errStyle}
	case slots.Draining:
		return stateText, warnStyle, jobCol{text: "removed when idle", style: dimStyle}
	default:
		return stateText, dimStyle, jobCol{}
	}
}

func ageOrDash(sec int) string {
	if sec <= 0 {
		return "—"
	}
	return (time.Duration(sec) * time.Second).Round(time.Second).String()
}

// sanitizeCell guards the render path against control characters even though
// producers sanitize at the source (defense in depth for fields that arrive
// via the status JSON rather than the event bus).
func sanitizeCell(s string) string {
	return bus.Sanitize(s)
}

// truncate is rune-aware so multibyte job names never render as mojibake.
func truncate(s string, n int) string {
	s = bus.Sanitize(s)
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n <= 1 {
		return "…"
	}
	return string(r[:n-1]) + "…"
}
