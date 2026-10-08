package main

import (
	"fmt"
	"proxyblob/internal/operator"
	"time"

	"github.com/jedib0t/go-pretty/table"
)

// RenderListenerTable formats listener information into a human-readable table.
func RenderListenerTable(listeners []operator.ListenerInfo, defaultID string) string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)

	t.AppendHeader(table.Row{
		"Name",
		"Status",
		"Protocol",
		"Started At",
	})

	// ANSI color codes
	const (
		colorReset = "\033[0m"
		colorRed   = "\033[31m"
		colorGreen = "\033[32m"
	)

	for _, l := range listeners {
		marker := ""
		if l.ID == defaultID {
			marker = " (default)"
		}

		startedAt := ""
		if l.Status == "running" && !l.StartedAt.IsZero() {
			startedAt = l.StartedAt.Format("2006-01-02 15:04:05")
		}

		// Color the status: green for running, red for stopped
		statusColor := colorReset
		switch l.Status {
		case "running":
			statusColor = colorGreen
		case "stopped":
			statusColor = colorRed
		}
		statusDisplay := statusColor + l.Status + colorReset

		t.AppendRow(table.Row{
			l.ID + marker,
			statusDisplay,
			l.Protocol,
			startedAt,
		})
	}

	return t.Render()
}

// RenderAgentTable formats agent information at now. Authorization expiry is
// informational: it neither guarantees liveness nor schedules disconnection.
func RenderAgentTable(agents []operator.AgentInfo, now time.Time) string {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)

	t.AppendHeader(table.Row{
		"Agent ID",
		"Info",
		"Listener",
		"Proxy Port",
		"Connected At",
		"Last Seen",
		"Session Expires (UTC)",
		"Authorization Remaining",
	})

	for _, a := range agents {
		expiry, remaining := "unknown", "unknown"
		if !a.SessionExpiry.IsZero() {
			expiry = a.SessionExpiry.UTC().Format(time.RFC3339Nano)
			if !a.SessionExpiry.After(now) {
				remaining = "expired"
			} else if a.SessionExpiry.Sub(now) < time.Second {
				remaining = "<1s"
			} else {
				remaining = a.SessionExpiry.Sub(now).Truncate(time.Second).String()
			}
		}
		t.AppendRow(table.Row{
			a.ID,
			a.Info,
			a.ListenerID,
			a.ProxyPort,
			a.CreatedAt.Format("2006-01-02 15:04:05"),
			formatRelativeTime(a.LastSeenTime()),
			expiry, remaining,
		})
	}

	return t.Render()
}

// formatRelativeTime returns a human-readable relative time string.
func formatRelativeTime(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return fmt.Sprintf("%ds ago", int(d.Seconds()))
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	default:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	}
}
