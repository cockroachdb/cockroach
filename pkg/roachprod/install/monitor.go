// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package install

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"text/template"

	"github.com/alessio/shellescape"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/errors"
)

//go:embed scripts/monitor_remote.sh
var monitorRemoteScript string

//go:embed scripts/monitor_local.sh
var monitorLocalScript string

// MonitorEvent is an interface for events that can be sent by the
// monitor.
type MonitorEvent interface {
	String() string
}

// MonitorProcessRunning represents the cockroach process running on a
// node.
type MonitorProcessRunning struct {
	VirtualClusterName string
	SQLInstance        int
	PID                string
}

// MonitorProcessDead represents the cockroach process dying on a node.
type MonitorProcessDead struct {
	VirtualClusterName string
	SQLInstance        int
	ExitCode           string
}

type MonitorError struct {
	Err error
}

// MonitorNoCockroachProcessesError is the error returned when the
// monitor is called on a node that is not running a `cockroach`
// process by the time the monitor runs.
var MonitorNoCockroachProcessesError = errors.New("no cockroach processes running")

// NodeMonitorInfo is a message describing a cockroach process' status.
type NodeMonitorInfo struct {
	// The index of the node (in a SyncedCluster) at which the message originated.
	Node Node
	// Event describes what happened to the node; it is one of
	// MonitorProcessRunning, sent when cockroach is running on a node;
	// MonitorProcessDead, when the cockroach process stops running on a
	// node; or MonitorError, typically indicate networking issues or
	// nodes that have (physically) shut down.
	Event MonitorEvent
}

func (nmi NodeMonitorInfo) String() string {
	return fmt.Sprintf("n%d: %s", nmi.Node, nmi.Event.String())
}

// MonitorOpts is used to pass the options needed by Monitor.
type MonitorOpts struct {
	OneShot bool // Report the status of all targeted nodes once, then exit.
}

type monitorProcess struct {
	processID     int
	lastProcessID int
	status        string
}

type monitorNode struct {
	cluster   *SyncedCluster
	node      Node
	processes map[string]*monitorProcess
	sendEvent func(info NodeMonitorInfo)
	opts      MonitorOpts
}

func virtualClusterDesc(name string, instance int) string {
	if name == SystemInterfaceName {
		return "system interface"
	}
	return fmt.Sprintf("virtual cluster %q, instance %d", name, instance)
}

func (e MonitorProcessRunning) String() string {
	return fmt.Sprintf("cockroach process for %s is running (PID: %s)",
		virtualClusterDesc(e.VirtualClusterName, e.SQLInstance), e.PID,
	)
}

func (e MonitorProcessDead) String() string {
	return fmt.Sprintf("cockroach process for %s died (exit code %s)",
		virtualClusterDesc(e.VirtualClusterName, e.SQLInstance), e.ExitCode,
	)
}
func (e MonitorError) String() string {
	return fmt.Sprintf("error: %s", e.Err.Error())
}

func (m *monitorNode) reset() {
	// Reset all process IDs to 0. Each process ID will be updated from the
	// monitor script output frame. If a process ID is not updated, it is
	// considered dead.
	for _, process := range m.processes {
		process.lastProcessID = process.processID
		process.processID = 0
	}
}

// checkForProcessChanges compares the process IDs from the current and previous
// monitor script output frames. If a change is detected, it emits an event.
func (m *monitorNode) checkForProcessChanges() {
	// Check for dead, or new processes by comparing previous process IDs.
	for vcLabel, process := range m.processes {
		name, instance, err := VirtualClusterInfoFromLabel(vcLabel)
		pid := fmt.Sprintf("%d", process.processID)
		if err != nil {
			parseError := errors.Wrap(err, "failed to parse virtual cluster label")
			m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{parseError}})
			continue
		}
		// Output a dead event whenever the processID changes from a nonzero value
		// to any other value. In particular, we emit a dead event when the node
		// stops (lastProcessID is nonzero, processID is zero), but not when the
		// process then starts again (lastProcessID is zero, processID is nonzero).
		if process.lastProcessID != process.processID {
			status := process.status
			if process.lastProcessID != 0 {
				if process.processID != 0 {
					status = "unknown"
				}
				m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorProcessDead{
					VirtualClusterName: name, SQLInstance: instance, ExitCode: status,
				}})
			}
			if process.processID != 0 {
				m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorProcessRunning{
					VirtualClusterName: name, SQLInstance: instance, PID: pid,
				}})
			}
		}
	}
}

// processMonitorOutput processes the output of the monitor script. It sets the
// process IDs and statuses of the cockroach processes running on the node, and
// updates the last process IDs for comparison in the next iteration.
func (m *monitorNode) processMonitorOutput(lines []string) {
	if len(lines)%2 != 0 {
		m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{errors.New("bad frame from script")}})
		return
	}

	// Reset & update process IDs from the monitor script output frame.
	m.reset()
	for i := 0; i < len(lines); i += 2 {
		vcLine := strings.Split(lines[i], "=")
		if len(vcLine) != 2 {
			m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{errors.New("failed to parse vcs line")}})
			continue
		}
		processID := 0
		if vcLine[1] != "" {
			var parseErr error
			processID, parseErr = strconv.Atoi(vcLine[1])
			if parseErr != nil {
				parseErr = errors.Wrap(parseErr, "failed to parse process ID")
				m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{parseErr}})
				continue
			}
		}
		statusLine := strings.Split(lines[i+1], "=")
		if len(statusLine) != 2 {
			m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{errors.New("failed to parse status line")}})
			continue
		}
		process := m.processes[vcLine[0]]
		if process == nil {
			process = &monitorProcess{}
			m.processes[vcLine[0]] = process
		}
		process.processID = processID
		process.status = statusLine[1]
	}
}

func (m *monitorNode) prepareTemplate() (string, error) {
	params := struct {
		OneShot           bool
		RoachprodEnvRegex string
	}{
		OneShot:           m.opts.OneShot,
		RoachprodEnvRegex: m.cluster.roachprodEnvRegex(m.node),
	}
	monitorScript := monitorRemoteScript
	if m.cluster.IsLocal() {
		monitorScript = monitorLocalScript
	}
	t := template.Must(template.New("monitor").
		Funcs(template.FuncMap{"shesc": func(i interface{}) string {
			return shellescape.Quote(fmt.Sprint(i))
		}}).
		Delims("#{", "#}").
		Parse(monitorScript))

	var buf bytes.Buffer
	if err := t.Execute(&buf, params); err != nil {
		return "", errors.Wrap(err, "failed to execute template")
	}
	return buf.String(), nil
}

func (m *monitorNode) monitorNode(ctx context.Context, l *logger.Logger) {
	script, err := m.prepareTemplate()
	if err != nil {
		m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{err}})
		return
	}

	// This is the exception to funneling all SSH traffic through `c.runCmdOnSingleNode`
	sess := m.cluster.newSession(l, m.node, script, withDebugDisabled())
	defer sess.Close()

	p, err := sess.StdoutPipe()
	if err != nil {
		pipeErr := errors.Wrap(err, "failed to read stdout pipe")
		m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{pipeErr}})
		return
	}
	// Request a PTY so that the script will receive a SIGPIPE when the
	// session is closed.
	if err = sess.RequestPty(); err != nil {
		ptyErr := errors.Wrap(err, "failed to request PTY")
		m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{ptyErr}})
		return
	}

	var readerWg sync.WaitGroup
	readerWg.Add(1)
	go func(p io.Reader) {
		defer readerWg.Done()
		r := bufio.NewReader(p)
		lines := make([]string, 0)
		for {
			lineData, _, err := r.ReadLine()
			line := strings.TrimSpace(string(lineData))
			if err == io.EOF {
				// Only report EOF errors if we are not in a one-shot mode.
				if !m.opts.OneShot {
					m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{err}})
				}
				return
			}
			if err != nil {
				err := errors.Wrap(err, "error reading from session")
				m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{err}})
				return
			}
			// An empty line indicates the end of a frame. Process the frame.
			if len(line) == 0 {
				m.processMonitorOutput(lines)
				m.checkForProcessChanges()
				lines = make([]string, 0)
				continue
			}
			lines = append(lines, line)
		}
	}(p)
	if err := sess.Start(); err != nil {
		err := errors.Wrap(err, "failed to start session")
		m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{err}})
		return
	}

	// Watch for context cancellation, which can happen if the test
	// fails, or if the monitor loop exits.
	go func() {
		<-ctx.Done()
		sess.Close()
		if pc, ok := p.(io.ReadCloser); ok {
			_ = pc.Close()
		}
	}()

	readerWg.Wait()
	// We must call `sess.Wait()` only after finishing reading from the stdout
	// pipe. Otherwise, it can be closed under us, causing the reader to loop
	// infinitely receiving a non-`io.EOF` error.
	if err := sess.Wait(); err != nil {
		// If we got an error waiting for the session but the context
		// is already canceled, do not send an error through the
		// channel; context cancelation happens at the user's request
		// or when the test finishes. In either case, the monitor
		// should quiesce. Reporting the error is confusing and can be
		// noisy in the case of multiple monitors.
		if ctx.Err() != nil {
			return
		}

		err := errors.Wrap(err, "failed to wait for session")
		m.sendEvent(NodeMonitorInfo{Node: m.node, Event: MonitorError{err}})
		return
	}
}

// Monitor writes NodeMonitorInfo for the cluster nodes to the returned channel.
// Infos sent to the channel always have the Node the event refers to, and the
// event itself. See documentation for NodeMonitorInfo for possible event types.
//
// If OneShot is true, infos are retrieved only once for each node and the
// channel is subsequently closed; otherwise the process continues indefinitely
// (emitting new information as the status of the cockroach process changes).
//
// Monitor can detect new processes, started after monitor, and will start
// monitoring them, but it cannot detect processes that have already died before
// the monitor starts.
func (c *SyncedCluster) Monitor(
	l *logger.Logger, ctx context.Context, opts MonitorOpts,
) chan NodeMonitorInfo {
	ch := make(chan NodeMonitorInfo)
	nodes := c.TargetNodes()
	var wg sync.WaitGroup
	monitorCtx, cancel := context.WithCancel(ctx)

	// sendEvent sends the NodeMonitorInfo passed through the channel
	// that is listened to by the caller. Bails if the context is
	// canceled.
	sendEvent := func(info NodeMonitorInfo) {
		// if the monitor's context is already canceled, do not attempt to
		// send the error down the channel, as it is most likely *caused*
		// by the cancelation itself.
		if monitorCtx.Err() != nil {
			return
		}

		select {
		case ch <- info:
			// We were able to send the info through the channel.
		case <-monitorCtx.Done():
			// Don't block trying to send the info.
		}
	}

	// Start monitoring each node.
	wg.Add(len(nodes))
	for _, node := range nodes {
		go func(node Node) {
			defer wg.Done()
			m := &monitorNode{
				cluster:   c,
				node:      node,
				sendEvent: sendEvent,
				processes: make(map[string]*monitorProcess),
				opts:      opts,
			}
			m.monitorNode(monitorCtx, l)
		}(node)
	}

	// Wait for all monitoring goroutines to finish.
	go func() {
		wg.Wait()
		cancel()
		close(ch)
	}()

	return ch
}
