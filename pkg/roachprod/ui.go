// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachprod

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/roachprod/logger"
	"github.com/cockroachdb/cockroach/pkg/roachprod/ui"
	"github.com/creack/pty"
)

// createBroadcastLogger creates a logger that writes to both the original logger
// and broadcasts to WebSocket clients.
func createBroadcastLogger(originalLogger *logger.Logger, broadcaster ui.LogBroadcaster) (*logger.Logger, error) {
	// Wrap stdout and stderr with broadcast writers
	broadcastStdout := ui.NewBroadcastWriter(originalLogger.Stdout, broadcaster)
	broadcastStderr := ui.NewBroadcastWriter(originalLogger.Stderr, broadcaster)

	// Create a new logger config with the broadcast writers
	cfg := &logger.Config{
		Stdout: broadcastStdout,
		Stderr: broadcastStderr,
	}

	// Create and return the new logger (without a file path, so it just uses the writers)
	return cfg.NewLogger("")
}

// openBrowser opens the specified URL in the default browser
func openBrowser(url string) error {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("open", url)
	case "linux":
		cmd = exec.Command("xdg-open", url)
	case "windows":
		cmd = exec.Command("rundll32", "url.dll,FileProtocolHandler", url)
	default:
		return fmt.Errorf("unsupported platform")
	}
	return cmd.Start()
}

// StartUIServer starts the roachprod web UI server on the specified port.
func StartUIServer(port int) error {
	l := config.Logger
	server := ui.NewServer(port, l)

	// Create a broadcast logger that wraps the config logger and sends to WebSocket
	broadcastLogger, err := createBroadcastLogger(l, server.GetLogBroadcaster())
	if err != nil {
		return fmt.Errorf("failed to create broadcast logger: %w", err)
	}

	// Create and set the cluster manager with the broadcast logger
	clusterManager := NewUIClusterManager(broadcastLogger)
	server.SetClusterManager(clusterManager)

	// Set the NewCluster function
	server.SetNewCluster(func(logger *logger.Logger, name string) (ui.ClusterOperations, error) {
		cluster, err := NewCluster(logger, name)
		if err != nil {
			return nil, err
		}
		return &clusterAdapter{cluster}, nil
	})

	// Open browser after a short delay to allow server to start
	go func() {
		time.Sleep(500 * time.Millisecond)
		url := fmt.Sprintf("http://localhost:%d", port)
		if err := openBrowser(url); err != nil {
			l.Printf("Could not open browser automatically: %v", err)
			l.Printf("Please open %s manually", url)
		}
	}()

	// Start the server
	return server.Start(context.Background())
}

// clusterAdapter adapts install.SyncedCluster to ui.ClusterOperations.
type clusterAdapter struct {
	*install.SyncedCluster
}

func (c *clusterAdapter) Nodes() []ui.Node {
	nodes := make([]ui.Node, len(c.SyncedCluster.Nodes))
	for i := range c.SyncedCluster.Nodes {
		nodes[i] = ui.Node{Index: i + 1}
	}
	return nodes
}

func (c *clusterAdapter) NewSession(l *logger.Logger, node ui.Node, cmd string, args []string) ui.Session {
	// Get the actual install.Node
	if node.Index < 1 || node.Index > len(c.SyncedCluster.Nodes) {
		return nil
	}
	installNode := c.SyncedCluster.Nodes[node.Index-1]

	// Validate that installNode is within VMs bounds
	if installNode < 1 || installNode > install.Node(len(c.SyncedCluster.VMs)) {
		return nil
	}

	// Get user and host from VMs
	user := c.SyncedCluster.VMs[installNode-1].RemoteUser
	host := c.SyncedCluster.Host(installNode)

	//  Build the command with args
	fullCmd := cmd
	if len(args) > 0 {
		fullCmd = cmd + " " + strings.Join(args, " ")
	}

	return newSSHSession(l, user, host, fullCmd)
}

// newSSHSession creates a new SSH session.
// This duplicates some logic from install.newRemoteSession but is necessary
// because that function is unexported.
func newSSHSession(l *logger.Logger, user, host, cmd string) *sshSession {
	args := []string{
		user + "@" + host,
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "StrictHostKeyChecking=no",
		"-o", "ServerAliveInterval=60",
		"-o", "ConnectTimeout=5",
		"-q", // quiet mode
	}

	// Add SSH auth args (identity files)
	for _, name := range config.DefaultPubKeyNames {
		keyPath := filepath.Join(config.SSHDirectory, name)
		if _, err := os.Stat(keyPath); err == nil {
			args = append(args, "-i", keyPath)
		}
	}

	// For interactive shell, we need to set TERM and force bash to be interactive
	// Wrap the command to set environment variables
	if cmd == "/bin/bash" {
		cmd = "TERM=xterm-256color /bin/bash -i"
	} else if strings.Contains(cmd, "exec /bin/bash'") {
		// Command uses bash -c 'export ... exec /bin/bash' pattern
		// Add TERM export and -i flag to the exec
		cmd = strings.Replace(cmd, "exec /bin/bash'", "export TERM=xterm-256color; exec /bin/bash -i'", 1)
	} else if strings.Contains(cmd, "&& /bin/bash") {
		// Command has export statements like "export COLUMNS=X LINES=Y && /bin/bash"
		// Add TERM export and -i flag
		cmd = strings.Replace(cmd, "&& /bin/bash", "&& export TERM=xterm-256color && /bin/bash -i", 1)
	} else if strings.HasSuffix(cmd, "/bin/bash") {
		// Command has environment variables like "COLUMNS=X LINES=Y /bin/bash"
		// Add TERM and -i flag
		cmd = strings.Replace(cmd, "/bin/bash", "TERM=xterm-256color /bin/bash -i", 1)
	}

	args = append(args, cmd)

	// Debug: log the full SSH command
	l.Printf("DEBUG: SSH command: ssh %v", args)

	ctx, cancel := context.WithCancel(context.Background())
	execCmd := exec.CommandContext(ctx, "ssh", args...)

	return &sshSession{
		Cmd:    execCmd,
		cancel: cancel,
	}
}

// sshSession wraps exec.Cmd with PTY support to implement ui.Session interface.
type sshSession struct {
	*exec.Cmd
	cancel     func()
	ptmx       *os.File // PTY master file
	stdin      io.Reader
	stdout     io.Writer
	stderr     io.Writer
	mu         sync.Mutex // protects ptyRequested and started
	ptyRequested bool
	started    bool
}

func (s *sshSession) RequestPty() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Add the `-tt` option for pseudo-terminal (force allocation)
	// Args structure: ["ssh", "user@host", "-o", "...", "-o", "...", ..., cmd]
	if len(s.Cmd.Args) > 1 {
		// Insert "-tt" as Args[1], shifting everything else right
		newArgs := make([]string, 0, len(s.Cmd.Args)+1)
		newArgs = append(newArgs, s.Cmd.Args[0]) // "ssh"
		newArgs = append(newArgs, "-tt")          // force PTY
		newArgs = append(newArgs, s.Cmd.Args[1:]...)
		s.Cmd.Args = newArgs
	}
	s.ptyRequested = true
	return nil
}

func (s *sshSession) SetStdin(r io.Reader) {
	s.stdin = r
}

func (s *sshSession) SetStdout(w io.Writer) {
	s.stdout = w
}

func (s *sshSession) SetStderr(w io.Writer) {
	s.stderr = w
}

func (s *sshSession) Start() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.started {
		return fmt.Errorf("session already started")
	}
	s.started = true

	if !s.ptyRequested {
		// No PTY requested, use normal exec
		s.Cmd.Stdin = s.stdin
		s.Cmd.Stdout = s.stdout
		s.Cmd.Stderr = s.stderr
		return s.Cmd.Start()
	}

	// Start the SSH command with a PTY with default size
	// We'll resize it immediately after if needed
	ptmx, err := pty.Start(s.Cmd)
	if err != nil {
		return fmt.Errorf("failed to start with PTY: %w", err)
	}
	s.ptmx = ptmx

	// Copy stdin to PTY
	if s.stdin != nil {
		go func() {
			_, _ = io.Copy(ptmx, s.stdin)
		}()
	}

	// Copy PTY to stdout
	if s.stdout != nil {
		go func() {
			_, _ = io.Copy(s.stdout, ptmx)
		}()
	}

	return nil
}

func (s *sshSession) Wait() error {
	err := s.Cmd.Wait()
	if s.ptmx != nil {
		s.ptmx.Close()
	}
	return err
}

func (s *sshSession) Resize(cols, rows int) error {
	s.mu.Lock()
	ptmx := s.ptmx
	s.mu.Unlock()

	if ptmx == nil {
		fmt.Fprintf(os.Stderr, "DEBUG Resize: ptmx is nil, cannot resize\n")
		return nil // No PTY, resize not supported
	}

	fmt.Fprintf(os.Stderr, "DEBUG Resize: Setting PTY size to cols=%d, rows=%d\n", cols, rows)

	size := &pty.Winsize{
		Rows: uint16(rows),
		Cols: uint16(cols),
	}

	err := pty.Setsize(ptmx, size)
	if err != nil {
		fmt.Fprintf(os.Stderr, "DEBUG Resize: pty.Setsize failed: %v\n", err)
	} else {
		fmt.Fprintf(os.Stderr, "DEBUG Resize: pty.Setsize succeeded\n")
	}
	return err
}

func (s *sshSession) Kill() error {
	s.cancel()
	if s.ptmx != nil {
		s.ptmx.Close()
	}
	if s.Process != nil {
		return s.Process.Kill()
	}
	return nil
}

// NewCluster creates a new SyncedCluster for the given cluster name.
// This is exported for use by the UI package.
func NewCluster(l *logger.Logger, name string, opts ...install.ClusterSettingOption) (*install.SyncedCluster, error) {
	return newCluster(l, name, opts...)
}
