// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// createUpgrader creates a WebSocket upgrader with the correct port in CheckOrigin
func createUpgrader(port int) websocket.Upgrader {
	return websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			// Only allow connections from localhost to prevent CSWSH attacks
			origin := r.Header.Get("Origin")
			expectedLocalhost := fmt.Sprintf("http://localhost:%d", port)
			expected127 := fmt.Sprintf("http://127.0.0.1:%d", port)
			return origin == expectedLocalhost ||
				origin == expected127 ||
				origin == "" // Allow non-browser clients (no Origin header)
		},
	}
}

// resizeMessage represents a terminal resize message from the frontend
type resizeMessage struct {
	Type string `json:"type"`
	Cols int    `json:"cols"`
	Rows int    `json:"rows"`
}

// handleSSH handles WebSocket connections for SSH sessions.
func (s *Server) handleSSH(w http.ResponseWriter, r *http.Request) {
	// Extract cluster name and node from URL path: /api/ssh/{cluster}/{node}
	path := strings.TrimPrefix(r.URL.Path, "/api/ssh/")
	parts := strings.Split(path, "/")

	if len(parts) < 2 {
		s.writeError(w, http.StatusBadRequest, "Invalid URL format, expected /api/ssh/{cluster}/{node}")
		return
	}

	clusterName := parts[0]
	nodeStr := parts[1]

	// Parse node number
	nodeNum, err := strconv.Atoi(nodeStr)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("Invalid node number: %s", nodeStr))
		return
	}

	// Upgrade HTTP connection to WebSocket
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Printf("Failed to upgrade WebSocket: %v", err)
		return
	}
	defer func() { _ = conn.Close() }()

	s.logger.Printf("SSH WebSocket connection established for %s:%d", clusterName, nodeNum)

	// Create a pipe for bidirectional communication
	clientReader, clientWriter := io.Pipe()
	serverReader, serverWriter := io.Pipe()

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channel to receive terminal dimensions from WebSocket messages
	termDimsChan := make(chan resizeMessage, 10)

	// Goroutine to read from WebSocket and write to SSH stdin
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = clientWriter.Close() }()
		defer cancel() // Cancel context when WebSocket closes
		defer close(termDimsChan)

		for {
			messageType, data, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
					s.logger.Printf("WebSocket read error: %v", err)
				}
				return
			}

			if messageType == websocket.TextMessage || messageType == websocket.BinaryMessage {
				// Try to parse as JSON resize message
				var resizeMsg resizeMessage
				if err := json.Unmarshal(data, &resizeMsg); err == nil && resizeMsg.Type == "resize" {
					s.logger.Printf("Received terminal resize: cols=%d, rows=%d", resizeMsg.Cols, resizeMsg.Rows)
					termDimsChan <- resizeMsg
					continue // Don't send resize messages to SSH stdin
				}

				// Normal data - send to SSH stdin
				if _, err := clientWriter.Write(data); err != nil {
					s.logger.Printf("Failed to write to SSH stdin: %v", err)
					return
				}
			}
		}
	}()

	// Goroutine to read from SSH stdout and write to WebSocket
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer func() { _ = serverReader.Close() }()

		buf := make([]byte, 4096)
		for {
			n, err := serverReader.Read(buf)
			if err != nil {
				if err != io.EOF {
					s.logger.Printf("Failed to read from SSH stdout: %v", err)
				}
				return
			}

			if err := conn.WriteMessage(websocket.BinaryMessage, buf[:n]); err != nil {
				s.logger.Printf("Failed to write to WebSocket: %v", err)
				return
			}
		}
	}()

	// Run SSH session in a goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		if sshErr := s.runSSHSession(ctx, clusterName, nodeNum, clientReader, serverWriter, termDimsChan); sshErr != nil {
			s.logger.Printf("SSH session error: %v", sshErr)
			_ = conn.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("Error: %v\r\n", sshErr)))
		}
	}()

	// Wait for goroutines to finish
	wg.Wait()

	s.logger.Printf("SSH WebSocket connection closed for %s:%d", clusterName, nodeNum)
}

// runSSHSession executes an SSH session with the given cluster node.
func (s *Server) runSSHSession(
	ctx context.Context,
	clusterName string,
	nodeNum int,
	stdin io.Reader,
	stdout io.Writer,
	termDimsChan <-chan resizeMessage,
) error {
	s.logger.Printf("runSSHSession starting for %s:%d", clusterName, nodeNum)

	// Try to receive initial terminal dimensions with a timeout
	var termCols, termRows int
	timeout := time.After(500 * time.Millisecond)
	select {
	case resizeMsg := <-termDimsChan:
		termCols = resizeMsg.Cols
		termRows = resizeMsg.Rows
		s.logger.Printf("Received initial terminal dimensions: cols=%d, rows=%d", termCols, termRows)
	case <-timeout:
		// Use default dimensions if no resize message received
		termCols = 80
		termRows = 24
		s.logger.Printf("No resize message received within timeout, using defaults: cols=%d, rows=%d", termCols, termRows)
	case <-ctx.Done():
		return ctx.Err()
	}

	// Load the cluster using roachprod's NewCluster
	cluster, err := s.NewCluster(s.logger, clusterName)
	if err != nil {
		s.logger.Printf("Failed to load cluster: %v", err)
		return fmt.Errorf("failed to load cluster %s: %w", clusterName, err)
	}
	s.logger.Printf("Cluster loaded successfully")

	// Get cluster nodes
	nodes := cluster.Nodes()
	s.logger.Printf("Cluster has %d nodes", len(nodes))

	// Validate node number
	if nodeNum < 1 || nodeNum > len(nodes) {
		return fmt.Errorf("invalid node number %d (cluster has %d nodes)", nodeNum, len(nodes))
	}

	// Get the node
	node := nodes[nodeNum-1]
	s.logger.Printf("Selected node: %+v", node)

	// Create a remote session for interactive shell
	cmd := "/bin/bash"
	sess := cluster.NewSession(s.logger, node, cmd, nil)
	if sess == nil {
		s.logger.Printf("NewSession returned nil!")
		return fmt.Errorf("failed to create session")
	}
	s.logger.Printf("Session created")

	// Request a PTY for interactive terminal
	if err := sess.RequestPty(); err != nil {
		s.logger.Printf("Failed to request PTY: %v", err)
		return fmt.Errorf("failed to request PTY: %w", err)
	}
	s.logger.Printf("PTY requested")

	// Set up stdin/stdout
	sess.SetStdin(stdin)
	sess.SetStdout(stdout)
	sess.SetStderr(stdout)
	s.logger.Printf("stdin/stdout/stderr configured")

	// Start the session
	s.logger.Printf("Starting SSH session...")
	if err := sess.Start(); err != nil {
		s.logger.Printf("Failed to start SSH session: %v", err)
		return fmt.Errorf("failed to start SSH session: %w", err)
	}
	s.logger.Printf("SSH session started successfully")

	// Set initial terminal dimensions
	if err := sess.Resize(termCols, termRows); err != nil {
		s.logger.Printf("Failed to set initial terminal size: %v", err)
	} else {
		s.logger.Printf("Set initial terminal size: cols=%d, rows=%d", termCols, termRows)
	}

	// Start a goroutine to handle resize messages
	go func() {
		for {
			select {
			case resizeMsg := <-termDimsChan:
				s.logger.Printf("Resizing terminal: cols=%d, rows=%d", resizeMsg.Cols, resizeMsg.Rows)
				if err := sess.Resize(resizeMsg.Cols, resizeMsg.Rows); err != nil {
					s.logger.Printf("Failed to resize terminal: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for the session to complete
	s.logger.Printf("Waiting for session to complete...")
	err = sess.Wait()
	s.logger.Printf("Session completed with error: %v", err)
	return err
}

// handleLogs handles WebSocket connections for real-time log streaming.
func (s *Server) handleLogs(w http.ResponseWriter, r *http.Request) {
	// Upgrade HTTP connection to WebSocket
	conn, err := s.upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Printf("Failed to upgrade WebSocket for logs: %v", err)
		return
	}

	s.logger.Printf("Log WebSocket connection established")

	// Add client to broadcaster
	s.logBroadcaster.AddClient(conn)
	defer s.logBroadcaster.RemoveClient(conn)

	// Keep connection alive and listen for close
	for {
		_, _, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				s.logger.Printf("Log WebSocket read error: %v", err)
			}
			break
		}
	}

	s.logger.Printf("Log WebSocket connection closed")
}

// BroadcastLog sends a log message to all connected log clients.
func (s *Server) BroadcastLog(message string) {
	s.logBroadcaster.Broadcast(message)
}
