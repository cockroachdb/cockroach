// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"strings"
	"sync"

	"github.com/gorilla/websocket"
)

const recentLogBufferSize = 100

// logBroadcasterImpl manages WebSocket connections for broadcasting logs.
type logBroadcasterImpl struct {
	mu         sync.RWMutex
	clients    map[*websocket.Conn]bool
	recentLogs []string // Ring buffer of recent log messages
	logIndex   int      // Current position in ring buffer
}

// NewLogBroadcaster creates a new log broadcaster.
func NewLogBroadcaster() *logBroadcasterImpl {
	return &logBroadcasterImpl{
		clients:    make(map[*websocket.Conn]bool),
		recentLogs: make([]string, recentLogBufferSize),
	}
}

// AddClient registers a new WebSocket client.
func (lb *logBroadcasterImpl) AddClient(conn *websocket.Conn) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.clients[conn] = true
}

// RemoveClient unregisters a WebSocket client.
func (lb *logBroadcasterImpl) RemoveClient(conn *websocket.Conn) {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	delete(lb.clients, conn)
	_ = conn.Close() // Ignore close errors
}

// Broadcast sends a log message to all connected clients.
func (lb *logBroadcasterImpl) Broadcast(message string) {
	lb.mu.Lock()
	// Store message in ring buffer
	lb.recentLogs[lb.logIndex] = message
	lb.logIndex = (lb.logIndex + 1) % recentLogBufferSize
	lb.mu.Unlock()

	lb.mu.RLock()
	defer lb.mu.RUnlock()

	for client := range lb.clients {
		err := client.WriteMessage(websocket.TextMessage, []byte(message))
		if err != nil {
			// Client disconnected, will be cleaned up by the read loop
			continue
		}
	}
}

// CheckForSSOError checks recent log messages for AWS SSO token errors.
func (lb *logBroadcasterImpl) CheckForSSOError() bool {
	lb.mu.RLock()
	defer lb.mu.RUnlock()

	for _, msg := range lb.recentLogs {
		if strings.Contains(msg, "Error loading SSO Token") ||
		   strings.Contains(msg, "Token has expired") ||
		   strings.Contains(msg, "Error when retrieving token from sso") {
			return true
		}
	}
	return false
}

// ClearRecentLogs clears the recent logs buffer.
func (lb *logBroadcasterImpl) ClearRecentLogs() {
	lb.mu.Lock()
	defer lb.mu.Unlock()
	lb.recentLogs = make([]string, recentLogBufferSize)
	lb.logIndex = 0
}
