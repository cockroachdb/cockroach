// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ui

import (
	"sync"

	"github.com/gorilla/websocket"
)

// logBroadcasterImpl manages WebSocket connections for broadcasting logs.
type logBroadcasterImpl struct {
	mu      sync.RWMutex
	clients map[*websocket.Conn]bool
}

// NewLogBroadcaster creates a new log broadcaster.
func NewLogBroadcaster() *logBroadcasterImpl {
	return &logBroadcasterImpl{
		clients: make(map[*websocket.Conn]bool),
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
	conn.Close()
}

// Broadcast sends a log message to all connected clients.
func (lb *logBroadcasterImpl) Broadcast(message string) {
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
