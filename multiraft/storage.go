// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package multiraft

import (
	"reflect"

	"github.com/golang/glog"
)

// LogEntry represents a persistent log entry.  Payloads are opaque to the raft system.
// TODO(bdarnell): we will need both opaque payloads for the application and raft-subsystem
// payloads for membership changes.
type LogEntry struct {
	Term    int
	Payload []byte
}

// GroupMetadata represents the persistent state of a group.
type GroupMetadata struct {
	Members     []NodeID
	CurrentTerm int
	VotedFor    NodeID
}

// Equal compares two GroupMetadatas.
func (g *GroupMetadata) Equal(other *GroupMetadata) bool {
	// TODO(bdarnell): write a real equality function once membership features have finalized.
	return reflect.DeepEqual(g, other)
}

// GroupPersistentState is a unified view of the readable data (except for log entries)
// about a group; used by Storage.LoadGroups.
type GroupPersistentState struct {
	GroupID      GroupID
	Metadata     GroupMetadata
	LastLogIndex int
	LastLogTerm  int
}

// LogEntryState is used by Storage.GetLogEntries to bundle a LogEntry with its index
// and an optional error.
type LogEntryState struct {
	Index int
	Entry LogEntry
	Error error
}

// The Storage interface is supplied by the application to manage persistent storage
// of raft data.
type Storage interface {
	// LoadGroups is called at startup to load all previously-existing groups.
	// The returned channel should be closed once all groups have been loaded.
	LoadGroups() <-chan *GroupPersistentState

	// SetGroupMetadata is called to update the metadata for the given group.
	SetGroupMetadata(groupID GroupID, metadata *GroupMetadata) error

	// AppendLogEntries is called to add entries to the log.
	AppendLogEntries(groupID GroupID, firstIndex int, entries []*LogEntry) error

	// TruncateLog is called to delete all log entries with index > lastIndex.
	TruncateLog(groupID GroupID, lastIndex int) error

	// GetLogEntry is called to synchronously retrieve an entry from the log.
	GetLogEntry(groupID GroupID, index int) (*LogEntry, error)

	// GetLogEntries is called to asynchronously retrieve entries from the log,
	// from firstIndex to lastIndex inclusive.  If there is an error the storage
	// layer should send one LogEntryState with a non-nil error and then close the
	// channel.
	GetLogEntries(groupID GroupID, firstIndex, lastIndex int, ch chan<- *LogEntryState)
}

type memoryGroup struct {
	metadata GroupMetadata
}

// MemoryStorage is an in-memory implementation of Storage for testing.
type MemoryStorage struct {
	groups map[GroupID]*memoryGroup
}

// Verifying implementation of Storage interface.
var _ Storage = (*MemoryStorage)(nil)

// NewMemoryStorage creates a MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{make(map[GroupID]*memoryGroup)}
}

// LoadGroups implements the Storage interface.
func (m *MemoryStorage) LoadGroups() <-chan *GroupPersistentState {
	// TODO(bdarnell): replay the group state.
	ch := make(chan *GroupPersistentState)
	close(ch)
	return ch
}

// SetGroupMetadata implements the Storage interface.
func (m *MemoryStorage) SetGroupMetadata(groupID GroupID, metadata *GroupMetadata) error {
	m.getGroup(groupID).metadata = *metadata
	return nil
}

// AppendLogEntries implements the Storage interface.
func (m *MemoryStorage) AppendLogEntries(groupID GroupID, firstIndex int,
	entries []*LogEntry) error {
	panic("unimplemented")
}

// TruncateLog implements the Storage interface.
func (m *MemoryStorage) TruncateLog(groupID GroupID, lastIndex int) error {
	panic("unimplemented")
}

// GetLogEntry implements the Storage interface.
func (m *MemoryStorage) GetLogEntry(groupID GroupID, index int) (*LogEntry, error) {
	panic("unimplemented")
}

// GetLogEntries implements the Storage interface.
func (m *MemoryStorage) GetLogEntries(groupID GroupID, firstIndex, lastIndex int,
	ch chan<- *LogEntryState) {
	panic("unimplemented")
}

// getGroup returns a mutable memoryGroup object, creating if necessary.
func (m *MemoryStorage) getGroup(groupID GroupID) *memoryGroup {
	g, ok := m.groups[groupID]
	if !ok {
		g = &memoryGroup{}
		m.groups[groupID] = g
	}
	return g
}

// groupWriteRequest represents a set of changes to make to a group.
type groupWriteRequest struct {
	metadata  *GroupMetadata
	nextIndex int
	entries   []*LogEntry
}

// newGroupWriteRequest creates a groupWriteRequest.
func newGroupWriteRequest() *groupWriteRequest {
	return &groupWriteRequest{nextIndex: -1}
}

// writeRequest is a collection of groupWriteRequests.
type writeRequest struct {
	groups map[GroupID]*groupWriteRequest
}

// newWriteRequest creates a writeRequest.
func newWriteRequest() *writeRequest {
	return &writeRequest{make(map[GroupID]*groupWriteRequest)}
}

// groupWriteResponse represents the final state of a persistent group.
// metadata may be nil and lastIndex and lastTerm may be -1 if the respective
// state was not changed (which may be because there were no changes in the request
// or due to an error)
type groupWriteResponse struct {
	metadata  *GroupMetadata
	lastIndex int
	lastTerm  int
}

// writeResponse is a collection of groupWriteResponses.
type writeResponse struct {
	groups map[GroupID]*groupWriteResponse
}

// writeTask manages a goroutine that interacts with the storage system.
type writeTask struct {
	storage Storage
	stopper chan struct{}

	// ready is an unbuffered channel used for synchronization.  If writes to this channel do not
	// block, the writeTask is ready to receive a request.
	ready chan struct{}

	// For every request written to 'in', one response will be written to 'out'.
	in  chan *writeRequest
	out chan *writeResponse
}

// newWriteTask creates a writeTask.  The caller should start the task after creating it.
func newWriteTask(storage Storage) *writeTask {
	return &writeTask{
		storage: storage,
		stopper: make(chan struct{}),
		ready:   make(chan struct{}),
		in:      make(chan *writeRequest, 1),
		out:     make(chan *writeResponse, 1),
	}
}

// start runs the storage loop.  Blocks until stopped, so should be run in a goroutine.
func (w *writeTask) start() {
	for {
		var request *writeRequest
		select {
		case <-w.ready:
			continue
		case <-w.stopper:
			return
		case request = <-w.in:
		}
		glog.V(6).Infof("writeTask got request %#v", *request)
		response := &writeResponse{make(map[GroupID]*groupWriteResponse)}

		for groupID, groupReq := range request.groups {
			groupResp := &groupWriteResponse{nil, -1, -1}
			response.groups[groupID] = groupResp
			if groupReq.metadata != nil {
				err := w.storage.SetGroupMetadata(groupID, groupReq.metadata)
				if err != nil {
					continue
				}
				groupResp.metadata = groupReq.metadata
			}
			if len(groupReq.entries) > 0 {
				err := w.storage.AppendLogEntries(groupID, groupReq.nextIndex, groupReq.entries)
				if err != nil {
					continue
				}
				groupResp.lastIndex = groupReq.nextIndex + len(groupReq.entries) - 1
				groupResp.lastTerm = groupReq.entries[len(groupReq.entries)-1].Term
			}
		}
		w.out <- response
	}
}

// stop the running task.
func (w *writeTask) stop() {
	close(w.stopper)
}
