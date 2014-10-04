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
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package multiraft

import (
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// LogEntryType is the type of a LogEntry.
type LogEntryType int8

// LogEntryCommand is for application-level commands sent via MultiRaft.SendCommand;
// other LogEntryTypes are for internal use.
const (
	LogEntryCommand LogEntryType = iota
	LogEntryChangeMembership
)

// LogEntry represents a persistent log entry. Payloads are interpreted according to
// the Type field; Payloads of LogEntryCommand are opaque to the raft system.
type LogEntry struct {
	Term    int
	Index   int
	Type    LogEntryType
	Payload []byte
}

// ChangeMembershipOperation indicates the operation being performed by a ChangeMembershipPayload.
type ChangeMembershipOperation int8

// Values for ChangeMembershipOperation.
const (
	// ChangeMembershipAddObserver adds a non-voting node. The given node will
	// retrieve a snapshot and catch up with logs.
	ChangeMembershipAddObserver ChangeMembershipOperation = iota
	// ChangeMembershipRemoveObserver removes a non-voting node.
	ChangeMembershipRemoveObserver

	// ChangeMembershipAddMember adds a full (voting) node. The given node must already be an
	// observer; it will be removed from the Observers list when this
	// operation is processed.
	// TODO(bdarnell): enforce the requirement that a node be added as an observer first.
	ChangeMembershipAddMember

	// ChangeMembershipRemoveMember removes a voting node. It is not possible to remove the
	// last node; the result of attempting to do so is undefined.
	ChangeMembershipRemoveMember
)

// ChangeMembershipPayload is the Payload of an entry with Type LogEntryChangeMembership.
// Nodes are added or removed one at a time to minimize the risk of quorum failures in
// the new configuration.
type ChangeMembershipPayload struct {
	Operation ChangeMembershipOperation
	Node      NodeID
}

// GroupElectionState records the votes this node has made so that it will not change its
// vote after a restart.
type GroupElectionState struct {
	// CurrentTerm is the highest term this node has seen.
	CurrentTerm int

	// VotedFor is the node this node has voted for in CurrentTerm's election. It is zero
	// If this node has not yet voted in CurrentTerm.
	VotedFor NodeID
}

// Equal compares two GroupElectionStates.
func (g *GroupElectionState) Equal(other *GroupElectionState) bool {
	return other != nil && g.CurrentTerm == other.CurrentTerm && g.VotedFor == other.VotedFor
}

// GroupMembers maintains the members of the group.
type GroupMembers struct {
	// Members contains the current members of the group and is always non-empty.
	Members []NodeID

	// Observers receive logs for the group but do not participate in elections
	// or quorum decisions. When a new node is added to the group it is initially
	// an Observer until it has caught up to the current log position.
	Observers []NodeID
}

// GroupPersistentState is a unified view of the readable data (except for log entries)
// about a group; used by Storage.LoadGroups.
type GroupPersistentState struct {
	GroupID       GroupID
	ElectionState GroupElectionState
	Members       GroupMembers
	LastLogIndex  int
	LastLogTerm   int
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

	// SetGroupElectionState is called to update the election state for the given group.
	SetGroupElectionState(groupID GroupID, electionState *GroupElectionState) error

	// AppendLogEntries is called to add entries to the log. The entries will always span
	// a contiguous range of indices just after the current end of the log.
	AppendLogEntries(groupID GroupID, entries []*LogEntry) error

	// TruncateLog is called to delete all log entries with index > lastIndex.
	TruncateLog(groupID GroupID, lastIndex int) error

	// GetLogEntry is called to synchronously retrieve an entry from the log.
	GetLogEntry(groupID GroupID, index int) (*LogEntry, error)

	// GetLogEntries is called to asynchronously retrieve entries from the log,
	// from firstIndex to lastIndex inclusive. If there is an error the storage
	// layer should send one LogEntryState with a non-nil error and then close the
	// channel.
	GetLogEntries(groupID GroupID, firstIndex, lastIndex int, ch chan<- *LogEntryState)
}

type memoryGroup struct {
	electionState GroupElectionState
	entries       []*LogEntry
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

// SetGroupElectionState implements the Storage interface.
func (m *MemoryStorage) SetGroupElectionState(groupID GroupID,
	electionState *GroupElectionState) error {
	m.getGroup(groupID).electionState = *electionState
	return nil
}

// AppendLogEntries implements the Storage interface.
func (m *MemoryStorage) AppendLogEntries(groupID GroupID, entries []*LogEntry) error {
	g := m.getGroup(groupID)
	for i, entry := range entries {
		expectedIndex := len(g.entries) + i
		if expectedIndex != entry.Index {
			return util.Errorf("log index mismatch: expected %v but was %v", expectedIndex, entry.Index)
		}
	}
	g.entries = append(g.entries, entries...)
	return nil
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
	g := m.getGroup(groupID)
	for i := firstIndex; i <= lastIndex; i++ {
		ch <- &LogEntryState{i, *g.entries[i], nil}
	}
	close(ch)
}

// getGroup returns a mutable memoryGroup object, creating if necessary.
func (m *MemoryStorage) getGroup(groupID GroupID) *memoryGroup {
	g, ok := m.groups[groupID]
	if !ok {
		g = &memoryGroup{
			// Start with a dummy entry because the raft paper uses 1-based indexing.
			entries: []*LogEntry{nil},
		}
		m.groups[groupID] = g
	}
	return g
}

// groupWriteRequest represents a set of changes to make to a group.
type groupWriteRequest struct {
	electionState *GroupElectionState
	entries       []*LogEntry
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
	electionState *GroupElectionState
	lastIndex     int
	lastTerm      int
	entries       []*LogEntry
}

// writeResponse is a collection of groupWriteResponses.
type writeResponse struct {
	groups map[GroupID]*groupWriteResponse
}

// writeTask manages a goroutine that interacts with the storage system.
type writeTask struct {
	storage Storage
	stopper chan struct{}

	// ready is an unbuffered channel used for synchronization. If writes to this channel do not
	// block, the writeTask is ready to receive a request.
	ready chan struct{}

	// For every request written to 'in', one response will be written to 'out'.
	in  chan *writeRequest
	out chan *writeResponse
}

// newWriteTask creates a writeTask. The caller should start the task after creating it.
func newWriteTask(storage Storage) *writeTask {
	return &writeTask{
		storage: storage,
		stopper: make(chan struct{}),
		ready:   make(chan struct{}),
		in:      make(chan *writeRequest, 1),
		out:     make(chan *writeResponse, 1),
	}
}

// start runs the storage loop. Blocks until stopped, so should be run in a goroutine.
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
		log.V(6).Infof("writeTask got request %#v", *request)
		response := &writeResponse{make(map[GroupID]*groupWriteResponse)}

		for groupID, groupReq := range request.groups {
			groupResp := &groupWriteResponse{nil, -1, -1, groupReq.entries}
			response.groups[groupID] = groupResp
			if groupReq.electionState != nil {
				err := w.storage.SetGroupElectionState(groupID, groupReq.electionState)
				if err != nil {
					continue
				}
				groupResp.electionState = groupReq.electionState
			}
			if len(groupReq.entries) > 0 {
				err := w.storage.AppendLogEntries(groupID, groupReq.entries)
				if err != nil {
					continue
				}
				groupResp.lastIndex = groupReq.entries[len(groupReq.entries)-1].Index
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
