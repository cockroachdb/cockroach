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
	"sync"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

// WriteableGroupStorage represents a single group within a Storage.
// It is implemented by *raft.MemoryStorage.
type WriteableGroupStorage interface {
	raft.Storage
	Append(entries []raftpb.Entry) error
	SetHardState(st raftpb.HardState) error
}

var _ WriteableGroupStorage = (*raft.MemoryStorage)(nil)

// The Storage interface is supplied by the application to manage persistent storage
// of raft data.
type Storage interface {
	GroupStorage(groupID uint64) WriteableGroupStorage
}

// MemoryStorage is an in-memory implementation of Storage for testing.
type MemoryStorage struct {
	groups map[uint64]WriteableGroupStorage
	mu     sync.Mutex
}

// Verifying implementation of Storage interface.
var _ Storage = (*MemoryStorage)(nil)

// NewMemoryStorage creates a MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		groups: make(map[uint64]WriteableGroupStorage),
	}
}

// GroupStorage implements the Storage interface.
func (m *MemoryStorage) GroupStorage(groupID uint64) WriteableGroupStorage {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[groupID]
	if !ok {
		g = raft.NewMemoryStorage()
		m.groups[groupID] = g
	}
	return g
}

// groupWriteRequest represents a set of changes to make to a group.
type groupWriteRequest struct {
	state    raftpb.HardState
	entries  []raftpb.Entry
	snapshot raftpb.Snapshot
}

// writeRequest is a collection of groupWriteRequests.
type writeRequest struct {
	groups map[uint64]*groupWriteRequest
}

// newWriteRequest creates a writeRequest.
func newWriteRequest() *writeRequest {
	return &writeRequest{make(map[uint64]*groupWriteRequest)}
}

// groupWriteResponse represents the final state of a persistent group.
// metadata may be nil and lastIndex and lastTerm may be -1 if the respective
// state was not changed (which may be because there were no changes in the request
// or due to an error)
type groupWriteResponse struct {
	state     raftpb.HardState
	lastIndex int
	lastTerm  int
	entries   []raftpb.Entry
}

// writeResponse is a collection of groupWriteResponses.
type writeResponse struct {
	groups map[uint64]*groupWriteResponse
}

// writeTask manages a goroutine that interacts with the storage system.
type writeTask struct {
	storage Storage
	stopper *util.Stopper

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
		stopper: util.NewStopper(1),
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
		case <-w.stopper.ShouldStop():
			w.stopper.SetStopped()
			return
		case request = <-w.in:
		}
		log.V(6).Infof("writeTask got request %#v", *request)
		response := &writeResponse{make(map[uint64]*groupWriteResponse)}

		for groupID, groupReq := range request.groups {
			group := w.storage.GroupStorage(groupID)
			groupResp := &groupWriteResponse{raftpb.HardState{}, -1, -1, groupReq.entries}
			response.groups[groupID] = groupResp
			if !raft.IsEmptyHardState(groupReq.state) {
				err := group.SetHardState(groupReq.state)
				if err != nil {
					panic(err) // TODO(bdarnell): mark this node dead on storage errors
				}
				groupResp.state = groupReq.state
			}
			if len(groupReq.entries) > 0 {
				group.Append(groupReq.entries)
			}
		}
		w.out <- response
	}
}

// stop the running task.
func (w *writeTask) stop() {
	w.stopper.Stop()
}
