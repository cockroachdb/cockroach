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

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/coreos/etcd/raft"
	"github.com/coreos/etcd/raft/raftpb"
)

// WriteableGroupStorage represents a single group within a Storage.
// It is implemented by *raft.MemoryStorage.
type WriteableGroupStorage interface {
	raft.Storage
	Append(entries []raftpb.Entry) error
	ApplySnapshot(snap raftpb.Snapshot) error
	SetHardState(st raftpb.HardState) error
}

var _ WriteableGroupStorage = (*raft.MemoryStorage)(nil)

// The Storage interface is supplied by the application to manage persistent storage
// of raft data.
type Storage interface {
	// GroupStorage returns an interface which can be used to access the
	// storage for the specified group. May return ErrGroupDeleted if
	// the group cannot be found or if the given replica ID is known to
	// be out of date. The replicaID may be zero if the replica ID is
	// not known; replica-staleness checks should be disabled in this
	// case.
	GroupStorage(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (WriteableGroupStorage, error)

	ReplicaDescriptor(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error)
	ReplicaIDForStore(groupID roachpb.RangeID, storeID roachpb.StoreID) (roachpb.ReplicaID, error)
	ReplicasFromSnapshot(snap raftpb.Snapshot) ([]roachpb.ReplicaDescriptor, error)
}

// The StateMachine interface is supplied by the application to manage a persistent
// state machine (in Cockroach the StateMachine and the Storage are the same thing
// but they are logically distinct and systems like etcd keep them separate).
type StateMachine interface {
	// AppliedIndex returns the last index which has been applied to the given group's
	// state machine.
	AppliedIndex(groupID roachpb.RangeID) (uint64, error)
}

// MemoryStorage is an in-memory implementation of Storage for testing.
type MemoryStorage struct {
	groups map[roachpb.RangeID]WriteableGroupStorage
	mu     sync.Mutex
}

// Verifying implementation of Storage interface.
var _ Storage = (*MemoryStorage)(nil)

// NewMemoryStorage creates a MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		groups: make(map[roachpb.RangeID]WriteableGroupStorage),
	}
}

// GroupStorage implements the Storage interface.
func (m *MemoryStorage) GroupStorage(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (WriteableGroupStorage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[groupID]
	if !ok {
		g = raft.NewMemoryStorage()
		m.groups[groupID] = g
	}
	return g, nil
}

// ReplicaDescriptor implements the Storage interface by returning a
// dummy descriptor.
func (m *MemoryStorage) ReplicaDescriptor(groupID roachpb.RangeID, replicaID roachpb.ReplicaID) (roachpb.ReplicaDescriptor, error) {
	return roachpb.ReplicaDescriptor{
		ReplicaID: replicaID,
		NodeID:    roachpb.NodeID(replicaID),
		StoreID:   roachpb.StoreID(replicaID),
	}, nil
}

// ReplicaIDForStore implements the Storage interface.
func (m *MemoryStorage) ReplicaIDForStore(groupID roachpb.RangeID, storeID roachpb.StoreID) (roachpb.ReplicaID, error) {
	return roachpb.ReplicaID(storeID), nil
}

// ReplicasFromSnapshot implements the Storage interface.
func (m *MemoryStorage) ReplicasFromSnapshot(_ raftpb.Snapshot) ([]roachpb.ReplicaDescriptor, error) {
	return nil, nil
}

// groupWriteRequest represents a set of changes to make to a group.
type groupWriteRequest struct {
	replicaID roachpb.ReplicaID
	state     raftpb.HardState
	entries   []raftpb.Entry
	snapshot  raftpb.Snapshot
}

// writeRequest is a collection of groupWriteRequests.
type writeRequest struct {
	groups map[roachpb.RangeID]*groupWriteRequest
}

// newWriteRequest creates a writeRequest.
func newWriteRequest() *writeRequest {
	return &writeRequest{make(map[roachpb.RangeID]*groupWriteRequest)}
}

// groupWriteResponse represents the final state of a persistent group.
// lastIndex and lastTerm may be -1 if the respective state was not
// changed (which may be because there were no changes in the request
// or due to an error).
type groupWriteResponse struct {
	state     raftpb.HardState
	lastIndex int
	lastTerm  int
	entries   []raftpb.Entry
}

// writeResponse is a collection of groupWriteResponses.
type writeResponse struct {
	groups map[roachpb.RangeID]*groupWriteResponse
}

// writeTask manages a goroutine that interacts with the storage system.
type writeTask struct {
	storage Storage

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
		ready:   make(chan struct{}),
		in:      make(chan *writeRequest, 1),
		out:     make(chan *writeResponse, 1),
	}
}

// start runs the storage loop in a goroutine.
func (w *writeTask) start(stopper *stop.Stopper) {
	stopper.RunWorker(func() {
		for {
			var request *writeRequest
			select {
			case <-w.ready:
				continue
			case <-stopper.ShouldStop():
				return
			case request = <-w.in:
			}
			if log.V(6) {
				log.Infof("writeTask got request %#v", *request)
			}
			response := &writeResponse{make(map[roachpb.RangeID]*groupWriteResponse)}

			for groupID, groupReq := range request.groups {
				group, err := w.storage.GroupStorage(groupID, groupReq.replicaID)
				if err == ErrGroupDeleted {
					if log.V(4) {
						log.Infof("dropping write to deleted group %v", groupID)
					}
					continue
				} else if err != nil {
					log.Fatalf("GroupStorage(group %s, replica %s) failed: %s", groupID,
						groupReq.replicaID, err)
				}
				groupResp := &groupWriteResponse{raftpb.HardState{}, -1, -1, groupReq.entries}
				response.groups[groupID] = groupResp
				if !raft.IsEmptyHardState(groupReq.state) {
					err := group.SetHardState(groupReq.state)
					if err != nil {
						panic(err) // TODO(bdarnell): mark this node dead on storage errors
					}
					groupResp.state = groupReq.state
				}
				if !raft.IsEmptySnap(groupReq.snapshot) {
					err := group.ApplySnapshot(groupReq.snapshot)
					if err != nil {
						panic(err) // TODO(bdarnell)
					}
				}
				if len(groupReq.entries) > 0 {
					err := group.Append(groupReq.entries)
					if err != nil {
						panic(err) // TODO(bdarnell)
					}
				}
			}
			w.out <- response
		}
	})
}
