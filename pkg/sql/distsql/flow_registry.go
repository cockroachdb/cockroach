// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License.
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsql

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/util/syncutil"
)

// flowEntry is a structure associated with a (potential) flow.
// All fields are protected by the flowRegistry mutex.
type flowEntry struct {
	// waitCh is set if one or more clients are waiting for the flow; the
	// channel gets closed when the flow is registered.
	waitCh chan struct{}

	// refCount is used to allow multiple clients to wait for a flow - if the
	// flow never shows up, the refCount is used to decide which client cleans
	// up the entry.
	refCount int

	flow *Flow
}

// flowRegistry allows clients to look up flows by ID and to wait for flows to
// be registered. Multiple clients can wait concurrently for the same flow.
type flowRegistry struct {
	mu    syncutil.Mutex
	flows map[FlowID]*flowEntry
}

func makeFlowRegistry() *flowRegistry {
	fr := &flowRegistry{
		flows: make(map[FlowID]*flowEntry),
	}
	return fr
}

// getEntryLocked returns the flowEntry associated with the id. If the entry
// doesn't exist, one is created and inserted into the map.
// It should only be called while holding the mutex.
func (fr *flowRegistry) getEntryLocked(id FlowID) *flowEntry {
	entry, ok := fr.flows[id]
	if !ok {
		entry = &flowEntry{}
		fr.flows[id] = entry
	}
	return entry
}

// releaseEntryLocked decreases the refCount in the entry for the given id, and
// cleans up the entry if the refCount reaches 0.
// It should only be called while holding the mutex.
func (fr *flowRegistry) releaseEntryLocked(id FlowID) {
	entry := fr.flows[id]
	if entry.refCount > 1 {
		entry.refCount--
	} else {
		if entry.refCount != 1 {
			panic(fmt.Sprintf("invalid refCount: %d", entry.refCount))
		}
		delete(fr.flows, id)
	}
}

// RegisterFlow makes a flow accessible to LookupFlow. Any concurrent LookupFlow
// calls that are waiting for this flow are woken up.
// It is expected that UnregisterFlow will be called at some point to remove the
// flow from the registry.
func (fr *flowRegistry) RegisterFlow(id FlowID, f *Flow) {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		panic("flow already registered")
	}
	// Take a reference that will be removed by UnregisterFlow.
	entry.refCount++
	entry.flow = f
	// If there are any waiters, wake them up by closing waitCh.
	if entry.waitCh != nil {
		close(entry.waitCh)
	}
}

// UnregisterFlow removes a flow from the registry. Any subsequent LookupFlow
// calls will time out.
func (fr *flowRegistry) UnregisterFlow(id FlowID) {
	fr.mu.Lock()
	fr.releaseEntryLocked(id)
	fr.mu.Unlock()
}

// LookupFlow returns the registered flow with the given ID. If no such flow is
// registered, waits until it gets registered - up to the given timeout. If the
// timeout elapses, returns nil.
func (fr *flowRegistry) LookupFlow(id FlowID, timeout time.Duration) *Flow {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		return entry.flow
	}

	// Flow not registered (at least not yet).

	// Set up a channel that gets closed when the flow shows up, or when the
	// timeout elapses. The channel might have been created already if there are
	// other waiters for the same id.
	if entry.waitCh != nil {
		entry.waitCh = make(chan struct{})
	}
	entry.refCount++
	fr.mu.Unlock()

	// Wait until waitCh gets closed or the timeout elapses.
	select {
	case <-entry.waitCh:
	case <-time.After(timeout):
	}

	fr.mu.Lock()

	fr.releaseEntryLocked(id)

	return entry.flow
}
