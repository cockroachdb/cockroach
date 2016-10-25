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
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/pkg/errors"
)

// flowStreamTimeout is the amount of time incoming streams wait for a flow to
// be set up before erroring out.
const flowStreamTimeout time.Duration = 4000 * time.Millisecond

// inboundStreamInfo represents the endpoint where a data stream from another
// node connects to a flow. The external node initiates this process through a
// FlowStream RPC, which uses (*Flow).connectInboundStream() to associate the
// stream to a receiver to push rows to.
type inboundStreamInfo struct {
	// receiver is the entity that will receive rows from another host, which is
	// part of a processor (normally an input synchronizer).
	//
	// During a FlowStream RPC, rows are pushed to this entity using the
	// RowReceiver interface.
	receiver  RowReceiver
	connected bool
	// if set, indicates that we waited too long for an inbound connection.
	timedOut bool
	// finished is set if we have signaled that the stream is done transferring
	// rows (to the flow's wait group).
	finished bool

	// waitGroup to signal on when finished.
	waitGroup *sync.WaitGroup
}

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

	// inboundStreams are streams that receive data from other hosts, through
	// the FlowStream API.
	inboundStreams map[StreamID]*inboundStreamInfo

	// streamTimer is a timer that fires after a timeout and verifies that all
	// inbound streams have been connected.
	streamTimer *time.Timer
}

// flowRegistry allows clients to look up flows by ID and to wait for flows to
// be registered. Multiple clients can wait concurrently for the same flow.
type flowRegistry struct {
	syncutil.Mutex
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
func (fr *flowRegistry) RegisterFlow(
	id FlowID, f *Flow, inboundStreams map[StreamID]*inboundStreamInfo,
) {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		panic("flow already registered")
	}
	// Take a reference that will be removed by UnregisterFlow.
	entry.refCount++
	entry.flow = f
	entry.inboundStreams = inboundStreams
	// If there are any waiters, wake them up by closing waitCh.
	if entry.waitCh != nil {
		close(entry.waitCh)
	}

	if len(inboundStreams) > 0 {
		// Set up a function to time out inbound streams after a while.
		entry.streamTimer = time.AfterFunc(flowStreamTimeout, func() {
			fr.Lock()
			defer fr.Unlock()
			numTimedOut := 0
			for _, is := range entry.inboundStreams {
				if is.timedOut {
					panic("stream already timed out")
				}
				if !is.connected {
					is.timedOut = true
					numTimedOut++
					is.receiver.Close(errors.Errorf("no inbound stream connection"))
					fr.finishInboundStreamLocked(is)
				}
			}
			if numTimedOut != 0 {
				log.Errorf(entry.flow.Context, "%d inbound streams timed out after %s; stopping flow",
					numTimedOut, flowStreamTimeout)
			}
		})
	}
}

// UnregisterFlow removes a flow from the registry. Any subsequent LookupFlow
// calls will time out.
func (fr *flowRegistry) UnregisterFlow(id FlowID) {
	fr.Lock()
	entry := fr.flows[id]
	if entry.streamTimer != nil {
		entry.streamTimer.Stop()
		entry.streamTimer = nil
	}
	fr.releaseEntryLocked(id)
	fr.Unlock()
}

// waitForFlowLocked returns the flowEntry of a registered flow with the given
// ID. If no such flow is registered, waits until it gets registered - up to the
// given timeout. If the timeout elapses, returns nil. It should only be called
// while holding the mutex. The mutex is temporarily unlocked if we need to
// wait.
func (fr *flowRegistry) waitForFlowLocked(id FlowID, timeout time.Duration) *flowEntry {
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		return entry
	}

	// Flow not registered (at least not yet).

	// Set up a channel that gets closed when the flow shows up, or when the
	// timeout elapses. The channel might have been created already if there are
	// other waiters for the same id.
	if entry.waitCh != nil {
		entry.waitCh = make(chan struct{})
	}
	entry.refCount++
	fr.Unlock()

	// Wait until waitCh gets closed or the timeout elapses.
	select {
	case <-entry.waitCh:
	case <-time.After(timeout):
	}

	fr.Lock()

	fr.releaseEntryLocked(id)
	if entry.flow == nil {
		return nil
	}

	return entry
}

// LookupFlow returns the registered flow with the given ID. If no such flow is
// registered, waits until it gets registered - up to the given timeout. If the
// timeout elapses, returns nil.
func (fr *flowRegistry) LookupFlow(id FlowID, timeout time.Duration) *Flow {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.waitForFlowLocked(id, timeout)
	if entry == nil {
		return nil
	}
	return entry.flow
}

// ConnectInboundStream finds the inboundStreamInfo for the given ID and marks it
// as connected. FinishInboundStream must be called after the rows are
// transferred.
func (fr *flowRegistry) ConnectInboundStream(
	flowID FlowID, streamID StreamID,
) (*Flow, *inboundStreamInfo, error) {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.waitForFlowLocked(flowID, flowStreamTimeout)
	if entry == nil {
		return nil, nil, errors.Errorf("flow %s not found", flowID)
	}

	s, ok := entry.inboundStreams[streamID]
	if !ok {
		return nil, nil, errors.Errorf("flow %s: no inbound stream %d", flowID, streamID)
	}
	if s.connected {
		return nil, nil, errors.Errorf("flow %s: inbound stream %d already connected", flowID, streamID)
	}
	if s.timedOut {
		return nil, nil, errors.Errorf("flow %s: inbound stream %d came too late", flowID, streamID)
	}
	s.connected = true
	return entry.flow, s, nil
}

func (fr *flowRegistry) finishInboundStreamLocked(is *inboundStreamInfo) {
	if !is.connected && !is.timedOut {
		panic("finishing inbound stream that didn't connect or time out")
	}
	if is.finished {
		panic("double finish")
	}
	is.finished = true
	is.waitGroup.Done()
}

// FinishInboundStream is to be called when we are done transferring rows for a
// stream previously connected via ConnectInboundStream.
func (fr *flowRegistry) FinishInboundStream(is *inboundStreamInfo) {
	fr.Lock()
	defer fr.Unlock()
	fr.finishInboundStreamLocked(is)
}
