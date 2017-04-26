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

package distsqlrun

import (
	"fmt"
	"sync"
	"time"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
)

// flowStreamDefaultTimeout is the amount of time incoming streams wait for a flow to
// be set up before erroring out.
const flowStreamDefaultTimeout time.Duration = 10 * time.Second

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
type flowEntry struct {
	// waitCh is set if one or more clients are waiting for the flow; the
	// channel gets closed when the flow is registered.
	waitCh chan struct{}

	// refCount is used to allow multiple clients to wait for a flow - if the
	// flow never shows up, the refCount is used to decide which client cleans
	// up the entry.
	refCount int

	flow *Flow

	// inboundStreams are streams that receive data from other hosts, through the
	// FlowStream API. All fields in the inboundStreamInfos are protected by the
	// flowRegistry mutex (except the receiver, whose methods can be called
	// freely).
	inboundStreams map[StreamID]*inboundStreamInfo

	// streamTimer is a timer that fires after a timeout and verifies that all
	// inbound streams have been connected.
	streamTimer *time.Timer
}

// flowRegistry allows clients to look up flows by ID and to wait for flows to
// be registered. Multiple clients can wait concurrently for the same flow.
type flowRegistry struct {
	syncutil.Mutex
	// All fields in the flowEntry's are protected by the flowRegistry mutex,
	// except flow, whose methods can be called freely.
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

// RegisterFlow makes a flow accessible to ConnectInboundStream. Any concurrent
// ConnectInboundStream calls that are waiting for this flow are woken up.
//
// It is expected that UnregisterFlow will be called at some point to remove the
// flow from the registry.
//
// inboundStreams are all the remote streams that will be connected into this
// flow. If any of them is not connected within timeout, errors are propagated.
func (fr *flowRegistry) RegisterFlow(
	ctx context.Context,
	id FlowID,
	f *Flow,
	inboundStreams map[StreamID]*inboundStreamInfo,
	timeout time.Duration,
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
		entry.streamTimer = time.AfterFunc(timeout, func() {
			fr.Lock()
			defer fr.Unlock()
			numTimedOut := 0
			for streamID, is := range entry.inboundStreams {
				if is.timedOut {
					panic("stream already marked as timed out")
				}
				if !is.connected {
					is.timedOut = true
					numTimedOut++
					// We're giving up waiting for this inbound stream. Send an error to
					// its consumer; the error will propagate and eventually drain all the
					// processors.
					is.receiver.Push(
						nil, /* row */
						ProducerMetadata{Err: errors.Errorf("no inbound stream connection")})
					is.receiver.ProducerDone()
					fr.finishInboundStreamLocked(id, streamID)
				}
			}
			if numTimedOut != 0 {
				// The span in the context might be finished by the time this runs. In
				// principle, we could ForkCtxSpan() beforehand, but we don't want to
				// create the extra span every time.
				timeoutCtx := opentracing.ContextWithSpan(ctx, nil)
				log.Errorf(
					timeoutCtx,
					"flow id:%s : %d inbound streams timed out after %s; propagated error throughout flow",
					id,
					numTimedOut,
					timeout,
				)
			}
		})
	}
}

// UnregisterFlow removes a flow from the registry. Any subsequent
// ConnectInboundStream calls for the flow will fail to find it and time out.
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
func (fr *flowRegistry) waitForFlowLocked(
	ctx context.Context, id FlowID, timeout time.Duration,
) *flowEntry {
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		return entry
	}

	// Flow not registered (at least not yet).

	// Set up a channel that gets closed when the flow shows up, or when the
	// timeout elapses. The channel might have been created already if there are
	// other waiters for the same id.
	waitCh := entry.waitCh
	if waitCh == nil {
		waitCh = make(chan struct{})
		entry.waitCh = waitCh
	}
	entry.refCount++
	fr.Unlock()

	select {
	case <-waitCh:
	case <-time.After(timeout):
	case <-ctx.Done():
	}

	fr.Lock()

	fr.releaseEntryLocked(id)
	if entry.flow == nil {
		return nil
	}

	return entry
}

// ConnectInboundStream finds the inboundStreamInfo for the given ID and marks it
// as connected. It waits up to timeout for the stream to be registered with the
// registry.
//
// It returns the Flow that the stream is connecting to, the receiver that the
// stream must push data to and a cleanup function that must be called to
// unregister the flow from the registry after all the data has been pushed.
//
// The cleanup function will decrement the flow's WaitGroup, so that Flow.Wait()
// is not blocked on this stream any more.
func (fr *flowRegistry) ConnectInboundStream(
	ctx context.Context, flowID FlowID, streamID StreamID, timeout time.Duration,
) (*Flow, RowReceiver, func(), error) {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.waitForFlowLocked(ctx, flowID, timeout)
	if entry == nil {
		return nil, nil, nil, errors.Errorf("flow %s not found", flowID)
	}

	s, ok := entry.inboundStreams[streamID]
	if !ok {
		return nil, nil, nil, errors.Errorf("flow %s: no inbound stream %d", flowID, streamID)
	}
	if s.connected {
		return nil, nil, nil, errors.Errorf("flow %s: inbound stream %d already connected", flowID, streamID)
	}
	if s.timedOut {
		return nil, nil, nil, errors.Errorf("flow %s: inbound stream %d came too late", flowID, streamID)
	}
	s.connected = true
	cleanup := func() {
		fr.Lock()
		fr.finishInboundStreamLocked(flowID, streamID)
		fr.Unlock()
	}
	return entry.flow, s.receiver, cleanup, nil
}

func (fr *flowRegistry) finishInboundStreamLocked(fid FlowID, sid StreamID) {
	flowEntry := fr.getEntryLocked(fid)
	streamEntry := flowEntry.inboundStreams[sid]

	if !streamEntry.connected && !streamEntry.timedOut {
		panic("finising inbound stream that didn't connect or time out")
	}
	if streamEntry.finished {
		panic("double finish")
	}

	streamEntry.finished = true
	streamEntry.waitGroup.Done()
}
