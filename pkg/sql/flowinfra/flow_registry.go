// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package flowinfra

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// errNoInboundStreamConnection is the error propagated through the flow when
// the timeout to setup the flow is exceeded.
var errNoInboundStreamConnection = errors.New("no inbound stream connection")

// IsNoInboundStreamConnectionError returns true if err's Cause is an
// errNoInboundStreamConnection.
func IsNoInboundStreamConnectionError(err error) bool {
	return errors.Is(err, errNoInboundStreamConnection)
}

// SettingFlowStreamTimeout is a cluster setting that sets the default flow
// stream timeout.
var SettingFlowStreamTimeout = settings.RegisterDurationSetting(
	"sql.distsql.flow_stream_timeout",
	"amount of time incoming streams wait for a flow to be set up before erroring out",
	10*time.Second,
	settings.NonNegativeDuration,
)

// expectedConnectionTime is the expected time taken by a flow to connect to its
// consumers.
const expectedConnectionTime time.Duration = 500 * time.Millisecond

// InboundStreamInfo represents the endpoint where a data stream from another
// node connects to a flow. The external node initiates this process through a
// FlowStream RPC, which uses (*Flow).connectInboundStream() to associate the
// stream to a receiver to push rows to.
type InboundStreamInfo struct {
	// receiver is the entity that will receive rows from another host, which is
	// part of a processor (normally an input synchronizer) for row-based
	// execution and a colrpc.Inbox for vectorized execution.
	//
	// During a FlowStream RPC, the stream is handed off to this strategy to
	// process.
	receiver  InboundStreamHandler
	connected bool
	// if set, indicates that we waited too long for an inbound connection, or
	// we don't want this stream to connect anymore due to flow cancellation.
	canceled bool
	// finished is set if we have signaled that the stream is done transferring
	// rows (to the flow's wait group).
	finished bool

	// waitGroup to signal on when finished.
	waitGroup *sync.WaitGroup
}

// NewInboundStreamInfo returns a new InboundStreamInfo.
func NewInboundStreamInfo(
	receiver InboundStreamHandler, waitGroup *sync.WaitGroup,
) *InboundStreamInfo {
	return &InboundStreamInfo{
		receiver:  receiver,
		waitGroup: waitGroup,
	}
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

	flow *FlowBase

	// inboundStreams are streams that receive data from other hosts, through the
	// FlowStream API. All fields in the inboundStreamInfos are protected by the
	// FlowRegistry mutex (except the receiver, whose methods can be called
	// freely).
	inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo

	// streamTimer is a timer that fires after a timeout and verifies that all
	// inbound streams have been connected.
	streamTimer *time.Timer
}

// FlowRegistry allows clients to look up flows by ID and to wait for flows to
// be registered. Multiple clients can wait concurrently for the same flow.
type FlowRegistry struct {
	syncutil.Mutex

	// All fields in the flowEntry's are protected by the FlowRegistry mutex,
	// except flow, whose methods can be called freely.
	flows map[execinfrapb.FlowID]*flowEntry

	// draining specifies whether the FlowRegistry is in drain mode. If it is,
	// the FlowRegistry will not accept new flows.
	draining bool

	// flowDone is signaled whenever the size of flows decreases.
	flowDone *sync.Cond

	// testingRunBeforeDrainSleep is a testing knob executed when a draining
	// FlowRegistry has no registered flows but must still wait for a minimum time
	// for any incoming flows to register.
	testingRunBeforeDrainSleep func()
}

// NewFlowRegistry creates a new FlowRegistry.
func NewFlowRegistry() *FlowRegistry {
	fr := &FlowRegistry{flows: make(map[execinfrapb.FlowID]*flowEntry)}
	fr.flowDone = sync.NewCond(fr)
	return fr
}

// getEntryLocked returns the flowEntry associated with the id. If the entry
// doesn't exist, one is created and inserted into the map.
// It should only be called while holding the mutex.
func (fr *FlowRegistry) getEntryLocked(id execinfrapb.FlowID) *flowEntry {
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
func (fr *FlowRegistry) releaseEntryLocked(id execinfrapb.FlowID) {
	entry := fr.flows[id]
	if entry.refCount > 1 {
		entry.refCount--
	} else {
		if entry.refCount != 1 {
			panic(errors.AssertionFailedf("invalid refCount: %d", entry.refCount))
		}
		delete(fr.flows, id)
		fr.flowDone.Signal()
	}
}

type flowRetryableError struct {
	cause error
}

func (e *flowRetryableError) Error() string {
	return fmt.Sprintf("flow retryable error: %+v", e.cause)
}

// IsFlowRetryableError returns true if an error represents a retryable
// flow error.
func IsFlowRetryableError(e error) bool {
	return errors.HasType(e, (*flowRetryableError)(nil))
}

// RegisterFlow makes a flow accessible to ConnectInboundStream. Any concurrent
// ConnectInboundStream calls that are waiting for this flow are woken up.
//
// It is expected that UnregisterFlow will be called at some point to remove the
// flow from the registry.
//
// inboundStreams are all the remote streams that will be connected into this
// flow. If any of them is not connected within timeout, errors are propagated.
// The inboundStreams are expected to have been initialized with their
// WaitGroups (the group should have been incremented). RegisterFlow takes
// responsibility for calling Done() on that WaitGroup; this responsibility will
// be forwarded forward by ConnectInboundStream. In case this method returns an
// error, the WaitGroup will be decremented.
func (fr *FlowRegistry) RegisterFlow(
	ctx context.Context,
	id execinfrapb.FlowID,
	f *FlowBase,
	inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo,
	timeout time.Duration,
) (retErr error) {
	fr.Lock()
	defer fr.Unlock()
	defer func() {
		if retErr != nil {
			for _, stream := range inboundStreams {
				stream.waitGroup.Done()
			}
		}
	}()

	draining := fr.draining
	if f.Cfg != nil {
		if knobs, ok := f.Cfg.TestingKnobs.Flowinfra.(*TestingKnobs); ok && knobs != nil && knobs.FlowRegistryDraining != nil {
			draining = knobs.FlowRegistryDraining()
		}
	}

	if draining {
		return &flowRetryableError{cause: errors.Errorf(
			"could not register flowID %d because the registry is draining",
			id,
		)}
	}
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		return errors.Errorf(
			"flow already registered: flowID: %s.\n"+
				"Current flow: %+v\nExisting flow: %+v",
			f.spec.FlowID, f.spec, entry.flow.spec)
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
			// We're giving up waiting for these inbound streams. We will push an
			// error to its consumer after fr.Unlock; the error will propagate and
			// eventually drain all the processors.
			timedOutReceivers := fr.cancelPendingStreamsLocked(id)
			fr.Unlock()
			if len(timedOutReceivers) != 0 {
				// The span in the context might be finished by the time this runs. In
				// principle, we could ForkSpan() beforehand, but we don't want to
				// create the extra span every time.
				timeoutCtx := tracing.ContextWithSpan(ctx, nil)
				log.Errorf(
					timeoutCtx,
					"flow id:%s : %d inbound streams timed out after %s; propagated error throughout flow",
					id,
					len(timedOutReceivers),
					timeout,
				)
			}
			for _, r := range timedOutReceivers {
				go func(r InboundStreamHandler) {
					r.Timeout(errNoInboundStreamConnection)
				}(r)
			}
		})
	}
	return nil
}

// cancelPendingStreamsLocked cancels all of the streams that haven't been
// connected yet in this flow, by setting them to finished and ending their
// wait group. The method returns the list of RowReceivers corresponding to the
// streams that were canceled. The caller is expected to send those
// RowReceivers a cancellation message - this method can't do it because sending
// those messages shouldn't happen under the flow registry's lock.
func (fr *FlowRegistry) cancelPendingStreamsLocked(id execinfrapb.FlowID) []InboundStreamHandler {
	entry := fr.flows[id]
	if entry == nil || entry.flow == nil {
		return nil
	}
	pendingReceivers := make([]InboundStreamHandler, 0)
	for streamID, is := range entry.inboundStreams {
		// Connected, non-finished inbound streams will get an error
		// returned in ProcessInboundStream(). Non-connected streams
		// are handled below.
		if !is.connected && !is.finished && !is.canceled {
			is.canceled = true
			pendingReceivers = append(pendingReceivers, is.receiver)
			fr.finishInboundStreamLocked(id, streamID)
		}
	}
	return pendingReceivers
}

// UnregisterFlow removes a flow from the registry. Any subsequent
// ConnectInboundStream calls for the flow will fail to find it and time out.
func (fr *FlowRegistry) UnregisterFlow(id execinfrapb.FlowID) {
	fr.Lock()
	entry := fr.flows[id]
	if entry.streamTimer != nil {
		entry.streamTimer.Stop()
		entry.streamTimer = nil
	}
	fr.releaseEntryLocked(id)
	fr.Unlock()
}

// waitForFlowLocked  waits until the flow with the given id gets registered -
// up to the given timeout - and returns the flowEntry. If the timeout elapses,
// returns nil. It should only be called while holding the mutex. The mutex is
// temporarily unlocked if we need to wait.
// It is illegal to call this if the flow is already connected.
func (fr *FlowRegistry) waitForFlowLocked(
	ctx context.Context, id execinfrapb.FlowID, timeout time.Duration,
) *flowEntry {
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		log.Fatalf(ctx, "waitForFlowLocked called for a flow that's already registered: %d", id)
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

// Drain waits at most flowDrainWait for currently running flows to finish and
// at least minFlowDrainWait for any incoming flows to be registered. If there
// are still flows active after flowDrainWait, Drain waits an extra
// expectedConnectionTime so that any flows that were registered at the end of
// the time window have a reasonable amount of time to connect to their
// consumers, thus unblocking them.
// The FlowRegistry rejects any new flows once it has finished draining.
//
// Note that since local flows are not added to the registry, they are not
// waited for. However, this is fine since there should be no local flows
// running when the FlowRegistry drains as the draining logic starts with
// draining all client connections to a node.
//
// The reporter callback, if non-nil, is called on a best effort basis
// to report work that needed to be done and which may or may not have
// been done by the time this call returns. See the explanation in
// pkg/server/drain.go for details.
func (fr *FlowRegistry) Drain(
	flowDrainWait time.Duration,
	minFlowDrainWait time.Duration,
	reporter func(int, redact.SafeString),
) {
	allFlowsDone := make(chan struct{}, 1)
	start := timeutil.Now()
	stopWaiting := false

	sleep := func(t time.Duration) {
		if fr.testingRunBeforeDrainSleep != nil {
			fr.testingRunBeforeDrainSleep()
		}
		time.Sleep(t)
	}

	defer func() {
		// At this stage, we have either hit the flowDrainWait timeout or we have no
		// flows left. We wait for an expectedConnectionTime longer so that we give
		// any flows that were registered in the
		// flowDrainWait - expectedConnectionTime window enough time to establish
		// connections to their consumers so that the consumers do not block for a
		// long time waiting for a connection to be established.
		fr.Lock()
		fr.draining = true
		if len(fr.flows) > 0 {
			fr.Unlock()
			time.Sleep(expectedConnectionTime)
			fr.Lock()
		}
		fr.Unlock()
	}()

	fr.Lock()
	if len(fr.flows) == 0 {
		fr.Unlock()
		sleep(minFlowDrainWait)
		fr.Lock()
		// No flows were registered, return.
		if len(fr.flows) == 0 {
			fr.Unlock()
			return
		}
	}
	if reporter != nil {
		// Report progress to the Drain RPC.
		reporter(len(fr.flows), "distSQL execution flows")
	}

	go func() {
		select {
		case <-time.After(flowDrainWait):
			fr.Lock()
			stopWaiting = true
			fr.flowDone.Signal()
			fr.Unlock()
		case <-allFlowsDone:
		}
	}()

	for !(stopWaiting || len(fr.flows) == 0) {
		fr.flowDone.Wait()
	}
	fr.Unlock()

	// If we spent less time waiting for all registered flows to finish, wait
	// for the minimum time for any new incoming flows and wait for these to
	// finish.
	waitTime := timeutil.Since(start)
	if waitTime < minFlowDrainWait {
		sleep(minFlowDrainWait - waitTime)
		fr.Lock()
		for !(stopWaiting || len(fr.flows) == 0) {
			fr.flowDone.Wait()
		}
		fr.Unlock()
	}

	allFlowsDone <- struct{}{}
}

// Undrain causes the FlowRegistry to start accepting flows again.
func (fr *FlowRegistry) Undrain() {
	fr.Lock()
	fr.draining = false
	fr.Unlock()
}

// ConnectInboundStream finds the InboundStreamInfo for the given
// <flowID,streamID> pair and marks it as connected. It waits up to timeout for
// the stream to be registered with the registry. It also sends the handshake
// messages to the producer of the stream.
//
// stream is the inbound stream.
//
// It returns the Flow that the stream is connecting to, the receiver that the
// stream must push data to and a cleanup function that must be called to
// unregister the flow from the registry after all the data has been pushed.
//
// The cleanup function will decrement the flow's WaitGroup, so that Flow.Wait()
// is not blocked on this stream any more.
// In case an error is returned, the cleanup function is nil, the Flow is not
// considered connected and is not cleaned up.
func (fr *FlowRegistry) ConnectInboundStream(
	ctx context.Context,
	flowID execinfrapb.FlowID,
	streamID execinfrapb.StreamID,
	stream execinfrapb.DistSQL_FlowStreamServer,
	timeout time.Duration,
) (_ *FlowBase, _ InboundStreamHandler, _ func(), retErr error) {
	fr.Lock()
	defer fr.Unlock()

	entry := fr.getEntryLocked(flowID)
	if entry.flow == nil {
		// Send the handshake message informing the producer that the consumer has
		// not been scheduled yet. Another handshake will be sent below once the
		// consumer has been connected.
		deadline := timeutil.Now().Add(timeout)
		if err := stream.Send(&execinfrapb.ConsumerSignal{
			Handshake: &execinfrapb.ConsumerHandshake{
				ConsumerScheduled:        false,
				ConsumerScheduleDeadline: &deadline,
				Version:                  execinfra.Version,
				MinAcceptedVersion:       execinfra.MinAcceptedVersion,
			},
		}); err != nil {
			// TODO(andrei): We failed to send a message to the producer; we'll return
			// an error and leave this stream with connected == false so it times out
			// later. We could call finishInboundStreamLocked() now so that the flow
			// doesn't wait for the timeout and we could remember the error for the
			// consumer if the consumer comes later, but I'm not sure what the best
			// way to do that is. Similarly for the 2nd handshake message below,
			// except there we already have the consumer and we can push the error.
			return nil, nil, nil, err
		}
		entry = fr.waitForFlowLocked(ctx, flowID, timeout)
		if entry == nil {
			return nil, nil, nil, errors.Errorf("flow %s not found", flowID)
		}
	}

	s, ok := entry.inboundStreams[streamID]
	if !ok {
		return nil, nil, nil, errors.Errorf("flow %s: no inbound stream %d", flowID, streamID)
	}
	if s.connected {
		return nil, nil, nil, errors.Errorf("flow %s: inbound stream %d already connected", flowID, streamID)
	}
	if s.canceled {
		return nil, nil, nil, errors.Errorf("flow %s: inbound stream %d came too late", flowID, streamID)
	}

	// We now mark the stream as connected but, if an error happens later because
	// the handshake fails, we reset the state; we want the stream to be
	// considered timed out when the moment comes just as if this connection
	// attempt never happened.
	s.connected = true
	defer func() {
		if retErr != nil {
			s.connected = false
		}
	}()

	if err := stream.Send(&execinfrapb.ConsumerSignal{
		Handshake: &execinfrapb.ConsumerHandshake{
			ConsumerScheduled:  true,
			Version:            execinfra.Version,
			MinAcceptedVersion: execinfra.MinAcceptedVersion,
		},
	}); err != nil {
		return nil, nil, nil, err
	}

	cleanup := func() {
		fr.Lock()
		fr.finishInboundStreamLocked(flowID, streamID)
		fr.Unlock()
	}
	return entry.flow, s.receiver, cleanup, nil
}

func (fr *FlowRegistry) finishInboundStreamLocked(
	fid execinfrapb.FlowID, sid execinfrapb.StreamID,
) {
	flowEntry := fr.getEntryLocked(fid)
	streamEntry := flowEntry.inboundStreams[sid]

	if !streamEntry.connected && !streamEntry.canceled {
		panic("finishing inbound stream that didn't connect or time out")
	}
	if streamEntry.finished {
		panic("double finish")
	}

	streamEntry.finished = true
	streamEntry.waitGroup.Done()
}
