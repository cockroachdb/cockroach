// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package flowinfra

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/gogo/protobuf/proto"
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
	settings.ApplicationLevel,
	"sql.distsql.flow_stream_timeout",
	"amount of time incoming streams wait for a flow to be set up before erroring out",
	10*time.Second,
	settings.NonNegativeDuration,
	settings.WithName("sql.distsql.flow_stream.timeout"),
)

// expectedConnectionTime is the expected time taken by a flow to connect to its
// consumers.
const expectedConnectionTime time.Duration = 500 * time.Millisecond

// InboundStreamInfo represents the endpoint where a data stream from another
// node connects to a flow. The external node initiates this process through a
// FlowStream RPC, which uses FlowRegistry.ConnectInboundStream() to associate
// the stream to a receiver to push rows to.
type InboundStreamInfo struct {
	mu struct {
		syncutil.Mutex
		connected bool
		// if set, indicates that we waited too long for an inbound connection,
		// or we don't want this stream to connect anymore due to flow
		// cancellation.
		canceled bool
		// finished is set when onFinish has already been called (which signaled
		// that the stream is done transferring rows to the flow's wait group).
		finished bool
	}
	// receiver is the entity that will receive rows from another host, which is
	// part of a processor (normally an input synchronizer) for row-based
	// execution and a colrpc.Inbox for vectorized execution.
	//
	// During a FlowStream RPC, the stream is handed off to this strategy to
	// process.
	receiver InboundStreamHandler
	// onFinish will be called when the corresponding inbound stream is done.
	onFinish func()
}

// NewInboundStreamInfo returns a new InboundStreamInfo.
func NewInboundStreamInfo(
	receiver InboundStreamHandler, waitGroup *sync.WaitGroup,
) *InboundStreamInfo {
	return &InboundStreamInfo{
		receiver: receiver,
		// Do not hold onto the whole wait group to limit the coupling of the
		// InboundStreamInfo with the FlowBase.
		onFinish: waitGroup.Done,
	}
}

// finishLocked marks s as finished and calls onFinish. The mutex of s must be
// held when calling this method.
func (s *InboundStreamInfo) finishLocked() {
	if !s.mu.connected && !s.mu.canceled {
		panic("finishing inbound stream that didn't connect or time out")
	}
	if s.mu.finished {
		panic("double finish")
	}
	s.mu.finished = true
	s.onFinish()
}

// cancelIfNotConnected cancels and finishes s if it's not marked as connected,
// finished, or canceled, and sends the provided error to the pending receiver
// if so. A boolean indicating whether s is canceled is returned.
func (s *InboundStreamInfo) cancelIfNotConnected(err error) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.connected || s.mu.finished || s.mu.canceled {
		return false
	}
	s.cancelLocked(err)
	return true
}

// cancelLocked marks s as canceled, finishes it, and sends the given error to
// the receiver. The mutex of s must be held when calling this method.
func (s *InboundStreamInfo) cancelLocked(err error) {
	s.mu.AssertHeld()
	s.mu.canceled = true
	s.finishLocked()
	// Since Timeout might block, we must send the error in a new goroutine.
	go func() {
		s.receiver.Timeout(err)
	}()
}

// finish is the same as finishLocked when the mutex of s is not held already.
func (s *InboundStreamInfo) finish() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.finishLocked()
}

// flowEntry is a structure associated with a (potential) flow.
//
// All fields in flowEntry are protected by the FlowRegistry mutex and, thus,
// must be accessed while holding the lock.
type flowEntry struct {
	// waitCh is set if one or more clients are waiting for the flow; the
	// channel gets closed when the flow is registered.
	waitCh chan struct{}

	// refCount is used to allow multiple clients to wait for a flow - if the
	// flow never shows up, the refCount is used to decide which client cleans
	// up the entry.
	refCount int

	flow *FlowBase

	// inboundStreams are streams that receive data from other hosts, through
	// the FlowStream API. This map is set in Flow.Setup(), so it is safe to
	// lookup into concurrently later.
	//
	// Each InboundStreamInfo has its own mutex, separate from the FlowRegistry
	// mutex. These two mutexes should **not** be held at the same time.
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

var _ errors.Wrapper = &flowRetryableError{}

func (e *flowRetryableError) Error() string { return e.cause.Error() }
func (e *flowRetryableError) Cause() error  { return e.cause }
func (e *flowRetryableError) Unwrap() error { return e.Cause() }

func decodeFlowRetryableError(
	_ context.Context, cause error, _ string, _ []string, _ proto.Message,
) error {
	return &flowRetryableError{cause: cause}
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*flowRetryableError)(nil)), decodeFlowRetryableError)
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
// responsibility for finishing the inboundStreams (which will call Done() on
// that WaitGroup); this responsibility will be forwarded forward by
// ConnectInboundStream. In case this method returns an error, the
// inboundStreams are finished here.
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
				stream.onFinish()
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
			"could not register flow because the registry is draining",
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
			// We're giving up waiting for these inbound streams. We will push
			// an error to its consumer; the error will propagate and eventually
			// drain all the processors.
			numTimedOutReceivers := fr.cancelPendingStreams(id, errNoInboundStreamConnection)
			if numTimedOutReceivers != 0 {
				// The span in the context might be finished by the time this runs. In
				// principle, we could ForkSpan() beforehand, but we don't want to
				// create the extra span every time.
				timeoutCtx := tracing.ContextWithSpan(ctx, nil)
				log.Errorf(
					timeoutCtx,
					"flow id:%s : %d inbound streams timed out after %s; propagated error throughout flow",
					id,
					numTimedOutReceivers,
					timeout,
				)
			}
		})
	}
	return nil
}

// cancelPendingStreams cancels all of the streams that haven't been connected
// yet in this flow, by setting them to finished and ending their wait group.
// All InboundStreamHandlers of these streams are timed out with the provided
// error. The number of such timed out receivers is returned.
//
// This method should be called without holding the mutex of the FlowRegistry.
func (fr *FlowRegistry) cancelPendingStreams(
	id execinfrapb.FlowID, pendingReceiverErr error,
) (numTimedOutReceivers int) {
	var inboundStreams map[execinfrapb.StreamID]*InboundStreamInfo
	fr.Lock()
	entry := fr.flows[id]
	if entry != nil && entry.flow != nil {
		// It is safe to access the inboundStreams map without holding the
		// FlowRegistry mutex.
		inboundStreams = entry.inboundStreams
	}
	fr.Unlock()
	if inboundStreams == nil {
		return 0
	}
	for _, is := range inboundStreams {
		// Connected, non-finished inbound streams will get an error returned in
		// ProcessInboundStream(). Handle non-connected streams here.
		if is.cancelIfNotConnected(pendingReceiverErr) {
			numTimedOutReceivers++
		}
	}
	return numTimedOutReceivers
}

// UnregisterFlow removes a flow from the registry. Any subsequent
// ConnectInboundStream calls for the flow will fail to find it and time out.
func (fr *FlowRegistry) UnregisterFlow(id execinfrapb.FlowID) {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.flows[id]
	if entry.streamTimer != nil {
		entry.streamTimer.Stop()
		entry.streamTimer = nil
	}
	fr.releaseEntryLocked(id)
}

// waitForFlow waits until the flow with the given id gets registered - up to
// the given timeout - and returns the FlowBase. If the timeout elapses, returns
// nil. It should only be called without holding the mutex.
func (fr *FlowRegistry) waitForFlow(
	ctx context.Context, id execinfrapb.FlowID, timeout time.Duration,
) *FlowBase {
	fr.Lock()
	defer fr.Unlock()
	entry := fr.getEntryLocked(id)
	if entry.flow != nil {
		// The flow has just arrived (when the caller temporarily released the
		// lock to send a handshake message on the gRPC stream).
		return entry.flow
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
	fr.Unlock()

	select {
	case <-waitCh:
	case <-time.After(timeout):
	case <-ctx.Done():
	}

	fr.Lock()
	return entry.flow
}

// Drain waits at most flowDrainWait for currently running flows to finish and
// at least minFlowDrainWait for any incoming flows to be registered. If there
// are still flows active after flowDrainWait, Drain waits an extra
// expectedConnectionTime so that any flows that were registered at the end of
// the time window have a reasonable amount of time to connect to their
// consumers, thus unblocking them. All flows that are still running at this
// point are canceled.
//
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
		// Now cancel all still running flows.
		for _, f := range fr.flows {
			if f.flow != nil {
				// f.flow might be nil when ConnectInboundStream() was
				// called, but the consumer of that inbound stream hasn't
				// been scheduled yet.
				f.flow.Cancel()
			}
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

// undrain causes the FlowRegistry to start accepting flows again.
func (fr *FlowRegistry) undrain() {
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
) (_ *FlowBase, _ InboundStreamHandler, cleanup func(), retErr error) {
	fr.Lock()
	entry := fr.getEntryLocked(flowID)
	flow := entry.flow
	// Take a reference that is always released at the end of this method. In
	// the happy case (when we end up returning a flow that we connected to),
	// that flow also took reference in RegisterFlow, so the ref count won't go
	// below one; in all other cases we want to make sure to delete the entry
	// from the registry if we're holding the only reference.
	entry.refCount++
	fr.Unlock()
	defer func() {
		fr.Lock()
		defer fr.Unlock()
		fr.releaseEntryLocked(flowID)
	}()
	if flow == nil {
		// Send the handshake message informing the producer that the consumer has
		// not been scheduled yet. Another handshake will be sent below once the
		// consumer has been connected.
		deadline := timeutil.Now().Add(timeout)
		if err := stream.Send(&execinfrapb.ConsumerSignal{
			Handshake: &execinfrapb.ConsumerHandshake{
				ConsumerScheduled:        false,
				ConsumerScheduleDeadline: &deadline,
			},
		}); err != nil {
			return nil, nil, nil, err
		}
		flow = fr.waitForFlow(ctx, flowID, timeout)
		if flow == nil {
			return nil, nil, nil, errors.Errorf("flow %s not found", flowID)
		}
	}

	defer func() {
		if retErr != nil {
			// If any error is encountered below, we know that the distributed
			// query execution will fail, so we cancel the flow on this node. If
			// this node is the gateway, this might actually be required for
			// proper shutdown of the whole distributed plan.
			flow.Cancel()
		}
	}()

	// entry.inboundStreams is safe to access without holding the mutex since
	// the map is not modified after Flow.Setup.
	s, ok := entry.inboundStreams[streamID]
	if !ok {
		return nil, nil, nil, errors.Errorf("flow %s: no inbound stream %d", flowID, streamID)
	}

	// Don't mark s as connected until after the handshake succeeds.
	handshakeErr := stream.Send(&execinfrapb.ConsumerSignal{
		Handshake: &execinfrapb.ConsumerHandshake{
			ConsumerScheduled: true,
		},
	})

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.mu.canceled {
		// Regardless of whether the handshake succeeded or not, this inbound
		// stream has already been canceled and properly finished.
		return nil, nil, nil, errors.Errorf("flow %s: inbound stream %d came too late", flowID, streamID)
	}
	if handshakeErr != nil {
		// The handshake failed, so we're canceling this stream.
		s.cancelLocked(handshakeErr)
		return nil, nil, nil, handshakeErr
	}
	if s.mu.connected {
		// This is unexpected - the FlowStream RPC was issued twice by the
		// outboxes for the same stream. We are processing the second RPC call
		// right now, so there is another goroutine that will handle the
		// cleanup, so we defer the cleanup to that goroutine.
		return nil, nil, nil, errors.AssertionFailedf("flow %s: inbound stream %d already connected", flowID, streamID)
	}
	s.mu.connected = true
	return flow, s.receiver, s.finish, nil
}
