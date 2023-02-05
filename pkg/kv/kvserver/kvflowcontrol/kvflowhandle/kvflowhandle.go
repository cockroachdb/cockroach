// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowhandle

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowtokentracker"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Handle is a concrete implementation of the kvflowcontrol.Handle
// interface. It's held on replicas initiating replication traffic, managing
// multiple Streams (one per active replica) underneath.
type Handle struct {
	controller kvflowcontrol.Controller

	mu struct {
		syncutil.Mutex
		connections []*connectedStream
		// perStreamTokenTracker tracks flow token deductions for each stream.
		// It's used to release tokens back to the controller once log entries
		// (identified by their log positions) have been admitted below-raft,
		// streams disconnect, or the handle closed entirely.
		perStreamTokenTracker map[kvflowcontrol.Stream]*kvflowtokentracker.Tracker
		// perStreamLowerBound tracks on a per-stream basis the log position
		// below which we're not allowed to deduct tokens. It's used only for
		// assertions
		perStreamLowerBound map[kvflowcontrol.Stream]kvflowcontrolpb.RaftLogPosition
		closed              bool
	}
}

// New constructs a new Handle.
func New(controller kvflowcontrol.Controller) *Handle {
	h := &Handle{
		controller: controller,
	}
	h.mu.perStreamTokenTracker = map[kvflowcontrol.Stream]*kvflowtokentracker.Tracker{}
	h.mu.perStreamLowerBound = map[kvflowcontrol.Stream]kvflowcontrolpb.RaftLogPosition{}
	return h
}

var _ kvflowcontrol.Handle = &Handle{}

// Admit is part of the kvflowcontrol.Handle interface.
func (h *Handle) Admit(ctx context.Context, pri admissionpb.WorkPriority, ct time.Time) error {
	if h == nil {
		// TODO(irfansharif): This can happen if we're proposing immediately on
		// a newly split off RHS that doesn't know it's a leader yet (so we
		// haven't initialized a handle). We don't want to deduct/track flow
		// tokens for it; the handle only has a lifetime while we explicitly
		// know that we're the leaseholder+leader. We need to pass this
		// information up to the caller somehow, to ensure we use the right
		// encoding. We'll need to do something like that anyway when
		// introducing cluster settings that disable flow control entirely or
		// for regular traffic. We risk returning tokens that we never deducted
		// otherwise.
		return nil
	}

	h.mu.Lock()
	if h.mu.closed {
		log.Fatalf(ctx, "operating on a closed handle")
	}
	connections := h.mu.connections
	h.mu.Unlock()
	for _, c := range connections {
		if err := h.controller.Admit(ctx, pri, ct, c); err != nil {
			return err
		}
	}
	return nil
}

// DeductTokensFor is part of the kvflowcontrol.Handle interface.
func (h *Handle) DeductTokensFor(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	pos kvflowcontrolpb.RaftLogPosition,
	tokens kvflowcontrol.Tokens,
) {
	if h == nil {
		// TODO(irfansharif): See TODO around nil receiver check in Admit().
		return
	}

	_ = h.deductTokensForInner(ctx, pri, pos, tokens)
}

func (h *Handle) deductTokensForInner(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	pos kvflowcontrolpb.RaftLogPosition,
	tokens kvflowcontrol.Tokens,
) (streams []kvflowcontrol.Stream) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.closed {
		log.Fatalf(ctx, "operating on a closed handle")
	}

	for _, c := range h.mu.connections {
		h.controller.DeductTokens(ctx, pri, tokens, c.Stream())
		h.mu.perStreamTokenTracker[c.Stream()].Track(ctx, pri, tokens, pos)

		lowerBound := h.mu.perStreamLowerBound[c.Stream()]
		if !(lowerBound.Less(pos)) {
			log.Fatalf(ctx, "observed regression in lower bound (%s <= %s)", pos, lowerBound)
		}
		h.mu.perStreamLowerBound[c.Stream()] = pos
		streams = append(streams, c.Stream())
	}
	return streams
}

// ReturnTokensUpto is part of the kvflowcontrol.Handle interface.
func (h *Handle) ReturnTokensUpto(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	pos kvflowcontrolpb.RaftLogPosition,
	stream kvflowcontrol.Stream,
) {
	if h == nil {
		// We're trying to release tokens to a handle that no longer exists,
		// likely because we've lost the lease and/or raft leadership since
		// we acquired flow tokens originally. At that point the handle was
		// closed, and all flow tokens were returned back to the controller.
		// There's nothing left for us to do here.
		//
		// NB: It's possible to have reacquired leadership and re-initialize a
		// handle. We still want to ignore token returns from earlier
		// terms/leases (which were already returned to the controller). To that
		// end, we rely on the handle being re-initialized with an empty tracker
		// -- there's simply nothing to double return. Also, when connecting
		// streams on fresh handles, we specify a lower-bound raft log position.
		// The log position corresponds to when the lease/leadership was
		// acquired (whichever comes after). This is used to assert against
		// regressions in token deductions (i.e. deducting tokens for indexes
		// lower than the current term/lease).
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.closed {
		log.Fatalf(ctx, "operating on a closed handle")
	}

	h.mu.perStreamTokenTracker[stream].Untrack(ctx, pri, pos,
		func(tokens kvflowcontrol.Tokens, _ kvflowcontrolpb.RaftLogPosition) {
			h.controller.ReturnTokens(ctx, pri, tokens, stream)
		},
	)

	lowerBound := h.mu.perStreamLowerBound[stream]
	if !(lowerBound.Less(pos)) {
		return // nothing left to do
	}
	h.mu.perStreamLowerBound[stream] = pos
}

// ConnectStream is part of the kvflowcontrol.Handle interface.
func (h *Handle) ConnectStream(
	ctx context.Context, pos kvflowcontrolpb.RaftLogPosition, stream kvflowcontrol.Stream,
) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.closed {
		log.Fatalf(ctx, "operating on a closed handle")
	}

	if _, ok := h.mu.perStreamTokenTracker[stream]; ok {
		log.Fatalf(ctx, "reconnecting already connected stream: %s", stream)
	}
	h.mu.connections = append(h.mu.connections, newConnectedStream(stream))
	sort.Slice(h.mu.connections, func(i, j int) bool { // sort for deadlock avoidance
		return h.mu.connections[i].Stream().StoreID < h.mu.connections[j].Stream().StoreID
	})
	h.mu.perStreamTokenTracker[stream] = kvflowtokentracker.New()
	h.mu.perStreamLowerBound[stream] = pos
}

// DisconnectStream is part of the kvflowcontrol.Handle interface.
func (h *Handle) DisconnectStream(ctx context.Context, stream kvflowcontrol.Stream) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnectStreamLocked(ctx, stream)
}

func (h *Handle) disconnectStreamLocked(ctx context.Context, stream kvflowcontrol.Stream) {
	if h.mu.closed {
		log.Fatalf(ctx, "operating on a closed handle")
	}
	if _, ok := h.mu.perStreamTokenTracker[stream]; !ok {
		log.Fatalf(ctx, "disconnecting non-existent stream: %s", stream)
	}

	h.mu.perStreamTokenTracker[stream].Iter(ctx,
		func(pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens, _ kvflowcontrolpb.RaftLogPosition) bool {
			h.controller.ReturnTokens(ctx, pri, tokens, stream)
			return true
		},
	)
	delete(h.mu.perStreamTokenTracker, stream)
	delete(h.mu.perStreamLowerBound, stream)

	streamIdx := -1
	for i := range h.mu.connections {
		if h.mu.connections[i].Stream() == stream {
			streamIdx = i
			break
		}
	}
	connection := h.mu.connections[streamIdx]
	connection.Disconnect()
	h.mu.connections = append(h.mu.connections[:streamIdx], h.mu.connections[streamIdx+1:]...)

	// TODO(irfansharif): Optionally record lower bound raft log positions for
	// disconnected streams to guard against regressions when (re-)connecting --
	// it must be done with higher positions.
}

// Close is part of the kvflowcontrol.Handle interface.
func (h *Handle) Close(ctx context.Context) {
	if h == nil {
		return // nothing to do
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.closed {
		log.Fatalf(ctx, "operating on a closed handle")
	}

	for _, connection := range h.mu.connections {
		h.disconnectStreamLocked(ctx, connection.Stream())
	}
	h.mu.closed = true
}

// TestingNonBlockingAdmit is a non-blocking alternative to Admit() for use in
// tests.
//   - it checks if we have a non-zero number of flow tokens for all connected
//     streams;
//   - if we do, we return immediately with admitted=true;
//   - if we don't, we return admitted=false and two sets of callbacks:
//     (i) signaled, which can be polled to check whether we're ready to try and
//     admitting again. There's one per underlying stream.
//     (ii) admit, which can be used to try and admit again. If still not
//     admitted, callers are to wait until they're signaled again. There's one
//     per underlying stream.
func (h *Handle) TestingNonBlockingAdmit(
	pri admissionpb.WorkPriority,
) (admitted bool, signaled []func() bool, admit []func() bool) {
	h.mu.Lock()
	if h.mu.closed {
		log.Fatalf(context.Background(), "operating on a closed handle")
	}
	connections := h.mu.connections
	h.mu.Unlock()

	type testingNonBlockingController interface {
		TestingNonBlockingAdmit(
			pri admissionpb.WorkPriority, connection kvflowcontrol.ConnectedStream,
		) (admitted bool, signaled func() bool, admit func() bool)
	}

	admitted = true
	testingController := h.controller.(testingNonBlockingController)
	for _, c := range connections {
		connectionAdmitted, connectionSignaled, connectionAdmit := testingController.TestingNonBlockingAdmit(pri, c)
		if connectionAdmitted {
			continue
		}

		admitted = false
		signaled = append(signaled, connectionSignaled)
		admit = append(admit, connectionAdmit)
	}
	return admitted, signaled, admit
}

// TestingDeductTokensForInner exposes deductTokensForInner for testing
// purposes.
func (h *Handle) TestingDeductTokensForInner(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	pos kvflowcontrolpb.RaftLogPosition,
	tokens kvflowcontrol.Tokens,
) []kvflowcontrol.Stream {
	return h.deductTokensForInner(ctx, pri, pos, tokens)
}
