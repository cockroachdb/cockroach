// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowhandle

import (
	"cmp"
	"context"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowtokentracker"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Handle is a concrete implementation of the kvflowcontrol.Handle
// interface. It's held on replicas initiating replication traffic, managing
// multiple Streams (one per active replica) underneath.
type Handle struct {
	controller kvflowcontrol.Controller
	metrics    *Metrics
	clock      *hlc.Clock
	rangeID    roachpb.RangeID
	tenantID   roachpb.TenantID

	mu struct {
		syncutil.Mutex
		connections []*connectedStream
		// perStreamTokenTracker tracks flow token deductions for each stream.
		// It's used to release tokens back to the controller once log entries
		// (identified by their log positions) have been admitted below-raft,
		// streams disconnect, or the handle closed entirely.
		perStreamTokenTracker map[kvflowcontrol.Stream]*kvflowtokentracker.Tracker
		closed                bool
	}
	knobs *kvflowcontrol.TestingKnobs
}

// New constructs a new Handle.
func New(
	controller kvflowcontrol.Controller,
	metrics *Metrics,
	clock *hlc.Clock,
	rangeID roachpb.RangeID,
	tenantID roachpb.TenantID,
	knobs *kvflowcontrol.TestingKnobs,
) *Handle {
	if metrics == nil { // only nil in tests
		metrics = NewMetrics(nil)
	}
	if knobs == nil {
		knobs = &kvflowcontrol.TestingKnobs{}
	}
	h := &Handle{
		controller: controller,
		metrics:    metrics,
		clock:      clock,
		rangeID:    rangeID,
		tenantID:   tenantID,
		knobs:      knobs,
	}
	h.mu.perStreamTokenTracker = map[kvflowcontrol.Stream]*kvflowtokentracker.Tracker{}
	return h
}

var _ kvflowcontrol.Handle = &Handle{}

// Admit is part of the kvflowcontrol.Handle interface.
func (h *Handle) Admit(
	ctx context.Context, pri admissionpb.WorkPriority, ct time.Time,
) (bool, error) {
	if h == nil {
		// TODO(irfansharif): This can happen if we're proposing immediately on
		// a newly split off RHS that doesn't know it's a leader yet (so we
		// haven't initialized a handle). We don't want to deduct/track flow
		// tokens for it; the handle only has a lifetime while we explicitly
		// know that we're the leaseholder+leader. It's ok for the caller to
		// later invoke ReturnTokensUpto even with a no-op DeductTokensFor since
		// it can only return what has been actually been deducted.
		//
		// As for cluster settings that disable flow control entirely or only
		// for regular traffic, that can be dealt with at the caller by not
		// calling .Admit() and ensuring we use the right raft entry encodings.
		return false, nil
	}

	h.mu.Lock()
	if h.mu.closed {
		h.mu.Unlock()
		log.Errorf(ctx, "operating on a closed handle")
		return false, nil
	}

	// NB: We're using a copy-on-write scheme elsewhere to maintain this slice
	// of sorted connections. Here (the performance critical read path) we
	// simply grab a reference to the connections slice under the mutex before
	// iterating through them further below and invoking the blocking
	// kvflowcontrol.Controller.Admit() for each one, something we want to do
	// without holding the mutex.
	connections := h.mu.connections
	h.mu.Unlock()

	class := admissionpb.WorkClassFromPri(pri)
	h.metrics.onWaiting(class)
	tstart := h.clock.PhysicalTime()

	// NB: We track whether the last stream was subject to flow control, this
	// helps us decide later if we should be deducting tokens for this work.
	var admitted bool
	for _, c := range connections {
		var err error
		admitted, err = h.controller.Admit(ctx, pri, ct, c)
		if err != nil {
			h.metrics.onErrored(class, h.clock.PhysicalTime().Sub(tstart))
			return false, err
		}
	}

	h.metrics.onAdmitted(class, h.clock.PhysicalTime().Sub(tstart))
	return admitted, nil
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
		log.Errorf(ctx, "operating on a closed handle")
		return nil // unused return value in production code
	}

	if fn := h.knobs.OverrideTokenDeduction; fn != nil {
		tokens = fn(tokens)
	}

	for _, c := range h.mu.connections {
		if h.mu.perStreamTokenTracker[c.Stream()].Track(ctx, pri, tokens, pos) {
			// Only deduct tokens if we're able to track them for subsequent
			// returns. We risk leaking flow tokens otherwise.
			h.controller.DeductTokens(ctx, pri, tokens, c.Stream())

			// TODO(irfansharif,aaditya): This accounts for 0.4% of
			// alloc_objects when running kv0/enc=false/nodes=3/cpu=9. Except
			// this return type is not used in production code. Clean it up as
			// part of #104154.
			streams = append(streams, c.Stream())
		}
	}
	return streams
}

// ReturnTokensUpto is part of the kvflowcontrol.Handle interface.
func (h *Handle) ReturnTokensUpto(
	ctx context.Context,
	pri admissionpb.WorkPriority,
	upto kvflowcontrolpb.RaftLogPosition,
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

	if !stream.TenantID.IsSet() {
		// NB: The tenant ID is set in the local fast path for token returns,
		// through the kvflowcontrol.Dispatch. Tecnically we could set the
		// tenant ID by looking up the local replica and reading it, but it's
		// easier to do it this way having captured it when the handle was
		// instantiated.
		stream.TenantID = h.tenantID
	}

	// TODO(irfansharif,aaditya): This mutex still shows up in profiles, ~0.3%
	// for kv0/enc=false/nodes=3/cpu=9. Maybe clean it up as part of #104154.
	h.mu.Lock()
	if h.mu.closed {
		h.mu.Unlock()
		log.Errorf(ctx, "operating on a closed handle")
		return
	}

	tokens := h.mu.perStreamTokenTracker[stream].Untrack(ctx, pri, upto)
	h.mu.Unlock()

	h.controller.ReturnTokens(ctx, pri, tokens, stream)
}

// ConnectStream is part of the kvflowcontrol.Handle interface.
func (h *Handle) ConnectStream(
	ctx context.Context, pos kvflowcontrolpb.RaftLogPosition, stream kvflowcontrol.Stream,
) {
	if !stream.TenantID.IsSet() {
		// See comment in (*Handle).ReturnTokensUpto above where this same check
		// exists. The callers here do typically have this set, but it doesn't
		// hurt to be defensive.
		stream.TenantID = h.tenantID
	}

	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.closed {
		log.Errorf(ctx, "operating on a closed handle")
		return
	}

	h.connectStreamLocked(ctx, pos, stream)
}

func (h *Handle) connectStreamLocked(
	ctx context.Context, pos kvflowcontrolpb.RaftLogPosition, stream kvflowcontrol.Stream,
) {
	if _, ok := h.mu.perStreamTokenTracker[stream]; ok {
		log.Fatalf(ctx, "reconnecting already connected stream: %s", stream)
	}

	connections := make([]*connectedStream, len(h.mu.connections)+1)
	copy(connections, h.mu.connections)
	connections[len(connections)-1] = newConnectedStream(stream)
	slices.SortFunc(connections, func(a, b *connectedStream) int {
		// Sort connections based on store IDs (this is the order in which we
		// invoke Controller.Admit) for predictability. If in the future we use
		// flow tokens for raft log catchup (see I11 and [^9] in
		// kvflowcontrol/doc.go), we may want to introduce an Admit-variant that
		// both blocks and deducts tokens before sending catchup MsgApps. In
		// that case, this sorting will help avoid deadlocks.
		return cmp.Compare(a.Stream().StoreID, b.Stream().StoreID)
	})
	// NB: We use a copy-on-write scheme when appending to the connections slice
	// -- the read path is what's performance critical.
	h.mu.connections = connections

	h.mu.perStreamTokenTracker[stream] = kvflowtokentracker.New(pos, stream, h.knobs)
	h.metrics.StreamsConnected.Inc(1)
	log.VInfof(ctx, 1, "connected to stream: %s", stream)
}

// DisconnectStream is part of the kvflowcontrol.Handle interface.
func (h *Handle) DisconnectStream(ctx context.Context, stream kvflowcontrol.Stream) {
	if !stream.TenantID.IsSet() {
		// See comment in (*Handle).ReturnTokensUpto above where this same check
		// exists. The callers here do typically have this set, but it doesn't
		// hurt to be defensive.
		stream.TenantID = h.tenantID
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.disconnectStreamLocked(ctx, stream)
}

// ResetStreams is part of the kvflowcontrol.Handle interface.
func (h *Handle) ResetStreams(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.mu.closed {
		log.Errorf(ctx, "operating on a closed handle")
		return
	}

	var streams []kvflowcontrol.Stream
	var lowerBounds []kvflowcontrolpb.RaftLogPosition
	for stream, tracker := range h.mu.perStreamTokenTracker {
		streams = append(streams, stream)
		lowerBounds = append(lowerBounds, tracker.LowerBound())
	}
	for i := range streams {
		h.disconnectStreamLocked(ctx, streams[i])
	}
	for i := range streams {
		h.connectStreamLocked(ctx, lowerBounds[i], streams[i])
	}
}

// Inspect is part of the kvflowcontrol.Handle interface.
func (h *Handle) Inspect(ctx context.Context) kvflowinspectpb.Handle {
	h.mu.Lock()
	defer h.mu.Unlock()

	handle := kvflowinspectpb.Handle{
		RangeID: h.rangeID,
	}
	for _, c := range h.mu.connections {
		connected := kvflowinspectpb.ConnectedStream{
			Stream:            h.controller.InspectStream(ctx, c.Stream()),
			TrackedDeductions: h.mu.perStreamTokenTracker[c.Stream()].Inspect(ctx),
		}
		handle.ConnectedStreams = append(handle.ConnectedStreams, connected)
	}
	return handle
}

func (h *Handle) disconnectStreamLocked(ctx context.Context, stream kvflowcontrol.Stream) {
	if h.mu.closed {
		log.Errorf(ctx, "operating on a closed handle")
		return
	}
	if _, ok := h.mu.perStreamTokenTracker[stream]; !ok {
		return
	}

	h.mu.perStreamTokenTracker[stream].Iter(ctx,
		func(pri admissionpb.WorkPriority, tokens kvflowcontrol.Tokens) {
			h.controller.ReturnTokens(ctx, pri, tokens, stream)
		},
	)
	delete(h.mu.perStreamTokenTracker, stream)

	connections := make([]*connectedStream, 0, len(h.mu.connections)-1)
	for i := range h.mu.connections {
		if h.mu.connections[i].Stream() == stream {
			h.mu.connections[i].Disconnect()
		} else {
			connections = append(connections, h.mu.connections[i])
		}
	}
	// NB: We use a copy-on-write scheme when splicing the connections slice --
	// the read path is what's performance critical.
	h.mu.connections = connections

	log.VInfof(ctx, 1, "disconnected stream: %s", stream)
	h.metrics.StreamsDisconnected.Inc(1)
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
		log.Errorf(ctx, "operating on a closed handle")
		return
	}

	var streams []kvflowcontrol.Stream
	for stream := range h.mu.perStreamTokenTracker {
		streams = append(streams, stream)
	}
	for _, stream := range streams {
		h.disconnectStreamLocked(ctx, stream)
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
	ctx context.Context, pri admissionpb.WorkPriority,
) (admitted bool, signaled []func() bool, admit []func() bool) {
	h.mu.Lock()
	if h.mu.closed {
		log.Fatalf(ctx, "operating on a closed handle")
	}
	connections := h.mu.connections
	h.mu.Unlock()

	type testingNonBlockingController interface {
		TestingNonBlockingAdmit(
			pri admissionpb.WorkPriority, connection kvflowcontrol.ConnectedStream,
		) (admitted bool, signaled func() bool, admit func() bool)
	}

	tstart := h.clock.PhysicalTime()
	class := admissionpb.WorkClassFromPri(pri)
	h.metrics.onWaiting(class)

	admitted = true
	controller := h.controller.(testingNonBlockingController)
	for _, c := range connections {
		connectionAdmitted, connectionSignaled, connectionAdmit := controller.TestingNonBlockingAdmit(pri, c)
		if connectionAdmitted {
			continue
		}

		admit = append(admit, func() bool {
			if connectionAdmit() {
				h.metrics.onAdmitted(class, h.clock.PhysicalTime().Sub(tstart))
				return true
			}
			return false
		})
		signaled = append(signaled, connectionSignaled)
		admitted = false
	}
	if admitted {
		h.metrics.onAdmitted(class, h.clock.PhysicalTime().Sub(tstart))
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
