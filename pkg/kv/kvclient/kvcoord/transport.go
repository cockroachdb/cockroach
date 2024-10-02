// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

//go:generate mockgen -package=kvcoord -destination=mocks_generated_test.go . Transport

// A SendOptions structure describes the algorithm for sending RPCs to one or
// more replicas, depending on error conditions and how many successful
// responses are required.
type SendOptions struct {
	class   rpc.ConnectionClass
	metrics *DistSenderMetrics
	// dontConsiderConnHealth, if set, makes the transport not take into
	// consideration the connection health when deciding the ordering for
	// replicas. When not set, replicas on nodes with unhealthy connections are
	// deprioritized.
	dontConsiderConnHealth bool
}

// TransportFactory encapsulates all interaction with the RPC
// subsystem, allowing it to be mocked out for testing. The factory
// function returns a Transport object which is used to send requests
// to one or more replicas in the slice.
//
// The caller is responsible for ordering the replicas in the slice according to
// the order in which the should be tried.
type TransportFactory func(SendOptions, ReplicaSlice) Transport

// Transport objects can send RPCs to one or more replicas of a range.
// All calls to Transport methods are made from a single thread, so
// Transports are not required to be thread-safe.
type Transport interface {
	// IsExhausted returns true if there are no more replicas to try.
	IsExhausted() bool

	// SendNext synchronously sends the BatchRequest rpc to the next replica.
	// May panic if the transport is exhausted.
	//
	// SendNext is also in charge of importing the remotely collected spans (if
	// any) into the local trace.
	SendNext(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error)

	// NextInternalClient returns the InternalClient to use for making RPC
	// calls.
	NextInternalClient(context.Context) (rpc.RestrictedInternalClient, error)

	// NextReplica returns the replica descriptor of the replica to be tried in
	// the next call to SendNext. MoveToFront will cause the return value to
	// change. Returns a zero value if the transport is exhausted.
	NextReplica() roachpb.ReplicaDescriptor

	// SkipReplica changes the replica that the next SendNext() call would sent to
	// - the replica that NextReplica() would return is skipped.
	SkipReplica()

	// MoveToFront locates the specified replica and moves it to the
	// front of the ordering of replicas to try. If the replica has
	// already been tried, it will be retried. Returns false if the specified
	// replica can't be found and thus can't be moved to the front of the
	// transport.
	MoveToFront(roachpb.ReplicaDescriptor) bool

	// Reset moves back to the first replica in the transport, according to the
	// current ordering. This may not be the same replica as MoveToFront if it was
	// called prior, which places the replica at the next rather than first index.
	Reset()

	// Release releases any resources held by this Transport.
	Release()
}

// These constants are used for the replica health map below.
const (
	healthUnhealthy = iota
	healthHealthy
)

// grpcTransportFactoryImpl is the default TransportFactory, using GRPC.
// Do not use this directly - use grpcTransportFactory instead.
//
// During race builds, we wrap this to hold on to and read all obtained
// requests in a tight loop, exposing data races; see transport_race.go.
func grpcTransportFactoryImpl(
	opts SendOptions, nodeDialer *nodedialer.Dialer, rs ReplicaSlice,
) Transport {
	transport := grpcTransportPool.Get().(*grpcTransport)
	// Grab the saved slice memory from grpcTransport.
	replicas := transport.replicas

	if cap(replicas) < len(rs) {
		replicas = make([]roachpb.ReplicaDescriptor, len(rs))
	} else {
		replicas = replicas[:len(rs)]
	}

	// We'll map the index of the replica descriptor in its slice to its health.
	var health util.FastIntMap
	for i := range rs {
		r := &rs[i]
		replicas[i] = r.ReplicaDescriptor
		healthy := nodeDialer.ConnHealth(r.NodeID, opts.class) == nil
		if healthy {
			health.Set(i, healthHealthy)
		} else {
			health.Set(i, healthUnhealthy)
		}
	}

	*transport = grpcTransport{
		opts:          opts,
		nodeDialer:    nodeDialer,
		class:         opts.class,
		replicas:      replicas,
		replicaHealth: health,
	}

	if !opts.dontConsiderConnHealth {
		// Put known-healthy replica first, while otherwise respecting the existing
		// ordering of the replicas.
		transport.splitHealthy()
	}

	return transport
}

type grpcTransport struct {
	opts       SendOptions
	nodeDialer *nodedialer.Dialer
	class      rpc.ConnectionClass

	replicas []roachpb.ReplicaDescriptor
	// replicaHealth maps replica index within the replicas slice to healthHealthy
	// if healthy, and healthUnhealthy if unhealthy. Used by splitHealthy.
	replicaHealth util.FastIntMap
	// nextReplicaIdx represents the index into replicas of the next replica to be
	// tried.
	nextReplicaIdx int
}

var grpcTransportPool = sync.Pool{
	New: func() interface{} { return &grpcTransport{} },
}

func (gt *grpcTransport) Release() {
	*gt = grpcTransport{
		replicas: gt.replicas[0:],
	}
	grpcTransportPool.Put(gt)
}

// IsExhausted returns false if there are any untried replicas remaining.
func (gt *grpcTransport) IsExhausted() bool {
	return gt.nextReplicaIdx >= len(gt.replicas)
}

// SendNext invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
func (gt *grpcTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	r := gt.replicas[gt.nextReplicaIdx]
	iface, err := gt.NextInternalClient(ctx)
	if err != nil {
		return nil, err
	}
	return gt.sendBatch(ctx, r.NodeID, iface, ba)
}

// NB: nodeID is unused, but accessible in stack traces.
func (gt *grpcTransport) sendBatch(
	ctx context.Context,
	nodeID roachpb.NodeID,
	iface rpc.RestrictedInternalClient,
	ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	// Bail out early if the context is already canceled. (GRPC will
	// detect this pretty quickly, but the first check of the context
	// in the local server comes pretty late)
	if ctx.Err() != nil {
		return nil, errors.Wrap(ctx.Err(), "aborted before batch send")
	}

	gt.opts.metrics.SentCount.Inc(1)
	if rpc.IsLocal(iface) {
		gt.opts.metrics.LocalSentCount.Inc(1)
	}
	log.VEvent(ctx, 2, "sending batch request")
	reply, err := iface.Batch(ctx, ba)
	log.VEvent(ctx, 2, "received batch response")
	// If we queried a remote node, perform extra validation.
	if reply != nil && !rpc.IsLocal(iface) {
		if err == nil {
			for i := range reply.Responses {
				err = reply.Responses[i].GetInner().Verify(ba.Requests[i].GetInner())
				if err != nil {
					log.Errorf(ctx, "verification of response for %s failed: %v", ba.Requests[i].GetInner(), err)
					break
				}
			}
		}
	}

	// Import the remotely collected spans, if any. Do this on error too, to get
	// traces in that case as well (or to at least have a chance).
	//
	// Note that the server fills in reply.CollectedSpans only on non-local
	// requests - see setupSpanForIncomingRPC. For local RPCs, the Tracer is
	// shared between the client and the server, and the server span is a child of
	// the server span.
	if reply != nil && len(reply.CollectedSpans) != 0 {
		span := tracing.SpanFromContext(ctx)
		if span == nil {
			return nil, errors.Errorf(
				"trying to ingest remote spans but there is no recording span set up")
		}
		span.ImportRemoteRecording(reply.CollectedSpans)
		// The field is cleared by the sender because if the spans are re-imported
		// by accident, duplicate spans may occur.
		reply.CollectedSpans = nil
	}
	if err != nil {
		return nil, errors.Wrapf(err, "ba: %s RPC error", ba.String())
	}
	return reply, nil
}

// NextInternalClient returns the next InternalClient to use for performing
// RPCs.
func (gt *grpcTransport) NextInternalClient(
	ctx context.Context,
) (rpc.RestrictedInternalClient, error) {
	r := gt.replicas[gt.nextReplicaIdx]
	gt.nextReplicaIdx++
	return gt.nodeDialer.DialInternalClient(ctx, r.NodeID, gt.class)
}

func (gt *grpcTransport) NextReplica() roachpb.ReplicaDescriptor {
	if gt.IsExhausted() {
		return roachpb.ReplicaDescriptor{}
	}
	return gt.replicas[gt.nextReplicaIdx]
}

// SkipReplica is part of the Transport interface.
func (gt *grpcTransport) SkipReplica() {
	if gt.IsExhausted() {
		return
	}
	gt.nextReplicaIdx++
}

func (gt *grpcTransport) MoveToFront(replica roachpb.ReplicaDescriptor) bool {
	for i := range gt.replicas {
		if gt.replicas[i].IsSame(replica) {
			// If we've already processed the replica, decrement the current
			// index before we swap.
			if i < gt.nextReplicaIdx {
				gt.nextReplicaIdx--
			}
			// Swap the client representing this replica to the front.
			gt.replicas[i], gt.replicas[gt.nextReplicaIdx] = gt.replicas[gt.nextReplicaIdx], gt.replicas[i]
			return true
		}
	}
	return false
}

func (gt *grpcTransport) Reset() {
	gt.nextReplicaIdx = 0
}

// splitHealthy splits the grpcTransport's replica slice into healthy replica
// and unhealthy replica, based on their connection state. Healthy replicas will
// be rearranged first in the replicas slice, and unhealthy replicas will be
// rearranged last. Within these two groups, the rearrangement will be stable.
func (gt *grpcTransport) splitHealthy() {
	sort.Stable((*byHealth)(gt))
}

// byHealth sorts a slice of replicas by their health with healthy first.
type byHealth grpcTransport

func (h *byHealth) Len() int { return len(h.replicas) }
func (h *byHealth) Swap(i, j int) {
	h.replicas[i], h.replicas[j] = h.replicas[j], h.replicas[i]
	oldI := h.replicaHealth.GetDefault(i)
	h.replicaHealth.Set(i, h.replicaHealth.GetDefault(j))
	h.replicaHealth.Set(j, oldI)
}
func (h *byHealth) Less(i, j int) bool {
	ih, ok := h.replicaHealth.Get(i)
	if !ok {
		panic(fmt.Sprintf("missing health info for %s", h.replicas[i]))
	}
	jh, ok := h.replicaHealth.Get(j)
	if !ok {
		panic(fmt.Sprintf("missing health info for %s", h.replicas[j]))
	}
	return ih == healthHealthy && jh != healthHealthy
}

// SenderTransportFactory wraps a client.Sender for use as a KV
// Transport. This is useful for tests that want to use DistSender
// without a full RPC stack.
func SenderTransportFactory(tracer *tracing.Tracer, sender kv.Sender) TransportFactory {
	return func(_ SendOptions, replicas ReplicaSlice) Transport {
		// Always send to the first replica.
		replica := replicas[0].ReplicaDescriptor
		return &senderTransport{tracer, sender, replica, false}
	}
}

type senderTransport struct {
	tracer  *tracing.Tracer
	sender  kv.Sender
	replica roachpb.ReplicaDescriptor

	// called is set once the RPC to the (one) replica is sent.
	called bool
}

func (s *senderTransport) IsExhausted() bool {
	return s.called
}

func (s *senderTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	if s.called {
		panic("called an exhausted transport")
	}
	s.called = true

	ba = ba.ShallowCopy()
	ba.Replica = s.replica
	log.Eventf(ctx, "%v", ba.String())
	br, pErr := s.sender.Send(ctx, ba)
	if br == nil {
		br = &kvpb.BatchResponse{}
	}
	if br.Error != nil {
		panic(kvpb.ErrorUnexpectedlySet(s.sender, br))
	}
	br.Error = pErr
	if pErr != nil {
		log.Eventf(ctx, "error: %v", pErr.String())
	}

	// Import the remotely collected spans, if any.
	if len(br.CollectedSpans) != 0 {
		span := tracing.SpanFromContext(ctx)
		if span == nil {
			panic("trying to ingest remote spans but there is no recording span set up")
		}
		span.ImportRemoteRecording(br.CollectedSpans)
		// The field is cleared by the sender because if the spans are re-imported
		// by accident, duplicate spans may occur.
		br.CollectedSpans = nil
	}

	return br, nil
}

func (s *senderTransport) NextInternalClient(
	ctx context.Context,
) (rpc.RestrictedInternalClient, error) {
	panic("unimplemented")
}

func (s *senderTransport) NextReplica() roachpb.ReplicaDescriptor {
	if s.IsExhausted() {
		return roachpb.ReplicaDescriptor{}
	}
	return s.replica
}

func (s *senderTransport) SkipReplica() {
	// Skipping the (only) replica makes the transport be exhausted.
	s.called = true
}

func (s *senderTransport) MoveToFront(replica roachpb.ReplicaDescriptor) bool {
	return true
}

func (s *senderTransport) Reset() {}

func (s *senderTransport) Release() {}
