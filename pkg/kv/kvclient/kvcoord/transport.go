// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/shuffle"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	opentracing "github.com/opentracing/opentracing-go"
)

// A SendOptions structure describes the algorithm for sending RPCs to one or
// more replicas, depending on error conditions and how many successful
// responses are required.
type SendOptions struct {
	class   rpc.ConnectionClass
	metrics *DistSenderMetrics
	// If set, the transport will try the replicas in the order in which they're
	// present in the descriptor, without looking at latency or health. A
	// leaseholder will still be tried first, if specified.
	orderOpt ReorderReplicasOpt
}

// TransportFactory encapsulates all interaction with the RPC
// subsystem, allowing it to be mocked out for testing. The factory
// function returns a Transport object which is used to send requests
// to one or more replicas in the slice.
//
// The caller is responsible for ordering the replicas in the slice according to
// the order in which the should be tried.
type TransportFactory func(
	ctx context.Context,
	opts SendOptions,
	curNode *roachpb.NodeDescriptor,
	nodeDescStore NodeDescStore,
	nodeDialer *nodedialer.Dialer,
	latencyFn LatencyFunc,
	desc *roachpb.RangeDescriptor,
	leaseholder roachpb.ReplicaID,
) (Transport, error)

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
	SendNext(context.Context, roachpb.BatchRequest) (*roachpb.BatchResponse, error)

	// NextInternalClient returns the InternalClient to use for making RPC
	// calls. Returns a context.Context which should be used when making RPC
	// calls on the returned server (This context is annotated to mark this
	// request as in-process and bypass ctx.Peer checks).
	NextInternalClient(context.Context) (context.Context, roachpb.InternalClient, error)

	// NextReplica returns the replica descriptor of the replica to be tried in
	// the next call to SendNext. MoveToFront will cause the return value to
	// change. Returns a zero value if the transport is exhausted.
	NextReplica() roachpb.ReplicaDescriptor

	// SkipReplica changes the replica that the next SendNext() call would sent to
	// - the replica that NextReplica() would return is skipped.
	SkipReplica()

	// Replicas returns all the replicas that this transport was configured with.
	// The replicas will be ordered as the transport will try them.
	Replicas() []roachpb.ReplicaDescriptor

	// MoveToFront locates the specified replica and moves it to the
	// front of the ordering of replicas to try. If the replica has
	// already been tried, it will be retried. If the specified replica
	// can't be found, this is a noop.
	MoveToFront(roachpb.ReplicaDescriptor)
}

// A LatencyFunc returns the latency from this node to a remote
// address and a bool indicating whether the latency is valid.
type LatencyFunc func(string) (time.Duration, bool)

// grpcTransportFactoryImpl is the default TransportFactory, using GRPC.
// Do not use this directly - use grpcTransportFactory instead.
//
// During race builds, we wrap this to hold on to and read all obtained
// requests in a tight loop, exposing data races; see transport_race.go.
func grpcTransportFactoryImpl(
	ctx context.Context,
	opts SendOptions,
	curNode *roachpb.NodeDescriptor,
	nodeDescStore NodeDescStore,
	nodeDialer *nodedialer.Dialer,
	latencyFn LatencyFunc,
	desc *roachpb.RangeDescriptor,
	leaseholder roachpb.ReplicaID,
) (Transport, error) {
	// Sanity check: if we've been given a leaseholder, it should be coherent with
	// the descriptor.
	var lh roachpb.ReplicaDescriptor
	if leaseholder != 0 {
		var ok bool
		if lh, ok = desc.GetReplicaDescriptorByID(leaseholder); !ok {
			log.Fatalf(ctx, "leaseholder not in descriptor; leaseholder: %s, desc: %s", leaseholder, desc)
		}
	}

	replicas, err := OrderReplicas(
		ctx, opts.orderOpt, curNode, nodeDescStore, nodeDialer.HealthCheckerForClass(opts.class),
		latencyFn, desc, lh)
	if err != nil {
		return nil, err
	}

	return &grpcTransport{
		opts:       opts,
		nodeDialer: nodeDialer,
		class:      opts.class,
		replicas:   replicas,
	}, nil
}

type ReorderReplicasOpt bool

const (
	OrderByLatency     ReorderReplicasOpt = false
	DontOrderByLatency ReorderReplicasOpt = true
)

// OrderReplicas orders a slice of replicas in the order in which they should be
// attempted for performing a request on the respective range.
//
// Non-voters are filtered out, with the exception of the leaseholder (if
// specified). Replicas whose node descriptor is not available in nodeDescStore
// are filtered out. Depending on orderOpt, the remaining replicas are ordered
// by expected latency. The leaseholder is then moved to the front. The
// connection health is then considered, according to healthChecker, and
// non-healthy replicas are de-prioritized.
//
// curNode, if not nil, is used to prioritize the local replica if it exists.
//
// leaseholder can be empty if it's not known or if the request
// doesn't need to be routed to the leaseholder.
func OrderReplicas(
	ctx context.Context,
	orderOpt ReorderReplicasOpt,
	curNode *roachpb.NodeDescriptor,
	nodeDescStore NodeDescStore,
	healthChecker nodedialer.HealthChecker,
	latencyFn LatencyFunc,
	desc *roachpb.RangeDescriptor,
	leaseholder roachpb.ReplicaDescriptor,
) ([]roachpb.ReplicaDescriptor, error) {
	// Learner replicas won't serve reads/writes, so we'll send only to the
	// `Voters` replicas. This is just an optimization to save a network hop,
	// everything would still work if we had `All` here.
	replicas := desc.Replicas().Voters()
	// The slice returned above might share memory with the descriptor, which we
	// don't want to modify, so we'll make a copy.
	replicasCpy := make([]roachpb.ReplicaDescriptor, len(replicas))
	copy(replicasCpy, replicas)
	replicas = replicasCpy
	// If we know a leaseholder, though, let's make sure we include it.
	if leaseholder != (roachpb.ReplicaDescriptor{}) && len(replicas) < len(desc.Replicas().All()) {
		found := false
		for _, v := range replicas {
			if v == leaseholder {
				found = true
				break
			}
		}
		if !found {
			log.Eventf(ctx, "the descriptor has the leaseholder as a learner; including it anyway")
			replicas = append(replicas, leaseholder)
		}
	}

	// Filter out nodes with descriptors not present in the nodeDescStore.
	rs := make(replicaSlice, 0, len(replicas))
	for _, r := range replicas {
		nd, err := nodeDescStore.GetNodeDescriptor(r.NodeID)
		if err != nil {
			if log.V(1) {
				log.Infof(ctx, "node %d is not gossiped: %v", r.NodeID, err)
			}
			continue
		}
		rs = append(rs, replicaInfo{
			ReplicaDescriptor: r,
			nodeDesc:          nd,
		})
	}
	if len(rs) == 0 {
		return nil, newSendError(
			fmt.Sprintf("no replica node addresses available via gossip for r%d", desc.RangeID))
	}

	// Rearrange the replicas so that they're ordered in expectation of
	// request latency. Leaseholder considerations come below.
	if orderOpt == OrderByLatency {
		sortByLatency(rs, curNode, latencyFn)
	}

	// If we were given a leaseholder, move it to the front.
	if leaseholder != (roachpb.ReplicaDescriptor{}) {
		rs.moveToFront(ctx, leaseholder.ReplicaID)
	}

	// Put known-healthy clients first, while otherwise respecting the existing
	// ordering of the replicas.
	health := make(map[roachpb.ReplicaDescriptor]bool)
	for _, r := range replicas {
		health[r] = healthChecker.ConnHealth(r.NodeID) == nil
	}
	for i := range rs {
		replicas[i] = rs[i].ReplicaDescriptor
	}
	splitHealthy(replicas, health)
	return replicas, nil
}

// A replicaSlice is a slice of ReplicaInfo.
type replicaSlice []replicaInfo

// replicaSlice implements shuffle.Interface.
var _ shuffle.Interface = replicaSlice{}

// Len returns the total number of replicas in the slice.
func (rs replicaSlice) Len() int { return len(rs) }

// Swap swaps the replicas with indexes i and j.
func (rs replicaSlice) Swap(i, j int) { rs[i], rs[j] = rs[j], rs[i] }

// MoveToFront moves the given replica to the front of the slice
// keeping the order of the remaining elements stable.
// The function will panic when invoked with an invalid index.
func (rs replicaSlice) moveToFront(ctx context.Context, r roachpb.ReplicaID) {
	var i int
	for i = 0; i < len(rs); i++ {
		if rs[i].ReplicaID == r {
			break
		}
	}
	if i == len(rs) {
		log.Fatalf(ctx, "replica %s not found in list %s", r, rs)
	}

	front := rs[i]
	// Move the first i elements one index to the right
	copy(rs[1:], rs[:i])
	rs[0] = front
}

// replicaInfo extends the Replica structure with the associated node
// descriptor.
type replicaInfo struct {
	roachpb.ReplicaDescriptor
	nodeDesc *roachpb.NodeDescriptor
}

func sortByLatency(rs replicaSlice, curNode *roachpb.NodeDescriptor, latencyFn LatencyFunc) {
	// If we don't know which node we're on, send the RPCs randomly.
	if curNode == nil {
		shuffle.Shuffle(rs)
		return
	}
	// Sort replicas by latency and then attribute affinity.
	sort.Slice(rs, func(i, j int) bool {
		// If there is a replica in local node, it sorts first.
		if rs[i].NodeID == curNode.NodeID {
			return true
		}
		if latencyFn != nil {
			latencyI, okI := latencyFn(rs[i].nodeDesc.Address.String())
			latencyJ, okJ := latencyFn(rs[j].nodeDesc.Address.String())
			if okI && okJ {
				return latencyI < latencyJ
			}
		}
		attrMatchI := localityMatch(curNode.Locality.Tiers, rs[i].nodeDesc.Locality.Tiers)
		attrMatchJ := localityMatch(curNode.Locality.Tiers, rs[j].nodeDesc.Locality.Tiers)
		// Longer locality matches sort first (the assumption is that
		// they'll have better latencies).
		return attrMatchI > attrMatchJ
	})
}

// localityMatch returns the number of consecutive locality tiers
// which match between a and b.
func localityMatch(a, b []roachpb.Tier) int {
	if len(a) == 0 {
		return 0
	}
	for i := range a {
		if i >= len(b) || a[i] != b[i] {
			return i
		}
	}
	return len(a)
}

type grpcTransport struct {
	opts       SendOptions
	nodeDialer *nodedialer.Dialer
	class      rpc.ConnectionClass

	replicas []roachpb.ReplicaDescriptor
	// nextReplicaIdx represents the index into replicas of the next replica to be
	// tried.
	nextReplicaIdx int
}

// IsExhausted returns false if there are any untried replicas remaining.
func (gt *grpcTransport) IsExhausted() bool {
	return gt.nextReplicaIdx >= len(gt.replicas)
}

// SendNext invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
func (gt *grpcTransport) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	r := gt.replicas[gt.nextReplicaIdx]
	ctx, iface, err := gt.NextInternalClient(ctx)
	if err != nil {
		return nil, err
	}

	ba.Replica = r
	return gt.sendBatch(ctx, r.NodeID, iface, ba)
}

// NB: nodeID is unused, but accessible in stack traces.
func (gt *grpcTransport) sendBatch(
	ctx context.Context, nodeID roachpb.NodeID, iface roachpb.InternalClient, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
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
	reply, err := iface.Batch(ctx, &ba)
	// If we queried a remote node, perform extra validation and
	// import trace spans.
	if reply != nil && !rpc.IsLocal(iface) {
		for i := range reply.Responses {
			if err := reply.Responses[i].GetInner().Verify(ba.Requests[i].GetInner()); err != nil {
				log.Errorf(ctx, "%v", err)
			}
		}
		// Import the remotely collected spans, if any.
		if len(reply.CollectedSpans) != 0 {
			span := opentracing.SpanFromContext(ctx)
			if span == nil {
				return nil, errors.Errorf(
					"trying to ingest remote spans but there is no recording span set up")
			}
			if err := tracing.ImportRemoteSpans(span, reply.CollectedSpans); err != nil {
				return nil, errors.Wrap(err, "error ingesting remote spans")
			}
		}
	}
	return reply, err
}

// NextInternalClient returns the next InternalClient to use for performing
// RPCs.
func (gt *grpcTransport) NextInternalClient(
	ctx context.Context,
) (context.Context, roachpb.InternalClient, error) {
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

func (gt *grpcTransport) MoveToFront(replica roachpb.ReplicaDescriptor) {
	for i := range gt.replicas {
		if gt.replicas[i] == replica {
			// If we've already processed the replica, decrement the current
			// index before we swap.
			if i < gt.nextReplicaIdx {
				gt.nextReplicaIdx--
			}
			// Swap the client representing this replica to the front.
			gt.replicas[i], gt.replicas[gt.nextReplicaIdx] = gt.replicas[gt.nextReplicaIdx], gt.replicas[i]
			return
		}
	}
}

func (gt *grpcTransport) Replicas() []roachpb.ReplicaDescriptor {
	return gt.replicas
}

// splitHealthy splits the provided client slice into healthy clients and
// unhealthy clients, based on their connection state. Healthy clients will be
// rearranged first in the slice, and unhealthy clients will be rearranged last.
// Within these two groups, the rearrangement will be stable.
func splitHealthy(replicas []roachpb.ReplicaDescriptor, health map[roachpb.ReplicaDescriptor]bool) {
	sort.Stable(byHealth{replicas: replicas, health: health})
}

// byHealth sorts a slice of batchClients by their health with healthy first.
type byHealth struct {
	replicas []roachpb.ReplicaDescriptor
	health   map[roachpb.ReplicaDescriptor]bool
}

func (h byHealth) Len() int { return len(h.replicas) }
func (h byHealth) Swap(i, j int) {
	h.replicas[i], h.replicas[j] = h.replicas[j], h.replicas[i]
}
func (h byHealth) Less(i, j int) bool {
	ih, ok := h.health[h.replicas[i]]
	if !ok {
		panic(fmt.Sprintf("missing health info for %s", h.replicas[i]))
	}
	jh, ok := h.health[h.replicas[j]]
	if !ok {
		panic(fmt.Sprintf("missing health info for %s", h.replicas[j]))
	}
	return ih && !jh
}

// SenderTransportFactory wraps a client.Sender for use as a KV
// Transport. This is useful for tests that want to use DistSender
// without a full RPC stack.
func SenderTransportFactory(tracer opentracing.Tracer, sender kv.Sender) TransportFactory {
	return func(
		ctx context.Context,
		opts SendOptions,
		curNode *roachpb.NodeDescriptor,
		nodeDescStore NodeDescStore,
		nodeDialer *nodedialer.Dialer,
		latencyFn LatencyFunc,
		desc *roachpb.RangeDescriptor,
		leaseholder roachpb.ReplicaID,
	) (Transport, error) {
		voters := desc.Replicas().Voters()
		if len(voters) == 0 {
			return nil, errors.Errorf("no voters in desc: %s", desc)
		}
		// Always send to the first voter replica.
		replica := desc.Replicas().Voters()[0]
		return &senderTransport{tracer, sender, replica, false}, nil
	}
}

type senderTransport struct {
	tracer  opentracing.Tracer
	sender  kv.Sender
	replica roachpb.ReplicaDescriptor

	// called is set once the RPC to the (one) replica is sent.
	called bool
}

func (s *senderTransport) IsExhausted() bool {
	return s.called
}

func (s *senderTransport) SendNext(
	ctx context.Context, ba roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	if s.called {
		panic("called an exhausted transport")
	}
	s.called = true

	ctx, cleanup := tracing.EnsureContext(ctx, s.tracer, "node" /* name */)
	defer cleanup()

	ba.Replica = s.replica
	log.Eventf(ctx, "%v", ba.String())
	br, pErr := s.sender.Send(ctx, ba)
	if br == nil {
		br = &roachpb.BatchResponse{}
	}
	if br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(s.sender, br))
	}
	br.Error = pErr
	if pErr != nil {
		log.Eventf(ctx, "error: %v", pErr.String())
	}

	// Import the remotely collected spans, if any.
	if len(br.CollectedSpans) != 0 {
		span := opentracing.SpanFromContext(ctx)
		if span == nil {
			panic("trying to ingest remote spans but there is no recording span set up")
		}
		if err := tracing.ImportRemoteSpans(span, br.CollectedSpans); err != nil {
			panic(err)
		}
	}

	return br, nil
}

func (s *senderTransport) NextInternalClient(
	ctx context.Context,
) (context.Context, roachpb.InternalClient, error) {
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

func (s *senderTransport) MoveToFront(replica roachpb.ReplicaDescriptor) {
}

func (s *senderTransport) Replicas() []roachpb.ReplicaDescriptor {
	return []roachpb.ReplicaDescriptor{s.replica}
}
