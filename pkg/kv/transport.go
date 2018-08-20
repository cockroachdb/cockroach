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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package kv

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// A SendOptions structure describes the algorithm for sending RPCs to one or
// more replicas, depending on error conditions and how many successful
// responses are required.
type SendOptions struct {
	metrics *DistSenderMetrics
}

type batchClient struct {
	nodeID    roachpb.NodeID
	args      roachpb.BatchRequest
	healthy   bool
	pending   bool
	retryable bool
	deadline  time.Time
}

// TransportFactory encapsulates all interaction with the RPC
// subsystem, allowing it to be mocked out for testing. The factory
// function returns a Transport object which is used to send the given
// arguments to one or more replicas in the slice.
//
// In addition to actually sending RPCs, the transport is responsible
// for ordering replicas in accordance with SendOptions.Ordering and
// transport-specific knowledge such as connection health or latency.
//
// TODO(bdarnell): clean up this crufty interface; it was extracted
// verbatim from the non-abstracted code.
type TransportFactory func(
	SendOptions, *nodedialer.Dialer, ReplicaSlice, roachpb.BatchRequest,
) (Transport, error)

// Transport objects can send RPCs to one or more replicas of a range.
// All calls to Transport methods are made from a single thread, so
// Transports are not required to be thread-safe.
type Transport interface {
	// IsExhausted returns true if there are no more replicas to try.
	IsExhausted() bool

	// GetPending returns the replica(s) to which requests are still pending.
	GetPending() []roachpb.ReplicaDescriptor

	// SendNext synchronously sends the rpc (captured at creation time) to the
	// next replica. May panic if the transport is exhausted.
	//
	// SendNext is also in charge of importing the remotely collected spans (if
	// any) into the local trace.
	SendNext(context.Context) (*roachpb.BatchResponse, error)

	// NextReplica returns the replica descriptor of the replica to be tried in
	// the next call to SendNext. MoveToFront will cause the return value to
	// change. Returns a zero value if the transport is exhausted.
	NextReplica() roachpb.ReplicaDescriptor

	// MoveToFront locates the specified replica and moves it to the
	// front of the ordering of replicas to try. If the replica has
	// already been tried, it will be retried. If the specified replica
	// can't be found, this is a noop.
	MoveToFront(roachpb.ReplicaDescriptor)

	// Close is called when the transport is no longer needed. It may
	// cancel any pending RPCs without writing any response to the channel.
	Close()
}

// grpcTransportFactoryImpl is the default TransportFactory, using GRPC.
// Do not use this directly - use grpcTransportFactory instead.
//
// During race builds, we wrap this to hold on to and read all obtained
// requests in a tight loop, exposing data races; see transport_race.go.
func grpcTransportFactoryImpl(
	opts SendOptions, nodeDialer *nodedialer.Dialer, replicas ReplicaSlice, args roachpb.BatchRequest,
) (Transport, error) {
	clients := make([]batchClient, 0, len(replicas))
	for _, replica := range replicas {
		argsCopy := args
		argsCopy.Replica = replica.ReplicaDescriptor
		healthy := nodeDialer.ConnHealth(replica.NodeID) == nil
		clients = append(clients, batchClient{
			nodeID:  replica.NodeID,
			args:    argsCopy,
			healthy: healthy,
		})
	}

	// Put known-healthy clients first.
	splitHealthy(clients)

	return &grpcTransport{
		opts:           opts,
		nodeDialer:     nodeDialer,
		orderedClients: clients,
	}, nil
}

type grpcTransport struct {
	opts            SendOptions
	nodeDialer      *nodedialer.Dialer
	clientIndex     int
	orderedClients  []batchClient
	clientPendingMu syncutil.Mutex // protects access to all batchClient pending flags
	closeWG         sync.WaitGroup // waits until all SendNext goroutines are done
	cancels         []func()       // called on Close()
}

// IsExhausted returns false if there are any untried replicas remaining. If
// there are none, it attempts to resurrect replicas which were tried but
// failed with a retryable error. If any where resurrected, returns false;
// true otherwise.
func (gt *grpcTransport) IsExhausted() bool {
	gt.clientPendingMu.Lock()
	defer gt.clientPendingMu.Unlock()
	if gt.clientIndex < len(gt.orderedClients) {
		return false
	}
	return !gt.maybeResurrectRetryablesLocked()
}

// GetPending returns the replica(s) to which requests are still pending.
func (gt *grpcTransport) GetPending() []roachpb.ReplicaDescriptor {
	gt.clientPendingMu.Lock()
	defer gt.clientPendingMu.Unlock()
	var pending []roachpb.ReplicaDescriptor
	for i := range gt.orderedClients {
		if gt.orderedClients[i].pending {
			pending = append(pending, gt.orderedClients[i].args.Replica)
		}
	}
	return pending
}

// maybeResurrectRetryablesLocked moves already-tried replicas which
// experienced a retryable error (currently this means a
// NotLeaseHolderError) into a newly-active state so that they can be
// retried. Returns true if any replicas were moved to active.
func (gt *grpcTransport) maybeResurrectRetryablesLocked() bool {
	var resurrect []batchClient
	for i := 0; i < gt.clientIndex; i++ {
		if c := gt.orderedClients[i]; !c.pending && c.retryable && timeutil.Since(c.deadline) >= 0 {
			resurrect = append(resurrect, c)
		}
	}
	for _, c := range resurrect {
		gt.moveToFrontLocked(c.args.Replica)
	}
	return len(resurrect) > 0
}

// SendNext invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
func (gt *grpcTransport) SendNext(ctx context.Context) (*roachpb.BatchResponse, error) {
	client := gt.orderedClients[gt.clientIndex]
	gt.clientIndex++

	gt.setState(client.args.Replica, true /* pending */, false /* retryable */)

	{
		var cancel func()
		ctx, cancel = context.WithCancel(ctx)
		gt.cancels = append(gt.cancels, cancel)
	}
	return gt.send(ctx, client)
}

func (gt *grpcTransport) send(
	ctx context.Context, client batchClient,
) (*roachpb.BatchResponse, error) {
	reply, err := func() (*roachpb.BatchResponse, error) {
		// Bail out early if the context is already canceled. (GRPC will
		// detect this pretty quickly, but the first check of the context
		// in the local server comes pretty late)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		gt.opts.metrics.SentCount.Inc(1)
		var iface roachpb.InternalClient
		var err error
		ctx, iface, err = gt.nodeDialer.DialInternalClient(ctx, client.nodeID)
		if err != nil {
			return nil, err
		}
		if rpc.IsLocal(iface) {
			gt.opts.metrics.LocalSentCount.Inc(1)
		}
		reply, err := iface.Batch(ctx, &client.args)
		// If we queried a remote node, perform extra validation and
		// import trace spans.
		if reply != nil && !rpc.IsLocal(iface) {
			for i := range reply.Responses {
				if err := reply.Responses[i].GetInner().Verify(client.args.Requests[i].GetInner()); err != nil {
					log.Error(ctx, err)
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
	}()

	// NotLeaseHolderErrors can be retried.
	var retryable bool
	if reply != nil && reply.Error != nil {
		// TODO(spencer): pass the lease expiration when setting the state
		// to set a more efficient deadline for retrying this replica.
		if _, ok := reply.Error.GetDetail().(*roachpb.NotLeaseHolderError); ok {
			retryable = true
		}
	}
	gt.setState(client.args.Replica, false /* pending */, retryable)

	return reply, err
}

func (gt *grpcTransport) NextReplica() roachpb.ReplicaDescriptor {
	if gt.IsExhausted() {
		return roachpb.ReplicaDescriptor{}
	}
	return gt.orderedClients[gt.clientIndex].args.Replica
}

func (gt *grpcTransport) MoveToFront(replica roachpb.ReplicaDescriptor) {
	gt.clientPendingMu.Lock()
	defer gt.clientPendingMu.Unlock()
	gt.moveToFrontLocked(replica)
}

func (gt *grpcTransport) moveToFrontLocked(replica roachpb.ReplicaDescriptor) {
	for i := range gt.orderedClients {
		if gt.orderedClients[i].args.Replica == replica {
			// If a call to this replica is active, don't move it.
			if gt.orderedClients[i].pending {
				return
			}
			// Clear the retryable bit as this replica is being made
			// available.
			gt.orderedClients[i].retryable = false
			gt.orderedClients[i].deadline = time.Time{}
			// If we've already processed the replica, decrement the current
			// index before we swap.
			if i < gt.clientIndex {
				gt.clientIndex--
			}
			// Swap the client representing this replica to the front.
			gt.orderedClients[i], gt.orderedClients[gt.clientIndex] =
				gt.orderedClients[gt.clientIndex], gt.orderedClients[i]
			return
		}
	}
}

func (gt *grpcTransport) Close() {
	for _, cancel := range gt.cancels {
		cancel()
	}
	gt.closeWG.Wait()
}

// NB: this method's callers may have a reference to the client they wish to
// mutate, but the clients reside in a slice which is shuffled via
// MoveToFront, making it unsafe to mutate the client through a reference to
// the slice.
func (gt *grpcTransport) setState(replica roachpb.ReplicaDescriptor, pending, retryable bool) {
	gt.clientPendingMu.Lock()
	defer gt.clientPendingMu.Unlock()
	for i := range gt.orderedClients {
		if gt.orderedClients[i].args.Replica == replica {
			gt.orderedClients[i].pending = pending
			gt.orderedClients[i].retryable = retryable
			if retryable {
				gt.orderedClients[i].deadline = timeutil.Now().Add(time.Second)
			}
			break
		}
	}
}

// splitHealthy splits the provided client slice into healthy clients and
// unhealthy clients, based on their connection state. Healthy clients will
// be rearranged first in the slice, and unhealthy clients will be rearranged
// last. Within these two groups, the rearrangement will be stable. The function
// will then return the number of healthy clients.
func splitHealthy(clients []batchClient) int {
	var nHealthy int
	sort.Stable(byHealth(clients))
	for _, client := range clients {
		if client.healthy {
			nHealthy++
		}
	}
	return nHealthy
}

// byHealth sorts a slice of batchClients by their health with healthy first.
type byHealth []batchClient

func (h byHealth) Len() int           { return len(h) }
func (h byHealth) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h byHealth) Less(i, j int) bool { return h[i].healthy && !h[j].healthy }

// SenderTransportFactory wraps a client.Sender for use as a KV
// Transport. This is useful for tests that want to use DistSender
// without a full RPC stack.
func SenderTransportFactory(tracer opentracing.Tracer, sender client.Sender) TransportFactory {
	return func(
		_ SendOptions, _ *nodedialer.Dialer, replicas ReplicaSlice, args roachpb.BatchRequest,
	) (Transport, error) {
		// Always send to the first replica.
		args.Replica = replicas[0].ReplicaDescriptor
		return &senderTransport{tracer, sender, args, false}, nil
	}
}

type senderTransport struct {
	tracer opentracing.Tracer
	sender client.Sender
	args   roachpb.BatchRequest

	called bool
}

func (s *senderTransport) IsExhausted() bool {
	return s.called
}

func (s *senderTransport) GetPending() []roachpb.ReplicaDescriptor {
	return []roachpb.ReplicaDescriptor{s.args.Replica}
}

func (s *senderTransport) SendNext(ctx context.Context) (*roachpb.BatchResponse, error) {
	if s.called {
		panic("called an exhausted transport")
	}
	s.called = true
	sp := s.tracer.StartSpan("node")
	defer sp.Finish()
	ctx = opentracing.ContextWithSpan(ctx, sp)
	log.Event(ctx, s.args.String())
	br, pErr := s.sender.Send(ctx, s.args)
	if br == nil {
		br = &roachpb.BatchResponse{}
	}
	if br.Error != nil {
		panic(roachpb.ErrorUnexpectedlySet(s.sender, br))
	}
	br.Error = pErr
	if pErr != nil {
		log.Event(ctx, "error: "+pErr.String())
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

func (s *senderTransport) NextReplica() roachpb.ReplicaDescriptor {
	if s.IsExhausted() {
		return roachpb.ReplicaDescriptor{}
	}
	return s.args.Replica
}

func (s *senderTransport) MoveToFront(replica roachpb.ReplicaDescriptor) {
}

func (s *senderTransport) Close() {
}
