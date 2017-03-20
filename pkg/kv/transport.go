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
//
// Author: Ben Darnell

package kv

import (
	"sort"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/pkg/kv/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/opentracing/opentracing-go"
)

// Allow local calls to be dispatched directly to the local server without
// sending an RPC.
var enableLocalCalls = envutil.EnvOrDefaultBool("COCKROACH_ENABLE_LOCAL_CALLS", true)

// A SendOptions structure describes the algorithm for sending RPCs to one or
// more replicas, depending on error conditions and how many successful
// responses are required.
type SendOptions struct {
	// SendNextTimeout is the duration after which RPCs are sent to
	// other replicas in a set.
	SendNextTimeout time.Duration

	transportFactory TransportFactory

	metrics *DistSenderMetrics
}

type batchClient struct {
	remoteAddr string
	conn       *grpc.ClientConn
	client     roachpb.InternalClient
	args       roachpb.BatchRequest
	healthy    bool
	retried    bool
	pending    bool
}

// BatchCall contains a response and an RPC error (note that the
// response contains its own roachpb.Error, which is separate from
// BatchCall.Err), and is analogous to the net/rpc.Call struct.
type BatchCall struct {
	Reply *roachpb.BatchResponse
	Err   error
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
	SendOptions, *rpc.Context, ReplicaSlice, roachpb.BatchRequest,
) (Transport, error)

// Transport objects can send RPCs to one or more replicas of a range.
// All calls to Transport methods are made from a single thread, so
// Transports are not required to be thread-safe.
type Transport interface {
	// IsExhausted returns true if there are no more replicas to try.
	IsExhausted() bool

	// SendNext sends the rpc (captured at creation time) to the next
	// replica. May panic if the transport is exhausted. Should not
	// block; the transport is responsible for starting other goroutines
	// as needed.
	SendNext(context.Context, chan<- BatchCall)

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
	opts SendOptions, rpcContext *rpc.Context, replicas ReplicaSlice, args roachpb.BatchRequest,
) (Transport, error) {
	clients := make([]batchClient, 0, len(replicas))
	for _, replica := range replicas {
		conn, err := rpcContext.GRPCDial(replica.NodeDesc.Address.String())
		if err != nil {
			return nil, err
		}
		argsCopy := args
		argsCopy.Replica = replica.ReplicaDescriptor
		remoteAddr := replica.NodeDesc.Address.String()
		clients = append(clients, batchClient{
			remoteAddr: remoteAddr,
			conn:       conn,
			client:     roachpb.NewInternalClient(conn),
			args:       argsCopy,
			healthy:    rpcContext.ConnHealth(remoteAddr) == nil,
		})
	}

	// Put known-unhealthy clients last.
	splitHealthy(clients)

	return &grpcTransport{
		opts:           opts,
		rpcContext:     rpcContext,
		orderedClients: clients,
	}, nil
}

type grpcTransport struct {
	opts            SendOptions
	rpcContext      *rpc.Context
	clientIndex     int
	orderedClients  []batchClient
	clientPendingMu syncutil.Mutex // protects access to all batchClient pending flags
}

func (gt *grpcTransport) IsExhausted() bool {
	return gt.clientIndex == len(gt.orderedClients)
}

// SendNext invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
func (gt *grpcTransport) SendNext(ctx context.Context, done chan<- BatchCall) {
	client := gt.orderedClients[gt.clientIndex]
	gt.clientIndex++
	gt.setPending(client.args.Replica, true)

	// Fork the original context as this async send may outlast the
	// caller's context.
	// TODO(andrei): We shouldn't have to fork the ctx here; it's sketchy that
	// these spans can outlast the caller's context. Instead, DistSender should
	// wait on all the RPCs that it sends and it should also have the ability to
	// cancel them when it received the first result.
	ctx, sp := tracing.ForkCtxSpan(ctx, "grpcTransport SendNext")
	go func() {
		gt.opts.metrics.SentCount.Inc(1)
		reply, err := func() (*roachpb.BatchResponse, error) {
			if enableLocalCalls {
				if localServer := gt.rpcContext.GetLocalInternalServerForAddr(client.remoteAddr); localServer != nil {
					// Clone the request. At the time of writing, Replica may mutate it
					// during command execution which can lead to data races.
					//
					// TODO(tamird): we should clone all of client.args.Header, but the
					// assertions in protoutil.Clone fire and there seems to be no
					// reasonable workaround.
					origTxn := client.args.Txn
					if origTxn != nil {
						clonedTxn := origTxn.Clone()
						client.args.Txn = &clonedTxn
					}
					gt.opts.metrics.LocalSentCount.Inc(1)
					log.VEvent(ctx, 2, "sending request to local server")
					return localServer.Batch(ctx, &client.args)
				}
			}

			log.VEventf(ctx, 2, "sending request to %s", client.remoteAddr)
			reply, err := client.client.Batch(ctx, &client.args)
			if reply != nil {
				for i := range reply.Responses {
					if err := reply.Responses[i].GetInner().Verify(client.args.Requests[i].GetInner()); err != nil {
						log.Error(ctx, err)
					}
				}
			}
			return reply, err
		}()
		gt.setPending(client.args.Replica, false)
		tracing.FinishSpan(sp)
		done <- BatchCall{Reply: reply, Err: err}
	}()
}

func (gt *grpcTransport) MoveToFront(replica roachpb.ReplicaDescriptor) {
	gt.clientPendingMu.Lock()
	defer gt.clientPendingMu.Unlock()
	for i := range gt.orderedClients {
		if gt.orderedClients[i].args.Replica == replica {
			// If a call to this replica is active or retried, don't move it.
			if gt.orderedClients[i].pending || gt.orderedClients[i].retried {
				return
			}
			// If we've already processed the replica, decrement the current
			// index before we swap.
			if i < gt.clientIndex {
				gt.orderedClients[i].retried = true
				gt.clientIndex--
			}
			// Swap the client representing this replica to the front.
			gt.orderedClients[i], gt.orderedClients[gt.clientIndex] =
				gt.orderedClients[gt.clientIndex], gt.orderedClients[i]
			return
		}
	}
}

func (*grpcTransport) Close() {
	// TODO(bdarnell): Save the cancel functions of all pending RPCs and
	// call them here. (it's fine to ignore them for now since they'll
	// time out anyway)
}

// NB: this method's callers may have a reference to the client they wish to
// mutate, but the clients reside in a slice which is shuffled via
// MoveToFront, making it unsafe to mutate the client through a reference to
// the slice.
func (gt *grpcTransport) setPending(replica roachpb.ReplicaDescriptor, pending bool) {
	gt.clientPendingMu.Lock()
	defer gt.clientPendingMu.Unlock()
	for i := range gt.orderedClients {
		if gt.orderedClients[i].args.Replica == replica {
			gt.orderedClients[i].pending = pending
			return
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
		_ SendOptions, _ *rpc.Context, _ ReplicaSlice, args roachpb.BatchRequest,
	) (Transport, error) {
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

func (s *senderTransport) SendNext(ctx context.Context, done chan<- BatchCall) {
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
	done <- BatchCall{Reply: br}
}

func (s *senderTransport) MoveToFront(replica roachpb.ReplicaDescriptor) {
}

func (s *senderTransport) Close() {
}
