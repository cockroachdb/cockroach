// Copyright 2014 The Cockroach Authors.
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
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"fmt"
	"math/rand"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// orderingPolicy is an enum for ordering strategies when there
// are multiple endpoints available.
type orderingPolicy int

const (
	// orderStable uses endpoints in the order provided.
	orderStable orderingPolicy = iota
	// orderRandom randomly orders available endpoints.
	orderRandom
)

// A SendOptions structure describes the algorithm for sending RPCs to one or
// more replicas, depending on error conditions and how many successful
// responses are required.
type SendOptions struct {
	context.Context // must not be nil
	// Ordering indicates how the available endpoints are ordered when
	// deciding which to send to (if there are more than one).
	Ordering orderingPolicy
	// SendNextTimeout is the duration after which RPCs are sent to
	// other replicas in a set.
	SendNextTimeout time.Duration
	// Timeout is the maximum duration of an RPC before failure.
	// 0 for no timeout.
	Timeout time.Duration

	transportFactory TransportFactory
}

func (so SendOptions) contextWithTimeout() (context.Context, func()) {
	if so.Timeout != 0 {
		return context.WithTimeout(so.Context, so.Timeout)
	}
	return so.Context, func() {}
}

type batchClient struct {
	remoteAddr string
	conn       *grpc.ClientConn
	client     roachpb.InternalClient
	args       roachpb.BatchRequest
}

func shuffleClients(clients []batchClient) {
	for i, n := 0, len(clients); i < n-1; i++ {
		j := rand.Intn(n-i) + i
		clients[i], clients[j] = clients[j], clients[i]
	}
}

// BatchCall contains a response and an RPC error (note that the
// response contains its own roachpb.Error, which is separate from
// BatchCall.Err), and is analogous to the net/rpc.Call struct.
type BatchCall struct {
	Reply *roachpb.BatchResponse
	Err   error
}

// Send sends one or more RPCs to clients specified by the slice of
// replicas. On success, Send returns the first successful reply. Otherwise,
// Send returns an error if and as soon as the number of failed RPCs exceeds
// the available endpoints less the number of required replies.
func send(opts SendOptions, replicas ReplicaSlice,
	args roachpb.BatchRequest, rpcContext *rpc.Context) (*roachpb.BatchResponse, error) {

	if len(replicas) < 1 {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("insufficient replicas (%d) to satisfy send request of %d",
				len(replicas), 1), false)
	}

	done := make(chan BatchCall, len(replicas))

	transportFactory := opts.transportFactory
	if transportFactory == nil {
		transportFactory = grpcTransportFactory
	}
	transport, err := transportFactory(opts, rpcContext, replicas, args)
	if err != nil {
		return nil, err
	}
	defer transport.Close()

	// Send the first request.
	pending := 1
	transport.SendNext(done)

	// Wait for completions. This loop will retry operations that fail
	// with errors that reflect per-replica state and may succeed on
	// other replicas.
	var sendNextTimer util.Timer
	defer sendNextTimer.Stop()
	for {
		sendNextTimer.Reset(opts.SendNextTimeout)
		select {
		case <-sendNextTimer.C:
			sendNextTimer.Read = true
			// On successive RPC timeouts, send to additional replicas if available.
			if !transport.IsExhausted() {
				log.Trace(opts.Context, "timeout, trying next peer")
				pending++
				transport.SendNext(done)
			}

		case call := <-done:
			pending--
			err := call.Err
			if err == nil {
				if log.V(2) {
					log.Infof("RPC reply: %+v", call.Reply)
				} else if log.V(1) && call.Reply.Error != nil {
					log.Infof("application error: %s", call.Reply.Error)
				}

				if !isPerReplicaError(call.Reply.Error) {
					return call.Reply, nil
				}

				// Extract the detail so it can be included in the error
				// message if this is our last replica.
				//
				// TODO(bdarnell): The last error is not necessarily the best
				// one to return; we may want to remember the "best" error
				// we've seen (for example, a NotLeaderError conveys more
				// information than a RangeNotFound).
				err = call.Reply.Error.GoError()
			} else if log.V(1) {
				log.Warningf("RPC error: %s", err)
			}

			// Send to additional replicas if available.
			if !transport.IsExhausted() {
				log.Trace(opts.Context, "error, trying next peer")
				pending++
				transport.SendNext(done)
			}
			if pending == 0 {
				return nil, roachpb.NewSendError(
					fmt.Sprintf("sending to all %d replicas failed; last error: %v",
						len(replicas), err), true)
			}
		}
	}
}

// splitHealthy splits the provided client slice into healthy clients and
// unhealthy clients, based on their connection state. Healthy clients will
// be rearranged first in the slice, and unhealthy clients will be rearranged
// last. Within these two groups, the rearrangement will be stable. The function
// will then return the number of healthy clients.
func splitHealthy(clients []batchClient) (int, error) {
	var nHealthy int
	for i, client := range clients {
		clientState, err := client.conn.State()
		if err != nil {
			// This should not currently happen with the default grpc.Picker.
			return 0, err
		}
		if clientState == grpc.Ready {
			clients[i], clients[nHealthy] = clients[nHealthy], clients[i]
			nHealthy++
		}
	}
	return nHealthy, nil
}

// isPerReplicaError returns true if the given error is likely to be
// unique to the replica that reported it, and retrying on other
// replicas is likely to produce different results.
func isPerReplicaError(pErr *roachpb.Error) bool {
	switch pErr.GetDetail().(type) {
	case *roachpb.RangeNotFoundError:
		return true
	case *roachpb.NodeUnavailableError:
		return true
		// TODO(bdarnell): add NotLeaderError here after refactoring so we
		// can interact with the leader cache from here.
	}
	return false
}
