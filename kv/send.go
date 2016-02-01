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
	"io"
	"math/rand"
	"net"
	netrpc "net/rpc"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/tracer"
	"github.com/gogo/protobuf/proto"
	"github.com/opentracing/opentracing-go"
)

// TODO(pmattis): We might also want to de-generalize this code so that it
// knows about the types of the request and response protos
// (Batch{Request,Response}). This would simplify the interface between
// DistSender.send() and send() (perhaps no need for getArgs/getReply).

// orderingPolicy is an enum for ordering strategies when there
// are multiple endpoints available.
type orderingPolicy int

const (
	// orderStable uses endpoints in the order provided.
	orderStable = iota
	// orderRandom randomly orders available endpoints.
	orderRandom
)

// A SendOptions structure describes the algorithm for sending RPCs to one or
// more replicas, depending on error conditions and how many successful
// responses are required.
type SendOptions struct {
	// Ordering indicates how the available endpoints are ordered when
	// deciding which to send to (if there are more than one).
	Ordering orderingPolicy
	// SendNextTimeout is the duration after which RPCs are sent to
	// other replicas in a set.
	SendNextTimeout time.Duration
	// Timeout is the maximum duration of an RPC before failure.
	// 0 for no timeout.
	Timeout time.Duration
	// If not nil, information about the request is added to this trace.
	Trace opentracing.Span
}

// An rpcError indicates a failure to send the RPC. rpcErrors are
// retryable.
type rpcError struct {
	error
}

func newRPCError(err error) rpcError {
	return rpcError{err}
}

// CanRetry implements the Retryable interface.
// TODO(tschottdorf): the way this is used by rpc/send suggests that it
// may be better if these weren't retriable - they are returned when the
// connection fails, i.e. for example when a node is down or the network
// fails. Retrying on such errors keeps the caller waiting for a long time
// and without a positive outlook.
func (r rpcError) CanRetry() bool { return true }

// Send sends one or more method RPCs to clients specified by the slice of
// endpoint addrs. Arguments for methods are obtained using the supplied
// getArgs function. Reply structs are obtained through the getReply()
// function. On success, Send returns the first successful reply. Otherwise,
// Send returns an error if and as soon as the number of failed RPCs exceeds
// the available endpoints less the number of required replies.
func send(opts SendOptions, method string, addrs []net.Addr, getArgs func(addr net.Addr) proto.Message,
	getReply func() proto.Message, context *rpc.Context) (proto.Message, error) {
	trace := opts.Trace // not thread safe!
	if trace == nil {
		trace = tracer.NilTrace()
	}

	if len(addrs) < 1 {
		return nil, roachpb.NewSendError(
			fmt.Sprintf("insufficient replicas (%d) to satisfy send request of %d",
				len(addrs), 1), false)
	}

	done := make(chan *netrpc.Call, len(addrs))

	var clients []*rpc.Client
	for _, addr := range addrs {
		clients = append(clients, rpc.NewClient(addr, context))
	}

	var orderedClients []*rpc.Client
	switch opts.Ordering {
	case orderStable:
		orderedClients = clients
	case orderRandom:
		// Randomly permute order, but keep known-unhealthy clients last.
		var healthy, unhealthy []*rpc.Client
		for _, client := range clients {
			select {
			case <-client.Healthy():
				healthy = append(healthy, client)
			default:
				unhealthy = append(unhealthy, client)
			}
		}
		for _, idx := range rand.Perm(len(healthy)) {
			orderedClients = append(orderedClients, healthy[idx])
		}
		for _, idx := range rand.Perm(len(unhealthy)) {
			orderedClients = append(orderedClients, unhealthy[idx])
		}
	}
	// TODO(spencer): going to need to also sort by affinity; closest
	// ping time should win. Makes sense to have the rpc client/server
	// heartbeat measure ping times. With a bit of seasoning, each
	// node will be able to order the healthy replicas based on latency.

	// Send the first request.
	sendOneFn(orderedClients[0], opts.Timeout, method, getArgs, getReply, context, trace, done)
	orderedClients = orderedClients[1:]

	var errors, retryableErrors int

	// Wait for completions.
	for {
		select {
		case call := <-done:
			if call.Error == nil {
				// Verify response data integrity if this is a proto response.
				if req, reqOk := call.Args.(roachpb.Request); reqOk {
					if resp, respOk := call.Reply.(roachpb.Response); respOk {
						if err := resp.Verify(req); err != nil {
							call.Error = err
						}
					} else {
						call.Error = util.Errorf("response to proto request must be a proto")
					}
				}
			}
			err := call.Error
			if err == nil {
				if log.V(2) {
					log.Infof("%s: successful reply: %+v", method, call.Reply)
				}

				return call.Reply.(proto.Message), nil
			}

			// Error handling.
			if log.V(1) {
				log.Warningf("%s: error reply: %s", method, err)
			}

			errors++

			// Since we have a reconnecting client here, disconnect errors are retryable.
			disconnected := err == netrpc.ErrShutdown || err == io.ErrUnexpectedEOF
			if retryErr, ok := err.(retry.Retryable); disconnected || (ok && retryErr.CanRetry()) {
				retryableErrors++
			}

			if remainingNonErrorRPCs := len(addrs) - errors; remainingNonErrorRPCs < 1 {
				return nil, roachpb.NewSendError(
					fmt.Sprintf("too many errors encountered (%d of %d total): %v",
						errors, len(clients), err), remainingNonErrorRPCs+retryableErrors >= 1)
			}
			// Send to additional replicas if available.
			if len(orderedClients) > 0 {
				trace.LogEvent("error, trying next peer")
				sendOneFn(orderedClients[0], opts.Timeout, method, getArgs, getReply, context, trace, done)
				orderedClients = orderedClients[1:]
			}

		case <-time.After(opts.SendNextTimeout):
			// On successive RPC timeouts, send to additional replicas if available.
			if len(orderedClients) > 0 {
				trace.LogEvent("timeout, trying next peer")
				sendOneFn(orderedClients[0], opts.Timeout, method, getArgs, getReply, context, trace, done)
				orderedClients = orderedClients[1:]
			}
		}
	}
}

// Allow local calls to be dispatched directly to the local server without
// sending an RPC.
//
// TODO(pmattis): We should either always enable local calls or remove this
// support. Revisit once the current performance work has completed.
var enableLocalCalls = os.Getenv("ENABLE_LOCAL_CALLS") == "1"

// sendOneFn is overwritten in tests to mock sendOne.
var sendOneFn = sendOne

// sendOne invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
//
// Do not call directly, but instead use sendOneFn. Tests mock out this method
// via sendOneFn in order to test various error cases.
func sendOne(client *rpc.Client, timeout time.Duration, method string,
	getArgs func(addr net.Addr) proto.Message, getReply func() proto.Message,
	context *rpc.Context, trace opentracing.Span, done chan *netrpc.Call) {

	addr := client.RemoteAddr()
	args := getArgs(addr)
	if args == nil {
		done <- &netrpc.Call{Error: newRPCError(
			util.Errorf("nil arguments returned for client %s", addr))}
		return
	}

	if log.V(2) {
		log.Infof("%s: sending request to %s: %+v", method, addr, args)
	}
	trace.LogEvent(fmt.Sprintf("sending to %s", addr))

	if enableLocalCalls && context.LocalServer != nil && addr.String() == context.LocalAddr {
		if context.LocalServer.LocalCall(method, args, done) {
			return
		}
	}

	reply := getReply()

	// Don't bother firing off a goroutine in the common case where a client
	// is already healthy.
	select {
	case <-client.Healthy():
		client.Go(method, args, reply, done)
		return
	default:
	}

	go func() {
		var timeoutChan <-chan time.Time
		if timeout != 0 {
			timeoutChan = time.After(timeout)
		}
		select {
		case <-client.Healthy():
			client.Go(method, args, reply, done)
		case <-client.Closed:
			done <- &netrpc.Call{Error: newRPCError(
				util.Errorf("rpc to %s failed as client connection was closed", method))}
		case <-timeoutChan:
			done <- &netrpc.Call{Error: newRPCError(
				util.Errorf("rpc to %s: client not ready after %s", method, timeout))}
		}
	}()
}
