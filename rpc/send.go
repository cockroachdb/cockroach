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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package rpc

import (
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/rpc"
	"time"

	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
	"github.com/cockroachdb/cockroach/util/tracer"

	gogoproto "github.com/gogo/protobuf/proto"
)

// OrderingPolicy is an enum for ordering strategies when there
// are multiple endpoints available.
type OrderingPolicy int

const (
	// OrderStable uses endpoints in the order provided.
	OrderStable = iota
	// OrderRandom randomly orders available endpoints.
	OrderRandom
)

// An Options structure describes the algorithm for sending RPCs to
// one or more replicas, depending on error conditions and how many
// successful responses are required.
type Options struct {
	// N is the number of successful responses required.
	N int
	// Ordering indicates how the available endpoints are ordered when
	// deciding which to send to (if there are more than one).
	Ordering OrderingPolicy
	// SendNextTimeout is the duration after which RPCs are sent to
	// other replicas in a set.
	SendNextTimeout time.Duration
	// Timeout is the maximum duration of an RPC before failure.
	// 0 for no timeout.
	Timeout time.Duration
	// If not nil, information about the request is added to this trace.
	Trace *tracer.Trace
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

// sendOneFn is overwritten in tests to mock sendOne.
var sendOneFn = sendOne

// A SendError indicates that too many RPCs to the replica
// set failed to achieve requested number of successful responses.
// canRetry is set depending on the types of errors encountered.
type SendError struct {
	errMsg   string
	canRetry bool
}

// Error implements the error interface.
func (s SendError) Error() string {
	return "failed to send RPC: " + s.errMsg
}

// CanRetry implements the Retryable interface.
func (s SendError) CanRetry() bool { return s.canRetry }

// Send sends one or more method RPCs to clients specified by the
// slice of endpoint addrs. Arguments for methods are obtained using
// the supplied getArgs function. The number of required replies is
// given by opts.N. Reply structs are obtained through the getReply()
// function. On success, Send returns a slice of replies of length
// opts.N. Otherwise, Send returns an error if and as soon as the
// number of failed RPCs exceeds the available endpoints less the
// number of required replies.
func Send(opts Options, method string, addrs []net.Addr, getArgs func(addr net.Addr) gogoproto.Message,
	getReply func() gogoproto.Message, context *Context) ([]gogoproto.Message, error) {
	trace := opts.Trace // not thread safe!

	if opts.N <= 0 {
		return nil, SendError{
			errMsg:   fmt.Sprintf("opts.N must be positive: %d", opts.N),
			canRetry: false,
		}
	}

	if len(addrs) < opts.N {
		return nil, SendError{
			errMsg:   fmt.Sprintf("insufficient replicas (%d) to satisfy send request of %d", len(addrs), opts.N),
			canRetry: false,
		}
	}

	done := make(chan *rpc.Call, len(addrs))

	var clients []*Client
	for _, addr := range addrs {
		clients = append(clients, NewClient(addr, context))
	}

	var orderedClients []*Client
	switch opts.Ordering {
	case OrderStable:
		orderedClients = clients
	case OrderRandom:
		// Randomly permute order, but keep known-unhealthy clients last.
		var healthy, unhealthy []*Client
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

	sendFn := func(client *Client) {
		addr := client.RemoteAddr()

		if args := getArgs(addr); args != nil {
			if log.V(2) {
				log.Infof("%s: sending request to %s: %+v", method, addr, args)
			}
			trace.Event(fmt.Sprintf("sending to %s", addr))
			go sendOneFn(client, opts.Timeout, method, args, getReply(), done)
		} else {
			done <- &rpc.Call{Error: newRPCError(util.Errorf("nil arguments returned for client %s", addr))}
		}
	}

	// Start clients up to opts.N.
	head, tail := orderedClients[:opts.N], orderedClients[opts.N:]
	for _, client := range head {
		sendFn(client)
	}

	var replies []gogoproto.Message
	var errors, retryableErrors int

	// Wait for completions.
	for len(replies) < opts.N {
		select {
		case call := <-done:
			if call.Error == nil {
				// Verify response data integrity if this is a proto response.
				if req, reqOk := call.Args.(proto.Request); reqOk {
					if resp, respOk := call.Reply.(proto.Response); respOk {
						if err := resp.Verify(req); err != nil {
							call.Error = err
						}
					} else {
						call.Error = util.Error("response to proto request must be a proto")
					}
				}
			}
			err := call.Error
			if err == nil {
				if log.V(2) {
					log.Infof("%s: successful reply: %+v", method, call.Reply)
				}

				replies = append(replies, call.Reply.(gogoproto.Message))
				break // end the select
			}

			// Error handling.
			if log.V(1) {
				log.Warningf("%s: error reply: %s", method, err)
			}

			errors++

			// Since we have a reconnecting client here, disconnect errors are retryable.
			disconnected := err == rpc.ErrShutdown || err == io.ErrUnexpectedEOF
			if retryErr, ok := err.(retry.Retryable); disconnected || (ok && retryErr.CanRetry()) {
				retryableErrors++
			}

			if remainingNonErrorRPCs := len(addrs) - errors; remainingNonErrorRPCs < opts.N {
				return nil, SendError{
					errMsg:   fmt.Sprintf("too many errors encountered (%d of %d total): %v", errors, len(clients), err),
					canRetry: remainingNonErrorRPCs+retryableErrors >= opts.N,
				}
			}
			// Send to additional replicas if available.
			if len(tail) > 0 {
				trace.Event("error, trying next peer")
				sendFn(tail[0])
				tail = tail[1:]
			}

		case <-time.After(opts.SendNextTimeout):
			// On successive RPC timeouts, send to additional replicas if available.
			if len(tail) > 0 {
				trace.Event("timeout, trying next peer")
				sendFn(tail[0])
				tail = tail[1:]
			}
		}
	}
	return replies, nil
}

// sendOne invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
func sendOne(client *Client, timeout time.Duration, method string, args, reply gogoproto.Message, done chan *rpc.Call) {
	var timeoutChan <-chan time.Time
	if timeout != 0 {
		timeoutChan = time.After(timeout)
	}
	select {
	case <-client.Healthy():
		client.Go(method, args, reply, done)
	case <-client.Closed:
		done <- &rpc.Call{Error: newRPCError(util.Errorf("rpc to %s failed as client connection was closed", method))}
	case <-timeoutChan:
		done <- &rpc.Call{Error: newRPCError(util.Errorf("rpc to %s: client not ready after %s", method, timeout))}
	}
}
