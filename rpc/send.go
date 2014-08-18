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
// implied.  See the License for the specific language governing
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
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// An Options structure describes the algorithm for sending RPCs to
// one or more replicas, depending on error conditions and how many
// successful responses are required.
type Options struct {
	// N is the number of successful responses required.
	N int
	// SendNextTimeout is the duration after which RPCs are sent to
	// other replicas in a set.
	SendNextTimeout time.Duration
	// Timeout is the maximum duration of an RPC before failure.
	// 0 for no timeout.
	Timeout time.Duration
}

// An rpcError indicates a failure to send the RPC. rpcErrors are
// retryable.
type rpcError struct {
	errMsg string
}

// Error implements the error interface.
func (r rpcError) Error() string { return r.errMsg }

// CanRetry implements the Retryable interface.
func (r rpcError) CanRetry() bool { return true }

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

// Send sends one or more RPCs to clients specified by the keys of
// argsMap (with corresponding values of the map as arguments)
// according to availability and the number of required responses
// specified by opts.N. One or more replies are sent on the replyChan
// channel if successful; otherwise an error is returned on
// failure. Note that on error, some replies may have been sent on the
// channel. Send returns an error if the number of errors exceeds the
// possibility of attaining the required successful responses.
func Send(argsMap map[net.Addr]interface{}, method string, replyChanI interface{}, opts Options, tlsConfig *TLSConfig) error {
	if opts.N < len(argsMap) {
		return SendError{
			errMsg:   fmt.Sprintf("insufficient replicas (%d) to satisfy send request of %d", len(argsMap), opts.N),
			canRetry: false,
		}
	}

	// Build the slice of clients.
	var healthy, unhealthy []*Client
	for addr, args := range argsMap {
		client := NewClient(addr, nil, tlsConfig)
		delete(argsMap, addr)
		argsMap[client.Addr()] = args
		if client.IsHealthy() {
			healthy = append(healthy, client)
		} else {
			unhealthy = append(unhealthy, client)
		}
	}

	// Randomly permute order, but keep known-unhealthy clients
	// separate.
	// TODO(spencer): going to need to also sort by affinity; closest
	// ping time should win. Makes sense to have the rpc client/server
	// heartbeat measure ping times. With a bit of seasoning, each
	// node will be able to order the healthy replicas based on latency.
	var clients []*Client
	for _, idx := range rand.Perm(len(healthy)) {
		clients = append(clients, healthy[idx])
	}
	for _, idx := range rand.Perm(len(unhealthy)) {
		clients = append(clients, unhealthy[idx])
	}

	// Send RPCs to replicas as necessary to achieve opts.N successes.
	helperChan := make(chan interface{}, len(clients))
	N := opts.N
	errors := 0
	retryableErrors := 0
	successes := 0
	index := 0
	for {
		// Start clients up to N.
		for ; index < N; index++ {
			args := argsMap[clients[index].Addr()]
			if args == nil {
				helperChan <- util.Errorf("no arguments in map (len %d) for client %s", len(argsMap), clients[index].Addr())
				continue
			}
			reply := reflect.New(reflect.TypeOf(replyChanI).Elem().Elem()).Interface()
			if log.V(1) {
				log.Infof("%s: sending request to %s: %+v", method, clients[index].Addr(), args)
			}
			go sendOne(clients[index], opts.Timeout, method, args, reply, helperChan)
		}
		// Wait for completions.
		select {
		case r := <-helperChan:
			switch t := r.(type) {
			case error:
				errors++
				if retryErr, ok := t.(util.Retryable); ok && retryErr.CanRetry() {
					retryableErrors++
				}
				if log.V(1) {
					log.Warningf("%s: error reply: %+v", method, t)
				}
				remainingRPCs := len(clients) - errors
				if remainingRPCs < opts.N {
					return SendError{
						errMsg:   fmt.Sprintf("too many errors encountered (%d of %d total): %v", errors, len(clients), t),
						canRetry: retryableErrors+remainingRPCs > len(clients),
					}
				}
				// Send to additional replicas if available.
				if N < len(clients) {
					N++
				}
			default:
				successes++
				if log.V(1) {
					log.Infof("%s: successful reply: %+v", method, t)
				}
				reflect.ValueOf(replyChanI).Send(reflect.ValueOf(t))
				if successes == opts.N {
					return nil
				}
			}
		case <-time.After(opts.SendNextTimeout):
			// On successive RPC timeouts, send to additional replicas if available.
			if N < len(clients) {
				N++
			}
		}
	}

	return nil
}

// sendOne invokes the specified RPC on the supplied client when the
// client is ready. On success, the reply is sent on the channel;
// otherwise an error is sent.
func sendOne(client *Client, timeout time.Duration, method string, args, reply interface{}, c chan interface{}) {
	<-client.Ready
	call := client.Go(method, args, reply, nil)
	select {
	case <-call.Done:
		if call.Error != nil {
			// Handle cases which are retryable.
			switch call.Error {
			case rpc.ErrShutdown: // client connection fails: rpc/client.go
				fallthrough
			case io.ErrUnexpectedEOF: // server connection fails: rpc/client.go
				c <- rpcError{call.Error.Error()}
			default:
				// Otherwise, not retryable; just return error.
				c <- call.Error
			}
		} else {
			c <- reply
		}
	case <-client.Closed:
		c <- rpcError{fmt.Sprintf("rpc to %s failed as client connection was closed", method)}
	case <-time.After(timeout):
		c <- rpcError{fmt.Sprintf("rpc to %s timed out after %s", method, timeout)}
	}
}
