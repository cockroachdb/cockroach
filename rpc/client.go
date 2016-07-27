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
// permissions and limitations under the License.
//
// Author: Tristan Rice (rice@fn.lc)

package rpc

import (
	"github.com/cockroachdb/cockroach/roachpb"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

// ClientConn is a wrapper around grpc.ClientConn with additional state
// commands.
type ClientConn struct {
	*grpc.ClientConn

	ctx       *Context
	state     grpc.ConnectivityState
	stateChan chan grpc.ConnectivityState
}

// State returns whether the most recent heartbeat succeeded or not. This should
// not be used as a definite status of a nodes health and just used to
// prioritize healthy nodes over unhealthy ones.
func (cc *ClientConn) State() grpc.ConnectivityState {
	return cc.state
}

// setHealthy sets the health status of the connection.
func (cc *ClientConn) setHealthy(healthy bool) {
	if healthy {
		cc.state = grpc.Ready
	} else {
		cc.state = grpc.Shutdown
	}

	// Notify all waiting listeners to the health channel.
	for {
		select {
		case cc.stateChan <- cc.state:
		default:
			return
		}
	}
}

// StateChange returns a channel that will receive messages when the connection
// state changes.
func (cc *ClientConn) StateChange() <-chan grpc.ConnectivityState {
	return cc.stateChan
}

// WaitForStateChange blocks until the state changes to something other than the
// sourceState. It returns the new state or error.
func (cc *ClientConn) WaitForStateChange(ctx context.Context, sourceState grpc.ConnectivityState) (grpc.ConnectivityState, error) {
	for {
		select {
		case <-ctx.Done():
			return sourceState, ctx.Err()
		case <-cc.ctx.Stopper.ShouldStop():
			return sourceState, &roachpb.NodeUnavailableError{}
		case state := <-cc.stateChan:
			if state != sourceState {
				return state, nil
			}
		}
	}
}
