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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// TestClientConnState verifies that the state of a ClientConn changes after a
// heartbeat succeeds or fails.
func TestClientConnState(t *testing.T) {
	defer leaktest.AfterTest(t)()

	stopper := stop.NewStopper()
	defer stopper.Stop()

	// Can't be zero because that'd be an empty offset.
	clock := hlc.NewClock(time.Unix(0, 1).UnixNano)

	serverCtx := newNodeTestContext(clock, stopper)
	s, ln := newTestServer(t, serverCtx, true)
	remoteAddr := ln.Addr().String()

	heartbeat := &ManualHeartbeatService{
		ready:              make(chan struct{}),
		stopper:            stopper,
		clock:              clock,
		remoteClockMonitor: serverCtx.RemoteClocks,
	}
	RegisterHeartbeatServer(s, heartbeat)

	clientCtx := newNodeTestContext(clock, stopper)
	// Make the intervals and timeouts shorter to speed up the tests.
	clientCtx.HeartbeatInterval = 1 * time.Millisecond
	clientCtx.HeartbeatTimeout = 1 * time.Millisecond
	conn, err := clientCtx.GRPCDial(remoteAddr)
	if err != nil {
		t.Fatal(err)
	}

	// This code is inherently racy so when we need to verify heartbeats we want
	// them to always succeed.
	sendHeartbeats := func() func() {
		done := make(chan struct{})
		go func() {
			for {
				select {
				case <-done:
					return
				case heartbeat.ready <- struct{}{}:
				}
			}
		}()
		return func() {
			done <- struct{}{}
		}
	}

	// Should be healthy after the first successful heartbeat.
	stopHeartbeats := sendHeartbeats()
	util.SucceedsSoon(t, func() error {
		if conn.State() != grpc.Ready {
			return errors.Errorf("expected %s to be healthy", remoteAddr)
		}
		return nil
	})
	stopHeartbeats()

	// Should no longer be healthy after heartbeating stops.
	util.SucceedsSoon(t, func() error {
		if conn.State() != grpc.Shutdown {
			return errors.Errorf("expected %s to be unhealthy", remoteAddr)
		}
		return nil
	})

	// Should return to healthy after another successful heartbeat.
	stopHeartbeats = sendHeartbeats()
	util.SucceedsSoon(t, func() error {
		if conn.State() != grpc.Ready {
			return errors.Errorf("expected %s to be healthy", remoteAddr)
		}
		return nil
	})
	stopHeartbeats()
}
