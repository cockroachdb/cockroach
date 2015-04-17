// Copyright 2015 The Cockroach Authors.
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

package kv

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
)

// retryableLocalSender provides a retry option in the event of range
// splits. This sender is used only in unittests. In real-world use,
// the DistSender is responsible for retrying in the event of range
// key mismatches (i.e. splits / merges), but many tests in this
// package do not create nodes and RPC servers necessary to run a
// DistSender and instead rely on local sender only.
type retryableLocalSender struct {
	*LocalSender
	t *testing.T
}

func newRetryableLocalSender(lSender *LocalSender) *retryableLocalSender {
	return &retryableLocalSender{
		LocalSender: lSender,
	}
}

// Send implements the client.Sender interface.
func (rls *retryableLocalSender) Send(call *client.Call) {
	// Instant retry with max two attempts to handle the case of a
	// range split, which is exposed here as a RangeKeyMismatchError.
	// If we fail with two in a row, it's a fatal test error.
	retryOpts := util.RetryOptions{
		Tag:         fmt.Sprintf("routing %s locally", call.Method),
		MaxAttempts: 2,
	}
	err := util.RetryWithBackoff(retryOpts, func() (util.RetryStatus, error) {
		call.Reply.Header().Error = nil
		rls.LocalSender.Send(call)
		// Check for range key mismatch error (this could happen if
		// range was split between lookup and execution). In this case,
		// reset header.Replica and engage retry loop.
		if err := call.Reply.Header().GoError(); err != nil {
			if _, ok := err.(*proto.RangeKeyMismatchError); ok {
				// Clear request replica.
				call.Args.Header().Replica = proto.Replica{}
				return util.RetryContinue, nil
			}
		}
		return util.RetryBreak, nil
	})
	if err != nil {
		log.Fatalf("local sender did not succeed on two attempts: %s", err)
	}
}

// A TestLocalServer encapsulates an in-memory instantiation of a
// cockroach node with a single store using a local sender. Example
// usage of a TestLocalServer follows:
//
//   s := &server.TestLocalServer{}
//   if err := s.Start(); err != nil {
//     t.Fatal(err)
//   }
//   defer s.Stop()
//
// Note that the TestLocalServer is different from server.TestServer
// in that it doesn't use a distributed sender and doesn't start a
// server node. There is no RPC traffic.
type TestLocalServer struct {
	Manual  *hlc.ManualClock
	Clock   *hlc.Clock
	Gossip  *gossip.Gossip
	Eng     engine.Engine
	Store   *storage.Store
	DB      *client.KV
	lSender *retryableLocalSender
	Stopper *util.Stopper
}

// Start starts the TestServer by bootstrapping an in-memory store
// (defaults to maximum of 100M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.Addr after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (tls *TestLocalServer) Start() error {
	tls.Manual = hlc.NewManualClock(0)
	tls.Clock = hlc.NewClock(tls.Manual.UnixNano)
	rpcContext := rpc.NewContext(tls.Clock, rpc.LoadInsecureTLSConfig())
	tls.Gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	tls.Eng = engine.NewInMem(proto.Attributes{}, 50<<20)
	tls.lSender = newRetryableLocalSender(NewLocalSender())
	tls.Stopper = util.NewStopper()
	sender := NewTxnCoordSender(tls.lSender, tls.Clock, false, tls.Stopper)
	tls.DB = client.NewKV(nil, sender)
	tls.DB.User = storage.UserRoot
	transport := multiraft.NewLocalRPCTransport()
	tls.Stopper.AddCloser(transport)
	tls.Store = storage.NewStore(tls.Clock, tls.Eng, tls.DB, tls.Gossip, transport, storage.TestStoreConfig)
	if err := tls.Store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}, tls.Stopper); err != nil {
		return err
	}
	tls.lSender.AddStore(tls.Store)
	if err := tls.Store.BootstrapRange(); err != nil {
		return err
	}
	if err := tls.Store.Start(tls.Stopper); err != nil {
		return err
	}
	return nil
}

// Stop stops the TestServer.
func (tls *TestLocalServer) Stop() {
	tls.Stopper.Stop()
}
