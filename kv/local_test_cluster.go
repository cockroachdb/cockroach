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
func (rls *retryableLocalSender) Send(call client.Call) {
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

// A LocalTestCluster encapsulates an in-memory instantiation of a
// cockroach node with a single store using a local sender. Example
// usage of a LocalTestCluster follows:
//
//   s := &server.LocalTestCluster{}
//   if err := s.Start(); err != nil {
//     t.Fatal(err)
//   }
//   defer s.Stop()
//
// Note that the LocalTestCluster is different from server.TestCluster
// in that it doesn't use a distributed sender and doesn't start a
// server node. There is no RPC traffic.
type LocalTestCluster struct {
	Manual  *hlc.ManualClock
	Clock   *hlc.Clock
	Gossip  *gossip.Gossip
	Eng     engine.Engine
	Store   *storage.Store
	KV      *client.KV
	lSender *retryableLocalSender
	Stopper *util.Stopper
}

// Start starts the test cluster by bootstrapping an in-memory store
// (defaults to maximum of 50M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.Addr after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ltc *LocalTestCluster) Start() error {
	ltc.Manual = hlc.NewManualClock(0)
	ltc.Clock = hlc.NewClock(ltc.Manual.UnixNano)
	ltc.Stopper = util.NewStopper()
	rpcContext := rpc.NewContext(ltc.Clock, rpc.LoadInsecureTLSConfig(), ltc.Stopper)
	ltc.Gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	ltc.Eng = engine.NewInMem(proto.Attributes{}, 50<<20)
	ltc.lSender = newRetryableLocalSender(NewLocalSender())
	sender := NewTxnCoordSender(ltc.lSender, ltc.Clock, false, ltc.Stopper)
	ltc.KV = client.NewKV(nil, sender)
	ltc.KV.User = storage.UserRoot
	transport := multiraft.NewLocalRPCTransport()
	ltc.Stopper.AddCloser(transport)
	ltc.Store = storage.NewStore(ltc.Clock, ltc.Eng, ltc.KV, ltc.Gossip, transport, storage.TestStoreConfig)
	if err := ltc.Store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}, ltc.Stopper); err != nil {
		return err
	}
	ltc.lSender.AddStore(ltc.Store)
	if err := ltc.Store.BootstrapRange(); err != nil {
		return err
	}
	if err := ltc.Store.Start(ltc.Stopper); err != nil {
		return err
	}
	rng, err := ltc.Store.GetRange(1)
	if err != nil {
		return err
	}
	// Without this, we'll very sporadically have test failures here since
	// Raft commands are retried, bypassing the response cache.
	// TODO(tschottdorf): remove the trigger when we've fixed the above.
	rng.WaitForElection()
	return nil
}

// Stop stops the cluster.
func (ltc *LocalTestCluster) Stop() {
	ltc.Stopper.Stop()
}
