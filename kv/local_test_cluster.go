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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/retry"
	gogoproto "github.com/gogo/protobuf/proto"
)

// retryableLocalSender provides a retry option in the event of range
// splits. This sender is used only in unittests. In real-world use,
// the DistSender is responsible for retrying in the event of range
// key mismatches (i.e. splits / merges), but many tests in this
// package do not create nodes and RPC servers necessary to run a
// DistSender and instead rely on local sender only.
type retryableLocalSender struct {
	*LocalSender
}

func newRetryableLocalSender(lSender *LocalSender) *retryableLocalSender {
	return &retryableLocalSender{
		LocalSender: lSender,
	}
}

// Send implements the client.Sender interface.
func (rls *retryableLocalSender) Send(_ context.Context, call proto.Call) {
	// Instant retry to handle the case of a range split, which is
	// exposed here as a RangeKeyMismatchError.
	retryOpts := retry.Options{
		Tag: fmt.Sprintf("routing %s locally", call.Method()),
	}
	// In local tests, the RPCs are not actually sent over the wire. We
	// need to clone the Txn in order to avoid unexpected sharing
	// between TxnCoordSender and client.Txn.
	if header := call.Args.Header(); header.Txn != nil {
		header.Txn = gogoproto.Clone(header.Txn).(*proto.Transaction)
	}
	err := retry.WithBackoff(retryOpts, func() (retry.Status, error) {
		call.Reply.Header().Error = nil
		rls.LocalSender.Send(context.TODO(), call)
		// Check for range key mismatch error (this could happen if
		// range was split between lookup and execution). In this case,
		// reset header.Replica and engage retry loop.
		if err := call.Reply.Header().GoError(); err != nil {
			if _, ok := err.(*proto.RangeKeyMismatchError); ok {
				// Clear request replica.
				call.Args.Header().Replica = proto.Replica{}
				return retry.Continue, err
			}
		}
		return retry.Break, nil
	})
	if err != nil {
		panic(fmt.Sprintf("local sender did not succeed: %s", err))
	}
}

// A LocalTestCluster encapsulates an in-memory instantiation of a
// cockroach node with a single store using a local sender. Example
// usage of a LocalTestCluster follows:
//
//   s := &server.LocalTestCluster{}
//   s.Start(t)
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
	DB      *client.DB
	lSender *retryableLocalSender
	Sender  *TxnCoordSender
	Stopper *util.Stopper
}

// Start starts the test cluster by bootstrapping an in-memory store
// (defaults to maximum of 50M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.Addr after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ltc *LocalTestCluster) Start(t util.Tester) {
	ltc.Manual = hlc.NewManualClock(0)
	ltc.Clock = hlc.NewClock(ltc.Manual.UnixNano)
	ltc.Stopper = util.NewStopper()
	rpcContext := rpc.NewContext(testutils.NewTestBaseContext(), ltc.Clock, ltc.Stopper)
	ltc.Gossip = gossip.New(rpcContext, gossip.TestInterval, gossip.TestBootstrap)
	ltc.Eng = engine.NewInMem(proto.Attributes{}, 50<<20)
	ltc.lSender = newRetryableLocalSender(NewLocalSender())
	ltc.Sender = NewTxnCoordSender(ltc.lSender, ltc.Clock, false, ltc.Stopper)
	var err error
	if ltc.DB, err = client.Open("//root@", client.SenderOpt(ltc.Sender)); err != nil {
		t.Fatal(err)
	}
	transport := multiraft.NewLocalRPCTransport()
	ltc.Stopper.AddCloser(transport)
	ctx := storage.TestStoreContext
	ctx.Clock = ltc.Clock
	ctx.DB = ltc.DB
	ctx.Gossip = ltc.Gossip
	ctx.Transport = transport
	ltc.Store = storage.NewStore(ctx, ltc.Eng, &proto.NodeDescriptor{NodeID: 1})
	if err := ltc.Store.Bootstrap(proto.StoreIdent{NodeID: 1, StoreID: 1}, ltc.Stopper); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	ltc.lSender.AddStore(ltc.Store)
	if err := ltc.Store.BootstrapRange(); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	if err := ltc.Store.Start(ltc.Stopper); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
}

// Stop stops the cluster.
func (ltc *LocalTestCluster) Stop() {
	ltc.Stopper.Stop()
}
