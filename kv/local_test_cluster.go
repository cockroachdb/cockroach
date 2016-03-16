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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"time"

	opentracing "github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/gossip"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/rpc"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/storage/engine"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/hlc"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/metric"
	"github.com/cockroachdb/cockroach/util/stop"
	"github.com/cockroachdb/cockroach/util/tracing"
)

// A LocalTestCluster encapsulates an in-memory instantiation of a
// cockroach node with a single store using a local sender. Example
// usage of a LocalTestCluster follows:
//
//   s := &server.LocalTestCluster{}
//   s.Start(t)
//   defer s.Stop()
//
// Note that the LocalTestCluster is different from server.TestCluster
// in that although it uses a distributed sender, there is no RPC traffic.
type LocalTestCluster struct {
	Manual     *hlc.ManualClock
	Clock      *hlc.Clock
	Gossip     *gossip.Gossip
	Eng        engine.Engine
	Store      *storage.Store
	DB         *client.DB
	stores     *storage.Stores
	Sender     *TxnCoordSender
	distSender *DistSender
	Stopper    *stop.Stopper
	Latency    time.Duration // sleep for each RPC sent
	tester     util.Tester
}

// Start starts the test cluster by bootstrapping an in-memory store
// (defaults to maximum of 50M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.Addr after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ltc *LocalTestCluster) Start(t util.Tester) {
	nodeID := roachpb.NodeID(1)
	nodeDesc := &roachpb.NodeDescriptor{NodeID: nodeID}
	ltc.tester = t
	ltc.Manual = hlc.NewManualClock(0)
	ltc.Clock = hlc.NewClock(ltc.Manual.UnixNano)
	ltc.Stopper = stop.NewStopper()
	rpcContext := rpc.NewContext(testutils.NewNodeTestBaseContext(), ltc.Clock, ltc.Stopper)
	ltc.Gossip = gossip.New(rpcContext, nil, ltc.Stopper)
	ltc.Eng = engine.NewInMem(roachpb.Attributes{}, 50<<20, ltc.Stopper)

	ltc.stores = storage.NewStores(ltc.Clock)
	tracer := tracing.NewTracer()
	var rpcSend rpcSendFn = func(_ SendOptions, _ ReplicaSlice,
		args roachpb.BatchRequest, _ *rpc.Context) (*roachpb.BatchResponse, error) {
		if ltc.Latency > 0 {
			time.Sleep(ltc.Latency)
		}
		sp := tracer.StartSpan("node")
		defer sp.Finish()
		ctx := opentracing.ContextWithSpan(context.Background(), sp)
		log.Trace(ctx, args.String())
		br, pErr := ltc.stores.Send(ctx, args)
		if br == nil {
			br = &roachpb.BatchResponse{}
		}
		if br.Error != nil {
			panic(roachpb.ErrorUnexpectedlySet(ltc.stores, br))
		}
		br.Error = pErr
		if pErr != nil {
			log.Trace(ctx, "error: "+pErr.String())
		}
		return br, nil
	}
	retryOpts := GetDefaultDistSenderRetryOptions()
	retryOpts.Closer = ltc.Stopper.ShouldDrain()
	ltc.distSender = NewDistSender(&DistSenderContext{
		Clock: ltc.Clock,
		RangeDescriptorCacheSize: defaultRangeDescriptorCacheSize,
		RangeLookupMaxRanges:     defaultRangeLookupMaxRanges,
		LeaderCacheSize:          defaultLeaderCacheSize,
		RPCRetryOptions:          &retryOpts,
		nodeDescriptor:           nodeDesc,
		RPCSend:                  rpcSend,    // defined above
		RangeDescriptorDB:        ltc.stores, // for descriptor lookup
	}, ltc.Gossip)

	ltc.Sender = NewTxnCoordSender(ltc.distSender, ltc.Clock, false /* !linearizable */, tracer,
		ltc.Stopper, NewTxnMetrics(metric.NewRegistry()))
	ltc.DB = client.NewDB(ltc.Sender)
	transport := storage.NewDummyRaftTransport()
	ctx := storage.TestStoreContext()
	ctx.Clock = ltc.Clock
	ctx.DB = ltc.DB
	ctx.Gossip = ltc.Gossip
	ctx.Transport = transport
	ctx.Tracer = tracer
	ltc.Store = storage.NewStore(ctx, ltc.Eng, nodeDesc)
	if err := ltc.Store.Bootstrap(roachpb.StoreIdent{NodeID: nodeID, StoreID: 1}, ltc.Stopper); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	ltc.stores.AddStore(ltc.Store)
	if err := ltc.Store.BootstrapRange(nil); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	if err := ltc.Store.Start(ltc.Stopper); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	ltc.Gossip.SetNodeID(nodeDesc.NodeID)
	if err := ltc.Gossip.SetNodeDescriptor(nodeDesc); err != nil {
		t.Fatalf("unable to set node descriptor: %s", err)
	}
}

// Stop stops the cluster.
func (ltc *LocalTestCluster) Stop() {
	// If the test has failed, we don't attempt to clean up: This often hangs,
	// and leaktest will disable itself for the remaining tests so that no
	// unrelated errors occur from a dirty shutdown.
	if ltc.tester.Failed() {
		return
	}
	if r := recover(); r != nil {
		panic(r)
	}
	ltc.Stopper.Stop()
}
