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

package localtestcluster

import (
	"testing"
	"time"

	"github.com/opentracing/opentracing-go"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// A LocalTestCluster encapsulates an in-memory instantiation of a
// cockroach node with a single store using a local sender. Example
// usage of a LocalTestCluster follows:
//
//   s := &LocalTestCluster{}
//   s.Start(t, testutils.NewNodeTestBaseContext(),
//           kv.InitSenderForLocalTestCluster)
//   defer s.Stop()
//
// Note that the LocalTestCluster is different from server.TestCluster
// in that although it uses a distributed sender, there is no RPC traffic.
type LocalTestCluster struct {
	Manual                   *hlc.ManualClock
	Clock                    *hlc.Clock
	Gossip                   *gossip.Gossip
	Eng                      engine.Engine
	Store                    *storage.Store
	StoreTestingKnobs        *storage.StoreTestingKnobs
	DBContext                *client.DBContext
	DB                       *client.DB
	RangeRetryOptions        *retry.Options
	Stores                   *storage.Stores
	Sender                   client.Sender
	Stopper                  *stop.Stopper
	Latency                  time.Duration // sleep for each RPC sent
	tester                   testing.TB
	DontRetryPushTxnFailures bool
}

// InitSenderFn is a callback used to initiate the txn coordinator (we don't
// do it directly from this package to avoid a dependency on kv).
type InitSenderFn func(
	nodeDesc *roachpb.NodeDescriptor,
	tracer opentracing.Tracer,
	clock *hlc.Clock,
	latency time.Duration,
	stores client.Sender,
	stopper *stop.Stopper,
	gossip *gossip.Gossip,
) client.Sender

// Start starts the test cluster by bootstrapping an in-memory store
// (defaults to maximum of 50M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.Addr after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ltc *LocalTestCluster) Start(t testing.TB, baseCtx *base.Config, initSender InitSenderFn) {
	ambient := log.AmbientContext{Tracer: tracing.NewTracer()}
	nc := &base.NodeIDContainer{}
	ambient.AddLogTag("n", nc)

	nodeID := roachpb.NodeID(1)
	nodeDesc := &roachpb.NodeDescriptor{NodeID: nodeID}

	ltc.tester = t
	ltc.Manual = hlc.NewManualClock(123)
	ltc.Clock = hlc.NewClock(ltc.Manual.UnixNano, 50*time.Millisecond)
	ltc.Stopper = stop.NewStopper()
	rpcContext := rpc.NewContext(ambient, baseCtx, ltc.Clock, ltc.Stopper)
	server := rpc.NewServer(rpcContext) // never started
	ltc.Gossip = gossip.New(ambient, nc, rpcContext, server, ltc.Stopper, metric.NewRegistry())
	ltc.Eng = engine.NewInMem(roachpb.Attributes{}, 50<<20)
	ltc.Stopper.AddCloser(ltc.Eng)

	ltc.Stores = storage.NewStores(ambient, ltc.Clock)

	ltc.Sender = initSender(nodeDesc, ambient.Tracer, ltc.Clock, ltc.Latency, ltc.Stores, ltc.Stopper,
		ltc.Gossip)
	if ltc.DBContext == nil {
		dbCtx := client.DefaultDBContext()
		ltc.DBContext = &dbCtx
	}
	ltc.DB = client.NewDBWithContext(ltc.Sender, ltc.Clock, *ltc.DBContext)
	transport := storage.NewDummyRaftTransport()
	cfg := storage.TestStoreConfig(ltc.Clock)
	// By default, disable the replica scanner and split queue, which
	// confuse tests using LocalTestCluster.
	if ltc.StoreTestingKnobs == nil {
		cfg.TestingKnobs.DisableScanner = true
		cfg.TestingKnobs.DisableSplitQueue = true
	} else {
		cfg.TestingKnobs = *ltc.StoreTestingKnobs
	}
	if ltc.RangeRetryOptions != nil {
		cfg.RangeRetryOptions = *ltc.RangeRetryOptions
	}
	cfg.DontRetryPushTxnFailures = ltc.DontRetryPushTxnFailures
	cfg.AmbientCtx = ambient
	cfg.DB = ltc.DB
	cfg.Gossip = ltc.Gossip
	active, renewal := storage.NodeLivenessDurations(
		storage.RaftElectionTimeout(cfg.RaftTickInterval, cfg.RaftElectionTimeoutTicks))
	cfg.NodeLiveness = storage.NewNodeLiveness(
		cfg.AmbientCtx,
		cfg.Clock,
		cfg.DB,
		cfg.Gossip,
		active,
		renewal,
	)
	cfg.StorePool = storage.NewStorePool(
		cfg.AmbientCtx,
		cfg.Gossip,
		cfg.Clock,
		storage.MakeStorePoolNodeLivenessFunc(cfg.NodeLiveness),
		settings.TestingDuration(storage.TestTimeUntilStoreDead),
		/* deterministic */ false,
	)
	cfg.Transport = transport
	cfg.MetricsSampleInterval = metric.TestSampleInterval
	cfg.HistogramWindowInterval = metric.TestSampleInterval
	ltc.Store = storage.NewStore(cfg, ltc.Eng, nodeDesc)
	if err := ltc.Store.Bootstrap(roachpb.StoreIdent{NodeID: nodeID, StoreID: 1}); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	ltc.Stores.AddStore(ltc.Store)
	if err := ltc.Store.BootstrapRange(nil); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	if err := ltc.Store.Start(context.Background(), ltc.Stopper); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	nc.Set(context.TODO(), nodeDesc.NodeID)
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
	ltc.Stopper.Stop(context.TODO())
}
