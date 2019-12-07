// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package localtestcluster

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/tscache"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	opentracing "github.com/opentracing/opentracing-go"
)

// A LocalTestCluster encapsulates an in-memory instantiation of a
// cockroach node with a single store using a local sender. Example
// usage of a LocalTestCluster follows:
//
//   s := &LocalTestCluster{}
//   s.Start(t, testutils.NewNodeTestBaseContext(),
//           kv.InitFactoryForLocalTestCluster)
//   defer s.Stop()
//
// Note that the LocalTestCluster is different from server.TestCluster
// in that although it uses a distributed sender, there is no RPC traffic.
type LocalTestCluster struct {
	Cfg               storage.StoreConfig
	Manual            *hlc.ManualClock
	Clock             *hlc.Clock
	Gossip            *gossip.Gossip
	Eng               engine.Engine
	Store             *storage.Store
	StoreTestingKnobs *storage.StoreTestingKnobs
	DBContext         *client.DBContext
	DB                *client.DB
	Stores            *storage.Stores
	Stopper           *stop.Stopper
	Latency           time.Duration // sleep for each RPC sent
	tester            testing.TB

	// DisableLivenessHeartbeat, if set, inhibits the heartbeat loop. Some tests
	// need this because, for example, the heartbeat loop increments some
	// transaction metrics.
	// However, note that without heartbeats, ranges with epoch-based leases
	// cannot be accessed because the leases cannot be granted.
	// See also DontCreateSystemRanges.
	DisableLivenessHeartbeat bool

	// DontCreateSystemRanges, if set, makes the cluster start with a single
	// range, not with all the system ranges (as regular cluster start).
	// If DisableLivenessHeartbeat is set, you probably want to also set this so
	// that ranges requiring epoch-based leases are not created automatically.
	DontCreateSystemRanges bool
}

// InitFactoryFn is a callback used to initiate the txn coordinator
// sender factory (we don't do it directly from this package to avoid
// a dependency on kv).
type InitFactoryFn func(
	st *cluster.Settings,
	nodeDesc *roachpb.NodeDescriptor,
	tracer opentracing.Tracer,
	clock *hlc.Clock,
	latency time.Duration,
	stores client.Sender,
	stopper *stop.Stopper,
	gossip *gossip.Gossip,
) client.TxnSenderFactory

// Start starts the test cluster by bootstrapping an in-memory store
// (defaults to maximum of 50M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.Addr after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ltc *LocalTestCluster) Start(t testing.TB, baseCtx *base.Config, initFactory InitFactoryFn) {
	ltc.Manual = hlc.NewManualClock(123)
	ltc.Clock = hlc.NewClock(ltc.Manual.UnixNano, 50*time.Millisecond)
	cfg := storage.TestStoreConfig(ltc.Clock)
	ambient := log.AmbientContext{Tracer: cfg.Settings.Tracer}
	nc := &base.NodeIDContainer{}
	ambient.AddLogTag("n", nc)

	nodeID := roachpb.NodeID(1)
	nodeDesc := &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr("tcp", "invalid.invalid:26257"),
	}

	ltc.tester = t
	ltc.Stopper = stop.NewStopper()
	cfg.RPCContext = rpc.NewContext(ambient, baseCtx, ltc.Clock, ltc.Stopper, cfg.Settings)
	cfg.RPCContext.NodeID.Set(ambient.AnnotateCtx(context.Background()), nodeID)
	c := &cfg.RPCContext.ClusterID
	server := rpc.NewServer(cfg.RPCContext) // never started
	ltc.Gossip = gossip.New(ambient, c, nc, cfg.RPCContext, server, ltc.Stopper, metric.NewRegistry(), roachpb.Locality{}, config.DefaultZoneConfigRef())
	ltc.Eng = engine.NewInMem(ambient.AnnotateCtx(context.Background()),
		engine.DefaultStorageEngine, roachpb.Attributes{}, 50<<20)
	ltc.Stopper.AddCloser(ltc.Eng)

	ltc.Stores = storage.NewStores(ambient, ltc.Clock, cluster.BinaryMinimumSupportedVersion, cluster.BinaryServerVersion)

	factory := initFactory(cfg.Settings, nodeDesc, ambient.Tracer, ltc.Clock, ltc.Latency, ltc.Stores, ltc.Stopper, ltc.Gossip)
	if ltc.DBContext == nil {
		dbCtx := client.DefaultDBContext()
		dbCtx.Stopper = ltc.Stopper
		ltc.DBContext = &dbCtx
	}
	ltc.DBContext.NodeID.Set(context.Background(), nodeID)
	ltc.DB = client.NewDBWithContext(cfg.AmbientCtx, factory, ltc.Clock, *ltc.DBContext)
	transport := storage.NewDummyRaftTransport(cfg.Settings)
	// By default, disable the replica scanner and split queue, which
	// confuse tests using LocalTestCluster.
	if ltc.StoreTestingKnobs == nil {
		cfg.TestingKnobs.DisableScanner = true
		cfg.TestingKnobs.DisableSplitQueue = true
	} else {
		cfg.TestingKnobs = *ltc.StoreTestingKnobs
	}
	cfg.AmbientCtx = ambient
	cfg.DB = ltc.DB
	cfg.Gossip = ltc.Gossip
	cfg.HistogramWindowInterval = metric.TestSampleInterval
	active, renewal := cfg.NodeLivenessDurations()
	cfg.NodeLiveness = storage.NewNodeLiveness(
		cfg.AmbientCtx,
		cfg.Clock,
		cfg.DB,
		[]engine.Engine{ltc.Eng},
		cfg.Gossip,
		active,
		renewal,
		cfg.Settings,
		cfg.HistogramWindowInterval,
	)
	storage.TimeUntilStoreDead.Override(&cfg.Settings.SV, storage.TestTimeUntilStoreDead)
	cfg.StorePool = storage.NewStorePool(
		cfg.AmbientCtx,
		cfg.Settings,
		cfg.Gossip,
		cfg.Clock,
		cfg.NodeLiveness.GetNodeCount,
		storage.MakeStorePoolNodeLivenessFunc(cfg.NodeLiveness),
		/* deterministic */ false,
	)
	cfg.Transport = transport
	cfg.TimestampCachePageSize = tscache.TestSklPageSize
	ctx := context.TODO()

	if err := storage.InitEngine(
		ctx, ltc.Eng, roachpb.StoreIdent{NodeID: nodeID, StoreID: 1},
		cluster.ClusterVersion{Version: cluster.BinaryServerVersion},
	); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}
	ltc.Store = storage.NewStore(ctx, cfg, ltc.Eng, nodeDesc)

	var initialValues []roachpb.KeyValue
	var splits []roachpb.RKey
	if !ltc.DontCreateSystemRanges {
		schema := sqlbase.MakeMetadataSchema(cfg.DefaultZoneConfig, cfg.DefaultSystemZoneConfig)
		var tableSplits []roachpb.RKey
		bootstrapVersion := cluster.ClusterVersion{Version: cluster.BinaryServerVersion}
		initialValues, tableSplits = schema.GetInitialValues(bootstrapVersion)
		splits = append(config.StaticSplits(), tableSplits...)
		sort.Slice(splits, func(i, j int) bool {
			return splits[i].Less(splits[j])
		})
	}

	if err := storage.WriteInitialClusterData(
		ctx,
		ltc.Eng,
		initialValues,
		cluster.BinaryServerVersion,
		1, /* numStores */
		splits,
		ltc.Clock.PhysicalNow(),
	); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}

	if !ltc.DisableLivenessHeartbeat {
		cfg.NodeLiveness.StartHeartbeat(ctx, ltc.Stopper, nil /* alive */)
	}

	if err := ltc.Store.Start(ctx, ltc.Stopper); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}

	ltc.Stores.AddStore(ltc.Store)
	nc.Set(ctx, nodeDesc.NodeID)
	if err := ltc.Gossip.SetNodeDescriptor(nodeDesc); err != nil {
		t.Fatalf("unable to set node descriptor: %s", err)
	}
	ltc.Cfg = cfg
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
