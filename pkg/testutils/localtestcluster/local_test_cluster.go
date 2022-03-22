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
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts/sidetransport"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/systemconfigwatcher"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigkvsubscriber"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
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
	AmbientCtx        log.AmbientContext
	Cfg               kvserver.StoreConfig
	Manual            *hlc.ManualClock
	Clock             *hlc.Clock
	Gossip            *gossip.Gossip
	Eng               storage.Engine
	Store             *kvserver.Store
	StoreTestingKnobs *kvserver.StoreTestingKnobs
	dbContext         *kv.DBContext
	DB                *kv.DB
	Stores            *kvserver.Stores
	stopper           *stop.Stopper
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
	ctx context.Context,
	st *cluster.Settings,
	nodeDesc *roachpb.NodeDescriptor,
	tracer *tracing.Tracer,
	clock *hlc.Clock,
	latency time.Duration,
	stores kv.Sender,
	stopper *stop.Stopper,
	gossip *gossip.Gossip,
) kv.TxnSenderFactory

// Stopper returns the Stopper.
func (ltc *LocalTestCluster) Stopper() *stop.Stopper {
	return ltc.stopper
}

// Start starts the test cluster by bootstrapping an in-memory store
// (defaults to maximum of 50M). The server is started, launching the
// node RPC server and all HTTP endpoints. Use the value of
// TestServer.Addr after Start() for client connections. Use Stop()
// to shutdown the server after the test completes.
func (ltc *LocalTestCluster) Start(t testing.TB, baseCtx *base.Config, initFactory InitFactoryFn) {
	manualClock := hlc.NewManualClock(123)
	clock := hlc.NewClock(manualClock.UnixNano, 50*time.Millisecond)
	cfg := kvserver.TestStoreConfig(clock)
	tr := cfg.AmbientCtx.Tracer
	ltc.stopper = stop.NewStopper(stop.WithTracer(tr))
	ltc.Manual = manualClock
	ltc.Clock = clock
	ambient := cfg.AmbientCtx

	nc := &base.NodeIDContainer{}
	ambient.AddLogTag("n", nc)
	ltc.AmbientCtx = ambient
	ctx := ambient.AnnotateCtx(context.Background())

	nodeID := roachpb.NodeID(1)
	nodeDesc := &roachpb.NodeDescriptor{
		NodeID:  nodeID,
		Address: util.MakeUnresolvedAddr("tcp", "invalid.invalid:26257"),
	}

	ltc.tester = t
	cfg.RPCContext = rpc.NewContext(ctx, rpc.ContextOptions{
		TenantID: roachpb.SystemTenantID,
		Config:   baseCtx,
		Clock:    ltc.Clock,
		Stopper:  ltc.stopper,
		Settings: cfg.Settings,
		NodeID:   nc,
	})
	cfg.RPCContext.NodeID.Set(ctx, nodeID)
	clusterID := cfg.RPCContext.ClusterID
	server := rpc.NewServer(cfg.RPCContext) // never started
	ltc.Gossip = gossip.New(ambient, clusterID, nc, cfg.RPCContext, server, ltc.stopper, metric.NewRegistry(), roachpb.Locality{}, zonepb.DefaultZoneConfigRef())
	var err error
	ltc.Eng, err = storage.Open(
		ctx,
		storage.InMemory(),
		storage.CacheSize(0),
		storage.MaxSize(50<<20 /* 50 MiB */),
	)
	if err != nil {
		t.Fatal(err)
	}
	ltc.stopper.AddCloser(ltc.Eng)

	ltc.Stores = kvserver.NewStores(ambient, ltc.Clock)

	factory := initFactory(ctx, cfg.Settings, nodeDesc, ltc.stopper.Tracer(), ltc.Clock, ltc.Latency, ltc.Stores, ltc.stopper, ltc.Gossip)

	var nodeIDContainer base.NodeIDContainer
	nodeIDContainer.Set(context.Background(), nodeID)

	ltc.dbContext = &kv.DBContext{
		UserPriority: roachpb.NormalUserPriority,
		Stopper:      ltc.stopper,
		NodeID:       base.NewSQLIDContainerForNode(&nodeIDContainer),
	}
	ltc.DB = kv.NewDBWithContext(cfg.AmbientCtx, factory, ltc.Clock, *ltc.dbContext)
	transport := kvserver.NewDummyRaftTransport(cfg.Settings, cfg.AmbientCtx.Tracer)
	// By default, disable the replica scanner and split queue, which
	// confuse tests using LocalTestCluster.
	if ltc.StoreTestingKnobs == nil {
		cfg.TestingKnobs.DisableScanner = true
		cfg.TestingKnobs.DisableSplitQueue = true
	} else {
		cfg.TestingKnobs = *ltc.StoreTestingKnobs
	}
	cfg.DB = ltc.DB
	cfg.Gossip = ltc.Gossip
	cfg.HistogramWindowInterval = metric.TestSampleInterval
	active, renewal := cfg.NodeLivenessDurations()
	cfg.NodeLiveness = liveness.NewNodeLiveness(liveness.NodeLivenessOptions{
		AmbientCtx:              cfg.AmbientCtx,
		Clock:                   cfg.Clock,
		DB:                      cfg.DB,
		Gossip:                  cfg.Gossip,
		LivenessThreshold:       active,
		RenewalDuration:         renewal,
		Settings:                cfg.Settings,
		HistogramWindowInterval: cfg.HistogramWindowInterval,
	})
	kvserver.TimeUntilStoreDead.Override(ctx, &cfg.Settings.SV, kvserver.TestTimeUntilStoreDead)
	cfg.StorePool = kvserver.NewStorePool(
		cfg.AmbientCtx,
		cfg.Settings,
		cfg.Gossip,
		cfg.Clock,
		cfg.NodeLiveness.GetNodeCount,
		kvserver.MakeStorePoolNodeLivenessFunc(cfg.NodeLiveness),
		/* deterministic */ false,
	)
	cfg.Transport = transport
	cfg.ClosedTimestampReceiver = sidetransport.NewReceiver(nc, ltc.stopper, ltc.Stores, nil /* testingKnobs */)

	if err := kvserver.WriteClusterVersion(ctx, ltc.Eng, clusterversion.TestingClusterVersion); err != nil {
		t.Fatalf("unable to write cluster version: %s", err)
	}
	if err := kvserver.InitEngine(
		ctx, ltc.Eng, roachpb.StoreIdent{NodeID: nodeID, StoreID: 1},
	); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}

	rangeFeedFactory, err := rangefeed.NewFactory(ltc.stopper, ltc.DB, cfg.Settings, nil /* knobs */)
	if err != nil {
		t.Fatal(err)
	}
	cfg.SpanConfigSubscriber = spanconfigkvsubscriber.New(
		clock,
		rangeFeedFactory,
		keys.SpanConfigurationsTableID,
		1<<20, /* 1 MB */
		cfg.DefaultSpanConfig,
		nil,
	)
	cfg.SystemConfigProvider = systemconfigwatcher.New(
		keys.SystemSQLCodec,
		cfg.Clock,
		rangeFeedFactory,
		zonepb.DefaultZoneConfigRef(),
	)

	ltc.Store = kvserver.NewStore(ctx, cfg, ltc.Eng, nodeDesc)

	var initialValues []roachpb.KeyValue
	var splits []roachpb.RKey
	if !ltc.DontCreateSystemRanges {
		schema := bootstrap.MakeMetadataSchema(
			keys.SystemSQLCodec, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
		)
		var tableSplits []roachpb.RKey
		initialValues, tableSplits = schema.GetInitialValues()
		splits = append(config.StaticSplits(), tableSplits...)
		sort.Slice(splits, func(i, j int) bool {
			return splits[i].Less(splits[j])
		})
	}

	if err := kvserver.WriteInitialClusterData(
		ctx,
		ltc.Eng,
		initialValues,
		clusterversion.TestingBinaryVersion,
		1, /* numStores */
		splits,
		ltc.Clock.PhysicalNow(),
		cfg.TestingKnobs,
	); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}

	// The heartbeat loop depends on gossip to retrieve the node ID, so we're
	// sure to set it first.
	nc.Set(ctx, nodeDesc.NodeID)
	if err := ltc.Gossip.SetNodeDescriptor(nodeDesc); err != nil {
		t.Fatalf("unable to set node descriptor: %s", err)
	}

	if !ltc.DisableLivenessHeartbeat {
		cfg.NodeLiveness.Start(ctx,
			liveness.NodeLivenessStartOptions{Stopper: ltc.stopper, Engines: []storage.Engine{ltc.Eng}})
	}

	if err := ltc.Store.Start(ctx, ltc.stopper); err != nil {
		t.Fatalf("unable to start local test cluster: %s", err)
	}

	ltc.Stores.AddStore(ltc.Store)
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
	ltc.stopper.Stop(context.TODO())
}
