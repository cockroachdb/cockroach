// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/gossip/resolver"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/netutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
)

// createTestNode creates an rpc server using the specified address,
// gossip instance, KV database and a node using the specified slice
// of engines. The server, clock and node are returned. If gossipBS is
// not nil, the gossip bootstrap address is set to gossipBS.
func createTestNode(
	addr net.Addr, engines []engine.Engine, gossipBS net.Addr, t *testing.T,
) (*grpc.Server, net.Addr, storage.StoreConfig, *Node, *stop.Stopper) {
	cfg := storage.TestStoreConfig(nil /* clock */)
	st := cfg.Settings

	stopper := stop.NewStopper()
	nodeRPCContext := rpc.NewContext(
		log.AmbientContext{Tracer: cfg.Settings.Tracer}, nodeTestBaseContext, cfg.Clock, stopper,
		cfg.Settings)
	cfg.RPCContext = nodeRPCContext
	cfg.ScanInterval = 10 * time.Hour
	grpcServer := rpc.NewServer(nodeRPCContext)
	cfg.Gossip = gossip.NewTest(
		0,
		nodeRPCContext,
		grpcServer,
		stopper,
		metric.NewRegistry(),
		cfg.DefaultZoneConfig,
	)
	retryOpts := base.DefaultRetryOptions()
	retryOpts.Closer = stopper.ShouldQuiesce()
	cfg.AmbientCtx.Tracer = st.Tracer
	distSender := kv.NewDistSender(kv.DistSenderConfig{
		AmbientCtx:      cfg.AmbientCtx,
		Settings:        st,
		Clock:           cfg.Clock,
		RPCContext:      nodeRPCContext,
		RPCRetryOptions: &retryOpts,
		NodeDialer:      nodedialer.New(nodeRPCContext, gossip.AddressResolver(cfg.Gossip)),
	}, cfg.Gossip)
	tsf := kv.NewTxnCoordSenderFactory(
		kv.TxnCoordSenderFactoryConfig{
			AmbientCtx: cfg.AmbientCtx,
			Settings:   st,
			Clock:      cfg.Clock,
			Stopper:    stopper,
		},
		distSender,
	)
	cfg.DB = client.NewDB(cfg.AmbientCtx, tsf, cfg.Clock)
	cfg.Transport = storage.NewDummyRaftTransport(st)
	active, renewal := cfg.NodeLivenessDurations()
	cfg.HistogramWindowInterval = metric.TestSampleInterval
	cfg.NodeLiveness = storage.NewNodeLiveness(
		cfg.AmbientCtx,
		cfg.Clock,
		cfg.DB,
		engines,
		cfg.Gossip,
		active,
		renewal,
		cfg.Settings,
		cfg.HistogramWindowInterval,
	)

	storage.TimeUntilStoreDead.Override(&cfg.Settings.SV, 10*time.Millisecond)
	cfg.StorePool = storage.NewStorePool(
		cfg.AmbientCtx,
		st,
		cfg.Gossip,
		cfg.Clock,
		cfg.NodeLiveness.GetNodeCount,
		storage.MakeStorePoolNodeLivenessFunc(cfg.NodeLiveness),
		/* deterministic */ false,
	)
	metricsRecorder := status.NewMetricsRecorder(cfg.Clock, cfg.NodeLiveness, nodeRPCContext, cfg.Gossip, st)
	node := NewNode(cfg, metricsRecorder, metric.NewRegistry(), stopper,
		kv.MakeTxnMetrics(metric.TestSampleInterval), nil, /* execCfg */
		&nodeRPCContext.ClusterID)
	roachpb.RegisterInternalServer(grpcServer, node)
	node.storeCfg.ClosedTimestamp.RegisterClosedTimestampServer(grpcServer)
	ln, err := netutil.ListenAndServeGRPC(stopper, grpcServer, addr)
	if err != nil {
		t.Fatal(err)
	}
	if gossipBS != nil {
		// Handle possibility of a :0 port specification.
		if gossipBS.Network() == addr.Network() && gossipBS.String() == addr.String() {
			gossipBS = ln.Addr()
		}
		r, err := resolver.NewResolverFromAddress(gossipBS)
		if err != nil {
			t.Fatal(err)
		}
		serverCfg := MakeConfig(context.TODO(), st)
		serverCfg.GossipBootstrapResolvers = []resolver.Resolver{r}
		filtered := serverCfg.FilterGossipBootstrapResolvers(
			context.Background(), ln.Addr(), ln.Addr(),
		)
		cfg.Gossip.Start(ln.Addr(), filtered)
	}
	return grpcServer, ln.Addr(), cfg, node, stopper
}

// createAndStartTestNode creates a new test node and starts it. The server and node are returned.
func createAndStartTestNode(
	ctx context.Context,
	addr net.Addr,
	engines []engine.Engine,
	gossipBS net.Addr,
	locality roachpb.Locality,
	t *testing.T,
) (*grpc.Server, net.Addr, *Node, *stop.Stopper) {
	grpcServer, addr, cfg, node, stopper := createTestNode(addr, engines, gossipBS, t)
	bootstrappedEngines, newEngines, cv, err := inspectEngines(
		ctx, engines,
		cluster.Version.BinaryMinSupportedVersion(cfg.Settings),
		cluster.Version.BinaryVersion(cfg.Settings),
		node.clusterID)
	if err != nil {
		t.Fatal(err)
	}
	// Starting the heartbeat is usually done by the server. This test needs it
	// because otherwise some of the initial ranges cannot be accessed (since
	// they need an epoch-based lease).
	cfg.NodeLiveness.StartHeartbeat(ctx, stopper, nil /* alive */)
	if err := node.start(ctx,
		addr,
		addr, // Note: this is not really a SQL address but these tests do not use SQL so all is fine.
		bootstrappedEngines, newEngines, "",
		roachpb.Attributes{}, locality, cv, []roachpb.LocalityAddress{},
		nil, /*nodeDescriptorCallback */
	); err != nil {
		stopper.Stop(ctx)
		t.Fatal(err)
	}

	return grpcServer, addr, node, stopper
}

func formatKeys(keys []roachpb.Key) string {
	var buf bytes.Buffer
	for i, key := range keys {
		fmt.Fprintf(&buf, "%d: %s\n", i, key)
	}
	return buf.String()
}

// keySlice implements sort.Interface.
type keySlice []roachpb.Key

func (s keySlice) Len() int           { return len(s) }
func (s keySlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s keySlice) Less(i, j int) bool { return bytes.Compare(s[i], s[j]) < 0 }

// TestBootstrapCluster verifies the results of bootstrapping a
// cluster. Uses an in memory engine.
func TestBootstrapCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	e := engine.NewDefaultInMem()
	defer e.Close()
	if _, err := bootstrapCluster(
		ctx, []engine.Engine{e}, cluster.TestingClusterVersion, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
	); err != nil {
		t.Fatal(err)
	}

	// Scan the complete contents of the local database directly from the engine.
	res, err := engine.MVCCScan(ctx, e, keys.LocalMax, roachpb.KeyMax, math.MaxInt64, hlc.MaxTimestamp, engine.MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var foundKeys keySlice
	for _, kv := range res.KVs {
		foundKeys = append(foundKeys, kv.Key)
	}
	var expectedKeys = keySlice{
		testutils.MakeKey(roachpb.Key("\x02"), roachpb.KeyMax),
		testutils.MakeKey(roachpb.Key("\x03"), roachpb.KeyMax),
		roachpb.Key("\x04bootstrap-version"),
		roachpb.Key("\x04node-idgen"),
		roachpb.Key("\x04range-idgen"),
		roachpb.Key("\x04store-idgen"),
	}
	for _, splitKey := range config.StaticSplits() {
		meta2Key := keys.RangeMetaKey(splitKey)
		expectedKeys = append(expectedKeys, meta2Key.AsRawKey())
	}

	// Add the initial keys for sql.
	kvs, tableSplits := GetBootstrapSchema(zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef()).GetInitialValues(cluster.TestingClusterVersion)
	for _, kv := range kvs {
		expectedKeys = append(expectedKeys, kv.Key)
	}
	for _, splitKey := range tableSplits {
		meta2Key := keys.RangeMetaKey(splitKey)
		expectedKeys = append(expectedKeys, meta2Key.AsRawKey())
	}

	// Resort the list. The sql values are not sorted.
	sort.Sort(expectedKeys)

	if !reflect.DeepEqual(foundKeys, expectedKeys) {
		t.Errorf("expected keys mismatch (found vs expected):\n%s\n  -- vs. -- \n\n%s",
			formatKeys(foundKeys), formatKeys(expectedKeys))
	}

	// TODO(spencer): check values.
}

// TestBootstrapNewStore starts a cluster with two unbootstrapped
// stores and verifies both stores are added and started.
func TestBootstrapNewStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	e := engine.NewDefaultInMem()
	if _, err := bootstrapCluster(
		ctx, []engine.Engine{e}, cluster.TestingClusterVersion, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
	); err != nil {
		t.Fatal(err)
	}

	// Start a new node with two new stores which will require bootstrapping.
	engines := Engines([]engine.Engine{
		e,
		engine.NewDefaultInMem(),
		engine.NewDefaultInMem(),
	})
	defer engines.Close()
	_, _, node, stopper := createAndStartTestNode(
		ctx,
		util.TestAddr,
		engines,
		util.TestAddr,
		roachpb.Locality{},
		t,
	)
	defer stopper.Stop(ctx)

	// Non-initialized stores (in this case the new in-memory-based
	// store) will be bootstrapped by the node upon start. This happens
	// in a goroutine, so we'll have to wait a bit until we can find the
	// new node.
	testutils.SucceedsSoon(t, func() error {
		if n := node.stores.GetStoreCount(); n != 3 {
			return errors.Errorf("expected 3 stores but got %d", n)
		}
		return nil
	})

	// Check whether all stores are started properly.
	if err := node.stores.VisitStores(func(s *storage.Store) error {
		if !s.IsStarted() {
			return errors.Errorf("fail to start store: %s", s)
		}
		return nil
	}); err != nil {
		t.Error(err)
	}
}

// TestNodeJoin verifies a new node is able to join a bootstrapped
// cluster consisting of one node.
func TestNodeJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	engineStopper := stop.NewStopper()
	defer engineStopper.Stop(ctx)
	e := engine.NewDefaultInMem()
	engineStopper.AddCloser(e)

	if _, err := bootstrapCluster(
		ctx, []engine.Engine{e}, cluster.TestingClusterVersion, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
	); err != nil {
		t.Fatal(err)
	}

	// Start the bootstrap node.
	engines1 := []engine.Engine{e}
	_, server1Addr, node1, stopper1 := createAndStartTestNode(
		ctx,
		util.TestAddr,
		engines1,
		util.TestAddr,
		roachpb.Locality{},
		t,
	)
	defer stopper1.Stop(ctx)

	// Create a new node.
	e2 := engine.NewDefaultInMem()
	engineStopper.AddCloser(e2)
	engines2 := []engine.Engine{e2}
	_, server2Addr, node2, stopper2 := createAndStartTestNode(
		ctx,
		util.TestAddr,
		engines2,
		server1Addr,
		roachpb.Locality{},
		t,
	)
	defer stopper2.Stop(ctx)

	// Verify new node is able to bootstrap its store.
	testutils.SucceedsSoon(t, func() error {
		if sc := node2.stores.GetStoreCount(); sc != 1 {
			return errors.Errorf("GetStoreCount() expected 1; got %d", sc)
		}
		return nil
	})

	// Verify node1 sees node2 via gossip and vice versa.
	node1Key := gossip.MakeNodeIDKey(node1.Descriptor.NodeID)
	node2Key := gossip.MakeNodeIDKey(node2.Descriptor.NodeID)
	testutils.SucceedsSoon(t, func() error {
		var nodeDesc1 roachpb.NodeDescriptor
		if err := node1.storeCfg.Gossip.GetInfoProto(node2Key, &nodeDesc1); err != nil {
			return err
		}
		if addr2Str, server2AddrStr := nodeDesc1.Address.String(), server2Addr.String(); addr2Str != server2AddrStr {
			return errors.Errorf("addr2 gossip %s doesn't match addr2 address %s", addr2Str, server2AddrStr)
		}
		var nodeDesc2 roachpb.NodeDescriptor
		if err := node2.storeCfg.Gossip.GetInfoProto(node1Key, &nodeDesc2); err != nil {
			return err
		}
		if addr1Str, server1AddrStr := nodeDesc2.Address.String(), server1Addr.String(); addr1Str != server1AddrStr {
			return errors.Errorf("addr1 gossip %s doesn't match addr1 address %s", addr1Str, server1AddrStr)
		}
		return nil
	})
}

// TestCorruptedClusterID verifies that a node fails to start when a
// store's cluster ID is empty.
func TestCorruptedClusterID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	e := engine.NewDefaultInMem()
	defer e.Close()

	if _, err := bootstrapCluster(
		ctx, []engine.Engine{e}, cluster.TestingClusterVersion, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
	); err != nil {
		t.Fatal(err)
	}

	// Set the cluster ID to the empty UUID.
	sIdent := roachpb.StoreIdent{
		ClusterID: uuid.UUID{},
		NodeID:    1,
		StoreID:   1,
	}
	if err := engine.MVCCPutProto(
		ctx, e, nil /* ms */, keys.StoreIdentKey(), hlc.Timestamp{}, nil /* txn */, &sIdent,
	); err != nil {
		t.Fatal(err)
	}

	engines := []engine.Engine{e}
	_, serverAddr, _, node, stopper := createTestNode(util.TestAddr, engines, nil, t)
	defer stopper.Stop(ctx)
	bootstrappedEngines, newEngines, cv, err := inspectEngines(
		ctx, engines,
		cluster.BinaryMinimumSupportedVersion,
		cluster.BinaryServerVersion,
		node.clusterID)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.start(
		ctx, serverAddr,
		serverAddr, // Note: this is not really a SQL address but the tests in this package do not use SQL so all is fine.
		bootstrappedEngines, newEngines, "",
		roachpb.Attributes{}, roachpb.Locality{}, cv,
		[]roachpb.LocalityAddress{},
		nil, /* nodeDescriptorCallback */
	); !testutils.IsError(err, "unidentified store") {
		t.Errorf("unexpected error %v", err)
	}
}

// compareNodeStatus ensures that the actual node status for the passed in
// node is updated correctly. It checks that the Node Descriptor, StoreIDs,
// RangeCount, StartedAt, ReplicatedRangeCount and are exactly correct and that
// the bytes and counts for Live, Key and Val are at least the expected value.
// And that UpdatedAt has increased.
// The latest actual stats are returned.
func compareNodeStatus(
	t *testing.T, ts *TestServer, expectedNodeStatus *statuspb.NodeStatus, testNumber int,
) *statuspb.NodeStatus {
	// ========================================
	// Read NodeStatus from server and validate top-level fields.
	// ========================================
	nodeStatusKey := keys.NodeStatusKey(ts.node.Descriptor.NodeID)
	nodeStatus := &statuspb.NodeStatus{}
	if err := ts.db.GetProto(context.TODO(), nodeStatusKey, nodeStatus); err != nil {
		t.Fatalf("%d: failure getting node status: %s", testNumber, err)
	}

	// Descriptor values should be exactly equal to expected.
	if a, e := nodeStatus.Desc, expectedNodeStatus.Desc; !reflect.DeepEqual(a, e) {
		t.Errorf("%d: Descriptor does not match expected.\nexpected: %s\nactual: %s", testNumber, &e, &a)
	}

	// ========================================
	// Ensure all expected stores are represented in the node status.
	// ========================================
	storesToMap := func(ns *statuspb.NodeStatus) map[roachpb.StoreID]statuspb.StoreStatus {
		strMap := make(map[roachpb.StoreID]statuspb.StoreStatus, len(ns.StoreStatuses))
		for _, str := range ns.StoreStatuses {
			strMap[str.Desc.StoreID] = str
		}
		return strMap
	}
	actualStores := storesToMap(nodeStatus)
	expectedStores := storesToMap(expectedNodeStatus)

	if a, e := len(actualStores), len(expectedStores); a != e {
		t.Errorf("%d: actual status contained %d stores, expected %d", testNumber, a, e)
	}
	for key := range expectedStores {
		if _, ok := actualStores[key]; !ok {
			t.Errorf("%d: actual node status did not contain expected store %d", testNumber, key)
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	// ========================================
	// Ensure all metric sets (node and store level) are consistent with
	// expected status.
	// ========================================

	// CompareMetricMaps accepts an actual and expected metric maps, along with
	// two lists of string keys. For metrics with keys in the 'equal' map, the
	// actual value must be equal to the expected value. For keys in the
	// 'greater' map, the actual value must be greater than or equal to the
	// expected value.
	compareMetricMaps := func(actual, expected map[string]float64, equal, greater []string) {
		// Make sure the actual value map contains all values in expected map.
		for key := range expected {
			if _, ok := actual[key]; !ok {
				t.Errorf("%d: actual node status did not contain expected metric %s", testNumber, key)
			}
		}
		if t.Failed() {
			return
		}

		// For each equal key, ensure that the actual value is equal to expected
		// key.
		for _, key := range equal {
			if _, ok := actual[key]; !ok {
				t.Errorf("%d, actual node status did not contain expected 'equal' metric key %s", testNumber, key)
				continue
			}
			if a, e := actual[key], expected[key]; a != e {
				t.Errorf("%d: %s does not match expected value.\nExpected %f, Actual %f", testNumber, key, e, a)
			}
		}
		for _, key := range greater {
			if _, ok := actual[key]; !ok {
				t.Errorf("%d: actual node status did not contain expected 'greater' metric key %s", testNumber, key)
				continue
			}
			if a, e := actual[key], expected[key]; a < e {
				t.Errorf("%d: %s is not greater than or equal to expected value.\nExpected %f, Actual %f", testNumber, key, e, a)
			}
		}
	}

	compareMetricMaps(nodeStatus.Metrics, expectedNodeStatus.Metrics, nil, []string{
		"exec.success",
		"exec.error",
	})

	for key := range actualStores {
		// Directly verify a subset of metrics which have predictable output.
		compareMetricMaps(actualStores[key].Metrics, expectedStores[key].Metrics,
			[]string{
				"replicas",
				"replicas.leaseholders",
			},
			[]string{
				"livecount",
				"keycount",
				"valcount",
			})
	}

	if t.Failed() {
		t.FailNow()
	}

	return nodeStatus
}

// TestNodeStatusWritten verifies that status summaries are written correctly for
// both the Node and stores within the node.
func TestNodeStatusWritten(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// ========================================
	// Start test server and wait for full initialization.
	// ========================================
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DisableEventLog: true,
	})
	defer srv.Stopper().Stop(context.TODO())
	ts := srv.(*TestServer)
	ctx := context.TODO()

	// Retrieve the first store from the Node.
	s, err := ts.node.stores.GetStore(roachpb.StoreID(1))
	if err != nil {
		t.Fatal(err)
	}

	s.WaitForInit()

	content := "junk"
	leftKey := "a"

	// Scan over all keys to "wake up" all replicas (force a lease holder election).
	if _, err := kvDB.Scan(context.TODO(), keys.MetaMax, keys.MaxKey, 0); err != nil {
		t.Fatal(err)
	}

	// Wait for full replication of initial ranges.
	initialRanges, err := ExpectedInitialRangeCount(kvDB, &ts.cfg.DefaultZoneConfig, &ts.cfg.DefaultSystemZoneConfig)
	if err != nil {
		t.Fatal(err)
	}
	testutils.SucceedsSoon(t, func() error {
		for i := 1; i <= initialRanges; i++ {
			if s.RaftStatus(roachpb.RangeID(i)) == nil {
				return errors.Errorf("Store %d replica %d is not present in raft", s.StoreID(), i)
			}
		}
		return nil
	})

	// ========================================
	// Construct an initial expectation for NodeStatus to compare to the first
	// status produced by the server.
	// ========================================
	expectedNodeStatus := &statuspb.NodeStatus{
		Desc:      ts.node.Descriptor,
		StartedAt: 0,
		UpdatedAt: 0,
		Metrics: map[string]float64{
			"exec.success": 0,
			"exec.error":   0,
		},
	}

	expectedStoreStatuses := make(map[roachpb.StoreID]statuspb.StoreStatus)
	if err := ts.node.stores.VisitStores(func(s *storage.Store) error {
		desc, err := s.Descriptor(false /* useCached */)
		if err != nil {
			t.Fatal(err)
		}
		expectedReplicas := 0
		if s.StoreID() == roachpb.StoreID(1) {
			expectedReplicas = initialRanges
		}
		stat := statuspb.StoreStatus{
			Desc: *desc,
			Metrics: map[string]float64{
				"replicas":              float64(expectedReplicas),
				"replicas.leaseholders": float64(expectedReplicas),
				"livebytes":             0,
				"keybytes":              0,
				"valbytes":              0,
				"livecount":             0,
				"keycount":              0,
				"valcount":              0,
			},
		}
		expectedNodeStatus.StoreStatuses = append(expectedNodeStatus.StoreStatuses, stat)
		expectedStoreStatuses[s.StoreID()] = stat
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// Function to force summaries to be written synchronously, including all
	// data currently in the event pipeline. Only one of the stores has
	// replicas, so there are no concerns related to quorum writes; if there
	// were multiple replicas, more care would need to be taken in the initial
	// syncFeed().
	forceWriteStatus := func() {
		if err := ts.node.computePeriodicMetrics(ctx, 0); err != nil {
			t.Fatalf("error publishing store statuses: %s", err)
		}

		if err := ts.WriteSummaries(); err != nil {
			t.Fatalf("error writing summaries: %s", err)
		}
	}

	// Verify initial status.
	forceWriteStatus()
	expectedNodeStatus = compareNodeStatus(t, ts, expectedNodeStatus, 1)
	for _, s := range expectedNodeStatus.StoreStatuses {
		expectedStoreStatuses[s.Desc.StoreID] = s
	}

	// ========================================
	// Put some data into the K/V store and confirm change to status.
	// ========================================

	splitKey := "b"
	rightKey := "c"

	// Write some values left and right of the proposed split key.
	if err := ts.db.Put(ctx, leftKey, content); err != nil {
		t.Fatal(err)
	}
	if err := ts.db.Put(ctx, rightKey, content); err != nil {
		t.Fatal(err)
	}

	// Increment metrics on the node
	expectedNodeStatus.Metrics["exec.success"] += 2

	// Increment metrics on the first store.
	store1 := expectedStoreStatuses[roachpb.StoreID(1)].Metrics
	store1["livecount"]++
	store1["keycount"]++
	store1["valcount"]++
	store1["livebytes"]++
	store1["keybytes"]++
	store1["valbytes"]++

	forceWriteStatus()
	expectedNodeStatus = compareNodeStatus(t, ts, expectedNodeStatus, 2)
	for _, s := range expectedNodeStatus.StoreStatuses {
		expectedStoreStatuses[s.Desc.StoreID] = s
	}

	// ========================================
	// Perform an admin split and verify that status is updated.
	// ========================================

	// Split the range.
	if err := ts.db.AdminSplit(context.TODO(), splitKey, splitKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
		t.Fatal(err)
	}

	// Write on both sides of the split to ensure that the raft machinery
	// is running.
	if err := ts.db.Put(ctx, leftKey, content); err != nil {
		t.Fatal(err)
	}
	if err := ts.db.Put(ctx, rightKey, content); err != nil {
		t.Fatal(err)
	}

	// Increment metrics on the node
	expectedNodeStatus.Metrics["exec.success"] += 2

	// Increment metrics on the first store.
	store1 = expectedStoreStatuses[roachpb.StoreID(1)].Metrics
	store1["replicas"]++
	store1["replicas.leaders"]++
	store1["replicas.leaseholders"]++
	store1["ranges"]++

	forceWriteStatus()
	expectedNodeStatus = compareNodeStatus(t, ts, expectedNodeStatus, 3)
	for _, s := range expectedNodeStatus.StoreStatuses {
		expectedStoreStatuses[s.Desc.StoreID] = s
	}
}

// TestStartNodeWithLocality creates a new node and store and starts them with a
// collection of different localities.
func TestStartNodeWithLocality(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	testLocalityWithNewNode := func(locality roachpb.Locality) {
		e := engine.NewDefaultInMem()
		defer e.Close()
		if _, err := bootstrapCluster(
			ctx, []engine.Engine{e}, cluster.TestingClusterVersion, zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
		); err != nil {
			t.Fatal(err)
		}
		_, _, node, stopper := createAndStartTestNode(
			ctx,
			util.TestAddr,
			[]engine.Engine{e},
			util.TestAddr,
			locality,
			t,
		)
		defer stopper.Stop(ctx)

		// Check the node to make sure the locality was propagated to its
		// nodeDescriptor.
		if !reflect.DeepEqual(node.Descriptor.Locality, locality) {
			t.Fatalf("expected node locality to be %s, but it was %s", locality, node.Descriptor.Locality)
		}

		// Check the store to make sure the locality was propagated to its
		// nodeDescriptor.
		if err := node.stores.VisitStores(func(store *storage.Store) error {
			desc, err := store.Descriptor(false /* useCached */)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(desc.Node.Locality, locality) {
				t.Fatalf("expected store's node locality to be %s, but it was %s", locality, desc.Node.Locality)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	testCases := []roachpb.Locality{
		{},
		{
			Tiers: []roachpb.Tier{
				{Key: "a", Value: "b"},
			},
		},
		{
			Tiers: []roachpb.Tier{
				{Key: "a", Value: "b"},
				{Key: "c", Value: "d"},
				{Key: "e", Value: "f"},
			},
		},
	}

	for _, testCase := range testCases {
		testLocalityWithNewNode(testCase)
	}
}

func TestNodeSendUnknownBatchRequest(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ba := roachpb.BatchRequest{
		Requests: make([]roachpb.RequestUnion, 1),
	}
	n := &Node{}
	br, err := n.batchInternal(context.Background(), &ba)
	if err != nil {
		t.Fatal(err)
	}
	if br.Error == nil {
		t.Fatal("no batch error returned")
	}
	if _, ok := br.Error.GetDetail().(*roachpb.UnsupportedRequestError); !ok {
		t.Fatalf("expected unsupported request, not %v", br.Error)
	}
}
