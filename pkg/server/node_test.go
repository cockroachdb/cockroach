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
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	e := storage.NewDefaultInMemForTesting()
	defer e.Close()
	require.NoError(t, kvserver.WriteClusterVersion(ctx, e, clusterversion.TestingClusterVersion))

	initCfg := initServerCfg{
		binaryMinSupportedVersion: clusterversion.TestingBinaryMinSupportedVersion,
		binaryVersion:             clusterversion.TestingBinaryVersion,
		defaultSystemZoneConfig:   *zonepb.DefaultZoneConfigRef(),
		defaultZoneConfig:         *zonepb.DefaultSystemZoneConfigRef(),
	}
	if _, err := bootstrapCluster(ctx, []storage.Engine{e}, initCfg); err != nil {
		t.Fatal(err)
	}

	// Scan the complete contents of the local database directly from the engine.
	res, err := storage.MVCCScan(ctx, e, keys.LocalMax, roachpb.KeyMax, hlc.MaxTimestamp, storage.MVCCScanOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var foundKeys keySlice
	for _, kv := range res.KVs {
		foundKeys = append(foundKeys, kv.Key)
	}
	const firstNodeID = 1
	var expectedKeys = keySlice{
		testutils.MakeKey(roachpb.Key("\x02"), roachpb.KeyMax),
		testutils.MakeKey(roachpb.Key("\x03"), roachpb.KeyMax),
		roachpb.Key("\x04bootstrap-version"),
		roachpb.Key("\x04node-idgen"),
		roachpb.Key("\x04range-idgen"),
		roachpb.Key("\x04store-idgen"),
		keys.NodeLivenessKey(firstNodeID),
	}
	for _, splitKey := range config.StaticSplits() {
		meta2Key := keys.RangeMetaKey(splitKey)
		expectedKeys = append(expectedKeys, meta2Key.AsRawKey())
	}

	// Add the initial keys for sql.
	kvs, tableSplits := GetBootstrapSchema(
		zonepb.DefaultZoneConfigRef(), zonepb.DefaultSystemZoneConfigRef(),
	).GetInitialValues()
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	path, cleanup := testutils.TempDir(t)
	defer cleanup()

	// Start server with persisted store so that it gets bootstrapped.
	{
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
			StoreSpecs: []base.StoreSpec{
				{Path: path},
			},
		})
		s.Stopper().Stop(ctx)
	}

	specs := []base.StoreSpec{
		{Path: path},
		{InMemory: true},
		{InMemory: true},
	}
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: specs,
	})
	defer s.Stopper().Stop(ctx)

	// Check whether all stores are started properly.
	testutils.SucceedsSoon(t, func() error {
		var n int
		err := s.GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
			if !s.IsStarted() {
				return fmt.Errorf("not started: %s", s)
			}
			n++
			return nil
		})
		if err != nil {
			return err
		}
		if exp := len(specs); exp != n {
			return fmt.Errorf("found only %d of %d stores", n, exp)
		}
		return nil
	})
}

// TestNodeJoin verifies a new node is able to join a bootstrapped
// cluster consisting of one node.
func TestNodeJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// For kicks, start both nodes in the cluster with two initially empty
	// engines. The first node is expected to bootstrap itself, so the second
	// one will join the first.
	perNode := map[int]base.TestServerArgs{}
	perNode[0] = base.TestServerArgs{
		StoreSpecs: []base.StoreSpec{
			{InMemory: true},
			{InMemory: true},
		},
	}
	perNode[1] = perNode[0]

	args := base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual, // saves time in this test
		ServerArgsPerNode: perNode,
	}

	numNodes := len(perNode)

	s := serverutils.StartNewTestCluster(t, numNodes, args)
	defer s.Stopper().Stop(ctx)

	// Verify all stores are initialized.
	for i := 0; i < numNodes; i++ {
		testutils.SucceedsSoon(t, func() error {
			exp := len(perNode[i].StoreSpecs)
			sc := s.Server(i).GetStores().(*kvserver.Stores).GetStoreCount()
			if sc != exp {
				return errors.Errorf("%d: saw only %d out of %d stores", i, sc, exp)
			}
			return nil
		})
	}

	// Verify node1 sees node2 via gossip and vice versa.
	node1Key := gossip.MakeNodeIDKey(s.Server(0).NodeID())
	node2Key := gossip.MakeNodeIDKey(s.Server(1).NodeID())
	server1Addr := s.Server(0).ServingRPCAddr()
	server2Addr := s.Server(1).ServingRPCAddr()
	testutils.SucceedsSoon(t, func() error {
		var nodeDesc1 roachpb.NodeDescriptor
		if err := s.Server(0).GossipI().(*gossip.Gossip).GetInfoProto(node2Key, &nodeDesc1); err != nil {
			return err
		}
		if addr2Str, server2AddrStr := nodeDesc1.Address.String(), server2Addr; addr2Str != server2AddrStr {
			return errors.Errorf("addr2 gossip %s doesn't match addr2 address %s", addr2Str, server2AddrStr)
		}
		var nodeDesc2 roachpb.NodeDescriptor
		if err := s.Server(1).GossipI().(*gossip.Gossip).GetInfoProto(node1Key, &nodeDesc2); err != nil {
			return err
		}
		if addr1Str, server1AddrStr := nodeDesc2.Address.String(), server1Addr; addr1Str != server1AddrStr {
			return errors.Errorf("addr1 gossip %s doesn't match addr1 address %s", addr1Str, server1AddrStr)
		}
		return nil
	})
}

// TestCorruptedClusterID verifies that a node fails to start when a
// store's cluster ID is empty.
func TestCorruptedClusterID(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	e := storage.NewDefaultInMemForTesting()
	defer e.Close()

	cv := clusterversion.TestingClusterVersion
	require.NoError(t, kvserver.WriteClusterVersion(ctx, e, cv))

	initCfg := initServerCfg{
		binaryMinSupportedVersion: clusterversion.TestingBinaryMinSupportedVersion,
		binaryVersion:             clusterversion.TestingBinaryVersion,
		defaultSystemZoneConfig:   *zonepb.DefaultZoneConfigRef(),
		defaultZoneConfig:         *zonepb.DefaultSystemZoneConfigRef(),
	}
	if _, err := bootstrapCluster(ctx, []storage.Engine{e}, initCfg); err != nil {
		t.Fatal(err)
	}

	// Set the cluster ID to the empty UUID.
	sIdent := roachpb.StoreIdent{
		ClusterID: uuid.UUID{},
		NodeID:    1,
		StoreID:   1,
	}
	if err := storage.MVCCPutProto(
		ctx, e, nil /* ms */, keys.StoreIdentKey(), hlc.Timestamp{}, nil /* txn */, &sIdent,
	); err != nil {
		t.Fatal(err)
	}

	_, err := inspectEngines(ctx, []storage.Engine{e}, cv.Version, cv.Version)
	if !testutils.IsError(err, `partially initialized`) {
		t.Fatal(err)
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
	if err := ts.db.GetProto(context.Background(), nodeStatusKey, nodeStatus); err != nil {
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
	defer log.Scope(t).Close(t)

	// ========================================
	// Start test server and wait for full initialization.
	// ========================================
	srv, _, kvDB := serverutils.StartServer(t, base.TestServerArgs{
		DisableEventLog: true,
	})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.(*TestServer)
	ctx := context.Background()

	// Retrieve the first store from the Node.
	s, err := ts.node.stores.GetStore(roachpb.StoreID(1))
	if err != nil {
		t.Fatal(err)
	}

	s.WaitForInit()

	content := "junk"
	leftKey := "a"

	// Scan over all keys to "wake up" all replicas (force a lease holder election).
	if _, err := kvDB.Scan(context.Background(), keys.MetaMax, keys.MaxKey, 0); err != nil {
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
	if err := ts.node.stores.VisitStores(func(s *kvserver.Store) error {
		desc, err := s.Descriptor(ctx, false /* useCached */)
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
	if err := ts.db.AdminSplit(context.Background(), splitKey, hlc.MaxTimestamp /* expirationTime */); err != nil {
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	testLocalityWithNewNode := func(locality roachpb.Locality) {
		args := base.TestServerArgs{
			Locality: locality,
		}
		s, _, _ := serverutils.StartServer(t, args)
		defer s.Stopper().Stop(ctx)

		// Check that the locality is present both on the Node and was also
		// handed to each StoreDescriptor.

		desc := s.Node().(*Node).Descriptor
		if !reflect.DeepEqual(desc.Locality, locality) {
			t.Fatalf("expected node locality to be %s, but it was %s", locality, desc.Locality)
		}

		if err := s.GetStores().(*kvserver.Stores).VisitStores(func(store *kvserver.Store) error {
			desc, err := store.Descriptor(ctx, false /* useCached */)
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
	defer log.Scope(t).Close(t)

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
