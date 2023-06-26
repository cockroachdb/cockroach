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
	"runtime/pprof"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
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
	require.NoError(t, kvstorage.WriteClusterVersion(ctx, e, clusterversion.TestingClusterVersion))

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
	require.NoError(t, kvstorage.WriteClusterVersion(ctx, e, cv))

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
		ctx, e, nil /* ms */, keys.StoreIdentKey(), hlc.Timestamp{}, hlc.ClockTimestamp{}, nil /* txn */, &sIdent,
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
	initialRanges, err := ExpectedInitialRangeCount(keys.SystemSQLCodec, &ts.cfg.DefaultZoneConfig, &ts.cfg.DefaultSystemZoneConfig)
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
		if err := ts.node.computeMetricsPeriodically(ctx, map[*kvserver.Store]*storage.MetricsForInterval{}, 0); err != nil {
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
	if err := ts.db.AdminSplit(
		context.Background(),
		splitKey,
		hlc.MaxTimestamp, /* expirationTime */
	); err != nil {
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

	ba := kvpb.BatchRequest{
		Requests: make([]kvpb.RequestUnion, 1),
	}
	n := &Node{}
	br, err := n.batchInternal(context.Background(), roachpb.SystemTenantID, &ba)
	if err != nil {
		t.Fatal(err)
	}
	if br.Error == nil {
		t.Fatal("no batch error returned")
	}
	if _, ok := br.Error.GetDetail().(*kvpb.UnsupportedRequestError); !ok {
		t.Fatalf("expected unsupported request, not %v", br.Error)
	}
}

// TestNodeBatchRequestPProfLabels tests that node.Batch copies pprof labels
// from the BatchRequest and applies them to the root context if CPU profiling
// with labels is enabled.
func TestNodeBatchRequestPProfLabels(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	observedProfileLabels := make(map[string]string)
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				TestingResponseFilter: func(ctx context.Context, ba *kvpb.BatchRequest, _ *kvpb.BatchResponse) *kvpb.Error {
					var foundBatch bool
					for _, ru := range ba.Requests {
						switch r := ru.GetInner().(type) {
						case *kvpb.PutRequest:
							if r.Header().Key.Equal(roachpb.Key("a")) {
								foundBatch = true
							}
						}
					}
					if foundBatch {
						pprof.ForLabels(ctx, func(key, value string) bool {
							observedProfileLabels[key] = value
							return true
						})
					}
					return nil
				},
			},
		},
	})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.(*TestServer)
	n := ts.GetNode()

	var ba kvpb.BatchRequest
	ba.RangeID = 1
	ba.Replica.StoreID = 1
	expectedProfileLabels := map[string]string{"key": "value", "key2": "value2"}
	ba.ProfileLabels = func() []string {
		var labels []string
		for k, v := range expectedProfileLabels {
			labels = append(labels, k, v)
		}
		return labels
	}()

	gr := kvpb.NewGet(roachpb.Key("a"), false)
	pr := kvpb.NewPut(gr.Header().Key, roachpb.Value{})
	ba.Add(gr, pr)

	// If CPU profiling with labels is not enabled, we should not observe any
	// pprof labels on the context.
	ctx := context.Background()
	_, _ = n.Batch(ctx, &ba)
	require.Equal(t, map[string]string{}, observedProfileLabels)

	require.NoError(t, ts.ClusterSettings().SetCPUProfiling(cluster.CPUProfileWithLabels))
	_, _ = n.Batch(ctx, &ba)

	require.Len(t, observedProfileLabels, 3)
	// Delete the labels for the range_str.
	delete(observedProfileLabels, "range_str")
	require.Equal(t, expectedProfileLabels, observedProfileLabels)
}

func TestNodeBatchRequestMetricsInc(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(context.Background())
	ts := srv.(*TestServer)

	n := ts.GetNode()
	bCurr := n.metrics.BatchCount.Count()
	getCurr := n.metrics.MethodCounts[kvpb.Get].Count()
	putCurr := n.metrics.MethodCounts[kvpb.Put].Count()

	var ba kvpb.BatchRequest
	ba.RangeID = 1
	ba.Replica.StoreID = 1

	gr := kvpb.NewGet(roachpb.Key("a"), false)
	pr := kvpb.NewPut(gr.Header().Key, roachpb.Value{})
	ba.Add(gr, pr)

	_, _ = n.Batch(context.Background(), &ba)
	bCurr++
	getCurr++
	putCurr++

	require.GreaterOrEqual(t, n.metrics.BatchCount.Count(), bCurr)
	require.GreaterOrEqual(t, n.metrics.MethodCounts[kvpb.Get].Count(), getCurr)
	require.GreaterOrEqual(t, n.metrics.MethodCounts[kvpb.Put].Count(), putCurr)
}

// TestNodeCrossLocalityMetrics verifies that
// updateCrossLocalityMetricsOnBatch{Request|Response} correctly updates
// cross-region, cross-zone byte count metrics for batch requests sent and batch
// responses received.
func TestNodeCrossLocalityMetrics(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	const expectedInc = 10

	metricsNames := []string{
		"batch_requests.bytes",
		"batch_requests.cross_region.bytes",
		"batch_requests.cross_zone.bytes",
		"batch_responses.bytes",
		"batch_responses.cross_region.bytes",
		"batch_responses.cross_zone.bytes"}
	for _, tc := range []struct {
		crossLocalityType    roachpb.LocalityComparisonType
		expectedMetricChange [6]int64
		forRequest           bool
	}{
		{crossLocalityType: roachpb.LocalityComparisonType_CROSS_REGION,
			expectedMetricChange: [6]int64{expectedInc, expectedInc, 0, 0, 0, 0},
			forRequest:           true,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE,
			expectedMetricChange: [6]int64{expectedInc, 0, expectedInc, 0, 0, 0},
			forRequest:           true,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE,
			expectedMetricChange: [6]int64{expectedInc, 0, 0, 0, 0, 0},
			forRequest:           true,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_CROSS_REGION,
			expectedMetricChange: [6]int64{0, 0, 0, expectedInc, expectedInc, 0},
			forRequest:           false,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_CROSS_ZONE,
			expectedMetricChange: [6]int64{0, 0, 0, expectedInc, 0, expectedInc},
			forRequest:           false,
		},
		{crossLocalityType: roachpb.LocalityComparisonType_SAME_REGION_SAME_ZONE,
			expectedMetricChange: [6]int64{0, 0, 0, expectedInc, 0, 0},
			forRequest:           false,
		},
	} {
		t.Run(fmt.Sprintf("%-v", tc.crossLocalityType), func(t *testing.T) {
			metrics := makeNodeMetrics(metric.NewRegistry(), 1)
			beforeMetrics, err := metrics.getNodeCounterMetrics(metricsNames)
			if err != nil {
				t.Fatal(err)
			}
			if tc.forRequest {
				metrics.updateCrossLocalityMetricsOnBatchRequest(tc.crossLocalityType, expectedInc)
			} else {
				metrics.updateCrossLocalityMetricsOnBatchResponse(tc.crossLocalityType, expectedInc)
			}

			afterMetrics, err := metrics.getNodeCounterMetrics(metricsNames)
			if err != nil {
				t.Fatal(err)
			}
			metricsDiff := getMapsDiff(beforeMetrics, afterMetrics)
			expectedDiff := make(map[string]int64, 6)
			for i, inc := range tc.expectedMetricChange {
				expectedDiff[metricsNames[i]] = inc
			}
			require.Equal(t, metricsDiff, expectedDiff)
		})
	}
}

func TestGetTenantWeights(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	specs := []base.StoreSpec{
		{InMemory: true},
		{InMemory: true},
	}
	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		StoreSpecs: specs,
	})
	defer s.Stopper().Stop(ctx)
	// Wait until both stores are started properly.
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
	// At this point, all ranges have the SystemTenantID. Create a split using
	// another tenant, which will cause that tenant to have a weight of 1 in the
	// relevant store(s).
	const otherTenantID = 5
	prefix := keys.MakeTenantPrefix(roachpb.MustMakeTenantID(otherTenantID))
	require.NoError(t, s.DB().AdminSplit(
		ctx,
		prefix,           /* splitKey */
		hlc.MaxTimestamp, /* expirationTime */
	))
	// The range can have replicas on multiple stores, so wait for the split to
	// be applied everywhere.
	stores := s.GetStores().(*kvserver.Stores)
	testutils.SucceedsSoon(t, func() error {
		return stores.VisitStores(func(s *kvserver.Store) error {
			r := s.LookupReplica(roachpb.RKey(prefix))
			if r != nil && !r.Desc().StartKey.Equal(prefix) {
				return errors.Errorf("waiting for split")
			}
			return nil
		})
	})
	// Unfortunately, the non-determinism of replica distribution can make this
	// test more complicated than the code it is trying to test, if we were to
	// validate exact counts. So we do some simple validation instead.
	weights := s.Node().(*Node).GetTenantWeights()
	// Both tenants have overall non-zero counts.
	require.Less(t, uint32(0), weights.Node[roachpb.SystemTenantID.ToUint64()])
	require.Less(t, uint32(0), weights.Node[otherTenantID])
	// There are two stores.
	require.Equal(t, 2, len(weights.Stores))
	// The sum of the values in the stores is equal to the node-level value.
	checkSum := func(tenantID uint64) {
		require.Equal(t, weights.Node[tenantID], weights.Stores[0].Weights[tenantID]+
			weights.Stores[1].Weights[tenantID])
	}
	checkSum(roachpb.SystemTenantID.ToUint64())
	checkSum(otherTenantID)
}

func TestDiskStatsMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Specs for two stores, one of which overrides the cluster-level
	// provisioned bandwidth.
	specs := []base.StoreSpec{
		{
			ProvisionedRateSpec: base.ProvisionedRateSpec{
				DiskName: "foo",
				// ProvisionedBandwidth is 0 so the cluster setting will be used.
				ProvisionedBandwidth: 0,
			},
		},
		{
			ProvisionedRateSpec: base.ProvisionedRateSpec{
				DiskName:             "bar",
				ProvisionedBandwidth: 200,
			},
		},
	}
	// Engines.
	engines := []storage.Engine{
		storage.NewDefaultInMemForTesting(),
		storage.NewDefaultInMemForTesting(),
	}
	defer func() {
		for i := range engines {
			engines[i].Close()
		}
	}()
	// "foo" has store-id 10, "bar" has store-id 5.
	engineIDs := []roachpb.StoreID{10, 5}
	for i := range engines {
		ident := roachpb.StoreIdent{StoreID: engineIDs[i]}
		require.NoError(t, storage.MVCCBlindPutProto(ctx, engines[i], nil, keys.StoreIdentKey(),
			hlc.Timestamp{}, hlc.ClockTimestamp{}, &ident, nil))
	}
	var dsm diskStatsMap
	clusterProvisionedBW := int64(150)

	// diskStatsMap contains nothing, so does not populate anything.
	stats, err := dsm.tryPopulateAdmissionDiskStats(ctx, clusterProvisionedBW, nil)
	require.NoError(t, err)
	require.Equal(t, 0, len(stats))

	// diskStatsMap initialized with these two stores.
	require.NoError(t, dsm.initDiskStatsMap(specs, engines))

	// diskStatsFunc returns stats for these two stores, and an unknown store.
	diskStatsFunc := func(context.Context) ([]status.DiskStats, error) {
		return []status.DiskStats{
			{
				Name:       "baz",
				ReadBytes:  100,
				WriteBytes: 200,
			},
			{
				Name:       "foo",
				ReadBytes:  500,
				WriteBytes: 1000,
			},
			{
				Name:       "bar",
				ReadBytes:  2000,
				WriteBytes: 2500,
			},
		}, nil
	}
	stats, err = dsm.tryPopulateAdmissionDiskStats(ctx, clusterProvisionedBW, diskStatsFunc)
	require.NoError(t, err)
	// The stats for the two stores are as expected.
	require.Equal(t, 2, len(stats))
	for i := range engineIDs {
		ds, ok := stats[engineIDs[i]]
		require.True(t, ok)
		var expectedDS admission.DiskStats
		switch engineIDs[i] {
		// "foo"
		case 10:
			expectedDS = admission.DiskStats{
				BytesRead: 500, BytesWritten: 1000, ProvisionedBandwidth: clusterProvisionedBW}
		// "bar"
		case 5:
			expectedDS = admission.DiskStats{
				BytesRead: 2000, BytesWritten: 2500, ProvisionedBandwidth: 200}
		}
		require.Equal(t, expectedDS, ds)
	}

	// disk stats are only retrieved for "foo".
	diskStatsFunc = func(context.Context) ([]status.DiskStats, error) {
		return []status.DiskStats{
			{
				Name:       "foo",
				ReadBytes:  3500,
				WriteBytes: 4500,
			},
		}, nil
	}
	stats, err = dsm.tryPopulateAdmissionDiskStats(ctx, clusterProvisionedBW, diskStatsFunc)
	require.NoError(t, err)
	require.Equal(t, 2, len(stats))
	for i := range engineIDs {
		ds, ok := stats[engineIDs[i]]
		require.True(t, ok)
		var expectedDS admission.DiskStats
		switch engineIDs[i] {
		// "foo"
		case 10:
			expectedDS = admission.DiskStats{
				BytesRead: 3500, BytesWritten: 4500, ProvisionedBandwidth: clusterProvisionedBW}
		// "bar". The read and write bytes are 0.
		case 5:
			expectedDS = admission.DiskStats{
				BytesRead: 0, BytesWritten: 0, ProvisionedBandwidth: 200}
		}
		require.Equal(t, expectedDS, ds)
	}
}
