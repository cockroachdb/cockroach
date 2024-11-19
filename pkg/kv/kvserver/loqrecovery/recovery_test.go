// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestQuorumRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		testTime, _ := time.Parse(time.RFC3339, "2022-02-24T01:40:00Z")
		env := quorumRecoveryEnv{
			uuidGen: NewSeqGen(1),
			clock:   timeutil.NewManualTime(testTime),
		}
		defer env.cleanupStores()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return env.Handle(t, *d)
		})
	})
}

func TestValidatePlanReplicaSet(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := func(storeID int, replicaStores ...int) (result loqrecoverypb.ReplicaInfo) {
		result.StoreID = roachpb.StoreID(storeID)
		result.NodeID = roachpb.NodeID(storeID)
		var replicas []roachpb.ReplicaDescriptor
		for i, rs := range replicaStores {
			replicas = append(replicas, roachpb.ReplicaDescriptor{
				NodeID: roachpb.NodeID(rs), StoreID: roachpb.StoreID(rs), ReplicaID: roachpb.ReplicaID(i)})
		}
		result.Desc = roachpb.RangeDescriptor{InternalReplicas: replicas}
		return
	}

	node1 := []loqrecoverypb.ReplicaInfo{desc(1, 1, 2, 3), desc(1, 3, 2, 1)}
	node2 := []loqrecoverypb.ReplicaInfo{desc(2, 1, 2, 3), desc(2, 2, 3, 1)}
	node3 := []loqrecoverypb.ReplicaInfo{desc(3, 3, 2, 1), desc(3, 2, 3, 1)}

	cluster := func(nodes ...[]loqrecoverypb.ReplicaInfo) (result []loqrecoverypb.ReplicaInfo) {
		for _, node := range nodes {
			result = append(result, node...)
		}
		return
	}

	storeSet := func(storeIDs ...int) storeIDSet {
		result := make(storeIDSet)
		for _, storeID := range storeIDs {
			result[roachpb.StoreID(storeID)] = struct{}{}
		}
		return result
	}

	locations := func(nodeIDs ...int) locationsMap {
		result := make(locationsMap)
		for _, id := range nodeIDs {
			result[roachpb.NodeID(id)] = storeSet(id)
		}
		return result
	}

	stores := func(storeIDs ...int) (result []roachpb.StoreID) {
		for _, storeID := range storeIDs {
			result = append(result, roachpb.StoreID(storeID))
		}
		return
	}

	nodes := func(nodeIDs ...int) (result []roachpb.NodeID) {
		for _, nodeID := range nodeIDs {
			result = append(result, roachpb.NodeID(nodeID))
		}
		return
	}

	for _, td := range []struct {
		name string
		// Test data.
		replicas   []loqrecoverypb.ReplicaInfo
		deadStores []roachpb.StoreID
		deadNodes  []roachpb.NodeID
		// Outcomes.
		liveStores      storeIDSet
		missingStoreIDs locationsMap
		err             bool
	}{
		{name: "no dead stores", replicas: cluster(node1, node2, node3), deadStores: nil, liveStores: storeSet(1, 2, 3), missingStoreIDs: locations()},
		{name: "no explicit dead stores", replicas: cluster(node1, node2), deadStores: nil, liveStores: storeSet(1, 2), missingStoreIDs: locations(3)},
		{name: "explicit dead stores match replicas", replicas: cluster(node2, node3), deadStores: stores(1), liveStores: storeSet(2, 3), missingStoreIDs: locations(1)},
		{name: "explicit dead stores with replica info", replicas: cluster(node1, node2), deadStores: stores(1), err: true},
		{name: "explicit dead stores mismatch", replicas: cluster(node1), deadStores: stores(2), err: true},
		{name: "explicit dead nodes match replicas", replicas: cluster(node2, node3), deadNodes: nodes(1), liveStores: storeSet(2, 3), missingStoreIDs: locations(1)},
		{name: "explicit dead nodes with replica info", replicas: cluster(node1, node2), deadNodes: nodes(1), err: true},
		{name: "explicit dead nodes mismatch", replicas: cluster(node1), deadNodes: nodes(2), err: true},
	} {
		t.Run(td.name, func(t *testing.T) {
			liveStoreIDs, missingStoreIDs, err := validateReplicaSets(td.replicas, td.deadStores, td.deadNodes)
			if td.err {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, td.liveStores, liveStoreIDs)
				require.Equal(t, td.missingStoreIDs, missingStoreIDs)
			}
		})
	}
}
