// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package loqrecovery

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestQuorumRecovery(t *testing.T) {
	defer leaktest.AfterTest(t)()

	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		env := quorumRecoveryEnv{}
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

	stores := func(storeIDs ...int) (result []roachpb.StoreID) {
		for _, storeID := range storeIDs {
			result = append(result, roachpb.StoreID(storeID))
		}
		return
	}

	for _, td := range []struct {
		name string
		// Test data.
		replicas   []loqrecoverypb.ReplicaInfo
		deadStores []roachpb.StoreID
		// Outcomes.
		liveStores      storeIDSet
		missingStoreIDs storeIDSet
		err             bool
	}{
		{name: "no dead stores", replicas: cluster(node1, node2, node3), deadStores: nil, liveStores: storeSet(1, 2, 3), missingStoreIDs: storeSet()},
		{name: "no explicit dead stores", replicas: cluster(node1, node2), deadStores: nil, liveStores: storeSet(1, 2), missingStoreIDs: storeSet(3)},
		{name: "explicit dead stores match replicas", replicas: cluster(node2, node3), deadStores: stores(1), liveStores: storeSet(2, 3), missingStoreIDs: storeSet(1)},
		{name: "explicit dead stores with replica info", replicas: cluster(node1, node2), deadStores: stores(1), err: true},
		{name: "explicit dead stores mismatch", replicas: cluster(node1), deadStores: stores(2), err: true},
	} {
		t.Run(td.name, func(t *testing.T) {
			liveStoreIDs, missingStoreIDs, err := validateReplicaSets(td.replicas, td.deadStores)
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
