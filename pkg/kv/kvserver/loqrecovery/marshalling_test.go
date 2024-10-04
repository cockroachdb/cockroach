// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package loqrecovery

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestJsonSerialization verifies that all fields serialized in JSON could be
// read back. This specific test addresses issues where default naming scheme
// may not work in combination with other tags correctly. e.g. repeated used
// with omitempty seem to use camelcase unless explicitly specified.
func TestJsonSerialization(t *testing.T) {
	defer leaktest.AfterTest(t)()

	newVersion := clusterversion.ByKey(clusterversion.BinaryVersionKey)

	rs := []loqrecoverypb.ReplicaInfo{
		{
			NodeID:  1,
			StoreID: 2,
			Desc: roachpb.RangeDescriptor{
				RangeID:  3,
				StartKey: roachpb.RKey(keys.MetaMin),
				EndKey:   roachpb.RKey(keys.MetaMax),
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						NodeID:    1,
						StoreID:   2,
						ReplicaID: 3,
						Type:      roachpb.VOTER_INCOMING,
					},
				},
				NextReplicaID: 4,
				Generation:    7,
			},
			RaftAppliedIndex:   13,
			RaftCommittedIndex: 19,
			RaftLogDescriptorChanges: []loqrecoverypb.DescriptorChangeInfo{
				{
					ChangeType: 1,
					Desc:       &roachpb.RangeDescriptor{},
					OtherDesc:  &roachpb.RangeDescriptor{},
				},
			},
			LocalAssumesLeaseholder: true,
		},
	}

	cr := loqrecoverypb.ClusterReplicaInfo{
		ClusterID: "id1",
		Version:   newVersion,
		LocalInfo: []loqrecoverypb.NodeReplicaInfo{
			{
				Replicas: rs,
			},
		},
		Descriptors: []roachpb.RangeDescriptor{
			{
				RangeID:  1,
				StartKey: roachpb.RKey(keys.MetaMin),
				EndKey:   roachpb.RKey(keys.MetaMax),
				InternalReplicas: []roachpb.ReplicaDescriptor{
					{
						NodeID:    1,
						StoreID:   1,
						ReplicaID: 3,
						Type:      1,
					},
				},
				NextReplicaID: 5,
				Generation:    4,
				StickyBit:     hlc.Timestamp{},
			},
		},
	}

	lcr := loqrecoverypb.ClusterReplicaInfo{
		ClusterID: "id1",
		Version:   legacyInfoFormatVersion,
		LocalInfo: []loqrecoverypb.NodeReplicaInfo{
			{
				Replicas: rs,
			},
		},
	}

	rup := []loqrecoverypb.ReplicaUpdate{
		{
			RangeID:      53,
			StartKey:     loqrecoverypb.RecoveryKey(keys.MetaMin),
			OldReplicaID: 7,
			NewReplica: roachpb.ReplicaDescriptor{
				NodeID:    1,
				StoreID:   1,
				ReplicaID: 17,
				Type:      0,
			},
			NextReplicaID: 18,
		},
	}

	pl := loqrecoverypb.ReplicaUpdatePlan{
		Updates:                 rup,
		PlanID:                  uuid.FromStringOrNil("00000001-0000-4000-8000-000000000000"),
		DecommissionedNodeIDs:   []roachpb.NodeID{4, 5},
		ClusterID:               "abc",
		StaleLeaseholderNodeIDs: []roachpb.NodeID{3},
		Version:                 newVersion,
	}

	lpl := loqrecoverypb.ReplicaUpdatePlan{
		Updates: rup,
	}

	t.Run("cluster replica info", func(t *testing.T) {
		out, err := MarshalReplicaInfo(cr)
		require.NoError(t, err, "failed to marshal replica info")
		ucr, err := UnmarshalReplicaInfo(out)
		require.NoError(t, err, "failed to unmarshal replica info")
		require.Equal(t, cr, ucr, "replica info before and after serialization")
	})

	t.Run("cluster replica info with v1 format", func(t *testing.T) {
		out, err := MarshalReplicaInfo(lcr)
		require.NoError(t, err, "failed to marshal legacy replica info")
		ucr, err := UnmarshalReplicaInfo(out)
		require.NoError(t, err, "failed to unmarshal replica info")
		require.Equal(t, ucr.Version, legacyInfoFormatVersion, "legacy format should have generated version")
		require.Empty(t, ucr.ClusterID, "legacy format should have no cluster id")
		require.Nil(t, ucr.Descriptors, "legacy format should have no descriptors")
		require.Equal(t, ucr.LocalInfo, ucr.LocalInfo, "replica info")

		// For collected replica info we want to check if raw object could be loaded
		// with a legacy loader in previous versions.
		jsonpb := protoutil.JSONPb{}
		var nr loqrecoverypb.NodeReplicaInfo
		require.NoError(t, jsonpb.Unmarshal(out, &nr), "failed to unmarshal with legacy unmashaler")
		require.Equal(t, lcr.LocalInfo[0], nr, "replica info before and after serialization")
	})

	t.Run("update plan", func(t *testing.T) {
		out, err := MarshalPlan(pl)
		require.NoError(t, err, "failed to marshal plan")
		upl, err := UnmarshalPlan(out)
		require.NoError(t, err, "failed to unmarshal plan")
		require.Equal(t, pl, upl, "plan before and after serialization")
	})

	t.Run("update plan with old version", func(t *testing.T) {
		out, err := MarshalPlan(lpl)
		require.NoError(t, err, "failed to marshal plan")
		upl, err := UnmarshalPlan(out)
		require.NoError(t, err, "failed to unmarshal plan")
		require.Equal(t, lpl, upl, "plan before and after serialization")
		require.Contains(t, string(out), "updates", "legacy plan format uses snake naming")
	})
}
