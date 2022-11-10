// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package slinstance

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestInstance_SetRegionalData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	settings := cluster.MakeTestingClusterSettingsWithVersions(
		clusterversion.TestingBinaryVersion,
		clusterversion.TestingBinaryMinSupportedVersion,
		true, /* initializeVersion */
	)

	buildTypeDesc := func(d *descpb.TypeDescriptor) catalog.TypeDescriptor {
		desc := typedesc.NewBuilder(d).BuildImmutable()
		res, _ := desc.(catalog.TypeDescriptor)
		return res
	}

	t.Run("nil type descriptor", func(t *testing.T) {
		sqlInstance := NewSQLInstance(nil, nil, nil, settings, nil, nil)
		require.NoError(t, sqlInstance.SetRegionalData(roachpb.Locality{}, nil))
		require.Empty(t, sqlInstance.currentRegion)
	})

	t.Run("invalid type descriptor", func(t *testing.T) {
		sqlInstance := NewSQLInstance(nil, nil, nil, settings, nil, nil)
		nonMrDesc := buildTypeDesc(&descpb.TypeDescriptor{Kind: descpb.TypeDescriptor_ENUM})
		require.Error(t, sqlInstance.SetRegionalData(roachpb.Locality{}, nonMrDesc))
		require.Empty(t, sqlInstance.currentRegion)
	})

	t.Run("no region locality", func(t *testing.T) {
		sqlInstance := NewSQLInstance(nil, nil, nil, settings, nil, nil)
		mrDesc := buildTypeDesc(&descpb.TypeDescriptor{
			Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
			RegionConfig: &descpb.TypeDescriptor_RegionConfig{
				PrimaryRegion: "us-east2",
			},
			EnumMembers: []descpb.TypeDescriptor_EnumMember{
				{
					LogicalRepresentation:  "us-east1",
					PhysicalRepresentation: []byte{1},
				},
				{
					LogicalRepresentation:  "us-east2",
					PhysicalRepresentation: []byte{2},
				},
			},
		})
		require.NoError(t, sqlInstance.SetRegionalData(roachpb.Locality{}, mrDesc))
		require.Equal(t, []byte{2}, sqlInstance.currentRegion)
	})

	t.Run("no ready regions", func(t *testing.T) {
		sqlInstance := NewSQLInstance(nil, nil, nil, settings, nil, nil)
		mrDesc := buildTypeDesc(&descpb.TypeDescriptor{
			Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
			RegionConfig: &descpb.TypeDescriptor_RegionConfig{
				PrimaryRegion: "us-east1",
			},
			EnumMembers: []descpb.TypeDescriptor_EnumMember{
				{
					LogicalRepresentation:  "us-east1",
					PhysicalRepresentation: []byte{1},
					Capability:             descpb.TypeDescriptor_EnumMember_READ_ONLY,
				},
			},
		})
		require.NoError(t, sqlInstance.SetRegionalData(roachpb.Locality{}, mrDesc))
		require.Empty(t, sqlInstance.currentRegion)
	})

	t.Run("region found", func(t *testing.T) {
		sqlInstance := NewSQLInstance(nil, nil, nil, settings, nil, nil)
		mrDesc := buildTypeDesc(&descpb.TypeDescriptor{
			Kind: descpb.TypeDescriptor_MULTIREGION_ENUM,
			RegionConfig: &descpb.TypeDescriptor_RegionConfig{
				PrimaryRegion: "us-east2",
			},
			EnumMembers: []descpb.TypeDescriptor_EnumMember{
				{
					LogicalRepresentation:  "us-east1",
					PhysicalRepresentation: []byte{1},
					Capability:             descpb.TypeDescriptor_EnumMember_READ_ONLY,
				},
				{
					LogicalRepresentation:  "us-east2",
					PhysicalRepresentation: []byte{2},
				},
				{
					LogicalRepresentation:  "us-east3",
					PhysicalRepresentation: []byte{3},
				},
			},
		})
		require.NoError(t, sqlInstance.SetRegionalData(roachpb.Locality{Tiers: []roachpb.Tier{
			{Key: "region", Value: "us-east3"},
		}}, mrDesc))
		require.Equal(t, []byte{3}, sqlInstance.currentRegion)
	})
}
