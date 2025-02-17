// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multiregionccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestGetLocalityRegionEnumPhysicalRepresentation is in the ccl package since
// it utilizes adding regions to a database.
func TestGetLocalityRegionEnumPhysicalRepresentation(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{})
	defer cleanup()

	tDB := sqlutils.MakeSQLRunner(sqlDB)
	tDB.Exec(t, `CREATE DATABASE foo PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3"`)

	s0 := tc.Server(0)
	idb := s0.InternalDB().(descs.DB)
	dbID := descpb.ID(sqlutils.QueryDatabaseID(t, sqlDB, "foo"))

	t.Run("with locality that exists", func(t *testing.T) {
		regionEnum, err := sql.GetLocalityRegionEnumPhysicalRepresentation(
			ctx, idb, dbID, roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: "us-east2"}},
			},
		)
		require.NoError(t, err)

		enumMembers := getEnumMembers(t, ctx, tc.Server(0), dbID)
		require.NotEmpty(t, enumMembers)
		require.Equal(t, enumMembers["us-east2"], regionEnum)
	})

	t.Run("with non-existent locality", func(t *testing.T) {
		regionEnum, err := sql.GetLocalityRegionEnumPhysicalRepresentation(
			ctx, idb, dbID, roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}},
			},
		)
		require.NoError(t, err)

		// Fallback to primary region if the locality is provided, but non-existent.
		enumMembers := getEnumMembers(t, ctx, tc.Server(0), dbID)
		require.NotEmpty(t, enumMembers)
		require.Equal(t, enumMembers["us-east1"], regionEnum)
	})

	t.Run("without locality", func(t *testing.T) {
		regionEnum, err := sql.GetLocalityRegionEnumPhysicalRepresentation(
			ctx, idb, dbID, roachpb.Locality{})
		require.NoError(t, err)

		// Fallback to primary region is locality information is missing.
		enumMembers := getEnumMembers(t, ctx, tc.Server(0), dbID)
		require.NotEmpty(t, enumMembers)
		require.Equal(t, enumMembers["us-east1"], regionEnum)
	})
}

func TestGetRegionEnumRepresentations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	tc, sqlDB, cleanup := multiregionccltestutils.TestingCreateMultiRegionCluster(
		t, 3 /* numServers */, base.TestingKnobs{})
	defer cleanup()

	tDB := sqlutils.MakeSQLRunner(sqlDB)
	tDB.Exec(t, `CREATE DATABASE foo PRIMARY REGION "us-east1" REGIONS "us-east1", "us-east2", "us-east3"`)

	dbID := descpb.ID(sqlutils.QueryDatabaseID(t, sqlDB, "foo"))
	err := sqltestutils.TestingDescsTxn(ctx, tc.Server(0), func(
		ctx context.Context, txn isql.Txn, col *descs.Collection,
	) error {
		enumReps, primaryRegion, err := sql.GetRegionEnumRepresentations(ctx, txn.KV(), dbID, col)
		require.NoError(t, err)

		require.Equal(t, catpb.RegionName("us-east1"), primaryRegion)
		require.Len(t, enumReps, 3)

		expEnumReps := getEnumMembers(t, ctx, tc.Server(0), dbID)
		require.Equal(t, len(expEnumReps), len(enumReps))

		for r, rep := range expEnumReps {
			res, ok := enumReps[catpb.RegionName(r)]
			require.True(t, ok)
			require.Equal(t, rep, res)
		}
		return nil
	})
	require.NoError(t, err)
}

func getEnumMembers(
	t *testing.T, ctx context.Context, ts serverutils.TestServerInterface, dbID descpb.ID,
) map[string][]byte {
	t.Helper()
	enumMembers := make(map[string][]byte)
	err := sqltestutils.TestingDescsTxn(ctx, ts, func(ctx context.Context, txn isql.Txn, descsCol *descs.Collection) error {
		dbDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Database(ctx, dbID)
		require.NoError(t, err)
		regionEnumID, err := dbDesc.MultiRegionEnumID()
		require.NoError(t, err)
		typeDesc, err := descsCol.ByIDWithLeased(txn.KV()).WithoutNonPublic().Get().Type(ctx, regionEnumID)
		require.NoError(t, err)
		regionEnumDesc := typeDesc.AsRegionEnumTypeDescriptor()
		require.NotNil(t, regionEnumDesc)
		for ord := 0; ord < regionEnumDesc.NumEnumMembers(); ord++ {
			enumMembers[regionEnumDesc.GetMemberLogicalRepresentation(ord)] = regionEnumDesc.GetMemberPhysicalRepresentation(ord)
		}
		return nil
	})
	require.NoError(t, err)
	return enumMembers
}
