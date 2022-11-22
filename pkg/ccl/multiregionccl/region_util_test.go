// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package multiregionccl_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/multiregionccl/multiregionccltestutils"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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

	s0 := tc.ServerTyped(0)
	ief := s0.InternalExecutorFactory().(descs.TxnManager)
	dbID := sqlutils.QueryDatabaseID(t, sqlDB, "foo")

	t.Run("with locality that exists", func(t *testing.T) {
		regionEnum, err := sql.GetLocalityRegionEnumPhysicalRepresentation(
			ctx, ief, s0.DB(), descpb.ID(dbID), roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: "us-east2"}},
			},
		)
		require.NoError(t, err)

		enumMembers := getEnumMembers(t, ctx, tc.Server(0), descpb.ID(dbID))
		require.NotEmpty(t, enumMembers)
		require.Equal(t, enumMembers["us-east2"], regionEnum)
	})

	t.Run("with non-existent locality", func(t *testing.T) {
		regionEnum, err := sql.GetLocalityRegionEnumPhysicalRepresentation(
			ctx, ief, s0.DB(), descpb.ID(dbID), roachpb.Locality{
				Tiers: []roachpb.Tier{{Key: "region", Value: "europe-west1"}},
			},
		)
		require.NoError(t, err)

		// Fallback to primary region if the locality is provided, but non-existent.
		enumMembers := getEnumMembers(t, ctx, tc.Server(0), descpb.ID(dbID))
		require.NotEmpty(t, enumMembers)
		require.Equal(t, enumMembers["us-east1"], regionEnum)
	})

	t.Run("without locality", func(t *testing.T) {
		regionEnum, err := sql.GetLocalityRegionEnumPhysicalRepresentation(
			ctx, ief, s0.DB(), descpb.ID(dbID), roachpb.Locality{})
		require.NoError(t, err)

		// Fallback to primary region is locality information is missing.
		enumMembers := getEnumMembers(t, ctx, tc.Server(0), descpb.ID(dbID))
		require.NotEmpty(t, enumMembers)
		require.Equal(t, enumMembers["us-east1"], regionEnum)
	})
}

func getEnumMembers(
	t *testing.T, ctx context.Context, ts serverutils.TestServerInterface, dbID descpb.ID,
) map[string][]byte {
	t.Helper()
	enumMembers := make(map[string][]byte)
	err := sql.TestingDescsTxn(ctx, ts, func(ctx context.Context, txn *kv.Txn, descsCol *descs.Collection) error {
		_, dbDesc, err := descsCol.GetImmutableDatabaseByID(ctx, txn, dbID,
			tree.DatabaseLookupFlags{Required: true})
		require.NoError(t, err)
		regionEnumID, err := dbDesc.MultiRegionEnumID()
		require.NoError(t, err)
		regionEnumDesc, err := descsCol.GetImmutableTypeByID(ctx, txn, regionEnumID,
			tree.ObjectLookupFlags{CommonLookupFlags: tree.CommonLookupFlags{Required: true}})
		require.NoError(t, err)
		for ord := 0; ord < regionEnumDesc.NumEnumMembers(); ord++ {
			enumMembers[regionEnumDesc.GetMemberLogicalRepresentation(ord)] = regionEnumDesc.GetMemberPhysicalRepresentation(ord)
		}
		return nil
	})
	require.NoError(t, err)
	return enumMembers
}
