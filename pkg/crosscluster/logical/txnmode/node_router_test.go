// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnmode

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/logical/ldrdecoder"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestNodeRouterRouting(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableSplitQueue: true,
				DisableMergeQueue: true,
			},
		},
	})
	defer srv.Stopper().Stop(ctx)
	s := srv.ApplicationLayer()

	runner := sqlutils.MakeSQLRunner(s.SQLConn(t))
	runner.Exec(t, "CREATE TABLE route_test (id INT PRIMARY KEY, val STRING)")
	runner.Exec(t, "ALTER TABLE route_test SPLIT AT VALUES (100), (200)")
	runner.Exec(t, "INSERT INTO route_test VALUES (50, 'a'), (150, 'b'), (250, 'c')")

	runner.Exec(t, "CREATE TABLE route_test2 (id INT PRIMARY KEY, val STRING)")
	runner.Exec(t, "ALTER TABLE route_test2 SPLIT AT VALUES (1)")

	// Collect expected range IDs for each split.
	rows := runner.QueryStr(t, "SELECT range_id FROM [SHOW RANGES FROM TABLE route_test] ORDER BY start_key")
	require.Len(t, rows, 3, "expected 3 ranges after splitting at 100 and 200")
	expectedRangeIDs := make([]roachpb.RangeID, 3)
	for i, row := range rows {
		id, err := strconv.Atoi(row[0])
		require.NoError(t, err)
		expectedRangeIDs[i] = roachpb.RangeID(id)
	}

	rows2 := runner.QueryStr(t, "SELECT range_id FROM [SHOW RANGES FROM TABLE route_test2] ORDER BY start_key")
	require.GreaterOrEqual(t, len(rows2), 2, "expected at least 2 ranges after splitting")
	table2RangeID, err := strconv.Atoi(rows2[1][0])
	require.NoError(t, err)

	var tableID descpb.ID
	runner.QueryRow(t, "SELECT 'route_test'::regclass::oid").Scan(&tableID)
	var table2ID descpb.ID
	runner.QueryRow(t, "SELECT 'route_test2'::regclass::oid").Scan(&table2ID)

	schema := execinfrapb.LDRSchema{
		TableMetadataByDestID: map[int32]descpb.TableDescriptor{
			int32(tableID):  {},
			int32(table2ID): {},
		},
	}

	codec := s.Codec()
	db := s.InternalDB().(descs.DB)
	rangeCache := srv.DistSenderI().(*kvcoord.DistSender).RangeDescriptorCache()
	rangeCache.Clear()

	router, err := newNodeRouter(ctx, codec, db, schema, rangeCache)
	require.NoError(t, err)

	tests := []struct {
		name            string
		tableID         descpb.ID
		rowID           int
		expectedRangeID roachpb.RangeID
	}{
		{name: "routes to range 1 (< 100)", tableID: tableID, rowID: 50, expectedRangeID: expectedRangeIDs[0]},
		{name: "routes to range 2 (100-200)", tableID: tableID, rowID: 150, expectedRangeID: expectedRangeIDs[1]},
		{name: "routes to range 3 (>= 200)", tableID: tableID, rowID: 250, expectedRangeID: expectedRangeIDs[2]},
		{name: "routes to second table", tableID: table2ID, rowID: 1, expectedRangeID: roachpb.RangeID(table2RangeID)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			writeSet := []ldrdecoder.DecodedRow{{
				TableID: tc.tableID,
				Row:     tree.Datums{tree.NewDInt(tree.DInt(tc.rowID)), tree.DNull},
			}}
			ri, err := router.lookupRange(ctx, writeSet)
			require.NoError(t, err)
			require.Equal(t, tc.expectedRangeID, ri.Desc.RangeID,
				fmt.Sprintf("row %d routed to range %d, expected %d",
					tc.rowID, ri.Desc.RangeID, tc.expectedRangeID))
		})
	}

	t.Run("missing table returns assertion error", func(t *testing.T) {
		writeSet := []ldrdecoder.DecodedRow{{
			TableID: descpb.ID(999),
			Row:     tree.Datums{tree.NewDInt(1)},
		}}
		_, err := router.lookupRange(ctx, writeSet)
		require.Error(t, err)
		require.True(t, errors.HasAssertionFailure(err))
	})

	t.Run("empty write set returns error", func(t *testing.T) {
		_, err := router.lookupRange(ctx, nil)
		require.Error(t, err)
	})
}
