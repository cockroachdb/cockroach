// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package ttljob_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttljob"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestSpanToQueryBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testCases := []struct {
		desc string
		// tablePKValues are PK values initially inserted into the table.
		tablePKValues []string
		// startPKValue is the PK value used to create the span start key.
		startPKValue string
		// truncateStartPKValue removes end bytes from startPKValue to cause a
		// decoding error.
		truncateStartPKValue bool
		// endPKValue is the PK value used to create the span end key.
		endPKValue string
		// truncateEndPKValue removes end bytes from endPKValue to cause a
		// decoding error.
		truncateEndPKValue  bool
		expectedHasRows     bool
		expectedBoundsStart string
		expectedBoundsEnd   string
	}{
		{
			desc:            "empty table",
			tablePKValues:   []string{},
			expectedHasRows: false,
		},
		{
			desc:                "start key < table value",
			tablePKValues:       []string{"B"},
			startPKValue:        "A",
			expectedHasRows:     true,
			expectedBoundsStart: "B",
			expectedBoundsEnd:   "B",
		},
		{
			desc:                "start key = table value",
			tablePKValues:       []string{"A"},
			startPKValue:        "A",
			expectedHasRows:     true,
			expectedBoundsStart: "A",
			expectedBoundsEnd:   "A",
		},
		{
			desc:            "start key > table value",
			tablePKValues:   []string{"A"},
			startPKValue:    "B",
			expectedHasRows: false,
		},
		{
			desc:            "end key < table value",
			tablePKValues:   []string{"B"},
			endPKValue:      "A",
			expectedHasRows: false,
		},
		{
			desc:            "end key = table value",
			tablePKValues:   []string{"A"},
			endPKValue:      "A",
			expectedHasRows: false,
		},
		{
			desc:                "end key > table value",
			tablePKValues:       []string{"A"},
			endPKValue:          "B",
			expectedHasRows:     true,
			expectedBoundsStart: "A",
			expectedBoundsEnd:   "A",
		},
		{
			desc:                "start key between values",
			tablePKValues:       []string{"A", "B", "D", "E"},
			startPKValue:        "C",
			expectedHasRows:     true,
			expectedBoundsStart: "D",
			expectedBoundsEnd:   "E",
		},
		{
			desc:                "end key between values",
			tablePKValues:       []string{"A", "B", "D", "E"},
			endPKValue:          "C",
			expectedHasRows:     true,
			expectedBoundsStart: "A",
			expectedBoundsEnd:   "B",
		},
		{
			desc:                 "truncated start key",
			tablePKValues:        []string{"A", "B", "C"},
			startPKValue:         "B",
			truncateStartPKValue: true,
			expectedHasRows:      true,
			expectedBoundsStart:  "B",
			expectedBoundsEnd:    "C",
		},
		{
			desc:                "truncated end key",
			tablePKValues:       []string{"A", "B", "C"},
			endPKValue:          "B",
			truncateEndPKValue:  true,
			expectedHasRows:     true,
			expectedBoundsStart: "A",
			expectedBoundsEnd:   "A",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {

			const tableName = "tbl"
			ctx := context.Background()
			codec := keys.SystemSQLCodec

			// Setup test cluster.
			testCluster := testcluster.StartTestCluster(t, 1, base.TestClusterArgs{})
			defer testCluster.Stopper().Stop(ctx)

			sqlRunner := sqlutils.MakeSQLRunner(testCluster.ServerConn(0))

			// Create table.
			sqlRunner.Exec(t, fmt.Sprintf("CREATE TABLE %s (id string PRIMARY KEY)", tableName))

			// Insert tablePKValues into table.
			if len(tc.tablePKValues) > 0 {
				insertValues := ""
				for i, val := range tc.tablePKValues {
					if i > 0 {
						insertValues += ", "
					}
					insertValues += "('" + val + "')"
				}
				sqlRunner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, insertValues))
			}

			// Get table descriptor.
			testServer := testCluster.Server(0)
			kvDB := testServer.DB()
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(
				kvDB,
				codec,
				"defaultdb", /* database */
				tableName,
			)
			primaryIndexDesc := tableDesc.GetPrimaryIndex().IndexDesc()
			pkColTypes, err := ttljob.GetPKColumnTypes(tableDesc, primaryIndexDesc)
			require.NoError(t, err)
			pkColDirs := primaryIndexDesc.KeyColumnDirections

			var alloc tree.DatumAlloc
			primaryIndexSpan := tableDesc.PrimaryIndexSpan(codec)

			createKey := func(pkValue string, truncateKey bool, defaultKey roachpb.Key) roachpb.Key {
				if len(pkValue) == 0 {
					return defaultKey
				}
				keyValue := replicationtestutils.EncodeKV(t, codec, tableDesc, pkValue)
				key := keyValue.Key
				if truncateKey {
					key = key[:len(key)-3]
					// Ensure truncated key cannot be decoded.
					_, err = rowenc.DecodeIndexKeyToDatums(codec, pkColTypes, pkColDirs, key, &alloc)
					require.ErrorContainsf(t, err, "did not find terminator 0x0 in buffer", "pkValue=%s", pkValue)
				}
				return key
			}

			// Create keys for test.
			startKey := createKey(tc.startPKValue, tc.truncateStartPKValue, primaryIndexSpan.Key)
			endKey := createKey(tc.endPKValue, tc.truncateEndPKValue, primaryIndexSpan.EndKey)

			// Run test function.
			actualBounds, actualHasRows, err := ttljob.SpanToQueryBounds(
				ctx,
				kvDB,
				codec,
				pkColTypes,
				pkColDirs,
				roachpb.Span{
					Key:    startKey,
					EndKey: endKey,
				},
				&alloc,
			)

			// Verify results.
			require.NoError(t, err)
			require.Equal(t, tc.expectedHasRows, actualHasRows)
			if actualHasRows {
				actualBoundsStart := string(*actualBounds.Start[0].(*tree.DString))
				require.Equalf(t, tc.expectedBoundsStart, actualBoundsStart, "start")
				actualBoundsEnd := string(*actualBounds.End[0].(*tree.DString))
				require.Equalf(t, tc.expectedBoundsEnd, actualBoundsEnd, "end")
			}
		})
	}
}
