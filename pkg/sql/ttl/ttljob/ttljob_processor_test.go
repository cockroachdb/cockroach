// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
package ttljob_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/crosscluster/replicationtestutils"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/ttl/ttljob"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
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
			srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
			defer srv.Stopper().Stop(ctx)
			codec := srv.ApplicationLayer().Codec()

			sqlRunner := sqlutils.MakeSQLRunner(sqlDB)

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
			tableDesc := desctestutils.TestingGetPublicTableDescriptor(
				kvDB,
				codec,
				"defaultdb", /* database */
				tableName,
			)
			primaryIndexDesc := tableDesc.GetPrimaryIndex().IndexDesc()
			pkColIDs := catalog.TableColMap{}
			for i, id := range primaryIndexDesc.KeyColumnIDs {
				pkColIDs.Set(id, i)
			}
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
					kvKeyValues := []kv.KeyValue{{Key: key, Value: &keyValue.Value}}
					// Ensure truncated key cannot be decoded.
					_, err = rowenc.DecodeIndexKeyToDatums(codec, pkColIDs, pkColTypes, pkColDirs, kvKeyValues, &alloc)
					require.ErrorContainsf(t, err, "did not find terminator 0x0 in buffer", "pkValue=%s", pkValue)
				}
				return key
			}

			// Create keys for test.
			startKey := createKey(tc.startPKValue, tc.truncateStartPKValue, primaryIndexSpan.Key)
			endKey := createKey(tc.endPKValue, tc.truncateEndPKValue, primaryIndexSpan.EndKey)

			// Run test function.
			actualBounds, actualHasRows, err := ttljob.SpanToQueryBounds(ctx, kvDB, codec, pkColIDs, pkColTypes, pkColDirs, 1, roachpb.Span{
				Key:    startKey,
				EndKey: endKey,
			}, &alloc)

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

func TestSpanToQueryBoundsCompositeKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderStress(t)
	skip.UnderRace(t)

	testCases := []struct {
		desc string
		// tablePKValues are PK values initially inserted into the table.
		tablePKValues [][]string
		// startPKValue is the PK value used to create the span start key.
		startPKValue []string
		// truncateStartPKValue removes end bytes from startPKValue to cause a
		// decoding error.
		truncateStartPKValue bool
		// endPKValue is the PK value used to create the span end key.
		endPKValue []string
		// truncateEndPKValue removes end bytes from endPKValue to cause a
		// decoding error.
		truncateEndPKValue  bool
		expectedHasRows     bool
		expectedBoundsStart []string
		expectedBoundsEnd   []string
	}{
		{
			desc:            "empty table",
			tablePKValues:   [][]string{},
			expectedHasRows: false,
		},
		{
			desc:                "start key < table value",
			tablePKValues:       [][]string{{"B", "2"}},
			startPKValue:        []string{"A", "1"},
			expectedHasRows:     true,
			expectedBoundsStart: []string{"B", "2"},
			expectedBoundsEnd:   []string{"B", "2"},
		},
		{
			desc:                "start key = table value",
			tablePKValues:       [][]string{{"A", "1"}},
			startPKValue:        []string{"A", "1"},
			expectedHasRows:     true,
			expectedBoundsStart: []string{"A", "1"},
			expectedBoundsEnd:   []string{"A", "1"},
		},
		{
			desc:            "start key > table value",
			tablePKValues:   [][]string{{"A", "1"}},
			startPKValue:    []string{"B", "2"},
			expectedHasRows: false,
		},
		{
			desc:            "end key < table value",
			tablePKValues:   [][]string{{"B", "2"}},
			endPKValue:      []string{"A", "1"},
			expectedHasRows: false,
		},
		{
			desc:            "end key = table value",
			tablePKValues:   [][]string{{"A", "1"}},
			endPKValue:      []string{"A", "1"},
			expectedHasRows: false,
		},
		{
			desc:                "end key > table value",
			tablePKValues:       [][]string{{"A", "1"}},
			endPKValue:          []string{"B", "2"},
			expectedHasRows:     true,
			expectedBoundsStart: []string{"A", "1"},
			expectedBoundsEnd:   []string{"A", "1"},
		},
		{
			desc:                "start key between values",
			tablePKValues:       [][]string{{"A", "1"}, {"B", "2"}, {"D", "4"}, {"E", "5"}},
			startPKValue:        []string{"C", "3"},
			expectedHasRows:     true,
			expectedBoundsStart: []string{"D", "4"},
			expectedBoundsEnd:   []string{"E", "5"},
		},
		{
			desc:                "end key between values",
			tablePKValues:       [][]string{{"A", "1"}, {"B", "2"}, {"D", "4"}, {"E", "5"}},
			endPKValue:          []string{"C", "3"},
			expectedHasRows:     true,
			expectedBoundsStart: []string{"A", "1"},
			expectedBoundsEnd:   []string{"B", "2"},
		},
		{
			desc:                 "truncated start key",
			tablePKValues:        [][]string{{"A", "1"}, {"B", "2"}, {"C", "3"}},
			startPKValue:         []string{"B", "2"},
			truncateStartPKValue: true,
			expectedHasRows:      true,
			expectedBoundsStart:  []string{"B", "2"},
			expectedBoundsEnd:    []string{"C", "3"},
		},
		{
			desc:                "truncated end key",
			tablePKValues:       [][]string{{"A", "1"}, {"B", "2"}, {"C", "3"}},
			endPKValue:          []string{"B", "2"},
			truncateEndPKValue:  true,
			expectedHasRows:     true,
			expectedBoundsStart: []string{"A", "1"},
			expectedBoundsEnd:   []string{"A", "1"},
		},
	}

	// Test with different column families, since this affects how the primary
	// key gets encoded.
	familyClauses := []string{
		"",
		"FAMILY (a, b), FAMILY (c),",
		"FAMILY (c), FAMILY (a, b),",
		"FAMILY (a), FAMILY (b), FAMILY (c),",
	}

	for _, tc := range testCases {
		for _, families := range familyClauses {
			t.Run(tc.desc, func(t *testing.T) {

				const tableName = "tbl"
				ctx := context.Background()
				srv, sqlDB, kvDB := serverutils.StartServer(t, base.TestServerArgs{})
				defer srv.Stopper().Stop(ctx)
				codec := srv.ApplicationLayer().Codec()

				sqlRunner := sqlutils.MakeSQLRunner(sqlDB)

				// Create table.
				sqlRunner.Exec(t, fmt.Sprintf(`
				CREATE TABLE %s (
					a string,
					b string COLLATE en_US_u_ks_level2,
					c STRING,
					%s
					PRIMARY KEY(a,b)
				)`, tableName, families))

				// Insert tablePKValues into table.
				if len(tc.tablePKValues) > 0 {
					insertValues := ""
					for i, val := range tc.tablePKValues {
						if i > 0 {
							insertValues += ", "
						}
						insertValues += "('" + strings.Join(val, "','") + "')"
					}
					sqlRunner.Exec(t, fmt.Sprintf("INSERT INTO %s VALUES %s", tableName, insertValues))
				}

				// Get table descriptor.
				tableDesc := desctestutils.TestingGetPublicTableDescriptor(
					kvDB,
					codec,
					"defaultdb", /* database */
					tableName,
				)
				primaryIndexDesc := tableDesc.GetPrimaryIndex().IndexDesc()
				pkColIDs := catalog.TableColMap{}
				for i, id := range primaryIndexDesc.KeyColumnIDs {
					pkColIDs.Set(id, i)
				}
				pkColTypes, err := ttljob.GetPKColumnTypes(tableDesc, primaryIndexDesc)
				require.NoError(t, err)
				pkColDirs := primaryIndexDesc.KeyColumnDirections

				var alloc tree.DatumAlloc
				primaryIndexSpan := tableDesc.PrimaryIndexSpan(codec)

				createKey := func(pkValue []string, truncateKey bool, defaultKey roachpb.Key) roachpb.Key {
					if len(pkValue) == 0 {
						return defaultKey
					}
					require.Equal(t, 2, len(pkValue))
					dString := tree.NewDString(pkValue[0])
					dCollatedString, err := alloc.NewDCollatedString(pkValue[1], "en_US_u_ks_level2")
					require.NoError(t, err)

					keyValues := replicationtestutils.EncodeKVs(t, codec, tableDesc, dString, dCollatedString)
					key := keyValues[0].Key
					if truncateKey {
						key = key[:len(key)-3]
						kvKeyValues := make([]kv.KeyValue, len(keyValues))
						for i := range keyValues {
							kvKeyValues[i] = kv.KeyValue{Key: key, Value: &keyValues[i].Value}
						}
						// Ensure truncated key cannot be decoded.
						_, err = rowenc.DecodeIndexKeyToDatums(codec, pkColIDs, pkColTypes, pkColDirs, kvKeyValues, &alloc)
						require.ErrorContainsf(t, err, "did not find terminator 0x0 in buffer", "pkValue=%s", pkValue)
					}
					return key
				}

				// Create keys for test.
				startKey := createKey(tc.startPKValue, tc.truncateStartPKValue, primaryIndexSpan.Key)
				endKey := createKey(tc.endPKValue, tc.truncateEndPKValue, primaryIndexSpan.EndKey)

				// Run test function.
				actualBounds, actualHasRows, err := ttljob.SpanToQueryBounds(
					ctx, kvDB, codec, pkColIDs, pkColTypes, pkColDirs, tableDesc.NumFamilies(),
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
					actualBoundsStart := []string{
						string(*actualBounds.Start[0].(*tree.DString)),
						actualBounds.Start[1].(*tree.DCollatedString).Contents,
					}
					require.Equalf(t, tc.expectedBoundsStart, actualBoundsStart, "start")
					actualBoundsEnd := []string{
						string(*actualBounds.End[0].(*tree.DString)),
						actualBounds.End[1].(*tree.DCollatedString).Contents,
					}
					require.Equalf(t, tc.expectedBoundsEnd, actualBoundsEnd, "end")
				}
			})
		}
	}
}
