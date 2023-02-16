// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestScanReverseScanTargetBytes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Sanity checks for the TargetBytes scan option. We're not checking the specifics here, just
	// that the plumbing works. TargetBytes is tested in-depth via TestMVCCHistories.

	const (
		tbNeg  = -1     // hard limit, should return no kv pairs
		tbNone = 0      // no limit, i.e. should return all kv pairs
		tbOne  = 1      // one byte = return first key only
		tbMid  = 50     // between first and second key, don't return second
		tbLots = 100000 // de facto ditto tbNone
	)
	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		testutils.RunTrueAndFalse(t, "allowEmpty", func(t *testing.T, allowEmpty bool) {
			testutils.RunTrueAndFalse(t, "requireNextBytes", func(t *testing.T, requireNextBytes bool) {
				for _, tb := range []int64{tbNeg, tbNone, tbOne, tbMid, tbLots} {
					t.Run(fmt.Sprintf("targetBytes=%d", tb), func(t *testing.T) {
						expN := 2
						if tb == tbNeg {
							expN = 0
						} else if tb == tbOne {
							if allowEmpty {
								expN = 0
							} else {
								expN = 1
							}
						} else if tb == tbMid {
							expN = 1
						}
						for _, sf := range []kvpb.ScanFormat{kvpb.KEY_VALUES, kvpb.BATCH_RESPONSE} {
							t.Run(fmt.Sprintf("format=%s", sf), func(t *testing.T) {
								testScanReverseScanInner(t, tb, sf, reverse, allowEmpty, expN)
							})
						}
					})
				}
			})
		})
	})
}

func testScanReverseScanInner(
	t *testing.T, tb int64, sf kvpb.ScanFormat, reverse bool, allowEmpty bool, expN int,
) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	k1, k2 := roachpb.Key("a"), roachpb.Key("b")
	ts := hlc.Timestamp{WallTime: 1}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write to k1 and k2.
	for _, k := range []roachpb.Key{k1, k2} {
		err := storage.MVCCPut(ctx, eng, nil, k, ts, hlc.ClockTimestamp{}, roachpb.MakeValueFromString("value-"+string(k)), nil)
		require.NoError(t, err)
	}

	var req kvpb.Request
	var resp kvpb.Response
	if !reverse {
		req = &kvpb.ScanRequest{ScanFormat: sf}
		resp = &kvpb.ScanResponse{}
	} else {
		req = &kvpb.ReverseScanRequest{ScanFormat: sf}
		resp = &kvpb.ReverseScanResponse{}
	}
	req.SetHeader(kvpb.RequestHeader{Key: k1, EndKey: roachpb.KeyMax})

	settings := cluster.MakeTestingClusterSettings()

	cArgs := CommandArgs{
		Args: req,
		Header: kvpb.Header{
			Timestamp:   ts,
			TargetBytes: tb,
			AllowEmpty:  allowEmpty,
		},
		EvalCtx: (&MockEvalCtx{ClusterSettings: settings}).EvalContext(),
	}

	if !reverse {
		_, err := Scan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	} else {
		_, err := ReverseScan(ctx, eng, cArgs, resp)
		require.NoError(t, err)
	}

	require.EqualValues(t, expN, resp.Header().NumKeys)
	if allowEmpty && tb > 0 {
		require.LessOrEqual(t, resp.Header().NumBytes, tb)
	} else if tb >= 0 {
		require.NotZero(t, resp.Header().NumBytes)
	}

	expSpan := &roachpb.Span{Key: k1, EndKey: roachpb.KeyMax}
	switch expN {
	case 0:
		if tb >= 0 && reverse {
			expSpan.EndKey = append(k2, 0)
		}
	case 1:
		if reverse {
			expSpan.EndKey = append(k1, 0)
		} else {
			expSpan.Key = k2
		}
	default:
		expSpan = nil
	}

	require.Equal(t, expSpan, resp.Header().ResumeSpan)
	if expSpan != nil {
		require.NotZero(t, resp.Header().ResumeReason)
		if tb < 0 {
			require.Zero(t, resp.Header().ResumeNextBytes)
		} else {
			require.NotZero(t, resp.Header().ResumeNextBytes)
		}
	}

	var rows []roachpb.KeyValue
	if !reverse {
		rows = resp.(*kvpb.ScanResponse).Rows
	} else {
		rows = resp.(*kvpb.ReverseScanResponse).Rows
	}

	if rows != nil {
		require.Len(t, rows, expN)
	}
}

// TestScanReverseScanWholeRows checks that WholeRowsOfSize is wired up
// correctly. Comprehensive testing is done e.g. in TestMVCCHistories.
func TestScanReverseScanWholeRows(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	ts := hlc.Timestamp{WallTime: 1}

	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	// Write 2 rows with 3 column families each.
	var rowKeys []roachpb.Key
	for r := 0; r < 2; r++ {
		for cf := uint32(0); cf < 3; cf++ {
			key := makeRowKey(t, r, cf)
			err := storage.MVCCPut(ctx, eng, nil, key, ts, hlc.ClockTimestamp{}, roachpb.MakeValueFromString("value"), nil)
			require.NoError(t, err)
			rowKeys = append(rowKeys, key)
		}
	}

	testutils.RunTrueAndFalse(t, "reverse", func(t *testing.T, reverse bool) {
		var req kvpb.Request
		var resp kvpb.Response
		if !reverse {
			req = &kvpb.ScanRequest{}
			resp = &kvpb.ScanResponse{}
		} else {
			req = &kvpb.ReverseScanRequest{}
			resp = &kvpb.ReverseScanResponse{}
		}
		req.SetHeader(kvpb.RequestHeader{Key: rowKeys[0], EndKey: roachpb.KeyMax})

		// Scan with limit of 5 keys. This should only return the first row (3 keys),
		// since they second row would yield 6 keys total.
		cArgs := CommandArgs{
			EvalCtx: (&MockEvalCtx{ClusterSettings: cluster.MakeTestingClusterSettings()}).EvalContext(),
			Args:    req,
			Header: kvpb.Header{
				Timestamp:          ts,
				MaxSpanRequestKeys: 5,
				WholeRowsOfSize:    3,
			},
		}

		if !reverse {
			_, err := Scan(ctx, eng, cArgs, resp)
			require.NoError(t, err)
		} else {
			_, err := ReverseScan(ctx, eng, cArgs, resp)
			require.NoError(t, err)
		}

		require.EqualValues(t, resp.Header().NumKeys, 3)
	})
}

// makeRowKey makes a key for a SQL row for use in tests, using the system
// tenant with table 1 index 1, and a single column.
func makeRowKey(t *testing.T, id int, columnFamily uint32) roachpb.Key {
	var colMap catalog.TableColMap
	colMap.Set(0, 0)

	var err error
	key := keys.SystemSQLCodec.IndexPrefix(1, 1)
	key, _, err = rowenc.EncodeColumns(
		[]descpb.ColumnID{0}, nil /* directions */, colMap,
		[]tree.Datum{tree.NewDInt(tree.DInt(id))}, key)
	require.NoError(t, err)
	return keys.MakeFamilyKey(key, columnFamily)
}
