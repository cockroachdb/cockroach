// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvcoord

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// Test that both the txnPipeliner and txnWriteBuffer correctly populate
// roachpb.LeafTxnInputState with and without the readsTree that narrows down
// the relevant set of spans. This test covers both interceptor implementations
// with the same test scenarios.
func TestTxnInterceptorsPopulateLeafInputState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Set up test keys with gaps to simulate real scenarios.
	keyA := roachpb.Key("table/1/primary/010")
	keyB := roachpb.Key("table/1/primary/025")
	keyC := roachpb.Key("table/1/secondary/030")
	keyD := roachpb.Key("table/2/primary/005")
	keyE := roachpb.Key("table/2/primary/050")
	keyF := roachpb.Key("table/3/primary/100")

	// Common test cases for both interceptors.
	testCases := []struct {
		name           string
		readsTree      interval.Tree
		expectedWrites []roachpb.Key
	}{
		{
			name:           "nil readsTree includes all writes",
			readsTree:      nil,
			expectedWrites: []roachpb.Key{keyA, keyB, keyC, keyD, keyE, keyF},
		},
		{
			name:           "empty readsTree includes no writes",
			readsTree:      interval.NewTree(interval.ExclusiveOverlapper),
			expectedWrites: []roachpb.Key{},
		},
		{
			name: "table 1 primary reads include table 1 primary writes",
			readsTree: func() interval.Tree {
				tree := interval.NewTree(interval.ExclusiveOverlapper)
				// Read span covering table 1 primary rows 015-030 (includes keyB but
				// not keyA).
				span := roachpb.Span{
					Key:    roachpb.Key("table/1/primary/015"),
					EndKey: roachpb.Key("table/1/primary/030"),
				}
				err := tree.Insert(intervalSpan(span), false)
				require.NoError(t, err)
				return tree
			}(),
			// Only keyB (row 025) is in range 015-030.
			expectedWrites: []roachpb.Key{keyB},
		},
		{
			name: "multiple table reads include writes from different tables",
			readsTree: func() interval.Tree {
				tree := interval.NewTree(interval.ExclusiveOverlapper)
				// Read span for table 1 secondary index.
				span1 := roachpb.Span{
					Key:    roachpb.Key("table/1/secondary/"),
					EndKey: roachpb.Key("table/1/secondary/zz"),
				}
				err := tree.Insert(intervalSpan(span1), false)
				require.NoError(t, err)
				// Read span for table 2 primary keys 040-060.
				span2 := roachpb.Span{
					Key:    roachpb.Key("table/2/primary/040"),
					EndKey: roachpb.Key("table/2/primary/060"),
				}
				err = tree.Insert(intervalSpan(span2), false)
				require.NoError(t, err)
				return tree
			}(),
			// Expect keyC (secondary index) and keyE (row 50).
			expectedWrites: []roachpb.Key{keyC, keyE},
		},
		{
			name: "read different table entirely",
			readsTree: func() interval.Tree {
				tree := interval.NewTree(interval.ExclusiveOverlapper)
				// Read span for table 4 (which has no writes).
				span := roachpb.Span{
					Key:    roachpb.Key("table/4/"),
					EndKey: roachpb.Key("table/4/zz"),
				}
				err := tree.Insert(intervalSpan(span), false)
				require.NoError(t, err)
				return tree
			}(),
			// No overlapping writes.
			expectedWrites: []roachpb.Key{},
		},
		{
			name: "precise single key read",
			readsTree: func() interval.Tree {
				tree := interval.NewTree(interval.ExclusiveOverlapper)
				// Exact point read of keyD.
				span := roachpb.Span{Key: keyD, EndKey: keyD.Next()}
				err := tree.Insert(intervalSpan(span), false)
				require.NoError(t, err)
				return tree
			}(),
			// Only the exact key.
			expectedWrites: []roachpb.Key{keyD},
		},
	}

	// Set up both interceptors with the same write sets.
	ctx := context.Background()
	tp, _ := makeMockTxnPipeliner(nil /* iter */)
	twb, _, _ := makeMockTxnWriteBuffer(ctx)

	// Set up in-flight writes.
	tp.ifWrites.insert(keyA, 3, lock.Intent)
	tp.ifWrites.insert(keyB, 7, lock.Intent)
	tp.ifWrites.insert(keyC, 8, lock.Intent)
	tp.ifWrites.insert(keyD, 12, lock.Intent)
	tp.ifWrites.insert(keyE, 15, lock.Intent)
	tp.ifWrites.insert(keyF, 20, lock.Intent)

	// Set up buffered writes, including multiple values for keyB.
	addBufferedWriteForTest(t, &twb, keyA, roachpb.Value{RawBytes: []byte("valueA1")}, 3)
	addBufferedWriteForTest(t, &twb, keyB, roachpb.Value{RawBytes: []byte("valueB1")}, 7)
	addBufferedWriteForTest(t, &twb, keyB, roachpb.Value{RawBytes: []byte("valueB2")}, 8)
	addBufferedWriteForTest(t, &twb, keyC, roachpb.Value{RawBytes: []byte("valueC1")}, 8)
	addBufferedWriteForTest(t, &twb, keyD, roachpb.Value{RawBytes: []byte("valueD1")}, 12)
	addBufferedWriteForTest(t, &twb, keyE, roachpb.Value{RawBytes: []byte("valueE1")}, 15)
	addBufferedWriteForTest(t, &twb, keyF, roachpb.Value{RawBytes: []byte("valueF1")}, 20)

	require.Equal(t, 6, tp.ifWrites.len())
	require.Equal(t, 6, twb.buffer.Len())

	// Test both interceptors with the same test cases.
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			leafState := &roachpb.LeafTxnInputState{}
			tp.populateLeafInputState(leafState, tc.readsTree)
			twb.populateLeafInputState(leafState, tc.readsTree)

			require.Equal(t, len(tc.expectedWrites), len(leafState.InFlightWrites))
			for i := range tc.expectedWrites {
				require.Equal(t, tc.expectedWrites[i], leafState.InFlightWrites[i].Key)
				require.Equal(t, tc.expectedWrites[i], leafState.BufferedWrites[i].Key)
				require.Greater(t, len(leafState.BufferedWrites[i].Vals), 0)

				// For keyB which has multiple writes, verify we get both values.
				if tc.expectedWrites[i].Equal(keyB) && len(tc.expectedWrites) > 0 {
					require.Equal(t, 2, len(leafState.BufferedWrites[i].Vals))
				}
			}
		})
	}

	// Test edge case: empty writes.
	t.Run("no writes", func(t *testing.T) {
		tpEmpty, _ := makeMockTxnPipeliner(nil /* iter */)
		twbEmpty, _, _ := makeMockTxnWriteBuffer(ctx)
		require.Equal(t, 0, tpEmpty.ifWrites.len())
		require.Equal(t, 0, twbEmpty.buffer.Len())
		leafState := &roachpb.LeafTxnInputState{}

		// Test with nil readsTree.
		tpEmpty.populateLeafInputState(leafState, nil)
		twbEmpty.populateLeafInputState(leafState, nil)
		require.Empty(t, leafState.InFlightWrites)
		require.Empty(t, leafState.BufferedWrites)

		// Test with empty reads tree.
		tree := interval.NewTree(interval.ExclusiveOverlapper)
		tpEmpty.populateLeafInputState(leafState, tree)
		twbEmpty.populateLeafInputState(leafState, tree)
		require.Empty(t, leafState.InFlightWrites)
		require.Empty(t, leafState.BufferedWrites)

		// Test with reads in the tree but no writes.
		treeWithReads := interval.NewTree(interval.ExclusiveOverlapper)
		span := roachpb.Span{
			Key:    roachpb.Key("table/1/primary/000"),
			EndKey: roachpb.Key("table/1/primary/999"),
		}
		err := treeWithReads.Insert(intervalSpan(span), false)
		require.NoError(t, err)
		tpEmpty.populateLeafInputState(leafState, treeWithReads)
		twbEmpty.populateLeafInputState(leafState, treeWithReads)
		require.Empty(t, leafState.InFlightWrites)
		require.Empty(t, leafState.BufferedWrites)
	})
}

// intervalSpan is a helper for converting roachpb.Span to interval.Interface.
type intervalSpan roachpb.Span

var _ interval.Interface = intervalSpan{}

func (is intervalSpan) ID() uintptr { return 0 }
func (is intervalSpan) Range() interval.Range {
	return interval.Range{Start: []byte(is.Key), End: []byte(is.EndKey)}
}

// addBufferedWriteForTest is a helper function to add buffered writes to
// the write buffer.
func addBufferedWriteForTest(
	t *testing.T, twb *txnWriteBuffer, key roachpb.Key, val roachpb.Value, seq enginepb.TxnSeq,
) {
	t.Helper()
	ctx := context.Background()
	txn := makeTxnProto()
	txn.Sequence = seq

	// Create a BatchRequest with a single PutRequest.
	ba := &kvpb.BatchRequest{}
	ba.Header = kvpb.Header{Txn: &txn}
	putReq := &kvpb.PutRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:      key,
			Sequence: seq,
		},
		Value: val,
	}
	ba.Add(putReq)

	br, pErr := twb.SendLocked(ctx, ba)
	require.Nil(t, pErr)
	require.NotNil(t, br)
}
