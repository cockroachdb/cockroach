// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestKnobsUseRangeTombstonesForPointDeletes tests that the
// UseRangeTombstonesForPointDeletes testing knob works properly.
func TestKnobsUseRangeTombstonesForPointDeletes(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	s, _, db := serverutils.StartServer(t, base.TestServerArgs{
		DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				DisableSplitQueue: true,
				EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
					UseRangeTombstonesForPointDeletes: true,
				},
			},
		},
	})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)
	eng := store.TODOEngine()
	txn := db.NewTxn(ctx, "test")

	// Write a non-transactional and transactional tombstone.
	_, err = db.Del(ctx, roachpb.Key("a"))
	require.NoError(t, err)

	_, err = txn.Del(ctx, roachpb.Key("b"))
	require.NoError(t, err)

	// Write some point keys and then delete them with DeleteRange,
	// both with and without a transaction.
	require.NoError(t, db.Put(ctx, "c", "value"))
	require.NoError(t, db.Put(ctx, "d", "value"))
	_, err = db.DelRange(ctx, "c", "e", false)
	require.NoError(t, err)

	require.NoError(t, db.Put(ctx, "e", "value"))
	require.NoError(t, db.Put(ctx, "f", "value"))
	_, err = txn.DelRange(ctx, "e", "g", false)
	require.NoError(t, err)

	// Commit the transaction.
	require.NoError(t, txn.Commit(ctx))

	// Run a scan to force intent resolution.
	_, err = db.Scan(ctx, "a", "z", 0)
	require.NoError(t, err)

	// Assert that they're now range tombstones.
	var rangeTombstones []storage.MVCCRangeKey
	for _, kvI := range storageutils.ScanKeySpan(t, eng, roachpb.Key("a"), roachpb.Key("z")) {
		switch kv := kvI.(type) {
		case storage.MVCCRangeKeyValue:
			kv.RangeKey.Timestamp = hlc.Timestamp{}
			kv.RangeKey.EncodedTimestampSuffix = nil
			rangeTombstones = append(rangeTombstones, kv.RangeKey)

		case storage.MVCCKeyValue:
			v, err := storage.DecodeMVCCValue(kv.Value)
			require.NoError(t, err)
			require.False(t, v.IsTombstone(), "found point tombstone at %s", kv.Key)

		default:
			t.Fatalf("unknown type %t", kvI)
		}
	}

	require.Equal(t, []storage.MVCCRangeKey{
		storageutils.RangeKey("a", "a\x00", 0),
		storageutils.RangeKey("b", "b\x00", 0),
		storageutils.RangeKey("c", "c\x00", 0),
		storageutils.RangeKey("d", "d\x00", 0),
		storageutils.RangeKey("e", "e\x00", 0),
		storageutils.RangeKey("f", "f\x00", 0),
	}, rangeTombstones)
}
