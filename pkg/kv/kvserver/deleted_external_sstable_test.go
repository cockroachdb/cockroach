// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// externalSSTTestCluster is a helper struct that has helper functions to set up
// and run the tests.
type externalSSTTestCluster struct {
	tc *testcluster.TestCluster
	db *kv.DB
}

// testSetup creates a test cluster with ranges split in this way:
// [a, d), [d, g), [g, z).
func (etc *externalSSTTestCluster) testSetup(t *testing.T) {
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable the merge queue and load-based splitting to avoid
					// automatic merges and splits that could interfere with the
					// test.
					DisableMergeQueue:         true,
					DisableLoadBasedSplitting: true,
					// Reduce the storage cache size, and disable automatic
					// compactions to avoid storage downloading/caching the
					// deleted file before actually deleting it.
					EngineKnobs: []storage.ConfigOption{storage.CacheSize(1), storage.MaxOpenFiles(1),
						storage.DisableAutomaticCompactions,
					},
				},
			},
		},
	})

	// Perform the range splits.
	splitKeys := []string{"z", "g", "d", "a"}
	for _, key := range splitKeys {
		tc.SplitRangeOrFatal(t, roachpb.Key(key))
	}

	// Up-replicate the ranges we split above.
	for _, key := range splitKeys {
		tc.AddVotersOrFatal(t, roachpb.Key(key), tc.Target(1), tc.Target(2))
	}

	etc.tc = tc
	etc.db = tc.Server(0).DB()
}

// createRangeFeed creates a RangeFeed for the given range and returns the
// RangeFeed channels, as well as a cancel function to cancel the RangeFeed.
func (etc *externalSSTTestCluster) createRangeFeed(
	ctx context.Context, startKey roachpb.Key, endKey roachpb.Key,
) (chan kvcoord.RangeFeedMessage, chan error, context.CancelFunc) {
	ds := etc.tc.Server(0).DistSenderI().(*kvcoord.DistSender)
	db := etc.tc.Server(0).DB()
	startTS := db.Clock().Now()
	evChan := make(chan kvcoord.RangeFeedMessage)
	rangeFeedErrChan := make(chan error, 1)
	ctxToCancel, cancel := context.WithCancel(ctx)
	descTableSpan := roachpb.Span{
		Key:    startKey,
		EndKey: endKey,
	}
	go func() {
		rangeFeedErrChan <- ds.RangeFeed(ctxToCancel,
			[]kvcoord.SpanTimePair{{Span: descTableSpan, StartAfter: startTS}}, evChan)
	}()

	return evChan, rangeFeedErrChan, cancel
}

// createExternalStorage creates an external storage client for the given URI.
func (etc *externalSSTTestCluster) createExternalStorage(
	ctx context.Context, URI string,
) (cloud.ExternalStorage, error) {
	extStore, err := cloud.EarlyBootExternalStorageFromURI(ctx,
		URI,
		base.ExternalIODirConfig{},
		etc.tc.ApplicationLayer(0).ClusterSettings(),
		nil, /* limiters */
		cloud.NilMetrics)
	return extStore, err
}

// createExternalSSTableFile creates an external SSTable file with the given
// file name, and populates it with key-value pairs for the given range.
// For example, if start is 'a' and end is 'c', the file will contain
// keys like: [a-0000, a-0001..., b-0000, b0001, ...].
func (etc *externalSSTTestCluster) createExternalSSTableFile(
	t *testing.T,
	ctx context.Context,
	externalStorage cloud.ExternalStorage,
	fileName string,
	start byte,
	end byte,
) error {

	// We need to populate the file with a bunch of key-value pairs. Otherwise,
	// the file might end up being cached in Pebble, and deleting it won't cause
	// requests to fail since they can access it from the block cache.
	type entry struct {
		Key   string
		Value string
	}

	// Create slice with initial capacity based on number of characters and
	// entries per character.
	numChars := int(end - start)
	entriesPerChar := 30000
	entries := make([]entry, 0, numChars*entriesPerChar)

	// Populate the slice with entries for each character between start and end.
	// Note that we need to populate a lot of data to avoid the file being
	// cached in Pebble.
	for c := start; c < end; c++ {
		prefix := string(c)
		for i := 0; i < entriesPerChar; i++ {
			entries = append(entries, entry{
				// Creates keys like "a-00001", "a-00002", etc.
				Key:   fmt.Sprintf("%s-%05d", prefix, i),
				Value: fmt.Sprintf("initial-value-%s-%05d", prefix, i),
			})
		}
	}

	// Create an SSTable from the entries above, and write it to the external
	// storage.
	kvs := make([]interface{}, 0, len(entries))
	for _, expKV := range entries {
		kvs = append(kvs, storageutils.PointKV(expKV.Key, 1, expKV.Value))
	}

	sst, _, _ := storageutils.MakeSST(t, etc.tc.ApplicationLayer(0).ClusterSettings(), kvs)
	w, err := externalStorage.Writer(ctx, fileName)
	if err != nil {
		return err
	}
	if _, err = w.Write(sst); err != nil {
		return err
	}
	return w.Close()
}

// linkExternalSSTableToFile links an external SSTable in the given span to
// the given file.
func (etc *externalSSTTestCluster) linkExternalSSTableToFile(
	ctx context.Context, startKey roachpb.Key, endKey roachpb.Key, URI string, fileName string,
) error {
	errLink := etc.db.LinkExternalSSTable(ctx, roachpb.Span{
		Key:    startKey,
		EndKey: endKey,
	}, kvpb.LinkExternalSSTableRequest_ExternalFile{
		Locator: URI,
		Path:    fileName,
		// Use a dummy file sizes.
		ApproximatePhysicalSize: uint64(512 * 1024 * 1024),
		BackingFileSize:         uint64(512 * 1024 * 1024),
		MVCCStats: &enginepb.MVCCStats{
			ContainsEstimates: 1,
			KeyBytes:          512 * 1024 * 1024,
			ValBytes:          512 * 1024 * 1024,
			KeyCount:          512 * 1024 * 1024,
			LiveCount:         512 * 1024 * 1024,
		},
	}, etc.db.Clock().Now())
	return errLink
}

// writeIntents performs some put operations over the [a,z]. Moreover, it starts
// two transactions but doesn't commit them. This will be used to test that
// operations that run on top of the external SSTable shouldn't cause any
// failures.
func (etc *externalSSTTestCluster) writeIntents(
	ctx context.Context, db *kv.DB,
) (*kv.Txn, *kv.Txn, error) {
	// Put some key-value pairs on top of the external SSTable.
	for i := range 26 {
		key := roachpb.Key(fmt.Sprintf("%c-intent", 'a'+i))
		if err := db.Put(ctx, key, "value"); err != nil {
			return nil, nil, err
		}
	}

	// Create a transaction and perform some writes, and do NOT commit it.
	pendingTxn1 := kv.NewTxn(ctx, db, 0)
	for i := range 26 {
		key := roachpb.Key(fmt.Sprintf("%c-txn1", 'a'+i))
		if err := pendingTxn1.Put(ctx, key, "pending value"); err != nil {
			return nil, nil, err
		}
	}

	pendingTxn2 := kv.NewTxn(ctx, db, 0)
	if err := pendingTxn1.Put(ctx, roachpb.Key("g-txn2"), "pending value"); err != nil {
		return nil, nil, err
	}
	if err := pendingTxn1.Put(ctx, roachpb.Key("a-txn1"), "pending value"); err != nil {
		return nil, nil, err
	}
	return pendingTxn1, pendingTxn2, nil
}

// putHelper is a helper function to put a key-value pair in the store.
func (etc *externalSSTTestCluster) putHelper(ctx context.Context, key roachpb.Key) error {
	b := kv.Batch{}
	b.Put(key, "value")
	return etc.db.Run(ctx, &b)
}

// getHelper is a helper function to get a key-value pair from the store.
func (etc *externalSSTTestCluster) getHelper(ctx context.Context, key roachpb.Key) error {
	b := kv.Batch{}
	b.Get(key)
	return etc.db.Run(ctx, &b)
}

// scanHelper is a helper function to scan a range of key-value pairs from the
// store.
func (etc *externalSSTTestCluster) scanHelper(
	ctx context.Context, start roachpb.Key, end roachpb.Key,
) error {
	b := kv.Batch{}
	b.Scan(start, end)
	return etc.db.Run(ctx, &b)
}

// deleteRangeHelper is a helper function to delete a range of key-value pairs
// from the store.
func (etc *externalSSTTestCluster) deleteRangeHelper(
	ctx context.Context, start roachpb.Key, end roachpb.Key,
) error {
	b := kv.Batch{}
	b.DelRange(start, end, false /* returnKeys */)
	return etc.db.Run(ctx, &b)
}

// mergeHelper issues an AdminMergeRequest for the provided key.
func (etc *externalSSTTestCluster) mergeHelper(ctx context.Context, key roachpb.Key) error {
	_, pErr := kv.SendWrapped(ctx, etc.db.NonTransactionalSender(), adminMergeArgs(key))
	return pErr.GoError()
}

// checkKeysPointToSameRangeDesc is a helper function to check if two or more
// keys point to the same range descriptor.
func (etc *externalSSTTestCluster) checkKeysPointToSameRangeDesc(
	t *testing.T, values ...string,
) error {
	if len(values) < 2 {
		return fmt.Errorf("checkKeysPointToSameRangeDesc needs at least 2 values to compare, got %d",
			len(values))
	}

	first := etc.tc.GetFirstStoreFromServer(t, 0).LookupReplica(roachpb.RKey(values[0])).Desc()

	for i := 1; i < len(values); i++ {
		if !etc.tc.GetFirstStoreFromServer(t, 0).
			LookupReplica(roachpb.RKey(values[i])).Desc().Equal(first) {
			return fmt.Errorf("values not equal:\nexpected: %v\nactual: %v", first, values[i])
		}
	}
	return nil
}

// TestGeneralOperationsWorkAsExpectedOnDeletedExternalSST tests that general
// operations (put, get, scan, delete, merge) work as expected on a range
// that has an external SSTable linked to a deleted file.
// TODO(ibrahim): Add split tests once we allow the EndTxn to abort during the
// splitTrigger
func TestGeneralOperationsWorkAsExpectedOnDeletedExternalSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()
	const externURI = "nodelocal://1/external-files"

	skip.UnderRace(t) // too slow under stressrace

	testCases := []struct {
		name                     string
		deletedExternalSpanStart roachpb.Key
		deletedExternalSpanEnd   roachpb.Key
		testFunc                 func(
			t *testing.T, ctx context.Context, etc *externalSSTTestCluster,
		)
	}{
		{
			name: "data ops with deleted span at range boundaries",
			// Original ranges: [a, d), [d, g), [g, z).
			// The deleted span is:     [d, g).
			deletedExternalSpanStart: roachpb.Key("d"),
			deletedExternalSpanEnd:   roachpb.Key("g"),
			testFunc: func(
				t *testing.T,
				ctx context.Context,
				etc *externalSSTTestCluster,
			) {
				// Data operations that operate on the deleted SSTable should
				// fail.
				require.Regexp(t, "no such file or directory",
					etc.scanHelper(ctx, roachpb.Key("a"), roachpb.Key("z")))

				require.Regexp(t, "no such file or directory",
					etc.getHelper(ctx, roachpb.Key("d-15000")))
				require.Regexp(t, "no such file or directory",
					etc.putHelper(ctx, roachpb.Key("d-15000")))
				require.Regexp(t, "no such file or directory",
					etc.deleteRangeHelper(ctx, roachpb.Key("a"), roachpb.Key("z")))

				// Data operations that don't operate on the deleted SSTable
				// should succeed.
				require.NoError(t, etc.scanHelper(ctx, roachpb.Key("a"), roachpb.Key("d")))
				require.NoError(t, etc.scanHelper(ctx, roachpb.Key("g"), roachpb.Key("z")))
				require.NoError(t, etc.getHelper(ctx, roachpb.Key("a")))
				require.NoError(t, etc.getHelper(ctx, roachpb.Key("y")))
				require.NoError(t, etc.putHelper(ctx, roachpb.Key("a")))
				require.NoError(t, etc.putHelper(ctx, roachpb.Key("y")))
				require.NoError(t,
					etc.deleteRangeHelper(ctx, roachpb.Key("a"), roachpb.Key("b")))
				require.NoError(t,
					etc.deleteRangeHelper(ctx, roachpb.Key("g"), roachpb.Key("h")))
			},
		},
		{
			name: "data ops with deleted span less than range boundaries",
			// Original ranges: [a,   d), [d,     g), [g,   z).
			// The deleted span is:         [e,f).
			deletedExternalSpanStart: roachpb.Key("e"),
			deletedExternalSpanEnd:   roachpb.Key("f"),
			testFunc: func(
				t *testing.T,
				ctx context.Context,
				etc *externalSSTTestCluster,
			) {
				// Data operations that operate on the deleted SSTable should
				// fail.
				require.Regexp(t, "no such file or directory",
					etc.scanHelper(ctx, roachpb.Key("a"), roachpb.Key("z")))
				require.Regexp(t, "no such file or directory",
					etc.getHelper(ctx, roachpb.Key("e-15000")))
				require.Regexp(t, "no such file or directory",
					etc.putHelper(ctx, roachpb.Key("e-15000")))
				require.Regexp(t, "no such file or directory",
					etc.deleteRangeHelper(ctx, roachpb.Key("a"), roachpb.Key("z")))

				// Data operations that don't operate on the deleted SSTable
				// should succeed.
				require.NoError(t, etc.scanHelper(ctx, roachpb.Key("a"), roachpb.Key("e")))
				require.NoError(t, etc.scanHelper(ctx, roachpb.Key("f"), roachpb.Key("z")))
				require.NoError(t, etc.getHelper(ctx, roachpb.Key("da")))
				require.NoError(t, etc.getHelper(ctx, roachpb.Key("fa")))
				require.NoError(t, etc.putHelper(ctx, roachpb.Key("da")))
				require.NoError(t, etc.putHelper(ctx, roachpb.Key("fa")))
				require.NoError(t,
					etc.deleteRangeHelper(ctx, roachpb.Key("a"), roachpb.Key("e")))
				require.NoError(t,
					etc.deleteRangeHelper(ctx, roachpb.Key("f"), roachpb.Key("h")))
			},
		},
		{
			name: "data ops with deleted span more than range boundaries",
			// Original ranges: [a, d), [d, g), [g, z).
			// The deleted span is [c,            h).
			deletedExternalSpanStart: roachpb.Key("c"),
			deletedExternalSpanEnd:   roachpb.Key("h"),
			testFunc: func(
				t *testing.T,
				ctx context.Context,
				etc *externalSSTTestCluster,
			) {
				// Data operations that operate on the deleted SSTable should
				// fail.
				require.Regexp(t, "no such file or directory",
					etc.scanHelper(ctx, roachpb.Key("a"), roachpb.Key("z")))
				require.Regexp(t, "no such file or directory",
					etc.getHelper(ctx, roachpb.Key("c-15000")))
				require.Regexp(t, "no such file or directory",
					etc.putHelper(ctx, roachpb.Key("c-15000")))
				require.Regexp(t, "no such file or directory",
					etc.deleteRangeHelper(ctx, roachpb.Key("a"), roachpb.Key("z")))

				// Data operations that don't operate on the deleted SSTable
				// should succeed.
				require.NoError(t, etc.scanHelper(ctx, roachpb.Key("a"), roachpb.Key("c")))
				require.NoError(t, etc.scanHelper(ctx, roachpb.Key("h"), roachpb.Key("z")))
				require.NoError(t, etc.getHelper(ctx, roachpb.Key("a")))
				require.NoError(t, etc.getHelper(ctx, roachpb.Key("y")))
				require.NoError(t, etc.putHelper(ctx, roachpb.Key("a")))
				require.NoError(t, etc.putHelper(ctx, roachpb.Key("y")))
				require.NoError(t,
					etc.deleteRangeHelper(ctx, roachpb.Key("a"), roachpb.Key("b")))
				require.NoError(t,
					etc.deleteRangeHelper(ctx, roachpb.Key("h"), roachpb.Key("i")))
			},
		},
		{
			name: "merge with deleted span at range boundaries",
			// Original ranges: [a, d), [d, g), [g, z).
			// The deleted span is:     [d, g).
			deletedExternalSpanStart: roachpb.Key("d"),
			deletedExternalSpanEnd:   roachpb.Key("g"),
			testFunc: func(t *testing.T,
				ctx context.Context,
				etc *externalSSTTestCluster,
			) {
				// Merges don't touch the deleted SSTable so they succeed.
				require.NoError(t, etc.mergeHelper(ctx, roachpb.Key("d")))
				require.NoError(t, etc.mergeHelper(ctx, roachpb.Key("a")))

				// Make sure that the ranges have been merged correctly.
				require.NoError(t, etc.checkKeysPointToSameRangeDesc(t, "a", "d", "g"))
			},
		},
		{
			name: "merge with deleted span at more than range boundaries",
			// Original ranges: [a, d), [d, g), [g, z).
			// The deleted span is [c,            h).
			deletedExternalSpanStart: roachpb.Key("c"),
			deletedExternalSpanEnd:   roachpb.Key("h"),
			testFunc: func(
				t *testing.T,
				ctx context.Context,
				etc *externalSSTTestCluster,
			) {
				// Merges don't touch the deleted SSTable so they succeed.
				require.NoError(t, etc.mergeHelper(ctx, roachpb.Key("d")))
				require.NoError(t, etc.mergeHelper(ctx, roachpb.Key("a")))

				// Make sure that the ranges have been merged correctly.
				require.NoError(t, etc.checkKeysPointToSameRangeDesc(t, "a", "d", "g"))
			},
		},
		{
			name: "merge with deleted span at less than range boundaries",
			// Original ranges: [a,   d), [d,     g), [g,   z).
			// The deleted span is:         [e,f).
			deletedExternalSpanStart: roachpb.Key("e"),
			deletedExternalSpanEnd:   roachpb.Key("f"),
			testFunc: func(
				t *testing.T,
				ctx context.Context,
				etc *externalSSTTestCluster,
			) {
				// Merges don't touch the deleted SSTable so they succeed.
				require.NoError(t, etc.mergeHelper(ctx, roachpb.Key("d")))
				require.NoError(t, etc.mergeHelper(ctx, roachpb.Key("a")))

				// Make sure that the ranges have been merged correctly.
				require.NoError(t, etc.checkKeysPointToSameRangeDesc(t, "a", "d", "g"))
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()
			etc := externalSSTTestCluster{}
			etc.testSetup(t)
			defer etc.tc.Stopper().Stop(ctx)

			// Create a RangeFeed to ensure that it won't cause crashes during the
			// test.
			_, _, cancel := etc.createRangeFeed(ctx, roachpb.Key("a"), roachpb.Key("z"))
			defer cancel()

			externalStorage, err := etc.createExternalStorage(ctx, externURI)
			require.NoError(t, err)

			firstStartChar := testCase.deletedExternalSpanStart[0]
			firstEndChar := testCase.deletedExternalSpanEnd[0]
			require.NoError(t, etc.createExternalSSTableFile(t, ctx, externalStorage,
				"file1.sst", firstStartChar, firstEndChar))
			require.NoError(t, etc.linkExternalSSTableToFile(ctx, testCase.deletedExternalSpanStart,
				testCase.deletedExternalSpanEnd, externURI, "file1.sst"))

			// Before deleting the file, run some data operations that will be
			// on top of the SSTable pointing to the soon-to-be deleted file.
			pendingTxn1, pendingTxn2, err := etc.writeIntents(ctx, etc.db)
			require.NoError(t, err)
			require.NoError(t, externalStorage.Delete(ctx, "file1.sst"))

			// We should be able to commit the transactions since they just have
			// point writes and they wouldn't need the deleted file.
			require.NoError(t, pendingTxn1.Commit(ctx))
			require.NoError(t, pendingTxn2.Commit(ctx))

			// Run the test function, and make sure that the store is consistent
			// afterward.
			testCase.testFunc(t, ctx, &etc)
		})
	}
}
