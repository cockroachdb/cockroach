// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/nodelocal"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// testSetup returns a test cluster with a scratch range split three-ways:
// [a, d), [d, g), [g, z).
func testSetup(t *testing.T, ctx context.Context) *testcluster.TestCluster {
	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual,
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					DisableMergeQueue:         true,
					DisableLoadBasedSplitting: true,
				},
			},
		},
	})

	// Perform the range splits.
	store := tc.GetFirstStoreFromServer(t, 0)
	splits := []*kvpb.AdminSplitRequest{
		adminSplitArgs(roachpb.Key("z")),
		adminSplitArgs(roachpb.Key("g")),
		adminSplitArgs(roachpb.Key("d")),
		adminSplitArgs(roachpb.Key("a")),
	}

	for _, split := range splits {
		_, pErr := kv.SendWrapped(context.Background(), store.TestSender(), split)
		if pErr != nil {
			t.Fatalf("%q: split unexpected error: %s", split.SplitKey, pErr)
		}
	}

	// Up-replicate the ranges we split above.
	desc1 := store.LookupReplica(roachpb.RKey("a")).Desc()
	desc2 := store.LookupReplica(roachpb.RKey("d")).Desc()
	desc3 := store.LookupReplica(roachpb.RKey("g")).Desc()

	tc.AddVotersOrFatal(t, desc1.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
	tc.AddVotersOrFatal(t, desc2.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))
	tc.AddVotersOrFatal(t, desc3.StartKey.AsRawKey(), tc.Target(1), tc.Target(2))

	// Issue some increments to the ranges we split above.
	incA := incrementArgs(roachpb.Key("a"), 1)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incA); pErr != nil {
		t.Fatal(pErr)
	}

	incD := incrementArgs(roachpb.Key("d"), 1)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incD); pErr != nil {
		t.Fatal(pErr)
	}

	incG := incrementArgs(roachpb.Key("g"), 1)
	if _, pErr := kv.SendWrapped(ctx, store.TestSender(), incG); pErr != nil {
		t.Fatal(pErr)
	}

	return tc
}

// createRangeFeed creates a RangeFeed for the given range and returns the
// RangeFeed channels, as well as a cancel function to cancel the RangeFeed.
func createRangeFeed(
	ctx context.Context, tc *testcluster.TestCluster, startKey roachpb.Key, endKey roachpb.Key,
) (chan kvcoord.RangeFeedMessage, chan error, context.CancelFunc) {
	ds := tc.Server(0).DistSenderI().(*kvcoord.DistSender)
	db := tc.Server(0).DB()
	startTS := db.Clock().Now()
	evChan := make(chan kvcoord.RangeFeedMessage)
	rangeFeedErrChan := make(chan error, 1)
	ctxToCancel, cancel := context.WithCancel(ctx)
	defer cancel()
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

func createFile(
	t *testing.T, ctx context.Context, store *kvserver.Store, URI string, fileName string,
) error {
	extStore, err := cloud.EarlyBootExternalStorageFromURI(ctx,
		URI,
		base.ExternalIODirConfig{},
		store.ClusterSettings(),
		nil, /* limiters */
		cloud.NilMetrics)

	if err != nil {
		return err
	}

	sst, _, _ := storageutils.MakeSST(t, store.ClusterSettings(), []interface{}{})
	w, err := extStore.Writer(ctx, fileName)
	if err != nil {
		return err
	}
	if _, err = w.Write(sst); err != nil {
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return nil
}

// linkExternalSSTableToNonExistentFile links an external SSTable to a
// non-existent file. This simulates the case where the SSTable file has been
// deleted from the file system before the node got the chance to download it.
func linkExternalSSTableToFile(
	ctx context.Context,
	store *kvserver.Store,
	startKey roachpb.Key,
	endKey roachpb.Key,
	URI string,
	fileName string,
) error {
	errLink := store.DB().LinkExternalSSTable(ctx, roachpb.Span{
		Key:    startKey,
		EndKey: endKey,
	}, kvpb.LinkExternalSSTableRequest_ExternalFile{
		Locator: URI,
		Path:    fileName,
		// Use a dummy file sizes.
		ApproximatePhysicalSize: uint64(1),
		BackingFileSize:         uint64(1),
		MVCCStats: &enginepb.MVCCStats{
			ContainsEstimates: 1,
			KeyBytes:          2,
			ValBytes:          10,
			KeyCount:          2,
			LiveCount:         2,
		},
	}, store.DB().Clock().Now())
	return errLink
}

// putHelper is a helper function to put a key-value pair in the store.
func putHelper(ctx context.Context, store *kvserver.Store, key roachpb.Key) error {
	b := kv.Batch{}
	b.Put(key, "value")
	return store.DB().Run(ctx, &b)
}

// getHelper is a helper function to get a key-value pair from the store.
func getHelper(ctx context.Context, store *kvserver.Store, key roachpb.Key) error {
	b := kv.Batch{}
	b.Get(key)
	return store.DB().Run(ctx, &b)
}

// scanHelper is a helper function to scan a range of key-value pairs from the
// store.
func scanHelper(ctx context.Context, store *kvserver.Store, start roachpb.Key, end roachpb.Key,
) error {
	b := kv.Batch{}
	b.Scan(start, end)
	return store.DB().Run(ctx, &b)
}

// deleteRangeHelper is a helper function to delete a range of key-value pairs
// from the store.
func deleteRangeHelper(
	ctx context.Context, store *kvserver.Store, start roachpb.Key, end roachpb.Key,
) error {
	b := kv.Batch{}
	b.DelRange(start, end, false /* returnKeys */)
	return store.DB().Run(ctx, &b)
}

// mergeHelper issues an AdminMergeRequest for the provided key.
func mergeHelper(ctx context.Context, store *kvserver.Store, key roachpb.Key) error {
	_, pErr := kv.SendWrapped(ctx, store.TestSender(), adminMergeArgs(key))
	return pErr.GoError()
}

// TestGeneralOperationsWorkAsExpectedOnDeletedExternalSST tests that general
// operations (put, get, scan, delete, split, merge) work as expected on a range
// that has an external SSTable linked to a deleted file.
func TestGeneralOperationsWorkAsExpectedOnDeletedExternalSST(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	defer nodelocal.ReplaceNodeLocalForTesting(t.TempDir())()
	const externURI = "nodelocal://1/external-files"

	testCases := []struct {
		name                     string
		deletedExternalSpanStart roachpb.Key
		deletedExternalSpanEnd   roachpb.Key
		testFunc                 func(
			t *testing.T, ctx context.Context, tc *testcluster.TestCluster, store *kvserver.Store,
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
				tc *testcluster.TestCluster,
				store *kvserver.Store,
			) {
				// Data operations that operate on the deleted SSTable should
				// fail.
				require.Regexp(t, "no such file or directory",
					scanHelper(ctx, store, roachpb.Key("a"), roachpb.Key("z")))
				require.Regexp(t, "no such file or directory",
					getHelper(ctx, store, roachpb.Key("d")))
				require.Regexp(t, "no such file or directory",
					putHelper(ctx, store, roachpb.Key("d")))
				require.Regexp(t, "no such file or directory",
					deleteRangeHelper(ctx, store, roachpb.Key("a"), roachpb.Key("z")))

				// Data operations that don't operate on the deleted SSTable
				// should succeed.
				require.NoError(t, scanHelper(ctx, store, roachpb.Key("a"), roachpb.Key("d")))
				require.NoError(t, scanHelper(ctx, store, roachpb.Key("g"), roachpb.Key("z")))
				require.NoError(t, getHelper(ctx, store, roachpb.Key("a")))
				require.NoError(t, getHelper(ctx, store, roachpb.Key("y")))
				require.NoError(t, putHelper(ctx, store, roachpb.Key("a")))
				require.NoError(t, putHelper(ctx, store, roachpb.Key("y")))
				require.NoError(t,
					deleteRangeHelper(ctx, store, roachpb.Key("a"), roachpb.Key("b")))
				require.NoError(t,
					deleteRangeHelper(ctx, store, roachpb.Key("g"), roachpb.Key("h")))
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
				tc *testcluster.TestCluster,
				store *kvserver.Store,
			) {
				// Data operations that operate on the deleted SSTable should
				// fail.
				require.Regexp(t, "no such file or directory",
					scanHelper(ctx, store, roachpb.Key("a"), roachpb.Key("z")))
				require.Regexp(t, "no such file or directory",
					getHelper(ctx, store, roachpb.Key("e")))
				require.Regexp(t, "no such file or directory",
					putHelper(ctx, store, roachpb.Key("e")))
				require.Regexp(t, "no such file or directory",
					deleteRangeHelper(ctx, store, roachpb.Key("a"), roachpb.Key("z")))

				// Data operations that don't operate on the deleted SSTable
				// should succeed.
				require.NoError(t, scanHelper(ctx, store, roachpb.Key("a"), roachpb.Key("e")))
				require.NoError(t, scanHelper(ctx, store, roachpb.Key("f"), roachpb.Key("z")))
				require.NoError(t, getHelper(ctx, store, roachpb.Key("da")))
				require.NoError(t, getHelper(ctx, store, roachpb.Key("fa")))
				require.NoError(t, putHelper(ctx, store, roachpb.Key("da")))
				require.NoError(t, putHelper(ctx, store, roachpb.Key("fa")))
				require.NoError(t,
					deleteRangeHelper(ctx, store, roachpb.Key("a"), roachpb.Key("e")))
				require.NoError(t,
					deleteRangeHelper(ctx, store, roachpb.Key("f"), roachpb.Key("h")))
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
				tc *testcluster.TestCluster,
				store *kvserver.Store,
			) {
				// Data operations that operate on the deleted SSTable should
				// fail.
				require.Regexp(t, "no such file or directory",
					scanHelper(ctx, store, roachpb.Key("a"), roachpb.Key("z")))
				require.Regexp(t, "no such file or directory",
					getHelper(ctx, store, roachpb.Key("c")))
				require.Regexp(t, "no such file or directory",
					putHelper(ctx, store, roachpb.Key("c")))
				require.Regexp(t, "no such file or directory",
					deleteRangeHelper(ctx, store, roachpb.Key("a"), roachpb.Key("z")))

				// Data operations that don't operate on the deleted SSTable
				// should succeed.
				require.NoError(t, scanHelper(ctx, store, roachpb.Key("a"), roachpb.Key("c")))
				require.NoError(t, scanHelper(ctx, store, roachpb.Key("h"), roachpb.Key("z")))
				require.NoError(t, getHelper(ctx, store, roachpb.Key("a")))
				require.NoError(t, getHelper(ctx, store, roachpb.Key("y")))
				require.NoError(t, putHelper(ctx, store, roachpb.Key("a")))
				require.NoError(t, putHelper(ctx, store, roachpb.Key("y")))
				require.NoError(t,
					deleteRangeHelper(ctx, store, roachpb.Key("a"), roachpb.Key("b")))
				require.NoError(t,
					deleteRangeHelper(ctx, store, roachpb.Key("h"), roachpb.Key("i")))
			},
		},
		// TODO(ibrahim): Add split tests after fixing the unexpected error.
		{
			name: "merge with deleted span at range boundaries",
			// Original ranges: [a, d), [d, g), [g, z).
			// The deleted span is:     [d, g).
			deletedExternalSpanStart: roachpb.Key("d"),
			deletedExternalSpanEnd:   roachpb.Key("g"),
			testFunc: func(t *testing.T,
				ctx context.Context,
				tc *testcluster.TestCluster,
				store *kvserver.Store,
			) {
				// Merges don't touch the deleted SSTable so they succeed.
				require.NoError(t, mergeHelper(ctx, store, roachpb.Key("d")))
				require.NoError(t, mergeHelper(ctx, store, roachpb.Key("a")))

				// Make sure that the ranges have been merged correctly.
				desc1 := store.LookupReplica(roachpb.RKey("a")).Desc()
				desc2 := store.LookupReplica(roachpb.RKey("d")).Desc()
				desc3 := store.LookupReplica(roachpb.RKey("g")).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)
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
				tc *testcluster.TestCluster,
				store *kvserver.Store,
			) {
				// Merges don't touch the deleted SSTable so they succeed.
				require.NoError(t, mergeHelper(ctx, store, roachpb.Key("d")))
				require.NoError(t, mergeHelper(ctx, store, roachpb.Key("a")))

				// Make sure that the ranges have been merged correctly.
				desc1 := store.LookupReplica(roachpb.RKey("a")).Desc()
				desc2 := store.LookupReplica(roachpb.RKey("d")).Desc()
				desc3 := store.LookupReplica(roachpb.RKey("g")).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)
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
				tc *testcluster.TestCluster,
				store *kvserver.Store,
			) {
				// Merges don't touch the deleted SSTable so they succeed.
				require.NoError(t, mergeHelper(ctx, store, roachpb.Key("d")))
				require.NoError(t, mergeHelper(ctx, store, roachpb.Key("a")))

				// Make sure that the ranges have been merged correctly.
				desc1 := store.LookupReplica(roachpb.RKey("a")).Desc()
				desc2 := store.LookupReplica(roachpb.RKey("d")).Desc()
				desc3 := store.LookupReplica(roachpb.RKey("g")).Desc()
				require.Equal(t, desc1, desc2)
				require.Equal(t, desc2, desc3)
			},
		},
	}
	for _, testCase := range testCases {
		t.Run(testCase.name, func(t *testing.T) {
			ctx := context.Background()

			tc := testSetup(t, ctx)
			defer tc.Stopper().Stop(ctx)
			store := tc.GetFirstStoreFromServer(t, 0)

			// Create a RangeFeed to ensure that it won't cause crashes during the
			// test.
			_, _, cancel := createRangeFeed(ctx, tc, roachpb.Key("a"), roachpb.Key("z"))
			defer cancel()

			// Create an external SSTable that points to non-existent file.
			require.NoError(t, linkExternalSSTableToFile(ctx, store,
				testCase.deletedExternalSpanStart,
				testCase.deletedExternalSpanEnd, externURI, "non-existent-file"))

			// Run the test function, and make sure that the store is consistent afterward.
			testCase.testFunc(t, ctx, tc, store)

			require.NoError(t, createFile(t, ctx, store, externURI, "file1"))
			require.NoError(t, linkExternalSSTableToFile(ctx, store,
				testCase.deletedExternalSpanStart,
				testCase.deletedExternalSpanEnd, externURI, "file1"))
		})
	}
}
