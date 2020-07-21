// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

type metaRecord struct {
	key  roachpb.Key
	desc *roachpb.RangeDescriptor
}
type metaSlice []metaRecord

// Implementation of sort.Interface.
func (ms metaSlice) Len() int           { return len(ms) }
func (ms metaSlice) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }
func (ms metaSlice) Less(i, j int) bool { return bytes.Compare(ms[i].key, ms[j].key) < 0 }

func meta1Key(key roachpb.RKey) []byte {
	return testutils.MakeKey(keys.Meta1Prefix, key)
}

func meta2Key(key roachpb.RKey) []byte {
	return testutils.MakeKey(keys.Meta2Prefix, key)
}

// TestUpdateRangeAddressing verifies range addressing records are
// correctly updated on creation of new range descriptors.
func TestUpdateRangeAddressing(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())
	store, _ := createTestStore(t, testStoreOpts{createSystemRanges: false}, stopper)
	// When split is false, merging treats the right range as the merged
	// range. With merging, expNewLeft indicates the addressing keys we
	// expect to be removed.
	testCases := []struct {
		split                   bool
		leftStart, leftEnd      roachpb.RKey
		rightStart, rightEnd    roachpb.RKey
		leftExpNew, rightExpNew [][]byte
	}{
		// Start out with whole range.
		{false, roachpb.RKeyMin, roachpb.RKeyMax, roachpb.RKeyMin, roachpb.RKeyMax,
			[][]byte{}, [][]byte{meta1Key(roachpb.RKeyMax), meta2Key(roachpb.RKeyMax)}},
		// Split KeyMin-KeyMax at key "a".
		{true, roachpb.RKeyMin, roachpb.RKey("a"), roachpb.RKey("a"), roachpb.RKeyMax,
			[][]byte{meta1Key(roachpb.RKeyMax), meta2Key(roachpb.RKey("a"))}, [][]byte{meta2Key(roachpb.RKeyMax)}},
		// Split "a"-KeyMax at key "z".
		{true, roachpb.RKey("a"), roachpb.RKey("z"), roachpb.RKey("z"), roachpb.RKeyMax,
			[][]byte{meta2Key(roachpb.RKey("z"))}, [][]byte{meta2Key(roachpb.RKeyMax)}},
		// Split "a"-"z" at key "m".
		{true, roachpb.RKey("a"), roachpb.RKey("m"), roachpb.RKey("m"), roachpb.RKey("z"),
			[][]byte{meta2Key(roachpb.RKey("m"))}, [][]byte{meta2Key(roachpb.RKey("z"))}},
		// Split KeyMin-"a" at meta2(m).
		{true, roachpb.RKeyMin, keys.RangeMetaKey(roachpb.RKey("m")), keys.RangeMetaKey(roachpb.RKey("m")), roachpb.RKey("a"),
			[][]byte{meta1Key(roachpb.RKey("m"))}, [][]byte{meta1Key(roachpb.RKeyMax), meta2Key(roachpb.RKey("a"))}},
		// Split meta2(m)-"a" at meta2(z).
		{true, keys.RangeMetaKey(roachpb.RKey("m")), keys.RangeMetaKey(roachpb.RKey("z")), keys.RangeMetaKey(roachpb.RKey("z")), roachpb.RKey("a"),
			[][]byte{meta1Key(roachpb.RKey("z"))}, [][]byte{meta1Key(roachpb.RKeyMax), meta2Key(roachpb.RKey("a"))}},
		// Split meta2(m)-meta2(z) at meta2(r).
		{true, keys.RangeMetaKey(roachpb.RKey("m")), keys.RangeMetaKey(roachpb.RKey("r")), keys.RangeMetaKey(roachpb.RKey("r")), keys.RangeMetaKey(roachpb.RKey("z")),
			[][]byte{meta1Key(roachpb.RKey("r"))}, [][]byte{meta1Key(roachpb.RKey("z"))}},

		// Now, merge all of our splits backwards...

		// Merge meta2(m)-meta2(z).
		{false, keys.RangeMetaKey(roachpb.RKey("m")), keys.RangeMetaKey(roachpb.RKey("r")), keys.RangeMetaKey(roachpb.RKey("m")), keys.RangeMetaKey(roachpb.RKey("z")),
			[][]byte{meta1Key(roachpb.RKey("r"))}, [][]byte{meta1Key(roachpb.RKey("z"))}},
		// Merge meta2(m)-"a".
		{false, keys.RangeMetaKey(roachpb.RKey("m")), keys.RangeMetaKey(roachpb.RKey("z")), keys.RangeMetaKey(roachpb.RKey("m")), roachpb.RKey("a"),
			[][]byte{meta1Key(roachpb.RKey("z"))}, [][]byte{meta1Key(roachpb.RKeyMax), meta2Key(roachpb.RKey("a"))}},
		// Merge KeyMin-"a".
		{false, roachpb.RKeyMin, keys.RangeMetaKey(roachpb.RKey("m")), roachpb.RKeyMin, roachpb.RKey("a"),
			[][]byte{meta1Key(roachpb.RKey("m"))}, [][]byte{meta1Key(roachpb.RKeyMax), meta2Key(roachpb.RKey("a"))}},
		// Merge "a"-"z".
		{false, roachpb.RKey("a"), roachpb.RKey("m"), roachpb.RKey("a"), roachpb.RKey("z"),
			[][]byte{meta2Key(roachpb.RKey("m"))}, [][]byte{meta2Key(roachpb.RKey("z"))}},
		// Merge "a"-KeyMax.
		{false, roachpb.RKey("a"), roachpb.RKey("z"), roachpb.RKey("a"), roachpb.RKeyMax,
			[][]byte{meta2Key(roachpb.RKey("z"))}, [][]byte{meta2Key(roachpb.RKeyMax)}},
		// Merge KeyMin-KeyMax.
		{false, roachpb.RKeyMin, roachpb.RKey("a"), roachpb.RKeyMin, roachpb.RKeyMax,
			[][]byte{meta2Key(roachpb.RKey("a"))}, [][]byte{meta1Key(roachpb.RKeyMax), meta2Key(roachpb.RKeyMax)}},
	}
	expMetas := metaSlice{}

	for i, test := range testCases {
		left := &roachpb.RangeDescriptor{RangeID: roachpb.RangeID(i * 2), StartKey: test.leftStart, EndKey: test.leftEnd}
		right := &roachpb.RangeDescriptor{RangeID: roachpb.RangeID(i*2 + 1), StartKey: test.rightStart, EndKey: test.rightEnd}
		b := &kv.Batch{}
		if test.split {
			if err := splitRangeAddressing(b, left, right); err != nil {
				t.Fatal(err)
			}
		} else {
			if err := mergeRangeAddressing(b, left, right); err != nil {
				t.Fatal(err)
			}
		}

		// This test constructs an overlapping batch (delete then put on the same
		// key), which is only allowed in a transaction. The test wants to send the
		// batch through the "client" interface (because that's what it used to
		// construct it); unfortunately we can't use the client's transactional
		// interface without sending through a TxnCoordSender (which initializes a
		// transaction id). Also, we need the TxnCoordSender to clean up the
		// intents, otherwise the MVCCScan that the test does below fails.
		actx := testutils.MakeAmbientCtx()
		tcsf := kvcoord.NewTxnCoordSenderFactory(
			kvcoord.TxnCoordSenderFactoryConfig{
				AmbientCtx: actx,
				Settings:   store.cfg.Settings,
				Clock:      store.cfg.Clock,
				Stopper:    stopper,
				Metrics:    kvcoord.MakeTxnMetrics(time.Second),
			},
			store.TestSender(),
		)
		db := kv.NewDB(actx, tcsf, store.cfg.Clock, stopper)
		ctx := context.Background()
		txn := kv.NewTxn(ctx, db, 0 /* gatewayNodeID */)
		if err := txn.Run(ctx, b); err != nil {
			t.Fatal(err)
		}
		if err := txn.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		// Scan meta keys directly from engine. We put this in a retry loop
		// because the application of all of a transactions committed writes
		// is not always synchronous with it committing. Cases where the
		// application of a write is asynchronous are:
		// - the write is on a different range than the transaction's record.
		//   Intent resolution will be asynchronous.
		// - the transaction performed a parallel commit. Explicitly committing
		//   the transaction will be asynchronous.
		// - [not yet implemented] the corresponding Raft log entry is committed
		//   but not applied before acknowledging the write. Applying the write
		//   to RocksDB will be asynchronous.
		var kvs []roachpb.KeyValue
		testutils.SucceedsSoon(t, func() error {
			res, err := storage.MVCCScan(ctx, store.Engine(), keys.MetaMin, keys.MetaMax,
				hlc.MaxTimestamp, storage.MVCCScanOptions{})
			if err != nil {
				// Wait for the intent to be resolved.
				if errors.HasType(err, (*roachpb.WriteIntentError)(nil)) {
					return err
				}
				t.Fatal(err)
			}
			kvs = res.KVs
			return nil
		})
		metas := metaSlice{}
		for _, kv := range kvs {
			scannedDesc := &roachpb.RangeDescriptor{}
			if err := kv.Value.GetProto(scannedDesc); err != nil {
				t.Fatal(err)
			}
			metas = append(metas, metaRecord{key: kv.Key, desc: scannedDesc})
		}

		// Continue to build up the expected metas slice, replacing any earlier
		// version of same key.
		addOrRemoveNew := func(keys [][]byte, desc *roachpb.RangeDescriptor, add bool) {
			for _, n := range keys {
				found := -1
				for i := range expMetas {
					if expMetas[i].key.Equal(roachpb.Key(n)) {
						found = i
						expMetas[i].desc = desc
						break
					}
				}
				if found == -1 && add {
					expMetas = append(expMetas, metaRecord{key: n, desc: desc})
				} else if found != -1 && !add {
					expMetas = append(expMetas[:found], expMetas[found+1:]...)
				}
			}
		}
		addOrRemoveNew(test.leftExpNew, left, test.split /* on split, add; on merge, remove */)
		addOrRemoveNew(test.rightExpNew, right, true)
		sort.Sort(expMetas)

		if test.split {
			if log.V(1) {
				log.Infof(ctx, "test case %d: split %q-%q at %q", i, left.StartKey, right.EndKey, left.EndKey)
			}
		} else {
			if log.V(1) {
				log.Infof(ctx, "test case %d: merge %q-%q + %q-%q", i, left.StartKey, left.EndKey, left.EndKey, right.EndKey)
			}
		}
		for _, meta := range metas {
			if log.V(1) {
				log.Infof(ctx, "%q", meta.key)
			}
		}

		if !reflect.DeepEqual(expMetas, metas) {
			t.Errorf("expected metas don't match")
			if len(expMetas) != len(metas) {
				t.Errorf("len(expMetas) != len(metas); %d != %d", len(expMetas), len(metas))
			} else {
				for j, meta := range expMetas {
					if !meta.key.Equal(metas[j].key) {
						fmt.Printf("%d: expected %q vs %q\n", j, meta.key, metas[j].key)
					}
					if !reflect.DeepEqual(meta.desc, metas[j].desc) {
						fmt.Printf("%d: expected %q vs %q and %s vs %s\n", j, meta.key, metas[j].key, meta.desc, metas[j].desc)
					}
				}
			}
		}
	}
}

// TestUpdateRangeAddressingSplitMeta1 verifies that it's an error to
// attempt to update range addressing records that would allow a split
// of meta1 records.
func TestUpdateRangeAddressingSplitMeta1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	left := &roachpb.RangeDescriptor{StartKey: roachpb.RKeyMin, EndKey: meta1Key(roachpb.RKey("a"))}
	right := &roachpb.RangeDescriptor{StartKey: meta1Key(roachpb.RKey("a")), EndKey: roachpb.RKeyMax}
	if err := splitRangeAddressing(&kv.Batch{}, left, right); err == nil {
		t.Error("expected failure trying to update addressing records for meta1 split")
	}
}
