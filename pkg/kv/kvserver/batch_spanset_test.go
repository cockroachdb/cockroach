// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSpanSetBatchBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("g")})
	outsideKey := storage.MakeMVCCMetadataKey(roachpb.Key("a"))
	outsideKey2 := storage.MakeMVCCMetadataKey(roachpb.Key("b"))
	outsideKey3 := storage.MakeMVCCMetadataKey(roachpb.Key("g"))
	outsideKey4 := storage.MakeMVCCMetadataKey(roachpb.Key("m"))
	insideKey := storage.MakeMVCCMetadataKey(roachpb.Key("c"))
	insideKey2 := storage.MakeMVCCMetadataKey(roachpb.Key("d"))
	insideKey3 := storage.MakeMVCCMetadataKey(roachpb.Key("f"))

	// Write values outside the range that we can try to read later.
	if err := eng.PutUnversioned(outsideKey.Key, []byte("value")); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}
	if err := eng.PutUnversioned(outsideKey3.Key, []byte("value")); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batch := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batch.Close()

	// Writes inside the range work. Write twice for later read testing.
	if err := batch.PutUnversioned(insideKey.Key, []byte("value")); err != nil {
		t.Fatalf("failed to write inside the range: %+v", err)
	}
	if err := batch.PutUnversioned(insideKey2.Key, []byte("value2")); err != nil {
		t.Fatalf("failed to write inside the range: %+v", err)
	}

	// Writes outside the range fail. We try to cover all write methods
	// in the failure case to make sure the CheckAllowed call is
	// present, but we don't attempt successful versions of all
	// methods since those are harder to set up.
	isWriteSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot write undeclared span")
	}

	t.Run("writes before range", func(t *testing.T) {
		if err := batch.ClearUnversioned(outsideKey.Key, storage.ClearOptions{}); !isWriteSpanErr(err) {
			t.Errorf("ClearUnversioned: unexpected error %v", err)
		}
		if err := batch.ClearRawRange(outsideKey.Key, outsideKey2.Key, true, true); !isWriteSpanErr(err) {
			t.Errorf("ClearRawRange: unexpected error %v", err)
		}
		{
			err := batch.ClearMVCCIteratorRange(outsideKey.Key, outsideKey2.Key, true, true)
			if !isWriteSpanErr(err) {
				t.Errorf("ClearMVCCIteratorRange: unexpected error %v", err)
			}
		}
		if err := batch.Merge(outsideKey, nil); !isWriteSpanErr(err) {
			t.Errorf("Merge: unexpected error %v", err)
		}
		if err := batch.PutUnversioned(outsideKey.Key, nil); !isWriteSpanErr(err) {
			t.Errorf("PutUnversioned: unexpected error %v", err)
		}
	})

	t.Run("writes after range", func(t *testing.T) {
		if err := batch.ClearUnversioned(outsideKey3.Key, storage.ClearOptions{}); !isWriteSpanErr(err) {
			t.Errorf("ClearUnversioned: unexpected error %v", err)
		}
		if err := batch.ClearRawRange(insideKey2.Key, outsideKey4.Key, true, true); !isWriteSpanErr(err) {
			t.Errorf("ClearRawRange: unexpected error %v", err)
		}
		{
			err := batch.ClearMVCCIteratorRange(outsideKey2.Key, outsideKey4.Key, true, true)
			if !isWriteSpanErr(err) {
				t.Errorf("ClearMVCCIteratorRange: unexpected error %v", err)
			}
		}
		if err := batch.Merge(outsideKey3, nil); !isWriteSpanErr(err) {
			t.Errorf("Merge: unexpected error %v", err)
		}
		if err := batch.PutUnversioned(outsideKey3.Key, nil); !isWriteSpanErr(err) {
			t.Errorf("PutUnversioned: unexpected error %v", err)
		}
	})

	t.Run("reads inside range", func(t *testing.T) {
		require.Equal(t, []byte("value"), storageutils.MVCCGetRaw(t, batch, insideKey))
		require.NoError(t, batch.MVCCIterate(context.Background(), insideKey.Key, insideKey2.Key,
			storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
			fs.UnknownReadCategory, func(v storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
				return nil
			}))
	})

	// Reads outside the range fail.
	isReadSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot read undeclared span")
	}

	t.Run("reads before range", func(t *testing.T) {
		if _, err := storageutils.MVCCGetRawWithError(t, batch, outsideKey); !isReadSpanErr(err) {
			t.Errorf("MVCCGet: unexpected error %v", err)
		}
		if err := batch.MVCCIterate(context.Background(), outsideKey.Key, insideKey2.Key,
			storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
			fs.UnknownReadCategory, func(v storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
				return errors.Errorf("unexpected callback: %v", v)
			}); !isReadSpanErr(err) {
			t.Errorf("MVCCIterate: unexpected error %v", err)
		}
	})

	t.Run("reads after range", func(t *testing.T) {
		if _, err := storageutils.MVCCGetRawWithError(t, batch, outsideKey3); !isReadSpanErr(err) {
			t.Errorf("MVCCGet: unexpected error %v", err)
		}
		if err := batch.MVCCIterate(context.Background(), insideKey2.Key, outsideKey4.Key,
			storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
			fs.UnknownReadCategory, func(v storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
				return errors.Errorf("unexpected callback: %v", v)
			}); !isReadSpanErr(err) {
			t.Errorf("MVCCIterate: unexpected error %v", err)
		}
	})

	t.Run("forward scans", func(t *testing.T) {
		iter, err := batch.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		// MVCCIterators check boundaries on seek and next/prev
		iter.SeekGE(outsideKey)
		if _, err := iter.Valid(); !isReadSpanErr(err) {
			t.Fatalf("Seek: unexpected error %v", err)
		}
		iter.SeekGE(outsideKey3)
		if _, err := iter.Valid(); !isReadSpanErr(err) {
			t.Fatalf("Seek: unexpected error %v", err)
		}
		// Seeking back in bounds restores validity.
		iter.SeekGE(insideKey)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.UnsafeKey(), insideKey) {
			t.Fatalf("expected key %s, got %s", insideKey, iter.UnsafeKey())
		}
		iter.Next()
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.UnsafeKey(), insideKey2) {
			t.Fatalf("expected key %s, got %s", insideKey2, iter.UnsafeKey())
		}
		// Scan out of bounds.
		iter.Next()
		if ok, err := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.UnsafeKey())
		} else if err != nil {
			// Scanning out of bounds sets Valid() to false but is not an error.
			t.Errorf("unexpected error on iterator: %+v", err)
		}
	})

	// Same test in reverse. We commit the batch and wrap an iterator on
	// the raw engine because we don't support bidirectional iteration
	// over a pending batch.
	if err := batch.Commit(true); err != nil {
		t.Fatal(err)
	}

	t.Run("reverse scans", func(t *testing.T) {
		innerIter, err := eng.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		iter := spanset.NewIterator(innerIter, &ss)

		defer iter.Close()
		iter.SeekLT(outsideKey4)
		if _, err := iter.Valid(); !isReadSpanErr(err) {
			t.Fatalf("SeekLT: unexpected error %v", err)
		}
		// Seeking back in bounds restores validity.
		iter.SeekLT(insideKey3)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.UnsafeKey(), insideKey2) {
			t.Fatalf("expected key %s, got %s", insideKey2, iter.UnsafeKey())
		}
		iter.Prev()
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.UnsafeKey(), insideKey) {
			t.Fatalf("expected key %s, got %s", insideKey, iter.UnsafeKey())
		}
		// Scan out of bounds.
		iter.Prev()
		if ok, err := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.UnsafeKey())
		} else if err != nil {
			t.Errorf("unexpected error on iterator: %+v", err)
		}

		// Seeking back in bounds restores validity.
		iter.SeekLT(insideKey2)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		// SeekLT to the lower bound is invalid.
		iter.SeekLT(insideKey)
		if ok, err := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.UnsafeKey())
		} else if !isReadSpanErr(err) {
			t.Fatalf("SeekLT: unexpected error %v", err)
		}
		// SeekLT to upper bound restores validity.
		iter.SeekLT(outsideKey3)
		if ok, err := iter.Valid(); !ok || err != nil {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
	})
}

func TestSpanSetBatchTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddMVCC(spanset.SpanReadOnly,
		roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}, hlc.Timestamp{WallTime: 2})
	ss.AddMVCC(spanset.SpanReadWrite,
		roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("f")}, hlc.Timestamp{WallTime: 2})

	rkey := storage.MakeMVCCMetadataKey(roachpb.Key("b"))
	wkey := storage.MakeMVCCMetadataKey(roachpb.Key("e"))

	value := []byte("value")

	// Write value that we can try to read later.
	if err := eng.PutUnversioned(rkey.Key, value); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batchNonMVCC := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 0})
	defer batchNonMVCC.Close()

	batchBefore := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 1})
	defer batchBefore.Close()

	batchDuring := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 2})
	defer batchDuring.Close()

	batchAfter := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 3})
	defer batchAfter.Close()

	// Writes.
	for _, batch := range []storage.Batch{batchAfter, batchDuring} {
		if err := batch.PutUnversioned(wkey.Key, value); err != nil {
			t.Fatalf("failed to write inside the range at same or greater ts than latch declaration: %+v", err)
		}
	}

	for _, batch := range []storage.Batch{batchBefore, batchNonMVCC} {
		if err := batch.PutUnversioned(wkey.Key, value); err == nil {
			t.Fatalf("was able to write inside the range at ts less than latch declaration: %+v", err)
		}
	}

	// We try to cover all write methods in the failure case to make sure
	// the CheckAllowedAt call is present, but we don't attempt to successful
	// versions of all methods since those are harder to set up.
	isWriteSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot write undeclared span")
	}

	for _, batch := range []storage.Batch{batchBefore, batchNonMVCC} {
		if err := batch.ClearUnversioned(wkey.Key, storage.ClearOptions{}); !isWriteSpanErr(err) {
			t.Errorf("ClearUnversioned: unexpected error %v", err)
		}
		{
			err := batch.ClearMVCCIteratorRange(wkey.Key, wkey.Key, true, true)
			if !isWriteSpanErr(err) {
				t.Errorf("ClearMVCCIteratorRange: unexpected error %v", err)
			}
		}
		if err := batch.Merge(wkey, nil); !isWriteSpanErr(err) {
			t.Errorf("Merge: unexpected error %v", err)
		}
		if err := batch.PutUnversioned(wkey.Key, nil); !isWriteSpanErr(err) {
			t.Errorf("PutUnversioned: unexpected error %v", err)
		}
	}

	// Reads.
	for _, batch := range []storage.Batch{batchBefore, batchDuring} {
		require.Equal(t, value, storageutils.MVCCGetRaw(t, batch, rkey))
	}

	isReadSpanErr := func(err error) bool {
		return testutils.IsError(err, "cannot read undeclared span")
	}

	for _, batch := range []storage.Batch{batchAfter, batchNonMVCC} {
		if _, err := storageutils.MVCCGetRawWithError(t, batch, rkey); !isReadSpanErr(err) {
			t.Errorf("Get: unexpected error %v", err)
		}

		if err := batch.MVCCIterate(context.Background(), rkey.Key, rkey.Key,
			storage.MVCCKeyAndIntentsIterKind, storage.IterKeyTypePointsOnly,
			fs.UnknownReadCategory, func(v storage.MVCCKeyValue, _ storage.MVCCRangeKeyStack) error {
				return errors.Errorf("unexpected callback: %v", v)
			}); !isReadSpanErr(err) {
			t.Errorf("MVCCIterate: unexpected error %v", err)
		}
	}
}

func TestSpanSetIteratorTimestamps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: roachpb.Key("a"), EndKey: roachpb.Key("c")}, hlc.Timestamp{WallTime: 1})
	ss.AddMVCC(spanset.SpanReadOnly, roachpb.Span{
		Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}, hlc.Timestamp{WallTime: 2})

	k1, v1 := storage.MakeMVCCMetadataKey(roachpb.Key("b")), []byte("b-value")
	k1e := storage.EngineKey{Key: k1.Key}
	k2, v2 := storage.MakeMVCCMetadataKey(roachpb.Key("d")), []byte("d-value")
	k2e := storage.EngineKey{Key: k2.Key}

	// Write values that we can try to read later.
	if err := eng.PutUnversioned(k1.Key, v1); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}
	if err := eng.PutUnversioned(k2.Key, v2); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batchNonMVCC := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 0})
	defer batchNonMVCC.Close()

	batchAt1 := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 1})
	defer batchAt1.Close()

	batchAt2 := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 2})
	defer batchAt2.Close()

	batchAt3 := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 3})
	defer batchAt3.Close()

	func() {
		// When accessing at t=1, we're able to read through latches declared at t=1 and t=2.
		iter, err := batchAt1.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		iter.SeekGE(k1)
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.UnsafeKey(), k1) {
			t.Fatalf("expected key %s, got %s", k1, iter.UnsafeKey())
		}

		iter.Next()
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.UnsafeKey(), k2) {
			t.Fatalf("expected key %s, got %s", k2, iter.UnsafeKey())
		}
	}()

	func() {
		// When accessing at t=2, we're only able to read through the latch declared at t=2.
		iter, err := batchAt2.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		iter.SeekGE(k1)
		if ok, _ := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.UnsafeKey())
		}

		iter.SeekGE(k2)
		if ok, err := iter.Valid(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		if !reflect.DeepEqual(iter.UnsafeKey(), k2) {
			t.Fatalf("expected key %s, got %s", k2, iter.UnsafeKey())
		}
	}()

	for _, batch := range []storage.Batch{batchAt3, batchNonMVCC} {
		// When accessing at t=3, we're unable to read through any of the declared latches.
		// Same is true when accessing without a timestamp.
		iter, err := batch.NewMVCCIterator(context.Background(), storage.MVCCKeyAndIntentsIterKind, storage.IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		iter.SeekGE(k1)
		if ok, _ := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.UnsafeKey())
		}

		iter.SeekGE(k2)
		if ok, _ := iter.Valid(); ok {
			t.Fatalf("expected invalid iterator; found valid at key %s", iter.UnsafeKey())
		}
	}

	// The behavior is the same as above for an EngineIterator.
	func() {
		// When accessing at t=1, we're able to read through latches declared at t=1 and t=2.
		iter, err := batchAt1.NewEngineIterator(context.Background(), storage.IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		if ok, err := iter.SeekEngineKeyGE(k1e); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		engineKey, err := iter.EngineKey()
		if err != nil {
			t.Fatalf("expected no error, got err=%v", err)
		}
		if !reflect.DeepEqual(engineKey, k1e) {
			t.Fatalf("expected key %s, got %s", k1e, engineKey)
		}

		if ok, err := iter.NextEngineKey(); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		engineKey, err = iter.EngineKey()
		if err != nil {
			t.Fatalf("expected no error, got err=%v", err)
		}
		if !reflect.DeepEqual(engineKey, k2e) {
			t.Fatalf("expected key %s, got %s", k2e, engineKey)
		}
	}()

	func() {
		// When accessing at t=2, we're only able to read through the latch declared at t=2.
		iter, err := batchAt2.NewEngineIterator(context.Background(), storage.IterOptions{UpperBound: roachpb.KeyMax})
		if err != nil {
			t.Fatal(err)
		}
		defer iter.Close()

		if ok, _ := iter.SeekEngineKeyGE(k1e); ok {
			engineKey, err := iter.EngineKey()
			t.Fatalf("expected invalid iterator; found valid at key %s, err=%v", engineKey, err)
		}

		if ok, err := iter.SeekEngineKeyGE(k2e); !ok {
			t.Fatalf("expected valid iterator, err=%v", err)
		}
		engineKey, err := iter.EngineKey()
		if err != nil {
			t.Fatalf("expected no error, got err=%v", err)
		}
		if !reflect.DeepEqual(engineKey, k2e) {
			t.Fatalf("expected key %s, got %s", k2e, engineKey)
		}
	}()
}

func TestSpanSetNonMVCCBatch(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()

	var ss spanset.SpanSet
	ss.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("c")})
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("d"), EndKey: roachpb.Key("f")})

	rkey := storage.MakeMVCCMetadataKey(roachpb.Key("b"))
	wkey := storage.MakeMVCCMetadataKey(roachpb.Key("e"))

	value := []byte("value")

	// Write value that we can try to read later.
	if err := eng.PutUnversioned(rkey.Key, value); err != nil {
		t.Fatalf("direct write failed: %+v", err)
	}

	batchNonMVCC := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batchNonMVCC.Close()

	batchMVCC := spanset.NewBatchAt(eng.NewBatch(), &ss, hlc.Timestamp{WallTime: 1})
	defer batchMVCC.Close()

	// Writes.
	for _, batch := range []storage.Batch{batchNonMVCC, batchMVCC} {
		if err := batch.PutUnversioned(wkey.Key, value); err != nil {
			t.Fatalf("write disallowed through non-MVCC latch: %+v", err)
		}
	}

	// Reads.
	for _, batch := range []storage.Batch{batchNonMVCC, batchMVCC} {
		require.Equal(t, value, storageutils.MVCCGetRaw(t, batch, rkey))
	}
}

// TestSpanSetMVCCResolveWriteIntentRange verifies that
// MVCCResolveWriteIntentRange does not stray outside of the passed-in
// key range (which it only used to do in this corner case tested here).
//
// See #20894.
func TestSpanSetMVCCResolveWriteIntentRange(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	eng := storage.NewDefaultInMemForTesting()
	defer eng.Close()
	ctx := context.Background()
	value := roachpb.MakeValueFromString("irrelevant")
	if _, err := storage.MVCCPut(
		ctx,
		eng,
		roachpb.Key("b"),
		hlc.Timestamp{WallTime: 10}, // irrelevant
		value,
		storage.MVCCWriteOptions{}, // irrelevant
	); err != nil {
		t.Fatal(err)
	}
	var ss spanset.SpanSet
	ss.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b\x00")})
	batch := spanset.NewBatch(eng.NewBatch(), &ss)
	defer batch.Close()
	intent := roachpb.LockUpdate{
		Span:   roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b\x00")},
		Txn:    enginepb.TxnMeta{ID: uuid.MakeV4()}, // unused
		Status: roachpb.PENDING,
	}
	if _, _, _, _, _, err := storage.MVCCResolveWriteIntentRange(
		ctx, batch, nil /* ms */, intent, storage.MVCCResolveWriteIntentRangeOptions{},
	); err != nil {
		t.Fatal(err)
	}
}
