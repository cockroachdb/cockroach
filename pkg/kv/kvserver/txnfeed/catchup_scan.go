// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txnfeed

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// CatchUpScan iterates over transaction record keys in the given range and
// emits TxnFeedCommitted events for all COMMITTED transaction records whose
// commit timestamp is after startTS (exclusive). The scan reads from the
// provided engine snapshot.
//
// Transaction records are range-local keys with the LocalTransactionSuffix.
// The scan uses a raw MVCCIterator to iterate all MVCC versions. This is
// necessary because a transaction record may have a tombstone on top (written
// by intent resolution or GC) that hides the COMMITTED record underneath.
// Iterating all versions lets us find COMMITTED records even when a newer
// tombstone exists.
func CatchUpScan(
	ctx context.Context,
	snap storage.Reader,
	rangeStartKey, rangeEndKey roachpb.RKey,
	startTS hlc.Timestamp,
	emitFn func(event *kvpb.TxnFeedEvent) error,
) error {
	startKey := keys.MakeRangeKeyPrefix(rangeStartKey)
	endKey := keys.MakeRangeKeyPrefix(rangeEndKey)

	// MinTimestamp is an optimization hint that allows the iterator to skip MVCC
	// versions below the given timestamp. This is safe because a transaction
	// record's MVCC write timestamp is always >= its commit timestamp, so any
	// record committed after startTS must have a write timestamp > startTS.
	iter, err := snap.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		KeyTypes:     storage.IterKeyTypePointsOnly,
		LowerBound:   startKey,
		UpperBound:   endKey,
		MinTimestamp: startTS.Next(),
		MaxTimestamp: hlc.MaxTimestamp,
		ReadCategory: fs.RangefeedReadCategory,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: startKey})
	for {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		key := iter.UnsafeKey()

		_, suffix, _, err := keys.DecodeRangeKey(key.Key)
		if err != nil {
			return errors.Wrap(err, "decoding range-local key")
		}
		if !bytes.Equal(suffix, keys.LocalTransactionSuffix.AsRawKey()) {
			iter.NextKey()
			continue
		}

		// Skip tombstones — the committed record may be at an older version.
		_, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
		if err != nil {
			return errors.Wrap(err, "checking tombstone")
		}
		if isTombstone {
			iter.Next()
			continue
		}

		v, err := iter.UnsafeValue()
		if err != nil {
			return errors.Wrap(err, "reading value")
		}
		mvccVal, err := storage.DecodeMVCCValue(v)
		if err != nil {
			return errors.Wrap(err, "decoding MVCC value")
		}

		var txn roachpb.Transaction
		if err := mvccVal.Value.GetProto(&txn); err != nil {
			return errors.Wrap(err, "unmarshaling transaction record")
		}

		if txn.Status != roachpb.COMMITTED {
			// A non-committed record will never have an older MVCC version
			// that is committed, so skip all versions of this key.
			iter.NextKey()
			continue
		}

		// A transaction record's MVCC timestamp may be more recent than startTS
		// even if the transaction committed before startTS (e.g. the record was
		// pushed). Filter on the actual commit timestamp.
		if txn.WriteTimestamp.LessEq(startTS) {
			iter.NextKey()
			continue
		}

		if err := emitFn(&kvpb.TxnFeedEvent{
			Committed: &kvpb.TxnFeedCommitted{
				TxnID:           txn.ID,
				AnchorKey:       txn.Key,
				CommitTimestamp: txn.WriteTimestamp,
				WriteSpans:      txn.LockSpans,
				ReadSpans:       txn.ReadSpans,
			},
		}); err != nil {
			return err
		}
		// Found the committed record; skip remaining versions of this key.
		iter.NextKey()
	}
}

// ScanUnresolvedTxnRecords scans for non-finalized (PENDING or STAGING)
// transaction records in the given range. For each such record, it calls fn
// with the transaction's ID and WriteTimestamp. This is used during processor
// initialization to populate the unresolved transaction queue before
// processing live events.
//
// Unlike CatchUpScan, this function only needs the latest version of each
// key (no tombstone digging) and has no timestamp filter.
func ScanUnresolvedTxnRecords(
	ctx context.Context,
	snap storage.Reader,
	rangeStartKey, rangeEndKey roachpb.RKey,
	fn func(txnID uuid.UUID, writeTS hlc.Timestamp),
) error {
	startKey := keys.MakeRangeKeyPrefix(rangeStartKey)
	endKey := keys.MakeRangeKeyPrefix(rangeEndKey)

	iter, err := snap.NewMVCCIterator(ctx, storage.MVCCKeyIterKind, storage.IterOptions{
		KeyTypes:     storage.IterKeyTypePointsOnly,
		LowerBound:   startKey,
		UpperBound:   endKey,
		ReadCategory: fs.RangefeedReadCategory,
	})
	if err != nil {
		return err
	}
	defer iter.Close()

	iter.SeekGE(storage.MVCCKey{Key: startKey})
	for {
		ok, err := iter.Valid()
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}

		key := iter.UnsafeKey()

		_, suffix, _, err := keys.DecodeRangeKey(key.Key)
		if err != nil {
			return errors.Wrap(err, "decoding range-local key")
		}
		if !bytes.Equal(suffix, keys.LocalTransactionSuffix.AsRawKey()) {
			iter.NextKey()
			continue
		}

		// Skip tombstones — we only care about live records.
		_, isTombstone, err := iter.MVCCValueLenAndIsTombstone()
		if err != nil {
			return errors.Wrap(err, "checking tombstone")
		}
		if isTombstone {
			iter.NextKey()
			continue
		}

		v, err := iter.UnsafeValue()
		if err != nil {
			return errors.Wrap(err, "reading value")
		}
		mvccVal, err := storage.DecodeMVCCValue(v)
		if err != nil {
			return errors.Wrap(err, "decoding MVCC value")
		}

		var txn roachpb.Transaction
		if err := mvccVal.Value.GetProto(&txn); err != nil {
			return errors.Wrap(err, "unmarshaling transaction record")
		}

		if !txn.Status.IsFinalized() {
			fn(txn.ID, txn.WriteTimestamp)
		}

		iter.NextKey()
	}
}
