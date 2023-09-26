// Copyright 2014 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// CollectIntentRows collects the provisional key-value pairs for each intent
// provided.
//
// The method accepts a reader and flag indicating whether a prefix iterator
// should be used when creating an iterator from the reader. This flexibility
// works around a limitation of the Engine.NewReadOnly interface where prefix
// iterators and non-prefix iterators pulled from the same read-only engine are
// not guaranteed to provide a consistent snapshot of the underlying engine.
// This function expects to be able to retrieve the corresponding provisional
// value for each of the provided intents. As such, it is critical that it
// observes the engine in the same state that it was in when the intent keys
// were originally collected. Because of this, callers are tasked with
// indicating whether the intents were originally collected using a prefix
// iterator or not.
//
// TODO(nvanbenschoten): remove the usePrefixIter complexity when we're fully on
// Pebble and can guarantee that all iterators created from a read-only engine
// are consistent.
//
// TODO(nvanbenschoten): mvccGetInternal should return the intent values
// directly when reading at the READ_UNCOMMITTED consistency level. Since this
// is only currently used for range lookups and when watching for a merge (both
// of which are off the hot path), this is ok for now.
func CollectIntentRows(
	ctx context.Context, reader storage.Reader, usePrefixIter bool, intents []roachpb.Intent,
) ([]roachpb.KeyValue, error) {
	if len(intents) == 0 {
		return nil, nil
	}
	res := make([]roachpb.KeyValue, 0, len(intents))
	for i := range intents {
		kv, err := readProvisionalVal(ctx, reader, usePrefixIter, &intents[i])
		if err != nil {
			if errors.HasType(err, (*kvpb.LockConflictError)(nil)) ||
				errors.HasType(err, (*kvpb.ReadWithinUncertaintyIntervalError)(nil)) {
				log.Fatalf(ctx, "unexpected %T in CollectIntentRows: %+v", err, err)
			}
			return nil, err
		}
		if kv.Value.IsPresent() {
			res = append(res, kv)
		}
	}
	return res, nil
}

// readProvisionalVal retrieves the provisional value for the provided intent
// using the reader and the specified access method (i.e. with or without the
// use of a prefix iterator). The function returns an empty KeyValue if the
// intent is found to contain a deletion tombstone as its provisional value.
func readProvisionalVal(
	ctx context.Context, reader storage.Reader, usePrefixIter bool, intent *roachpb.Intent,
) (roachpb.KeyValue, error) {
	if usePrefixIter {
		valRes, err := storage.MVCCGetAsTxn(
			ctx, reader, intent.Key, intent.Txn.WriteTimestamp, intent.Txn,
		)
		if err != nil {
			return roachpb.KeyValue{}, err
		}
		if valRes.Value == nil {
			// Intent is a deletion.
			return roachpb.KeyValue{}, nil
		}
		return roachpb.KeyValue{Key: intent.Key, Value: *valRes.Value}, nil
	}
	res, err := storage.MVCCScanAsTxn(
		ctx, reader, intent.Key, intent.Key.Next(), intent.Txn.WriteTimestamp, intent.Txn,
	)
	if err != nil {
		return roachpb.KeyValue{}, err
	}
	if len(res.KVs) > 1 {
		log.Fatalf(ctx, "multiple key-values returned from single-key scan: %+v", res.KVs)
	} else if len(res.KVs) == 0 {
		// Intent is a deletion.
		return roachpb.KeyValue{}, nil
	}
	return res.KVs[0], nil

}

// acquireLocksOnKeys acquires locks on each of the keys in the result of a
// {,Reverse}ScanRequest. The locks are held by the specified transaction with
// the supplied locks strength and durability. The list of LockAcquisitions is
// returned to the caller, which the caller must accumulate in its result set.
//
// It is possible to run into a lock conflict error when trying to acquire a
// lock on one of the keys. In such cases, a LockConflictError is returned to
// the caller.
func acquireLocksOnKeys(
	ctx context.Context,
	readWriter storage.ReadWriter,
	txn *roachpb.Transaction,
	str lock.Strength,
	dur lock.Durability,
	scanFmt kvpb.ScanFormat,
	scanRes *storage.MVCCScanResult,
) ([]roachpb.LockAcquisition, error) {
	acquiredLocks := make([]roachpb.LockAcquisition, scanRes.NumKeys)
	switch scanFmt {
	case kvpb.BATCH_RESPONSE:
		var i int
		err := storage.MVCCScanDecodeKeyValues(scanRes.KVData, func(key storage.MVCCKey, _ []byte) error {
			k := copyKey(key.Key)
			acq, err := acquireLockOnKey(ctx, readWriter, txn, str, dur, k)
			if err != nil {
				return err
			}
			acquiredLocks[i] = acq
			i++
			return nil
		})
		if err != nil {
			return nil, err
		}
		return acquiredLocks, nil
	case kvpb.KEY_VALUES:
		for i, row := range scanRes.KVs {
			k := copyKey(row.Key)
			acq, err := acquireLockOnKey(ctx, readWriter, txn, str, dur, k)
			if err != nil {
				return nil, err
			}
			acquiredLocks[i] = acq
		}
		return acquiredLocks, nil
	case kvpb.COL_BATCH_RESPONSE:
		return nil, errors.AssertionFailedf("unexpectedly acquiring unreplicated locks with COL_BATCH_RESPONSE scan format")
	default:
		panic("unexpected scanFormat")
	}
}

// acquireLockOnKey acquires a lock on the specified key. The lock is acquired
// by the specified transaction with the supplied lock strength and durability.
// The resultant lock acquisition struct is returned, which the caller must
// accumulate in its result set.
//
// It is possible for lock acquisition to run into a lock conflict error, in
// which case a LockConflictError is returned to the caller.
func acquireLockOnKey(
	ctx context.Context,
	readWriter storage.ReadWriter,
	txn *roachpb.Transaction,
	str lock.Strength,
	dur lock.Durability,
	key roachpb.Key,
) (roachpb.LockAcquisition, error) {
	// TODO(arul,nvanbenschoten): For now, we're only checking whether we have
	// access to a legit pebble.Writer for replicated lock acquisition. We're not
	// actually acquiring a replicated lock -- we can only do so once they're
	// fully supported in the storage package. Until then, we grab an unreplicated
	// lock regardless of what the caller asked us to do.
	if dur == lock.Replicated {
		// ShouldWriteLocalTimestamp is only implemented by a pebble.Writer; it'll
		// panic if we were on the read-only evaluation path, and only had access to
		// a pebble.ReadOnly.
		readWriter.ShouldWriteLocalTimestamps(ctx)
		// Regardless of what the caller asked for, we'll give it an unreplicated
		// lock.
		dur = lock.Unreplicated
	}
	switch dur {
	case lock.Unreplicated:
		// TODO(arul,nvanbenschoten): Call into MVCCCheckForAcquireLockHere.
	case lock.Replicated:
		// TODO(arul,nvanbenschoten): Call into MVCCAcquireLock here.
	default:
		panic("unexpected lock durability")
	}
	acq := roachpb.MakeLockAcquisition(txn, key, dur, str)
	return acq, nil
}

// copyKey copies the provided roachpb.Key into a new byte slice, returning the
// copy. It is used in acquireLocksOnKeys for two reasons:
//  1. the keys in an MVCCScanResult, regardless of the scan format used, point
//     to a small number of large, contiguous byte slices. These "MVCCScan
//     batches" contain keys and their associated values in the same backing
//     array. To avoid holding these entire backing arrays in memory and
//     preventing them from being garbage collected indefinitely, we copy the
//     key slices before coupling their lifetimes to those of the constructed
//     lock acquisitions.
//  2. the KV API has a contract that byte slices returned from KV will not be
//     mutated by higher levels. However, we have seen cases (e.g.#64228) where
//     this contract is broken due to bugs. To defensively guard against this
//     class of memory aliasing bug and prevent keys associated with unreplicated
//     locks from being corrupted, we copy them.
func copyKey(k roachpb.Key) roachpb.Key {
	k2 := make([]byte, len(k))
	copy(k2, k)
	return k2
}
