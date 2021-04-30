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

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
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
			if errors.HasType(err, (*roachpb.WriteIntentError)(nil)) ||
				errors.HasType(err, (*roachpb.ReadWithinUncertaintyIntervalError)(nil)) {
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
		val, _, err := storage.MVCCGetAsTxn(
			ctx, reader, intent.Key, intent.Txn.WriteTimestamp, intent.Txn,
		)
		if err != nil {
			return roachpb.KeyValue{}, err
		}
		if val == nil {
			// Intent is a deletion.
			return roachpb.KeyValue{}, nil
		}
		return roachpb.KeyValue{Key: intent.Key, Value: *val}, nil
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

// acquireUnreplicatedLocksOnKeys adds an unreplicated lock acquisition by the
// transaction to the provided result.Result for each key in the scan result.
func acquireUnreplicatedLocksOnKeys(
	res *result.Result,
	txn *roachpb.Transaction,
	scanFmt roachpb.ScanFormat,
	scanRes *storage.MVCCScanResult,
) error {
	res.Local.AcquiredLocks = make([]roachpb.LockAcquisition, scanRes.NumKeys)
	switch scanFmt {
	case roachpb.BATCH_RESPONSE:
		var i int
		return storage.MVCCScanDecodeKeyValues(scanRes.KVData, func(key storage.MVCCKey, _ []byte) error {
			res.Local.AcquiredLocks[i] = roachpb.MakeLockAcquisition(txn, copyKey(key.Key), lock.Unreplicated)
			i++
			return nil
		})
	case roachpb.KEY_VALUES:
		for i, row := range scanRes.KVs {
			res.Local.AcquiredLocks[i] = roachpb.MakeLockAcquisition(txn, copyKey(row.Key), lock.Unreplicated)
		}
		return nil
	default:
		panic("unexpected scanFormat")
	}
}

// copyKey copies the provided roachpb.Key into a new byte slice, returning the
// copy. It is used in acquireUnreplicatedLocksOnKeys for two reasons:
// 1. the keys in an MVCCScanResult, regardless of the scan format used, point
//    to a small number of large, contiguous byte slices. These "MVCCScan
//    batches" contain keys and their associated values in the same backing
//    array. To avoid holding these entire backing arrays in memory and
//    preventing them from being garbage collected indefinitely, we copy the key
//    slices before coupling their lifetimes to those of unreplicated locks.
// 2. the KV API has a contract that byte slices returned from KV will not be
//    mutated by higher levels. However, we have seen cases (e.g.#64228) where
//    this contract is broken due to bugs. To defensively guard against this
//    class of memory aliasing bug and prevent keys associated with unreplicated
//    locks from being corrupted, we copy them.
func copyKey(k roachpb.Key) roachpb.Key {
	k2 := make([]byte, len(k))
	copy(k2, k)
	return k2
}
