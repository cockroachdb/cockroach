// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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

// acquireLocksOnKeys checks for conflicts, and if none are found, acquires
// locks on each of the keys in the result of a {,Reverse}ScanRequest. The
// acquired locks are held by the specified transaction[1] with the supplied
// lock strength and durability. The list of LockAcquisitions is returned to the
// caller, which the caller must accumulate in its result set.
//
// Even though the function is called post evaluation, at which point requests
// have already sequenced with all locks in the in-memory lock table, there may
// still be (currently undiscovered) replicated locks. This is because the
// in-memory lock table only has a partial view of all locks for a range.
// Therefore, the first thing we do is check for replicated lock conflicts that
// may have been missed. If any are found, a LockConflictError is returned to
// the caller.
//
// [1] The caller is allowed to pass in a nil transaction; this means that
// acquireLocksOnKeys can be called on behalf of non-transactional requests.
// Non-transactional requests are not allowed to hold locks that outlive the
// lifespan of their request. As such, an empty list is returned for them.
// However, non-transactional requests do conflict with locks held by concurrent
// transactional requests, so they may return a LockConflictError.
func acquireLocksOnKeys(
	ctx context.Context,
	readWriter storage.ReadWriter,
	txn *roachpb.Transaction,
	str lock.Strength,
	dur lock.Durability,
	scanFmt kvpb.ScanFormat,
	scanRes *storage.MVCCScanResult,
	ms *enginepb.MVCCStats,
	settings *cluster.Settings,
) ([]roachpb.LockAcquisition, error) {
	acquiredLocks := make([]roachpb.LockAcquisition, 0, scanRes.NumKeys)
	switch scanFmt {
	case kvpb.BATCH_RESPONSE:
		err := storage.MVCCScanDecodeKeyValues(scanRes.KVData, func(key storage.MVCCKey, _ []byte) error {
			k := copyKey(key.Key)
			acq, err := acquireLockOnKey(ctx, readWriter, txn, str, dur, k, ms, settings)
			if err != nil {
				return err
			}
			if !acq.Empty() {
				acquiredLocks = append(acquiredLocks, acq)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
		return acquiredLocks, nil
	case kvpb.KEY_VALUES:
		for _, row := range scanRes.KVs {
			k := copyKey(row.Key)
			acq, err := acquireLockOnKey(ctx, readWriter, txn, str, dur, k, ms, settings)
			if err != nil {
				return nil, err
			}
			if !acq.Empty() {
				acquiredLocks = append(acquiredLocks, acq)
			}
		}
		return acquiredLocks, nil
	case kvpb.COL_BATCH_RESPONSE:
		return nil, errors.AssertionFailedf("unexpectedly acquiring unreplicated locks with COL_BATCH_RESPONSE scan format")
	default:
		panic("unexpected scanFormat")
	}
}

// acquireLockOnKey checks for conflicts, and if non are found, acquires a lock
// on the specified key. The lock is acquired by the specified transaction[1]
// with the supplied lock strength and durability. The resultant lock
// acquisition struct is returned, which the caller must accumulate in its
// result set.
//
// Even though the function is called post evaluation, at which point requests
// have already sequenced with all locks in the in-memory lock table, there may
// still be (currently undiscovered) replicated locks. This is because the
// in-memory lock table only has a partial view of all locks for a range.
// Therefore, the first thing we do is check for replicated lock conflicts that
// may have been missed. If any are found, a LockConflictError is returned to
// the caller.
//
// [1] The caller is allowed to pass in a nil transaction; this means that
// acquireLockOnKey can be called on behalf of non-transactional requests.
// Non-transactional requests are not allowed to hold locks that outlive the
// lifespan of their request. As such, an empty lock acquisition is returned for
// them. However, non-transactional requests do conflict with locks held by
// concurrent transactional requests, so they may return a LockConflictError.
func acquireLockOnKey(
	ctx context.Context,
	readWriter storage.ReadWriter,
	txn *roachpb.Transaction,
	str lock.Strength,
	dur lock.Durability,
	key roachpb.Key,
	ms *enginepb.MVCCStats,
	settings *cluster.Settings,
) (roachpb.LockAcquisition, error) {
	maxLockConflicts := storage.MaxConflictsPerLockConflictError.Get(&settings.SV)
	targetLockConflictBytes := storage.TargetBytesPerLockConflictError.Get(&settings.SV)
	if txn == nil {
		// Non-transactional requests are not allowed to acquire locks that outlive
		// the request's lifespan. However, they may conflict with locks held by
		// other concurrent transactional requests. Evaluation up until this point
		// has only scanned for (and not found any) conflicts with locks in the
		// in-memory lock table. This includes all unreplicated locks and contended
		// replicated locks. We haven't considered conflicts with un-contended
		// replicated locks -- do so now.
		//
		// NB: The supplied durability is insignificant for non-transactional
		// requests.
		return roachpb.LockAcquisition{},
			storage.MVCCCheckForAcquireLock(ctx, readWriter, txn, str, key, maxLockConflicts, targetLockConflictBytes)
	}
	switch dur {
	case lock.Unreplicated:
		// Evaluation up until this point has only scanned for (and not found any)
		// conflicts with locks in the in-memory lock table. This includes all
		// unreplicated locks and contended replicated locks. We haven't considered
		// conflicts with un-contended replicated locks -- we need to do so before
		// we can acquire our own unreplicated lock; do so now.
		err := storage.MVCCCheckForAcquireLock(ctx, readWriter, txn, str, key, maxLockConflicts, targetLockConflictBytes)
		if err != nil {
			return roachpb.LockAcquisition{}, err
		}
	case lock.Replicated:
		// Evaluation up until this point has only scanned for (and not found any)
		// conflicts with locks in the in-memory lock table. This includes all
		// unreplicated locks and contended replicated locks. We haven't considered
		// conflicts with un-contended replicated locks -- we need to do so before
		// we can acquire our own replicated lock; do that now, and also acquire
		// the replicated lock if no conflicts are found.
		if err := storage.MVCCAcquireLock(ctx, readWriter, &txn.TxnMeta, txn.IgnoredSeqNums, str, key, ms, maxLockConflicts, targetLockConflictBytes); err != nil {
			return roachpb.LockAcquisition{}, err
		}
	default:
		panic("unexpected lock durability")
	}
	acq := roachpb.MakeLockAcquisition(txn.TxnMeta, key, dur, str, txn.IgnoredSeqNums)
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

// txnBoundLockTableView is a transaction-bound view into an in-memory
// collections of key-level locks.
type txnBoundLockTableView interface {
	IsKeyLockedByConflictingTxn(
		context.Context, roachpb.Key, lock.Strength,
	) (bool, *enginepb.TxnMeta, error)
}

// requestBoundLockTableView combines a txnBoundLockTableView with the lock
// strength that an individual request is attempting to acquire.
type requestBoundLockTableView struct {
	inMemLockTableView txnBoundLockTableView
	replLockTableView  txnBoundLockTableView
	str                lock.Strength
}

var _ storage.LockTableView = &requestBoundLockTableView{}

var requestBoundLockTableViewPool = sync.Pool{
	New: func() interface{} { return new(requestBoundLockTableView) },
}

// newRequestBoundLockTableView creates a new requestBoundLockTableView.
func newRequestBoundLockTableView(
	reader storage.Reader,
	inMemLTV txnBoundLockTableView,
	txn *roachpb.Transaction,
	str lock.Strength,
) *requestBoundLockTableView {
	replLTV := newTxnBoundReplicatedLockTableView(reader, txn)
	rbLTV := makeRequestBoundLockTableView(inMemLTV, replLTV, str)
	return &rbLTV
}

// makeRequestBoundLockTableView returns a requestBoundLockTableView.
func makeRequestBoundLockTableView(
	inMemLTV txnBoundLockTableView, replLTV txnBoundLockTableView, str lock.Strength,
) requestBoundLockTableView {
	rbLTV := requestBoundLockTableViewPool.Get().(*requestBoundLockTableView)
	rbLTV.inMemLockTableView = inMemLTV
	rbLTV.replLockTableView = replLTV
	rbLTV.str = str
	return *rbLTV
}

// IsKeyLockedByConflictingTxn implements the storage.LockTableView interface.
func (ltv *requestBoundLockTableView) IsKeyLockedByConflictingTxn(
	ctx context.Context, key roachpb.Key,
) (bool, *enginepb.TxnMeta, error) {
	conflicts, txn, err := ltv.inMemLockTableView.IsKeyLockedByConflictingTxn(ctx, key, ltv.str)
	if conflicts || err != nil {
		return conflicts, txn, err
	}
	return ltv.replLockTableView.IsKeyLockedByConflictingTxn(ctx, key, ltv.str)
}

// Close implements the storage.LockTableView interface.
func (ltv *requestBoundLockTableView) Close() {
	*ltv = requestBoundLockTableView{} // reset
	requestBoundLockTableViewPool.Put(ltv)
}

// txnBoundReplicatedLockTableView provides a transaction bound view into the
// replicated lock table.
type txnBoundReplicatedLockTableView struct {
	reader storage.Reader
	txn    *roachpb.Transaction
}

var _ txnBoundLockTableView = &txnBoundReplicatedLockTableView{}

// newTxnBoundReplicatedLockTableView creates a new
// txnBoundReplicatedLockTableView.
func newTxnBoundReplicatedLockTableView(
	reader storage.Reader, txn *roachpb.Transaction,
) *txnBoundReplicatedLockTableView {
	if !reader.ConsistentIterators() {
		panic("cannot use inconsistent iterator to read from the lock table when determining whether " +
			"to skip a locked key or not")
	}
	return &txnBoundReplicatedLockTableView{
		reader: reader,
		txn:    txn,
	}
}

// IsKeyLockedByConflictingTxn implements the txnBoundLockTableView interface.
func (ltv *txnBoundReplicatedLockTableView) IsKeyLockedByConflictingTxn(
	ctx context.Context, key roachpb.Key, str lock.Strength,
) (bool, *enginepb.TxnMeta, error) {
	if str == lock.None {
		// Non-locking reads do not conflict with replicated locks, so no need to
		// check the replicated lock table.
		return false, nil, nil
	}
	// TODO(arul): We could conflict with multiple (shared) locks here but we're
	// only returning the first one. We could return all of them instead.
	err := storage.MVCCCheckForAcquireLock(ctx, ltv.reader, ltv.txn, str, key, 1, /* maxConflicts */
		0 /* targetLockConflictBytes */)
	if err != nil {
		if lcErr := (*kvpb.LockConflictError)(nil); errors.As(err, &lcErr) {
			return true, &lcErr.Locks[0].Txn, nil
		}
		return false, nil, err
	}
	return false, nil, nil
}
