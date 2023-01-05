// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// This file defines several interfaces as well as introduces a couple of
// components that power the direct columnar scans. The main idea of this
// feature is to use the injected decoding logic from SQL in order to process
// each KV and keep only the needed parts (i.e. necessary SQL columns). Those
// needed parts are then propagated back to the KV client as coldata.Batch'es
// (serialized in the Apache Arrow format).
//
// Here is an example outline of all components involved:
//
//      ┌────────────────────────────────────────────────┐
//      │                      SQL                       │
//      │________________________________________________│
//      │          colfetcher.ColBatchDirectScan         │
//      │                        │                       │
//      │                        ▼                       │
//      │                 row.txnKVFetcher               │
//      │    (behind the row.KVBatchFetcher interface)   │
//      └────────────────────────────────────────────────┘
//                               │
//                               ▼
//      ┌────────────────────────────────────────────────┐
//      │                    KV Client                   │
//      └────────────────────────────────────────────────┘
//                               │
//                               ▼
//      ┌────────────────────────────────────────────────┐
//      │                    KV Server                   │
//      │________________________________________________│
//      │           colfetcher.cFetcherWrapper           │
//      │ (behind the storage.CFetcherWrapper interface) │
//      │                        │                       │
//      │                        ▼                       │
//      │              colfetcher.cFetcher               │
//      │                        │                       │
//      │                        ▼                       │
//      │          storage.mvccScanFetchAdapter          │
//      │    (behind the storage.NextKVer interface)     │
//      │                        │                       │
//      │                        ▼                       │
//      │           storage.pebbleMVCCScanner            │
//      └────────────────────────────────────────────────┘
//
// On the KV client side, row.txnKVFetcher issues Scans and ReverseScans with
// the COL_BATCH_RESPONSE format and returns the response (which contains the
// columnar data) to the colfetcher.ColBatchDirectScan.
//
// On the KV server side, we create a storage.CFetcherWrapper that asks the
// colfetcher.cFetcher for the next coldata.Batch. The cFetcher, in turn,
// fetches the next KV, decodes it, and keeps only values for the needed SQL
// columns, discarding the rest of the KV. The KV is emitted by the
// mvccScanFetchAdapter which - via the singleResults struct - exposes access to
// the current KV that the pebbleMVCCScanner is pointing at.

// NextKVer can fetch a new KV from somewhere. If MVCCDecodingStrategy is set
// to MVCCDecodingRequired, the returned KV will include a timestamp.
type NextKVer interface {
	// NextKV returns the next kv from this NextKVer. Returns false if there are
	// no more kvs to fetch, the kv that was fetched, and any errors that may
	// have occurred.
	//
	// needsCopy is set to true when the caller should copy the returned
	// KeyValue. One example of when this happens is when the returned KV's byte
	// slices are the last reference into a larger backing byte slice. In such a
	// case, the next call to NextKV might potentially allocate a big chunk of
	// new memory, and by copying the returned KeyValue into a small slice that
	// the caller owns, we avoid retaining two large backing byte slices at
	// once.
	NextKV(context.Context, MVCCDecodingStrategy) (
		ok bool, kv roachpb.KeyValue, needsCopy bool, err error,
	)

	// GetLastEncodedKey returns the key that was returned on the last NextKV()
	// call. This method allows callers to access the key at different layers of
	// abstraction.
	GetLastEncodedKey() roachpb.Key
}

// CFetcherWrapper is a wrapper around a colfetcher.cFetcher that populates only
// the needed (according to fetchpb.IndexFetchSpec) vectors and returns a
// serialized representation of coldata.Batch.
type CFetcherWrapper interface {
	// NextBatch gives back the next column-oriented batch, serialized in Arrow
	// batch format.
	NextBatch(ctx context.Context) ([]byte, error)

	// Close release the resources held by this CFetcherWrapper. It *must* be
	// called after use of the wrapper.
	Close(ctx context.Context)

	// Methods below are only used when WholeRows option is used (which is the
	// case if the table has multiple column families, meaning that a single SQL
	// row can be comprised from multiple KVs).

	// ContinuesFirstRow returns true if the given key belongs to the same SQL
	// row as the first KV pair in the result (or if the result is empty). If
	// either key is not a valid SQL row key, returns false.
	ContinuesFirstRow(key roachpb.Key) bool

	// MaybeTrimPartialLastRow removes the last KV pairs from the result that
	// are part of the same SQL row as the given key, returning the earliest key
	// removed.
	MaybeTrimPartialLastRow(nextKey roachpb.Key) (roachpb.Key, error)

	// LastRowHasFinalColumnFamily returns true if the last key in the result is
	// the maximum column family ID of the row, i.e. we know that the row is
	// complete.
	LastRowHasFinalColumnFamily(reverse bool) bool
}

// GetCFetcherWrapper returns a CFetcherWrapper. It's injected from
// pkg/sql/colfetcher to avoid circular dependencies since storage can't depend
// on higher levels of the system.
var GetCFetcherWrapper func(
	ctx context.Context,
	fetcherAccount, converterAccount *mon.BoundAccount,
	indexFetchSpec *fetchpb.IndexFetchSpec,
	nextKVer NextKVer,
	startKey roachpb.Key,
) (CFetcherWrapper, error)

// onNextKVFn represents the transition that the mvccScanFetchAdapter needs to
// perform on the following NextKV() call.
type onNextKVFn int

const (
	_ onNextKVFn = iota
	// onNextKVSeek is the initial state of the mvccScanFetchAdapter where it
	// must seek to the start of the scan. The state machine will then
	// transition to the onNextKVAdvance state.
	onNextKVSeek
	// onNextKVAdvance is the main state of the mvccScanFetchAdapter where it
	// advances the scanner to the next KV (which is then returned on the NextKV
	// call). Once there are no more KVs to scan (either because the scan was
	// exhausted or some kind of limit was reached), the state machine will
	// transition to the onNextKVDone state.
	onNextKVAdvance
	// onNextKVDone is the final state of the mvccScanFetchAdapter which
	// indicates that the current scan is complete.
	onNextKVDone
)

// mvccScanFetchAdapter is a NextKVer that is powered directly by the
// pebbleMVCCScanner. Each time its NextKV is called, it advances the pebble
// iterator and returns a single KV. Note that the returned KV is only valid
// until the next call to NextKV.
type mvccScanFetchAdapter struct {
	scanner *pebbleMVCCScanner
	machine onNextKVFn
	results singleResults
}

var _ NextKVer = &mvccScanFetchAdapter{}

// NextKV implements the NextKVer interface.
func (f *mvccScanFetchAdapter) NextKV(
	ctx context.Context, mvccDecodingStrategy MVCCDecodingStrategy,
) (ok bool, kv roachpb.KeyValue, needsCopy bool, err error) {
	// Perform the action according to the current state.
	switch f.machine {
	case onNextKVSeek:
		if !f.scanner.seekToStartOfScan() {
			return false, roachpb.KeyValue{}, false, f.scanner.err
		}
		f.machine = onNextKVAdvance
	case onNextKVAdvance:
		if !f.scanner.advance() {
			// No more keys in the scan.
			return false, roachpb.KeyValue{}, false, nil
		}
	case onNextKVDone:
		// No more keys in the scan.
		return false, roachpb.KeyValue{}, false, nil
	}
	// Attempt to get one KV.
	ok, added := f.scanner.getOne(ctx)
	if !ok {
		// ok=false indicates that the iteration must stop, so we're done after
		// we process the current KV (if it was added).
		f.machine = onNextKVDone
	}
	if !added {
		// The KV wasn't added for whatever reason (e.g. it could have been
		// skipped over due to having been deleted), so just move on.
		return f.NextKV(ctx, mvccDecodingStrategy)
	}
	// We have a KV to return. Decode it according to mvccDecodingStrategy.
	kv = f.results.getLastKV()
	encKey := kv.Key
	if buildutil.CrdbTestBuild {
		if len(encKey) == 0 || len(kv.Value.RawBytes) == 0 {
			return false, kv, false, errors.AssertionFailedf("unexpectedly received an empty lastKV")
		}
	}
	switch mvccDecodingStrategy {
	case MVCCDecodingRequired:
		kv.Key, kv.Value.Timestamp, err = enginepb.DecodeKey(encKey)
		if err != nil {
			return false, kv, false, errors.AssertionFailedf("invalid encoded mvcc key: %x", encKey)
		}
	case MVCCDecodingNotRequired:
		kv.Key, _, ok = enginepb.SplitMVCCKey(encKey)
		if !ok {
			return false, kv, false, errors.AssertionFailedf("invalid encoded mvcc key: %x", encKey)
		}
	}
	// The caller must copy this KV if the table has multiple column families
	// (which is the case when wholeRows is set).
	needsCopy = f.scanner.wholeRows
	return true, kv, needsCopy, nil
}

// GetLastEncodedKey implements the NextKVer interface.
func (f *mvccScanFetchAdapter) GetLastEncodedKey() roachpb.Key {
	return f.results.encKey
}

// singleResults is an implementation of the results interface that is able to
// hold only a single KV at a time - all KVs are accumulated in the
// CFetcherWrapper.
type singleResults struct {
	wrapper      CFetcherWrapper
	onClear      func()
	count, bytes int64
	encKey       []byte
	value        []byte
}

var _ results = &singleResults{}

// clear implements the results interface.
func (s *singleResults) clear() {
	s.onClear()
	*s = singleResults{}
}

func singleResultsKVSizeOf(lenKey, lenValue int) int64 {
	// TODO(yuzefovich, 23.1): come up with a formula that better represents the
	// footprint of the serialized batches.
	return int64(lenKey + lenValue)
}

// sizeInfo implements the results interface.
func (s *singleResults) sizeInfo(lenKey, lenValue int) (numKeys, numBytes, numBytesInc int64) {
	numKeys = s.count
	// TODO(yuzefovich, 23.1): consider using the footprint of coldata.Batches
	// so far (or of serialized representations) here.
	numBytes = s.bytes
	numBytesInc = singleResultsKVSizeOf(lenKey, lenValue)
	return numKeys, numBytes, numBytesInc
}

// put implements the results interface.
func (s *singleResults) put(
	ctx context.Context, key []byte, value []byte, memAccount *mon.BoundAccount, _ int,
) error {
	bytesInc := singleResultsKVSizeOf(len(key), len(value))
	if err := memAccount.Grow(ctx, bytesInc); err != nil {
		return err
	}
	s.count++
	s.bytes += bytesInc
	s.encKey = key
	s.value = value
	return nil
}

// continuesFirstRow implements the results interface.
func (s *singleResults) continuesFirstRow(key roachpb.Key) bool {
	return s.wrapper.ContinuesFirstRow(key)
}

// maybeTrimPartialLastRow implements the results interface.
func (s *singleResults) maybeTrimPartialLastRow(key roachpb.Key) (roachpb.Key, error) {
	return s.wrapper.MaybeTrimPartialLastRow(key)
}

// lastRowHasFinalColumnFamily implements the results interface.
func (s *singleResults) lastRowHasFinalColumnFamily(reverse bool) bool {
	return s.wrapper.LastRowHasFinalColumnFamily(reverse)
}

func (s *singleResults) getLastKV() roachpb.KeyValue {
	return roachpb.KeyValue{
		Key:   s.encKey,
		Value: roachpb.Value{RawBytes: s.value},
	}
}

// MVCCScanToCols is like MVCCScan, but it returns KVData in a serialized
// columnar batch suitable for reading by colserde.RecordBatchDeserializer.
func MVCCScanToCols(
	ctx context.Context,
	reader Reader,
	indexFetchSpec *fetchpb.IndexFetchSpec,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
	st *cluster.Settings,
) (MVCCScanResult, error) {
	iter := newMVCCIterator(
		reader, timestamp, !opts.Tombstones, opts.DontInterleaveIntents, IterOptions{
			KeyTypes:   IterKeyTypePointsAndRanges,
			LowerBound: key,
			UpperBound: endKey,
		},
	)
	defer iter.Close()
	return mvccScanToCols(ctx, iter, indexFetchSpec, key, endKey, timestamp, opts, st)
}

func mvccScanToCols(
	ctx context.Context,
	iter MVCCIterator,
	indexFetchSpec *fetchpb.IndexFetchSpec,
	key, endKey roachpb.Key,
	timestamp hlc.Timestamp,
	opts MVCCScanOptions,
	st *cluster.Settings,
) (MVCCScanResult, error) {
	if len(endKey) == 0 {
		return MVCCScanResult{}, emptyKeyError()
	}
	if err := opts.validate(); err != nil {
		return MVCCScanResult{}, err
	}
	if opts.MaxKeys < 0 {
		return MVCCScanResult{
			ResumeSpan:   &roachpb.Span{Key: key, EndKey: endKey},
			ResumeReason: roachpb.RESUME_KEY_LIMIT,
		}, nil
	}
	if opts.TargetBytes < 0 {
		return MVCCScanResult{
			ResumeSpan:   &roachpb.Span{Key: key, EndKey: endKey},
			ResumeReason: roachpb.RESUME_BYTE_LIMIT,
		}, nil
	}

	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	defer mvccScanner.release()

	*mvccScanner = pebbleMVCCScanner{
		parent:           iter,
		memAccount:       opts.MemoryAccount,
		lockTable:        opts.LockTable,
		reverse:          opts.Reverse,
		start:            key,
		end:              endKey,
		ts:               timestamp,
		maxKeys:          opts.MaxKeys,
		targetBytes:      opts.TargetBytes,
		allowEmpty:       opts.AllowEmpty,
		wholeRows:        opts.WholeRowsOfSize > 1, // single-KV rows don't need processing
		maxIntents:       opts.MaxIntents,
		inconsistent:     opts.Inconsistent,
		skipLocked:       opts.SkipLocked,
		tombstones:       opts.Tombstones,
		failOnMoreRecent: opts.FailOnMoreRecent,
		keyBuf:           mvccScanner.keyBuf,
	}

	adapter := mvccScanFetchAdapter{
		scanner: mvccScanner,
		machine: onNextKVSeek,
	}
	mvccScanner.init(opts.Txn, opts.Uncertainty, &adapter.results)
	// Try to use the same root monitor (from the store) if the account is
	// provided.
	monitor := opts.MemoryAccount.Monitor()
	if monitor == nil {
		// If we don't have the monitor, then we create a "fake" one that is not
		// connected to the memory accounting system.
		monitor = mon.NewMonitor(
			"mvcc-scan-to-cols",
			mon.MemoryResource,
			nil,           /* curCount */
			nil,           /* maxHist */
			-1,            /* increment */
			math.MaxInt64, /* noteworthy */
			st,
		)
		monitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(math.MaxInt64))
		defer monitor.Stop(ctx)
	}
	fetcherAcc, converterAcc := monitor.MakeBoundAccount(), monitor.MakeBoundAccount()
	defer fetcherAcc.Close(ctx)
	defer converterAcc.Close(ctx)
	wrapper, err := GetCFetcherWrapper(
		ctx,
		&fetcherAcc,
		&converterAcc,
		indexFetchSpec,
		&adapter,
		key,
	)
	if err != nil {
		return MVCCScanResult{}, err
	}
	defer wrapper.Close(ctx)
	adapter.results.wrapper = wrapper

	var res MVCCScanResult
	adapter.results.onClear = func() {
		// Discard the accumulated batches on results.clear() call - the scan
		// will result in an error.
		res = MVCCScanResult{}
	}
	for {
		batch, err := wrapper.NextBatch(ctx)
		if err != nil {
			return res, err
		}
		if batch == nil {
			break
		}
		// We need to make a copy since the wrapper reuses underlying bytes
		// buffer.
		b := make([]byte, len(batch))
		copy(b, batch)
		res.KVData = append(res.KVData, b)
	}

	res.ResumeSpan, res.ResumeReason, res.ResumeNextBytes, err = mvccScanner.afterScan()
	if err != nil {
		return MVCCScanResult{}, err
	}
	if err = finalizeScanResult(ctx, mvccScanner, &res, opts.errOnIntents()); err != nil {
		return MVCCScanResult{}, err
	}
	return res, nil
}
