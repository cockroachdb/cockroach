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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
//      │                       SQL                      │
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
//      │          storage.mvccScanFetchAdapter ────────┐│
//      │    (behind the storage.NextKVer interface)    ││
//      │                        │                      ││
//      │                        ▼                      ││
//      │           storage.pebbleMVCCScanner           ││
//      │ (which put's KVs into storage.singleResults) <┘│
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
//
// Note that there is an additional link between components that is not shown on
// this diagram. In particular, the storage.singleResults struct has a handle on
// the storage.CFetcherWrapper in order to implement a couple of methods of the
// storage.results interface. That "upstream" link (although breaking the
// layering a bit) allows us to avoid a performance penalty for handling the
// case with multiple column families. (This case is handled by the
// storage.pebbleResults via tracking offsets into the pebbleResults.repr).

type PushKVFn func(ok bool, kv roachpb.KeyValue, needsCopy bool, err error)

// NextKVer can fetch a new KV from somewhere. If MVCCDecodingStrategy is set
// to MVCCDecodingRequired, the returned KV will include a timestamp.
type NextKVer interface {
	Init(PushKVFn, MVCCDecodingStrategy)

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
	NextKV(context.Context)
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
	pushKV  PushKVFn
	machine onNextKVFn
	results singleResults
}

var _ NextKVer = &mvccScanFetchAdapter{}

// Init implements the NextKVer interface.
func (f *mvccScanFetchAdapter) Init(pushKV PushKVFn, mvccDecodingStrategy MVCCDecodingStrategy) {
	f.pushKV = pushKV
	switch mvccDecodingStrategy {
	case MVCCDecodingRequired:
		f.results.onPut = func(key, value []byte) {
			ok := true
			kv := roachpb.KeyValue{Value: roachpb.Value{RawBytes: value}}
			var err error
			kv.Key, kv.Value.Timestamp, err = enginepb.DecodeKey(key)
			if err != nil {
				ok = false
				err = errors.AssertionFailedf("invalid encoded mvcc key: %x", key)
			}
			// The caller must copy this KV if the table has multiple column
			// families (which is the case when wholeRows is set).
			needsCopy := f.scanner.wholeRows
			pushKV(ok, kv, needsCopy, err)
		}
	case MVCCDecodingNotRequired:
		f.results.onPut = func(key, value []byte) {
			var ok bool
			kv := roachpb.KeyValue{Value: roachpb.Value{RawBytes: value}}
			var err error
			kv.Key, _, ok = enginepb.SplitMVCCKey(key)
			if !ok {
				err = errors.AssertionFailedf("invalid encoded mvcc key: %x", key)
			}
			// The caller must copy this KV if the table has multiple column
			// families (which is the case when wholeRows is set).
			needsCopy := f.scanner.wholeRows
			pushKV(ok, kv, needsCopy, err)
		}
	}
}

// NextKV implements the NextKVer interface.
func (f *mvccScanFetchAdapter) NextKV(ctx context.Context) {
	// Perform the action according to the current state.
	switch f.machine {
	case onNextKVSeek:
		if !f.scanner.seekToStartOfScan() {
			f.pushKV(false, roachpb.KeyValue{}, false, f.scanner.err)
			return
		}
		f.machine = onNextKVAdvance
	case onNextKVAdvance:
		if !f.scanner.advance() {
			// No more keys in the scan.
			f.pushKV(false, roachpb.KeyValue{}, false, nil)
			return
		}
	case onNextKVDone:
		// No more keys in the scan.
		f.pushKV(false, roachpb.KeyValue{}, false, nil)
		return
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
		f.NextKV(ctx)
	}
}

// singleResults is an implementation of the results interface that is able to
// hold only a single KV at a time - all KVs are "accumulated" in the
// colfetcher.cFetcher.
//
// Here is how all things fit together:
//   - the colfetcher.cFetcher calls NextKV on the mvccScanFetchAdapter;
//   - the mvccScanFetchAdapter advances the pebbleMVCCScanner to the next key;
//   - the mvccScanFetchAdapter asks the scanner to `getOne` which `put`s a new
//     KV into the `singleResults`. Importantly, the pebbleMVCCScanner is not
//     eagerly advancing further which allows us to just use the unstable
//     key-value from the pebbleMVCCScanner;
//   - the mvccScanFetchAdapter peeks into the `singleResults` struct to extract
//     the new KV, possibly decodes the timestamp, and returns it to the
//     colfetcher.cFetcher for processing;
//   - the colfetcher.cFetcher decodes the KV, and goes back to the first step.
type singleResults struct {
	wrapper      CFetcherWrapper
	maxFamilyID  uint32
	onClear      func()
	onPut        func(key, value []byte)
	count, bytes int64
	encKey       []byte
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
	s.onPut(key, value)
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
	key, _, ok := enginepb.SplitMVCCKey(s.encKey)
	if !ok {
		return false
	}
	colFamilyID, err := keys.DecodeFamilyKey(key)
	if err != nil {
		return false
	}
	if reverse {
		return colFamilyID == 0
	}
	return colFamilyID == s.maxFamilyID
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
	adapter := mvccScanFetchAdapter{machine: onNextKVSeek}
	adapter.results.maxFamilyID = uint32(indexFetchSpec.MaxFamilyID)
	ok, mvccScanner, res, err := mvccScanInit(iter, key, endKey, timestamp, opts, &adapter.results)
	if !ok {
		return res, err
	}
	defer mvccScanner.release()
	adapter.scanner = mvccScanner

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
