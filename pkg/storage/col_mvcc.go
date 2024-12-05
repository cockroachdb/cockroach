// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
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
// Note that there is an additional "implicit synchronization" between
// components that is not shown on this diagram. In particular,
// storage.singleResults.maybeTrimPartialLastRow must be in sync with the
// colfetcher.cFetcher which is achieved by
// - the cFetcher exposing access to the first key of the last incomplete SQL
//   row via the FirstKeyOfRowGetter,
// - the singleResults using that key as the resume key for the response,
// - and the cFetcher removing that last partial SQL row when NextKV() returns
//   partialRow=true.
// This "upstream" link (although breaking the layering a bit) allows us to
// avoid a performance penalty for handling the case with multiple column
// families. (This case is handled by the storage.pebbleResults via tracking
// offsets into the pebbleResults.repr.)
//
// This code structure deserves some elaboration. First, there is a mismatch
// between the "push" mode in which the pebbleMVCCScanner operates and the
// "pull" mode that the NextKVer exposes. The adaption between two different
// modes is achieved via the mvccScanFetcherAdapter grabbing (when the control
// returns to it) the current unstable KV pair from the singleResults struct
// which serves as a one KV pair buffer that the pebbleMVCCScanner `put`s into.
// Second, in order be able to use the unstable KV pair without performing a
// copy, the pebbleMVCCScanner stops at the current KV pair and returns the
// control flow (which is exactly what pebbleMVCCScanner.getOne does) back to
// the mvccScanFetcherAdapter, with the adapter advancing the scanner only when
// the next KV pair is needed.

// FirstKeyOfRowGetter returns the first key included into the last incomplete
// SQL row by the user of NextKVer. If the last row is complete, then nil is
// returned.
type FirstKeyOfRowGetter func() roachpb.Key

// NextKVer can fetch a new KV from somewhere. If MVCCDecodingStrategy is set
// to MVCCDecodingRequired, the returned KV will include a timestamp.
type NextKVer interface {
	// Init initializes the NextKVer. It returns a boolean indicating whether
	// the KVs returned by NextKV are stable (i.e. whether they will not be
	// invalidated by calling NextKV again).
	Init(getter FirstKeyOfRowGetter) (stableKVs bool)
	// NextKV returns the next kv from this NextKVer.
	// - ok=false indicates that there are no more kvs to fetch,
	// - partialRow indicates whether the fetch stopped in the middle of a SQL
	// row (in this case ok will be set to false),
	// - the kv that was fetched,
	// - any errors that may have occurred.
	//
	// When (ok=false,partialRow=true) is returned, the caller is expected to
	// discard all KVs that were part of the last SQL row that was incomplete.
	// The scan will be resumed from the key provided by the FirstKeyOfRowGetter
	// (provided in Init by the caller) obtained during this NextKV call.
	NextKV(context.Context, MVCCDecodingStrategy) (ok bool, partialRow bool, kv roachpb.KeyValue, err error)
}

// CFetcherWrapper is a wrapper around a colfetcher.cFetcher that populates only
// the needed (according to the fetchpb.IndexFetchSpec) vectors which are
// returned as coldata.Batch'es (either serialized or as is).
//
// Currently, non-enum user-defined types are unsupported when they are included
// as "needed" in the fetchpb.IndexFetchSpec (#92954).
type CFetcherWrapper interface {
	// NextBatch gives back the next column-oriented batch, possibly serialized
	// in Arrow batch format. All calls to NextBatch will use the same format
	// (i.e. either all batches are serialized or none are).
	//
	// Regardless of the return format, consequent calls to NextBatch do **not**
	// invalidate the result of the previous calls. Additionally, the memory
	// accounting for all returned batches throughout the lifetime of the
	// CFetcherWrapper is done against the provided in GetCFetcherWrapper()
	// memory account.
	NextBatch(ctx context.Context) ([]byte, coldata.Batch, error)

	// Close release the resources held by this CFetcherWrapper. It *must* be
	// called after use of the wrapper.
	Close(ctx context.Context)
}

// GetCFetcherWrapper returns a CFetcherWrapper. It's injected from
// pkg/sql/colfetcher to avoid circular dependencies since storage can't depend
// on higher levels of the system.
var GetCFetcherWrapper func(
	ctx context.Context,
	st *cluster.Settings,
	acc *mon.BoundAccount,
	indexFetchSpec *fetchpb.IndexFetchSpec,
	nextKVer NextKVer,
	startKey roachpb.Key,
	mustSerialize bool,
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
// iterator (possibly several times if some KVs need to be skipped) and returns
// a single KV. Note that the returned KV is only valid until the next call to
// NextKV.
type mvccScanFetchAdapter struct {
	scanner *pebbleMVCCScanner
	machine onNextKVFn
	results singleResults
}

var _ NextKVer = &mvccScanFetchAdapter{}

// Init implements the NextKVer interface.
func (f *mvccScanFetchAdapter) Init(firstKeyGetter FirstKeyOfRowGetter) (stableKVs bool) {
	f.results.firstKeyGetter = firstKeyGetter
	// The returned kv is never stable because it'll be invalidated by the
	// pebbleMVCCScanner on each NextKV() call.
	return false
}

// NextKV implements the NextKVer interface.
func (f *mvccScanFetchAdapter) NextKV(
	ctx context.Context, mvccDecodingStrategy MVCCDecodingStrategy,
) (ok bool, partialRow bool, kv roachpb.KeyValue, err error) {
	// Loop until we add a KV into the results (KVs might be skipped due to
	// having been deleted).
	// TODO(yuzefovich, 23.1): check whether having this loop has noticeable
	// impact on the performance.
	for added := false; !added; {
		// Perform the action according to the current state.
		switch f.machine {
		case onNextKVSeek:
			if !f.scanner.seekToStartOfScan() {
				return false, false, roachpb.KeyValue{}, f.scanner.err
			}
			f.machine = onNextKVAdvance
		case onNextKVAdvance:
			if !f.scanner.advance() {
				// No more keys in the scan.
				return false, false, roachpb.KeyValue{}, nil
			}
		case onNextKVDone:
			// No more keys in the scan.
			return false, f.results.partialRowTrimmed, roachpb.KeyValue{}, nil
		}
		ok, added = f.scanner.getOne(ctx)
		if !ok {
			// ok=false indicates that the iteration must stop, so we're done
			// after we process the current KV (if it was added).
			f.machine = onNextKVDone
		}
	}
	// We have a KV to return. Decode it according to mvccDecodingStrategy.
	kv = f.results.getLastKV()
	mvccKey := kv.Key
	if buildutil.CrdbTestBuild {
		if len(mvccKey) == 0 || len(kv.Value.RawBytes) == 0 {
			return false, false, kv, errors.AssertionFailedf("unexpectedly received an empty lastKV")
		}
	}
	switch mvccDecodingStrategy {
	case MVCCDecodingRequired:
		kv.Key, kv.Value.Timestamp, err = enginepb.DecodeKey(mvccKey)
		if err != nil {
			return false, false, kv, errors.AssertionFailedf("invalid encoded mvcc key: %x", mvccKey)
		}
	case MVCCDecodingNotRequired:
		kv.Key, _, ok = enginepb.SplitMVCCKey(mvccKey)
		if !ok {
			return false, false, kv, errors.AssertionFailedf("invalid encoded mvcc key: %x", mvccKey)
		}
	}
	return true, false, kv, nil
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
	maxKeysPerRow  uint32
	maxFamilyID    uint32
	onClear        func()
	count, bytes   int64
	mvccKey        []byte
	value          []byte
	firstKeyGetter FirstKeyOfRowGetter
	// firstRowKeyPrefix is a deep copy of the "row prefix" of the first SQL row
	// seen by the singleResults (only set when the table has multiple column
	// families).
	firstRowKeyPrefix []byte
	partialRowTrimmed bool
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
	ctx context.Context, mvccKey []byte, value []byte, memAccount *mon.BoundAccount, _ int,
) error {
	bytesInc := singleResultsKVSizeOf(len(mvccKey), len(value))
	if err := memAccount.Grow(ctx, bytesInc); err != nil {
		return err
	}
	s.count++
	s.bytes += bytesInc
	s.mvccKey = mvccKey
	s.value = value
	if s.count == 1 && s.maxKeysPerRow > 1 {
		// If this is the first key, and we have multiple column families, then
		// we store the deep-copied row prefix of this key. This is needed to
		// implement continuesFirstRow.
		key, _, ok := enginepb.SplitMVCCKey(mvccKey)
		if !ok {
			return errors.AssertionFailedf("invalid encoded mvcc key: %x", mvccKey)
		}
		firstRowKeyPrefix := getRowPrefix(key)
		s.firstRowKeyPrefix = make([]byte, len(firstRowKeyPrefix))
		copy(s.firstRowKeyPrefix, firstRowKeyPrefix)
	}
	return nil
}

// continuesFirstRow implements the results interface.
func (s *singleResults) continuesFirstRow(key roachpb.Key) bool {
	rowPrefix := getRowPrefix(key)
	if rowPrefix == nil {
		return false
	}
	return bytes.Equal(rowPrefix, s.firstRowKeyPrefix)
}

// maybeTrimPartialLastRow implements the results interface.
func (s *singleResults) maybeTrimPartialLastRow(key roachpb.Key) (roachpb.Key, error) {
	firstKeyOfRow := s.firstKeyGetter()
	// getRowPrefix handles the case of empty key, so we don't need to check
	// that explicitly upfront.
	if !bytes.Equal(getRowPrefix(firstKeyOfRow), getRowPrefix(key)) {
		// The given key is the first KV of the next row, so we will resume the
		// scan from this key.
		return key, nil
	}
	// The given key is part of the current last row, and it will be removed by
	// the cFetcher (since NextKV() will return partialRow=true before the row
	// can be completed), thus, we'll resume the scan from the first key in the
	// last row.
	s.partialRowTrimmed = true
	return firstKeyOfRow, nil
}

// lastRowHasFinalColumnFamily implements the results interface.
func (s *singleResults) lastRowHasFinalColumnFamily(reverse bool) bool {
	key, _, ok := enginepb.SplitMVCCKey(s.mvccKey)
	if !ok {
		return false
	}
	return keyHasFinalColumnFamily(key, s.maxFamilyID, reverse)
}

func (s *singleResults) getLastKV() roachpb.KeyValue {
	return roachpb.KeyValue{
		Key:   s.mvccKey,
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
	iter, err := newMVCCIterator(
		ctx, reader, timestamp, !opts.Tombstones, opts.DontInterleaveIntents, IterOptions{
			KeyTypes:     IterKeyTypePointsAndRanges,
			LowerBound:   key,
			UpperBound:   endKey,
			ReadCategory: opts.ReadCategory,
		},
	)
	if err != nil {
		return MVCCScanResult{}, err
	}
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
	mvccScanner := pebbleMVCCScannerPool.Get().(*pebbleMVCCScanner)
	adapter := mvccScanFetchAdapter{machine: onNextKVSeek}
	adapter.results.maxKeysPerRow = indexFetchSpec.MaxKeysPerRow
	adapter.results.maxFamilyID = uint32(indexFetchSpec.MaxFamilyID)
	ok, res, err := mvccScanInit(mvccScanner, iter, key, endKey, timestamp, opts, &adapter.results)
	if !ok {
		return res, err
	}
	defer mvccScanner.release()
	adapter.scanner = mvccScanner

	// Try to use the same root monitor (from the store) if the account is
	// provided.
	var monitor *mon.BytesMonitor
	if opts.MemoryAccount != nil {
		monitor = opts.MemoryAccount.Monitor()
	} else {
		// If we don't have the monitor, then we create a "fake" one that is not
		// connected to the memory accounting system.
		monitor = mon.NewMonitor(mon.Options{
			Name:     mon.MakeMonitorName("mvcc-scan-to-cols"),
			Settings: st,
		})
		monitor.Start(ctx, nil /* pool */, mon.NewStandaloneBudget(math.MaxInt64))
		defer monitor.Stop(ctx)
	}
	acc := monitor.MakeBoundAccount()
	defer acc.Close(ctx)
	_, isLocal := grpcutil.IsLocalRequestContext(ctx)
	// Note that the CFetcherWrapper might still serialize the batches even for
	// local requests.
	mustSerialize := !isLocal
	wrapper, err := GetCFetcherWrapper(
		ctx,
		st,
		&acc,
		indexFetchSpec,
		&adapter,
		key,
		mustSerialize,
	)
	if err != nil {
		return MVCCScanResult{}, err
	}
	defer wrapper.Close(ctx)

	adapter.results.onClear = func() {
		// Discard the accumulated batches on results.clear() call - the scan
		// will result in an error.
		res = MVCCScanResult{}
	}
	for {
		serializedBatch, colBatch, err := wrapper.NextBatch(ctx)
		if err != nil {
			return res, err
		}
		if serializedBatch == nil && colBatch == nil {
			break
		}
		if len(serializedBatch) > 0 {
			res.KVData = append(res.KVData, serializedBatch)
		} else {
			res.ColBatches = append(res.ColBatches, colBatch)
		}
	}
	if buildutil.CrdbTestBuild {
		if mustSerialize && len(res.ColBatches) > 0 {
			return MVCCScanResult{}, errors.AssertionFailedf(
				"in-memory batches returned by the CFetcherWrapper when serialization is required",
			)
		}
		if len(res.KVData) > 0 && len(res.ColBatches) > 0 {
			return MVCCScanResult{}, errors.AssertionFailedf(
				"both serialized and in-memory batches returned by the CFetcherWrapper",
			)
		}
	}

	res.ResumeSpan, res.ResumeReason, res.ResumeNextBytes, err = mvccScanner.afterScan()
	if err != nil {
		return MVCCScanResult{}, err
	}
	if err = finalizeScanResult(mvccScanner, &res, opts); err != nil {
		return MVCCScanResult{}, err
	}
	return res, nil
}
