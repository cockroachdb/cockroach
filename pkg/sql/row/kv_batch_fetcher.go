// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package row

import (
	"context"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/col/coldata"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colmem"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// getKVBatchSize returns the number of keys we request at a time.
// On a single node, 1000 was enough to avoid any performance degradation. On
// multi-node clusters, we want bigger chunks to make up for the higher latency.
//
// If forceProductionKVBatchSize is true, then the "production" value will be
// returned regardless of whether the build is metamorphic or not. This should
// only be used by tests the output of which differs if defaultKVBatchSize is
// randomized.
// TODO(radu): parameters like this should be configurable
func getKVBatchSize(forceProductionKVBatchSize bool) rowinfra.KeyLimit {
	if forceProductionKVBatchSize {
		return rowinfra.ProductionKVBatchSize
	}
	return defaultKVBatchSize
}

var defaultKVBatchSize = rowinfra.KeyLimit(util.ConstantWithMetamorphicTestValue(
	"kv-batch-size",
	int(rowinfra.ProductionKVBatchSize), /* defaultValue */
	1,                                   /* metamorphicValue */
))

var logAdmissionPacerErr = log.Every(100 * time.Millisecond)

// elasticCPUDurationPerLowPriReadResponse controls how many CPU tokens are allotted
// each time we seek admission for response handling during internally submitted
// low priority reads (like row-level TTL selects).
var elasticCPUDurationPerLowPriReadResponse = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"sqladmission.elastic_cpu.duration_per_low_pri_read_response",
	"controls how many CPU tokens are allotted for handling responses for internally submitted low priority reads",
	// NB: Experimentally, during TTL reads, we observed cumulative on-CPU time
	// by SQL processors >> 100ms, over the course of a single select fetching
	// many rows. So we pick a relatively high duration here.
	100*time.Millisecond,
	settings.DurationInRange(admission.MinElasticCPUDuration, admission.MaxElasticCPUDuration),
)

// internalLowPriReadElasticControlEnabled determines whether the sql portion of
// internally submitted low-priority reads (like row-level TTL selects)
// integrate with elastic CPU control.
var internalLowPriReadElasticControlEnabled = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"sqladmission.low_pri_read_response_elastic_control.enabled",
	"determines whether the sql portion of internally submitted reads integrate with elastic CPU controller",
	true,
)

// sendFunc is the function used to execute a KV batch; normally
// wraps (*kv.Txn).Send.
type sendFunc func(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error)

// identifiableSpans is a helper for keeping track of the roachpb.Spans with the
// corresponding spanIDs (when necessary).
type identifiableSpans struct {
	roachpb.Spans
	spanIDs []int
	// If set, then each span is deeply reset on pop().
	deepResetOnPop bool
}

// pop removes the first span as well as its span ID from s and returns them.
func (s *identifiableSpans) pop() (roachpb.Span, int) {
	origSpan := s.Spans[0]
	if s.deepResetOnPop {
		s.Spans[0] = roachpb.Span{}
	}
	s.Spans = s.Spans[1:]
	var spanID int
	if s.spanIDs != nil {
		spanID = s.spanIDs[0]
		s.spanIDs = s.spanIDs[1:]
	}
	return origSpan, spanID
}

// push appends the given span as well as its span ID to s.
func (s *identifiableSpans) push(span roachpb.Span, spanID int) {
	s.Spans = append(s.Spans, span)
	if s.spanIDs != nil {
		s.spanIDs = append(s.spanIDs, spanID)
	}
}

// reset sets up s for reuse - the underlying slices will be overwritten with
// future calls to push().
func (s *identifiableSpans) reset() {
	s.Spans = s.Spans[:0]
	s.spanIDs = s.spanIDs[:0]
}

// txnKVFetcher handles retrieval of key/values.
type txnKVFetcher struct {
	kvBatchFetcherHelper

	sendFn sendFunc
	// spans is the list of Spans that will be read by this KV Fetcher. If an
	// individual Span has only a start key, it will be interpreted as a
	// single-key fetch and may use a GetRequest under the hood.
	spans identifiableSpans
	// resumeSpans is the list of "resume spans" that were encountered on the
	// last fetch. Once the current BatchResponse is fully processed, this list
	// is transitioned into 'spans' to be read on the next fetch.
	//
	// Internally, depending on SpansHandlingMode, the underlying spans slice
	// can either be the same as the "original" spans slice provided in
	// SetupNextFetch or freshly allocated one. spanIDs is always the one that
	// was passed in SetupNextFetch.
	resumeSpans identifiableSpans
	// spansMode determines how the "original" spans slice must be handled.
	spansMode SpansHandlingMode

	curSpanID int

	// If firstBatchKeyLimit is set, the first batch is limited in number of keys
	// to this value and subsequent batches are larger (up to a limit, see
	// getKVBatchSize()). If not set, batches do not have a key limit (they might
	// still have a bytes limit as per batchBytesLimit).
	firstBatchKeyLimit rowinfra.KeyLimit
	// If batchBytesLimit is set, the batches are limited in response size. This
	// protects from OOMs, but comes at the cost of inhibiting DistSender-level
	// parallelism within a batch.
	//
	// If batchBytesLimit is not set, the assumption is that SQL *knows* that
	// there is only a "small" amount of data to be read (i.e. scanning `spans`
	// doesn't result in too much data), and wants to preserve concurrency for
	// this scans inside of DistSender.
	batchBytesLimit rowinfra.BytesLimit

	// scanFormat indicates the scan format that should be used for Scans and
	// ReverseScans. With COL_BATCH_RESPONSE scan format, indexFetchSpec must be
	// set.
	scanFormat     kvpb.ScanFormat
	indexFetchSpec *fetchpb.IndexFetchSpec

	reverse bool
	// lockStrength represents the locking mode to use when fetching KVs.
	lockStrength lock.Strength
	// lockWaitPolicy represents the policy to be used for handling conflicting
	// locks held by other active transactions.
	lockWaitPolicy lock.WaitPolicy
	// lockDurability represents the locking durability to use.
	lockDurability lock.Durability
	// lockTimeout specifies the maximum amount of time that the fetcher will
	// wait while attempting to acquire a lock on a key or while blocking on an
	// existing lock in order to perform a non-locking read on a key.
	lockTimeout time.Duration

	// alreadyFetched indicates whether fetch() has already been executed at
	// least once.
	alreadyFetched bool
	batchIdx       int
	reqsScratch    []kvpb.RequestUnion

	responses           []kvpb.ResponseUnion
	kvPairsRead         int64
	remainingBatches    [][]byte
	remainingColBatches []coldata.Batch

	// getResponseScratch is reused to return the result of Get requests.
	getResponseScratch [1]roachpb.KeyValue

	acc          *mon.BoundAccount
	accountedFor struct {
		// spans tracks the number of bytes that we've already registered with
		// acc in regard to 'spans' field.
		spans int64
		// originalSpans tracks the number of bytes that we've already
		// registered with acc in regard to the spans slice that was passed on
		// the last call to SetupNextFetch when spansMode is DoNotModifySpans.
		// In such a scenario, even though we might be done with that spans
		// slice, we must keep it accounted for according to the contract.
		originalSpans int64
		// batchResponse and reqsScratch track the number of bytes that we've
		// already registered with acc in regard to the batch response and
		// reqsScratch, respectively.
		batchResponse int64
		reqsScratch   int64
	}

	// If set, we will use the production value for kvBatchSize.
	forceProductionKVBatchSize bool

	// For request and response admission control.
	requestAdmissionHeader kvpb.AdmissionHeader
	responseAdmissionQ     *admission.WorkQueue
	admissionPacer         *admission.Pacer
}

var _ KVBatchFetcher = &txnKVFetcher{}

// getBatchKeyLimit returns the max size of the next batch. The size is
// expressed in number of result keys (i.e. this size will be used for
// MaxSpanRequestKeys).
func (f *txnKVFetcher) getBatchKeyLimit() rowinfra.KeyLimit {
	return f.getBatchKeyLimitForIdx(f.batchIdx)
}

func (f *txnKVFetcher) getBatchKeyLimitForIdx(batchIdx int) rowinfra.KeyLimit {
	if f.firstBatchKeyLimit == 0 {
		return 0
	}

	kvBatchSize := getKVBatchSize(f.forceProductionKVBatchSize)
	if f.firstBatchKeyLimit >= kvBatchSize {
		return kvBatchSize
	}

	// We grab the first batch according to the limit. If it turns out that we
	// need another batch, we grab a bigger batch. If that's still not enough,
	// we revert to the default batch size.
	switch batchIdx {
	case 0:
		return f.firstBatchKeyLimit

	case 1:
		// Make the second batch 10 times larger (but at most the default batch
		// size and at least 1/10 of the default batch size). Sample
		// progressions of batch sizes:
		//
		//  First batch |  Second batch  | Subsequent batches
		//  -----------------------------------------------
		//         1    |     10,000     |     100,000
		//       100    |     10,000     |     100,000
		//      5000    |     50,000     |     100,000
		//     10000    |    100,000     |     100,000
		secondBatch := f.firstBatchKeyLimit * 10
		switch {
		case secondBatch < kvBatchSize/10:
			return kvBatchSize / 10
		case secondBatch > kvBatchSize:
			return kvBatchSize
		default:
			return secondBatch
		}

	default:
		return kvBatchSize
	}
}

func makeTxnKVFetcherDefaultSendFunc(txn *kv.Txn, batchRequestsIssued *int64) sendFunc {
	return func(
		ctx context.Context,
		ba *kvpb.BatchRequest,
	) (*kvpb.BatchResponse, error) {
		res, err := txn.Send(ctx, ba)
		if err != nil {
			return nil, err.GoError()
		}
		*batchRequestsIssued++
		return res, nil
	}
}

type newTxnKVFetcherArgs struct {
	sendFn                     sendFunc
	reverse                    bool
	lockStrength               descpb.ScanLockingStrength
	lockWaitPolicy             descpb.ScanLockingWaitPolicy
	lockDurability             descpb.ScanLockingDurability
	lockTimeout                time.Duration
	acc                        *mon.BoundAccount
	forceProductionKVBatchSize bool
	kvPairsRead                *int64
	batchRequestsIssued        *int64

	admission struct { // groups AC-related fields
		requestHeader  kvpb.AdmissionHeader
		responseQ      *admission.WorkQueue
		pacerFactory   admission.PacerFactory
		settingsValues *settings.Values
	}
}

// newTxnKVFetcherInternal initializes a txnKVFetcher.
//
// The passed-in memory account is owned by the fetcher throughout its lifetime
// but is **not** closed - it is the caller's responsibility to close acc if it
// is non-nil.
func newTxnKVFetcherInternal(args newTxnKVFetcherArgs) *txnKVFetcher {
	f := &txnKVFetcher{
		sendFn: args.sendFn,
		// Default to BATCH_RESPONSE. The caller will override if needed.
		scanFormat:                 kvpb.BATCH_RESPONSE,
		reverse:                    args.reverse,
		lockStrength:               GetKeyLockingStrength(args.lockStrength),
		lockWaitPolicy:             GetWaitPolicy(args.lockWaitPolicy),
		lockDurability:             GetKeyLockingDurability(args.lockDurability),
		lockTimeout:                args.lockTimeout,
		acc:                        args.acc,
		forceProductionKVBatchSize: args.forceProductionKVBatchSize,
		requestAdmissionHeader:     args.admission.requestHeader,
		responseAdmissionQ:         args.admission.responseQ,
	}

	f.maybeInitAdmissionPacer(
		args.admission.requestHeader,
		args.admission.pacerFactory,
		args.admission.settingsValues,
	)
	f.kvBatchFetcherHelper.init(f.nextBatch, args.kvPairsRead, args.batchRequestsIssued)
	return f
}

// setTxnAndSendFn updates the txnKVFetcher with the new txn and sendFn. txn and
// sendFn are assumed to be non-nil.
func (f *txnKVFetcher) setTxnAndSendFn(txn *kv.Txn, sendFn sendFunc) {
	f.sendFn = sendFn
	f.requestAdmissionHeader = txn.AdmissionHeader()
	f.responseAdmissionQ = txn.DB().SQLKVResponseAdmissionQ

	f.admissionPacer.Close()
	f.maybeInitAdmissionPacer(txn.AdmissionHeader(), txn.DB().AdmissionPacerFactory, txn.DB().SettingsValues)
}

// maybeInitAdmissionPacer selectively initializes an admission.Pacer for work
// done as part of internally submitted low-priority reads (like row-level TTL
// selects).
func (f *txnKVFetcher) maybeInitAdmissionPacer(
	admissionHeader kvpb.AdmissionHeader, pacerFactory admission.PacerFactory, sv *settings.Values,
) {
	if sv == nil {
		// Only nil in tests and in SQL pods (we don't have admission pacing in
		// the latter anyway).
		return
	}
	admissionPri := admissionpb.WorkPriority(admissionHeader.Priority)
	if internalLowPriReadElasticControlEnabled.Get(sv) &&
		admissionPri < admissionpb.UserLowPri &&
		pacerFactory != nil {

		f.admissionPacer = pacerFactory.NewPacer(
			elasticCPUDurationPerLowPriReadResponse.Get(sv),
			admission.WorkInfo{
				// NB: This is either code that runs in physically isolated SQL
				// pods for secondary tenants, or for the system tenant, in
				// nodes running colocated SQL+KV code where all SQL code is run
				// on behalf of the one tenant. So from an AC perspective, the
				// tenant ID we pass through here is irrelevant.
				TenantID:   roachpb.SystemTenantID,
				Priority:   admissionPri,
				CreateTime: admissionHeader.CreateTime,
			})
	}
}

// SetupNextFetch sets up the Fetcher for the next set of spans.
//
// The fetcher will perform the memory accounting for the spans slice (if acc
// was provided in the constructor) and might modify it (depending on
// spansMode). The caller can only reuse the spans slice after all rows have
// been fetched (i.e. NextBatch returned MoreKVs=false) or the fetcher has been
// closed.
//
// The fetcher takes full ownership of the spanIDs slice - it can modify the
// slice, but it will **not** perform the memory accounting. It is the caller's
// responsibility to track the memory under the spanIDs slice, and the slice
// can only be reused once the fetcher has been closed. Notably, the capacity of
// the slice will not be increased by the fetcher.
//
// If spanIDs is non-nil, then it must be of the same length as spans.
//
// Batch limits can only be used if the spans are ordered or if spansCanOverlap
// is set.
//
// Note that if
// - spansCanOverlap is true
// - multiple spans are given
// - a single span touches multiple ranges
// - batch limits are used,
// then fetched rows from different spans can be interspersed with one another.
//
// Consider the following example: we have two ranges [a, b) and [b, c) and each
// has a single row inside ("a" and "b"). If SetupNextFetch were to be called
// with:
//
//	spans = [[a, c), [a, d)]  spanIDs = [0, 1]  batchBytesLimit = 2  spansCanOverlap = true
//
// then we would return
//
//	"a", spanID = 0
//	"a", spanID = 1
//
// on the first batch, and then
//
//	"b", spanID = 0
//	"b", spanID = 1.
//
// Note that since we never split ranges in the middle of SQL rows, the returned
// rows will still be complete (or the last row might be incomplete, but it'll
// be resumed by the next returned batch (when we have multiple column
// families)).
func (f *txnKVFetcher) SetupNextFetch(
	ctx context.Context,
	spans roachpb.Spans,
	spanIDs []int,
	spansMode SpansHandlingMode,
	batchBytesLimit rowinfra.BytesLimit,
	firstBatchKeyLimit rowinfra.KeyLimit,
	spansCanOverlap bool,
) error {
	f.reset(ctx)

	if firstBatchKeyLimit < 0 || (batchBytesLimit == 0 && firstBatchKeyLimit != 0) {
		// Passing firstBatchKeyLimit without batchBytesLimit doesn't make sense
		// - the only reason to not set batchBytesLimit is in order to get
		// DistSender-level parallelism, and setting firstBatchKeyLimit inhibits
		// that.
		return errors.Errorf("invalid batch limit %d (batchBytesLimit: %d)", firstBatchKeyLimit, batchBytesLimit)
	}

	if spansCanOverlap {
		if spanIDs == nil {
			return errors.AssertionFailedf("spanIDs must be non-nil when spansCanOverlap is true")
		}
	} else {
		if batchBytesLimit != 0 {
			// Verify the spans are ordered if a batch limit is used.
			for i := 1; i < len(spans); i++ {
				prevKey := spans[i-1].EndKey
				if prevKey == nil {
					// This is the case of a GetRequest.
					prevKey = spans[i-1].Key
				}
				if spans[i].Key.Compare(prevKey) < 0 {
					return errors.Errorf("unordered spans (%s %s)", spans[i-1], spans[i])
				}
			}
		} else if util.RaceEnabled {
			// Otherwise, just verify the spans don't contain consecutive
			// overlapping spans.
			for i := 1; i < len(spans); i++ {
				prevEndKey := spans[i-1].EndKey
				if prevEndKey == nil {
					prevEndKey = spans[i-1].Key
				}
				curEndKey := spans[i].EndKey
				if curEndKey == nil {
					curEndKey = spans[i].Key
				}
				if spans[i].Key.Compare(prevEndKey) >= 0 {
					// Current span's start key is greater than or equal to the
					// last span's end key - we're good.
					continue
				} else if curEndKey.Compare(spans[i-1].Key) <= 0 {
					// Current span's end key is less than or equal to the last
					// span's start key - also good.
					continue
				}
				// Otherwise, the two spans overlap, which isn't allowed - it
				// leaves us at risk of incorrect results, since the row fetcher
				// can't distinguish between identical rows in two different
				// batches.
				return errors.Errorf("overlapping neighbor spans (%s %s)", spans[i-1], spans[i])
			}
		}
	}

	f.batchBytesLimit = batchBytesLimit
	f.firstBatchKeyLimit = firstBatchKeyLimit

	// Account for the memory of the spans according to the contract of
	// SetupNextFetch.
	if f.acc != nil {
		newSpansAccountedFor := spans.MemUsage()
		if err := f.acc.Grow(ctx, newSpansAccountedFor); err != nil {
			return err
		}
		f.accountedFor.spans = newSpansAccountedFor
	}

	if spanIDs != nil && len(spans) != len(spanIDs) {
		return errors.AssertionFailedf("unexpectedly non-nil spanIDs slice has a different length than spans")
	}

	f.spans = identifiableSpans{
		Spans:   spans,
		spanIDs: spanIDs,
	}
	f.spansMode = spansMode
	if spansMode == DoNotModifySpans {
		// Currently, in DoNotModifySpans mode we never reuse the scratch spans
		// slice that we allocate between SetupNextFetch calls because that mode
		// is only used when SetupNextFetch is called only once. If that ever
		// changes, we'll need to keep the reference while keeping the proper
		// memory accounting.
		if f.reverse {
			// Reverse scans receive the spans in decreasing order, so we need
			// to revert the slice. However, we cannot modify the original spans
			// slice, so we need to copy and revert it straight into our own
			// scratch space.
			//
			// Note that for simplicity we delay the memory accounting for this
			// slice until we're done with the first fetch(). This seems
			// acceptable given that we did account for the keys inside the
			// spans, so we're only missing accounting for roachpb.Span objects
			// (i.e. the overhead). Currently, this code path should only be
			// triggered when we have a handful of spans to scan (because lookup
			// and index joins use CanModifySpans).
			spansScratch := make(roachpb.Spans, len(spans))
			for i, j := 0, len(spans)-1; i < len(spans); i, j = i+1, j-1 {
				spansScratch[i] = spans[j]
			}
			f.spans.Spans = spansScratch
			// We just allocated our own spans slice and will only use that
			// going forward, so it's safe to modify it - the original spans
			// slice won't be affected.
			f.spans.deepResetOnPop = true
			f.resumeSpans = f.spans
		} else {
			// For forward scans when we can't modify the spans slice, we don't
			// pre-allocate anything for the resume spans - we'll make that
			// allocation lazily in push().
			f.resumeSpans = identifiableSpans{
				spanIDs:        spanIDs,
				deepResetOnPop: true,
			}
		}
	} else {
		// We can modify the spans slice as we please, so we will also use it
		// as our scratch space.
		f.spans.deepResetOnPop = true
		f.resumeSpans = f.spans
		if buildutil.CrdbTestBuild && f.reverse {
			return errors.AssertionFailedf(
				"reverse is currently expected to be used only by" +
					"the table reader which doesn't allow spans modification",
			)
		}
	}

	return nil
}

// fetch retrieves spans from the kv layer.
func (f *txnKVFetcher) fetch(ctx context.Context) error {
	ba := &kvpb.BatchRequest{}
	ba.Header.WaitPolicy = f.lockWaitPolicy
	ba.Header.LockTimeout = f.lockTimeout
	ba.Header.TargetBytes = int64(f.batchBytesLimit)
	ba.Header.MaxSpanRequestKeys = int64(f.getBatchKeyLimit())
	if buildutil.CrdbTestBuild {
		if f.scanFormat == kvpb.COL_BATCH_RESPONSE && f.indexFetchSpec == nil {
			return errors.AssertionFailedf("IndexFetchSpec not provided with COL_BATCH_RESPONSE scan format")
		}
	}
	if f.indexFetchSpec != nil {
		ba.IndexFetchSpec = f.indexFetchSpec
		// SQL operators assume that rows are always complete in
		// coldata.Batch'es, so we must use the WholeRowsOfSize option in order
		// to tell the KV layer to never split SQL rows across the
		// BatchResponses.
		ba.Header.WholeRowsOfSize = int32(f.indexFetchSpec.MaxKeysPerRow)
	}
	ba.AdmissionHeader = f.requestAdmissionHeader
	ba.Requests = spansToRequests(
		f.spans.Spans, f.scanFormat, f.reverse, f.lockStrength, f.lockDurability, f.reqsScratch,
	)

	if log.ExpensiveLogEnabled(ctx, 2) {
		log.VEventf(ctx, 2, "Scan %s", f.spans)
	}

	monitoring := f.acc != nil

	const tokenFetchAllocation = 1 << 10
	if !monitoring || f.accountedFor.batchResponse < tokenFetchAllocation {
		// In case part of this batch ends up being evaluated locally, we want
		// that local evaluation to do memory accounting since we have reserved
		// negligible bytes. Ideally, we would split the memory reserved across
		// the various servers that DistSender will split this batch into, but we
		// do not yet have that capability.
		ba.AdmissionHeader.NoMemoryReservedAtSource = true
	}
	if monitoring && f.accountedFor.batchResponse < tokenFetchAllocation {
		// Pre-reserve a token fraction of the maximum amount of memory this scan
		// could return. Most of the time, scans won't use this amount of memory,
		// so it's unnecessary to reserve it all. We reserve something rather than
		// nothing at all to preserve some accounting.
		if err := f.acc.Resize(ctx, f.accountedFor.batchResponse, tokenFetchAllocation); err != nil {
			return err
		}
		f.accountedFor.batchResponse = tokenFetchAllocation
	}

	br, err := f.sendFn(ctx, ba)
	if err != nil {
		return err
	}
	if br != nil {
		f.responses = br.Responses
		f.kvPairsRead = 0
		for i := range f.responses {
			f.kvPairsRead += f.responses[i].GetInner().Header().NumKeys
		}
	} else {
		f.responses = nil
	}
	// TODO(yuzefovich): BatchResponse.Size ignores the overhead of the
	// GetResponse and ScanResponse structs. We should include it here.
	returnedBytes := int64(br.Size())
	if monitoring && (returnedBytes > int64(f.batchBytesLimit) || returnedBytes > f.accountedFor.batchResponse) {
		// Resize up to the actual amount of bytes we got back from the fetch,
		// but don't ratchet down below f.batchBytesLimit if we ever exceed it.
		// We would much prefer to over-account than under-account, especially when
		// we are in a situation where we have large batches caused by parallel
		// unlimited scans (index joins and lookup joins where cols are key).
		//
		// The reason we don't want to precisely account here is to hopefully
		// protect ourselves from "slop" in our memory handling. In general, we
		// expect that all SQL operators that buffer data for longer than a single
		// call to Next do their own accounting, so theoretically, by the time
		// this fetch method is called again, all memory will either be released
		// from the system or accounted for elsewhere. In reality, though, Go's
		// garbage collector has some lag between when the memory is no longer
		// referenced and when it is freed. Also, we're not perfect with
		// accounting by any means. When we start doing large fetches, it's more
		// likely that we'll expose ourselves to OOM conditions, so that's the
		// reasoning for why we never ratchet this account down past the maximum
		// fetch size once it's exceeded.
		if err := f.acc.Resize(ctx, f.accountedFor.batchResponse, returnedBytes); err != nil {
			return err
		}
		f.accountedFor.batchResponse = returnedBytes
	}

	// Do admission control after we've accounted for the response bytes.
	if err := f.maybeAdmitBatchResponse(ctx, br); err != nil {
		return err
	}

	f.batchIdx++
	f.resumeSpans.reset()
	f.alreadyFetched = true
	// Keep the reference to the requests slice in order to reuse in the future
	// after making sure to nil out the requests in order to lose references to
	// the underlying Get and Scan requests which could keep large byte slices
	// alive.
	f.reqsScratch = ba.Requests
	for i := range f.reqsScratch {
		f.reqsScratch[i] = kvpb.RequestUnion{}
	}
	if monitoring {
		reqsScratchMemUsage := requestUnionOverhead * int64(cap(f.reqsScratch))
		if err := f.acc.Resize(ctx, f.accountedFor.reqsScratch, reqsScratchMemUsage); err != nil {
			return err
		}
		f.accountedFor.reqsScratch = reqsScratchMemUsage
	}

	// TODO(radu): We should fetch the next chunk in the background instead of waiting for the next
	// call to fetch(). We can use a pool of workers to issue the KV ops which will also limit the
	// total number of fetches that happen in parallel (and thus the amount of resources we use).
	return nil
}

func (f *txnKVFetcher) maybeAdmitBatchResponse(ctx context.Context, br *kvpb.BatchResponse) error {
	if br == nil {
		return nil // nothing to do
	}

	if f.admissionPacer != nil {
		// If admissionPacer is initialized, we're using the elastic CPU control
		// mechanism (the work is elastic in nature and using the slots based
		// mechanism would permit high scheduling latencies). We want to limit
		// the CPU% used by SQL during internally submitted reads, like
		// row-level TTL selects. All that work happens on the same goroutine
		// doing this fetch, so is accounted for when invoking .Pace() as we
		// fetch KVs as part of our volcano operator iteration. See CPU profiles
		// posted on #98722.
		//
		// TODO(irfansharif): At the time of writing, SELECTs done by the TTL
		// job are not distributed at SQL level (since our DistSQL physical
		// planning heuristics deems it not worthy of distribution), and with
		// the local plan we only have a single goroutine (unless
		// maybeParallelizeLocalScans splits up the single scan into multiple
		// TableReader processors). This may change as part of
		// https://github.com/cockroachdb/cockroach/issues/82164 where CPU
		// intensive SQL work will happen on a different goroutine from the ones
		// that evaluate the BatchRequests, so the integration is tricker there.
		// If we're unable to integrate it well, we could disable usage of the
		// streamer to preserve this current form of pacing.
		//
		// TODO(irfansharif): Add tests for the SELECT queries issued by the TTL
		// to ensure that they have local plans with a single TableReader
		// processor in multi-node clusters.
		if err := f.admissionPacer.Pace(ctx); err != nil {
			// We're unable to pace things automatically -- shout loudly
			// semi-infrequently but don't fail the kv fetcher itself. At
			// worst we'd be over-admitting.
			if logAdmissionPacerErr.ShouldLog() {
				log.Errorf(ctx, "automatic pacing: %v", err)
			}
		}
	} else if f.responseAdmissionQ != nil {
		responseAdmission := admission.WorkInfo{
			TenantID:   roachpb.SystemTenantID,
			Priority:   admissionpb.WorkPriority(f.requestAdmissionHeader.Priority),
			CreateTime: f.requestAdmissionHeader.CreateTime,
		}
		if _, err := f.responseAdmissionQ.Admit(ctx, responseAdmission); err != nil {
			return err
		}
	}

	return nil
}

// popBatch returns the 0th "batch" in a slice of "batches", as well as the rest
// of the slice of the "batches". It nils the pointer to the 0th element before
// reslicing the outer slice.
//
// Note that since we nil out the 0th element, the caller of nextBatch() will
// have the only reference to it. As a result, the next time nextBatch() is
// called previously-returned element should become garbage, and we could shrink
// the memory usage accordingly. In other words, we're still accounting for some
// memory after it became garbage. However, given our history of
// under-accounting in most places, this seems acceptable.
func popBatch(
	batches [][]byte, colBatches []coldata.Batch,
) (
	batch []byte,
	remainingBatches [][]byte,
	colBatch coldata.Batch,
	remainingColBatches []coldata.Batch,
) {
	if batches != nil {
		batch, remainingBatches = batches[0], batches[1:]
		batches[0] = nil
		return batch, remainingBatches, nil, nil
	}
	colBatch, remainingColBatches = colBatches[0], colBatches[1:]
	colBatches[0] = nil
	return nil, nil, colBatch, remainingColBatches
}

func (f *txnKVFetcher) nextBatch(ctx context.Context) (resp KVBatchFetcherResponse, err error) {
	// The purpose of this loop is to unpack the two-level batch structure that is
	// returned from the KV layer.
	//
	// A particular BatchRequest from fetch will populate the f.responses field
	// with one response per request in the input BatchRequest. Each response
	// in the responses field itself can also have a list of "BatchResponses",
	// each of which is a byte slice containing result data from KV. Since this
	// function, by contract, returns just a single byte slice at a time, we store
	// the inner list as state for the next invocation to pop from.
	if len(f.remainingBatches) > 0 || len(f.remainingColBatches) > 0 {
		// Are there remaining data batches? If so, just pop one off from the
		// list and return it.
		var batchResp []byte
		var colBatch coldata.Batch
		batchResp, f.remainingBatches, colBatch, f.remainingColBatches = popBatch(f.remainingBatches, f.remainingColBatches)
		return KVBatchFetcherResponse{
			MoreKVs:       true,
			BatchResponse: batchResp,
			ColBatch:      colBatch,
			spanID:        f.curSpanID,
		}, nil
	}
	// There are no remaining data batches. Find the first non-empty ResponseUnion
	// in the list of unprocessed responses from the last BatchResponse we sent,
	// and process it.
	for len(f.responses) > 0 {
		reply := f.responses[0].GetInner()
		f.responses[0] = kvpb.ResponseUnion{}
		f.responses = f.responses[1:]
		// Get the original span right away since we might overwrite it with the
		// resume span below.
		var origSpan roachpb.Span
		origSpan, f.curSpanID = f.spans.pop()

		// Check whether we need to resume scanning this span.
		header := reply.Header()
		if header.NumKeys > 0 && f.resumeSpans.Len() > 0 {
			return KVBatchFetcherResponse{}, errors.Errorf(
				"span with results after resume span; it shouldn't happen given that "+
					"we're only scanning non-overlapping spans. New spans: %s",
				catalogkeys.PrettySpans(nil, f.spans.Spans, 0 /* skip */))
		}

		// Any requests that were not fully completed will have the ResumeSpan set.
		// Here we accumulate all of them.
		if resumeSpan := header.ResumeSpan; resumeSpan != nil {
			f.resumeSpans.push(*resumeSpan, f.curSpanID)
		}

		ret := KVBatchFetcherResponse{
			MoreKVs:     true,
			spanID:      f.curSpanID,
			kvPairsRead: f.kvPairsRead,
		}
		f.kvPairsRead = 0

		switch t := reply.(type) {
		case *kvpb.ScanResponse:
			if len(t.BatchResponses) > 0 || len(t.ColBatches.ColBatches) > 0 {
				ret.BatchResponse, f.remainingBatches, ret.ColBatch, f.remainingColBatches = popBatch(t.BatchResponses, t.ColBatches.ColBatches)
			}
			if len(t.Rows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse using KEY_VALUES response format",
				)
			}
			if len(t.IntentRows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse with non-nil IntentRows",
				)
			}
			// Note that ret.BatchResponse and ret.ColBatch might be nil when
			// the ScanResponse is empty, and the callers will skip over it.
			return ret, nil
		case *kvpb.ReverseScanResponse:
			if len(t.BatchResponses) > 0 || len(t.ColBatches.ColBatches) > 0 {
				ret.BatchResponse, f.remainingBatches, ret.ColBatch, f.remainingColBatches = popBatch(t.BatchResponses, t.ColBatches.ColBatches)
			}
			if len(t.Rows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse using KEY_VALUES response format",
				)
			}
			if len(t.IntentRows) > 0 {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf(
					"unexpectedly got a ScanResponse with non-nil IntentRows",
				)
			}
			// Note that ret.BatchResponse and ret.ColBatch might be nil when
			// the ReverseScanResponse is empty, and the callers will skip over
			// it.
			return ret, nil
		case *kvpb.GetResponse:
			if t.IntentValue != nil {
				return KVBatchFetcherResponse{}, errors.AssertionFailedf("unexpectedly got an IntentValue back from a SQL GetRequest %v", *t.IntentValue)
			}
			if t.Value == nil {
				// Nothing found in this particular response, let's continue to the next
				// one.
				continue
			}
			f.getResponseScratch[0] = roachpb.KeyValue{Key: origSpan.Key, Value: *t.Value}
			ret.KVs = f.getResponseScratch[:]
			return ret, nil
		}
	}
	// No more responses from the last BatchRequest.
	if f.alreadyFetched {
		if f.resumeSpans.Len() == 0 {
			// If we are out of keys, we can return and we're finished with the
			// fetch.
			return KVBatchFetcherResponse{MoreKVs: false}, nil
		}
		// We have some resume spans.
		f.spans = f.resumeSpans
		if f.acc != nil {
			if f.spansMode == DoNotModifySpans && f.accountedFor.originalSpans == 0 {
				// We just finished our first fetch, and we cannot modify the
				// spans. This means that we must be using our own scratch space
				// for resume spans from now on. The contract of SetupNextFetch
				// says that we must keep accounting for the "original" spans
				// slice too, so we do that here. (In other words, we need to
				// account for two slices going forward.)
				if err = f.acc.Grow(ctx, f.accountedFor.spans); err != nil {
					return KVBatchFetcherResponse{}, err
				}
				f.accountedFor.originalSpans = f.accountedFor.spans
				f.accountedFor.spans = 0
			}
			newSpansMemUsage := f.spans.MemUsage()
			if err = f.acc.Resize(ctx, f.accountedFor.spans, newSpansMemUsage); err != nil {
				return KVBatchFetcherResponse{}, err
			}
			f.accountedFor.spans = newSpansMemUsage
		}
	}
	// We have more work to do. Ask the KV layer to continue where it left off.
	if err := f.fetch(ctx); err != nil {
		return KVBatchFetcherResponse{}, err
	}
	// We've got more data to process, recurse and process it.
	return f.nextBatch(ctx)
}

func (f *txnKVFetcher) reset(ctx context.Context) {
	f.alreadyFetched = false
	f.batchIdx = 0
	f.responses = nil
	f.remainingBatches = nil
	f.remainingColBatches = nil
	f.spans = identifiableSpans{}
	f.resumeSpans = identifiableSpans{}
	// Release only the allocations made by this fetcher. Note that we're still
	// keeping the reference to reqsScratch, so we don't release the allocation
	// for it.
	f.acc.Shrink(ctx, f.accountedFor.spans+f.accountedFor.originalSpans+f.accountedFor.batchResponse)
	f.accountedFor.spans, f.accountedFor.originalSpans, f.accountedFor.batchResponse = 0, 0, 0
}

// Close releases the resources of this txnKVFetcher.
func (f *txnKVFetcher) Close(ctx context.Context) {
	f.reset(ctx)
	f.admissionPacer.Close()
}

const requestUnionOverhead = int64(unsafe.Sizeof(kvpb.RequestUnion{}))

// spansToRequests converts the provided spans to the corresponding requests. If
// a span doesn't have the EndKey set, then a Get request is used for it;
// otherwise, a Scan (or ReverseScan if reverse is true) request is used with
// the provided scan format.
//
// The provided reqsScratch is reused if it has enough capacity for all spans,
// if not, a new slice is allocated.
func spansToRequests(
	spans roachpb.Spans,
	scanFormat kvpb.ScanFormat,
	reverse bool,
	lockStrength lock.Strength,
	lockDurability lock.Durability,
	reqsScratch []kvpb.RequestUnion,
) []kvpb.RequestUnion {
	var reqs []kvpb.RequestUnion
	if cap(reqsScratch) >= len(spans) {
		reqs = reqsScratch[:len(spans)]
	} else {
		reqs = make([]kvpb.RequestUnion, len(spans))
	}
	// Detect the number of gets vs scans, so we can batch allocate all of the
	// requests precisely.
	nGets := 0
	for i := range spans {
		if spans[i].EndKey == nil {
			nGets++
		}
	}
	gets := make([]struct {
		req   kvpb.GetRequest
		union kvpb.RequestUnion_Get
	}, nGets)

	// curGet is incremented each time we fill in a GetRequest.
	curGet := 0
	if reverse {
		scans := make([]struct {
			req   kvpb.ReverseScanRequest
			union kvpb.RequestUnion_ReverseScan
		}, len(spans)-nGets)
		for i := range spans {
			if spans[i].EndKey == nil {
				// A span without an EndKey indicates that the caller is requesting a
				// single key fetch, which can be served using a GetRequest.
				gets[curGet].req.Key = spans[i].Key
				gets[curGet].req.KeyLockingStrength = lockStrength
				gets[curGet].req.KeyLockingDurability = lockDurability
				gets[curGet].union.Get = &gets[curGet].req
				reqs[i].Value = &gets[curGet].union
				curGet++
				continue
			}
			curScan := i - curGet
			scans[curScan].req.SetSpan(spans[i])
			scans[curScan].req.ScanFormat = scanFormat
			scans[curScan].req.KeyLockingStrength = lockStrength
			scans[curScan].req.KeyLockingDurability = lockDurability
			scans[curScan].union.ReverseScan = &scans[curScan].req
			reqs[i].Value = &scans[curScan].union
		}
	} else {
		scans := make([]struct {
			req   kvpb.ScanRequest
			union kvpb.RequestUnion_Scan
		}, len(spans)-nGets)
		for i := range spans {
			if spans[i].EndKey == nil {
				// A span without an EndKey indicates that the caller is requesting a
				// single key fetch, which can be served using a GetRequest.
				gets[curGet].req.Key = spans[i].Key
				gets[curGet].req.KeyLockingStrength = lockStrength
				gets[curGet].req.KeyLockingDurability = lockDurability
				gets[curGet].union.Get = &gets[curGet].req
				reqs[i].Value = &gets[curGet].union
				curGet++
				continue
			}
			curScan := i - curGet
			scans[curScan].req.SetSpan(spans[i])
			scans[curScan].req.ScanFormat = scanFormat
			scans[curScan].req.KeyLockingStrength = lockStrength
			scans[curScan].req.KeyLockingDurability = lockDurability
			scans[curScan].union.Scan = &scans[curScan].req
			reqs[i].Value = &scans[curScan].union
		}
	}
	return reqs
}

// kvBatchFetcherHelper is a small helper that extracts common logic for
// implementing some methods of the KVBatchFetcher interface related to
// observability.
type kvBatchFetcherHelper struct {
	nextBatch func(context.Context) (KVBatchFetcherResponse, error)
	atomics   struct {
		bytesRead           int64
		kvPairsRead         *int64
		batchRequestsIssued *int64
	}
}

func (h *kvBatchFetcherHelper) init(
	nextBatch func(context.Context) (KVBatchFetcherResponse, error),
	kvPairsRead, batchRequestsIssued *int64,
) {
	h.nextBatch = nextBatch
	h.atomics.kvPairsRead = kvPairsRead
	h.atomics.batchRequestsIssued = batchRequestsIssued
}

// NextBatch implements the KVBatchFetcher interface.
func (h *kvBatchFetcherHelper) NextBatch(ctx context.Context) (KVBatchFetcherResponse, error) {
	resp, err := h.nextBatch(ctx)
	if !resp.MoreKVs || err != nil {
		return resp, err
	}
	atomic.AddInt64(h.atomics.kvPairsRead, resp.kvPairsRead)
	// Note that if resp.ColBatch is nil, then GetBatchMemSize will return 0.
	// TODO(yuzefovich, 23.1): for resp.ColBatch this includes the decoded
	// footprint as well as the overhead of slices and whatnot which is
	// different from what "bytes read" is about. Figure out how we want to
	// track it here.
	nBytes := len(resp.BatchResponse) + int(colmem.GetBatchMemSize(resp.ColBatch))
	for i := range resp.KVs {
		nBytes += len(resp.KVs[i].Key)
		nBytes += len(resp.KVs[i].Value.RawBytes)
	}
	atomic.AddInt64(&h.atomics.bytesRead, int64(nBytes))
	return resp, nil
}

// GetBytesRead implements the KVBatchFetcher interface.
func (h *kvBatchFetcherHelper) GetBytesRead() int64 {
	if h == nil {
		return 0
	}
	return atomic.LoadInt64(&h.atomics.bytesRead)
}

// GetKVPairsRead implements the KVBatchFetcher interface.
func (h *kvBatchFetcherHelper) GetKVPairsRead() int64 {
	if h == nil || h.atomics.kvPairsRead == nil {
		return 0
	}
	return atomic.LoadInt64(h.atomics.kvPairsRead)
}

// GetBatchRequestsIssued implements the KVBatchFetcher interface.
func (h *kvBatchFetcherHelper) GetBatchRequestsIssued() int64 {
	if h == nil || h.atomics.batchRequestsIssued == nil {
		return 0
	}
	return atomic.LoadInt64(h.atomics.batchRequestsIssued)
}
