// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	kvpb "github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/mvccencoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
)

// Validate checks for violations of our kv api guarantees. The Steps must all
// have been applied and the kvs the result of those applications.
//
// For transactions, it is verified that all of their writes are present if and
// only if the transaction committed (which is inferred from the KV data on
// ambiguous results). Non-transactional read/write operations are treated as
// though they had been wrapped in a transaction and are verified accordingly.
//
// TODO(dan): Verify that there is no causality inversion between steps. That
// is, if transactions corresponding to two steps are sequential (i.e.
// txn1CommitStep.After < txn2BeginStep.Before) then the commit timestamps need
// to reflect this ordering.
//
// TODO(dan): Consider changing all of this validation to be based on the commit
// timestamp as given back by kv.Txn. This doesn't currently work for
// nontransactional read-only ops (i.e. single read or batch of only reads) but
// that could be fixed by altering the API to communicating the timestamp back.
//
// Splits and merges are not verified for anything other than that they did not
// return an error.
func Validate(steps []Step, kvs *Engine, dt *SeqTracker) []error {
	v, err := makeValidator(kvs, dt)
	if err != nil {
		return []error{err}
	}

	// The validator works via AOST-style queries over the kvs emitted by
	// RangeFeed. This means it can process steps in any order *except* that it
	// needs to see all txn usage in order. Generator currently only emits
	// ClosureTxnOperations, so it currently doesn't matter which order we process
	// these.
	//
	// Originally there were separate operations for Begin/Use/Commit/Rollback
	// Txn. If we add something like this back in (and I would like to), sorting
	// by `After` timestamp is sufficient to get us the necessary ordering. This
	// is because txns cannot be used concurrently, so none of the (Begin,After)
	// timespans for a given transaction can overlap.
	slices.SortFunc(steps, func(a, b Step) int { return a.After.Compare(b.After) })
	for _, s := range steps {
		v.processOp(s.Op)
	}

	var extraKVs []observedOp
	for seq, svs := range v.kvBySeq {
		for _, sv := range svs {
			mvccV, err := storage.DecodeMVCCValue(sv.Value)
			if err != nil {
				v.failures = append(v.failures, err)
				continue
			}
			kv := &observedWrite{
				Key:       sv.Key,
				EndKey:    sv.EndKey,
				Value:     mvccV.Value,
				Timestamp: sv.Timestamp,
				Seq:       seq,
			}
			extraKVs = append(extraKVs, kv)
		}
	}

	// These are writes that we saw in MVCC, but they weren't matched up to any
	// operation kvnemesis ran.
	if len(extraKVs) > 0 {
		err := errors.Errorf(`unclaimed writes: %s`, printObserved(extraKVs...))
		v.failures = append(v.failures, err)
	}

	return v.failures
}

// timeSpan represents a range of time with an inclusive start and an exclusive
// end.
type timeSpan struct {
	Start, End hlc.Timestamp
}

func (ts timeSpan) Intersect(o timeSpan) timeSpan {
	i := ts
	if i.Start.Less(o.Start) {
		i.Start = o.Start
	}
	if o.End.Less(i.End) {
		i.End = o.End
	}
	return i
}

func (ts timeSpan) IsEmpty() bool {
	return !ts.Start.Less(ts.End)
}

func (ts timeSpan) String() string {
	var start string
	if ts.Start == hlc.MinTimestamp {
		start = `<min>`
	} else {
		start = ts.Start.String()
	}
	var end string
	if ts.End == hlc.MaxTimestamp {
		end = `<max>`
	} else {
		end = ts.End.String()
	}
	return fmt.Sprintf(`[%s, %s)`, start, end)
}

// disjointTimeSpans represents a collection of timeSpan objects for when
// operations are valid. It exists as a convenience to be able to generate
// intersections when there are overlaps with other timeSpan collections.
type disjointTimeSpans []timeSpan

func (ts disjointTimeSpans) validIntersections(
	keyOpValidTimeSpans disjointTimeSpans,
) disjointTimeSpans {
	var newValidSpans disjointTimeSpans
	for _, existingValidSpan := range ts {
		for _, opValidSpan := range keyOpValidTimeSpans {
			intersection := existingValidSpan.Intersect(opValidSpan)
			if !intersection.IsEmpty() {
				newValidSpans = append(newValidSpans, intersection)
			}
		}
	}
	return newValidSpans
}

// multiKeyTimeSpan represents a collection of timeSpans: one for each key
// accessed by a ranged operation and one for the keys missed by the ranged
// operation.
type multiKeyTimeSpan struct {
	Keys []disjointTimeSpans
	Gaps disjointTimeSpans
}

func (mts multiKeyTimeSpan) Combined() disjointTimeSpans {
	validPossibilities := mts.Gaps
	for _, validKey := range mts.Keys {
		validPossibilities = validPossibilities.validIntersections(validKey)
	}
	return validPossibilities
}

func (mts multiKeyTimeSpan) String() string {
	var buf strings.Builder
	buf.WriteByte('{')
	for i, timeSpans := range mts.Keys {
		fmt.Fprintf(&buf, "%d:", i)
		for tsIdx, ts := range timeSpans {
			if tsIdx != 0 {
				fmt.Fprintf(&buf, ",")
			}
			fmt.Fprintf(&buf, "%s", ts)
		}
		fmt.Fprintf(&buf, ", ")
	}
	fmt.Fprintf(&buf, "gap:")
	for idx, gapSpan := range mts.Gaps {
		if idx != 0 {
			fmt.Fprintf(&buf, ",")
		}
		fmt.Fprintf(&buf, "%s", gapSpan)
	}
	fmt.Fprintf(&buf, "}")
	return buf.String()
}

// observedOp is the unification of an externally observed KV read or write.
// Validator collects these grouped by txn, then computes the time window that
// it would have been valid to observe this read or write, then asserts that all
// operations in a transaction have time at which they all overlap. (For any
// transaction containing a write, this will be a single time, but for read-only
// transactions it will usually be a range of times.)
type observedOp interface {
	observedMarker()
}

// An observedWrite is an effect of an operation.
type observedWrite struct {
	Key, EndKey roachpb.Key
	Value       roachpb.Value
	Seq         kvnemesisutil.Seq
	// A write is materialized if it has a timestamp.
	Timestamp     hlc.Timestamp
	IsDeleteRange bool
}

func (*observedWrite) observedMarker() {}

func (o *observedWrite) isDelete() bool {
	return !o.Value.IsPresent()
}

type observedRead struct {
	Key        roachpb.Key
	SkipLocked bool
	Value      roachpb.Value
	ValidTimes disjointTimeSpans
	// AlwaysValid indicates whether a read should be considered valid in the
	// entire interval [hlc.MinTimestamp, hlc.MaxTimestamp].
	//
	// A lock acquired by a locking read is kept until commit time, unless it's
	// rolled back by a savepoint; in that case, the lock is not released eagerly,
	// but it's also not guaranteed to be held (if the transaction is pushed, the
	// lock may be released). Serializable transactions refresh all reads at
	// commit time, so their reads should be validated even if they were rolled
	// back. Weaker-isolation transaction don't refresh their reads at commit
	// time, so if a locking read from such a transaction is rolled back, we
	// don't need to validate the read; it's valid at all times.
	AlwaysValid bool
	// DoNotObserveOnSavepointRollback indicates whether a read should be skipped
	// from validation if it's rolled back by a savepoint. It's set to true for
	// all locking reads from weak-isolation transactions.
	DoNotObserveOnSavepointRollback bool
}

func (*observedRead) observedMarker() {}

type observedScan struct {
	Span          roachpb.Span
	IsDeleteRange bool
	Reverse       bool
	SkipLocked    bool
	KVs           []roachpb.KeyValue
	Valid         multiKeyTimeSpan
}

func (*observedScan) observedMarker() {}

type savepointType int

const (
	create savepointType = iota
	release
	rollback
)

type observedSavepoint struct {
	ID   int
	Type savepointType
}

func (*observedSavepoint) observedMarker() {}

type validator struct {
	kvs *Engine

	// Observations for the current atomic unit. This is reset between units, in
	// checkAtomic, which then calls processOp (which might recurse owing to the
	// existence of txn closures, batches, etc).
	curObservations   []observedOp
	observationFilter observationFilter
	buffering         bufferingType

	// NB: The Generator carefully ensures that each value written is unique
	// globally over a run, so there's a 1:1 relationship between a value that was
	// written and the operation that wrote it.
	// kvsByKeyAndSeq map[keySeq]storage.MVCCKeyValue // TODO remove
	kvBySeq map[kvnemesisutil.Seq][]tsSpanVal

	failures []error
}

type tsSpanVal struct {
	roachpb.Span
	hlc.Timestamp
	Value []byte
}

func makeValidator(kvs *Engine, tr *SeqTracker) (*validator, error) {
	kvBySeq := make(map[kvnemesisutil.Seq][]tsSpanVal)
	var err error
	kvs.Iterate(func(key, endKey roachpb.Key, ts hlc.Timestamp, value []byte, iterErr error) {
		if err != nil {
			return
		}
		if iterErr != nil {
			err = errors.CombineErrors(err, iterErr)
			return
		}
		seq, ok := tr.Lookup(key, endKey, ts)
		if !ok {
			err = errors.AssertionFailedf("no seqno found for [%s,%s) @ %s, tracker is %v", key, endKey, ts, tr)
			return
		}
		v := tsSpanVal{
			Span:      roachpb.Span{Key: key, EndKey: endKey},
			Timestamp: ts,
			Value:     value,
		}
		kvBySeq[seq] = append(kvBySeq[seq], v)
	})
	if err != nil {
		return nil, err
	}

	return &validator{
		kvs:     kvs,
		kvBySeq: kvBySeq,
	}, nil
}

func (v *validator) tryConsumeWrite(key roachpb.Key, seq kvnemesisutil.Seq) (tsSpanVal, bool) {
	svs, ok := v.tryConsumeRangedWrite(seq, key, nil)
	if !ok {
		return tsSpanVal{}, false
	}
	if len(svs) != 1 {
		panic(fmt.Sprintf("expected exactly one element: %+v", svs))
	}
	return svs[0], true
}

func (v *validator) tryConsumeRangedWrite(
	seq kvnemesisutil.Seq, key, endKey roachpb.Key,
) ([]tsSpanVal, bool) {
	svs, ok := v.kvBySeq[seq]
	if !ok || len(svs) == 0 {
		return nil, false
	}
	opSpan := roachpb.Span{Key: key, EndKey: endKey}

	var consumed []tsSpanVal
	var remaining []tsSpanVal
	for i := range svs {
		cur := svs[i]
		if !opSpan.Contains(cur.Span) {
			// Operation must have written this write but doesn't want to consume it
			// right now, so skip it. For example, DeleteRange decomposes into point
			// deletes and will look these deletes up here one by one. If an operation
			// truly wrote outside of its span, this will cause a failure in
			// validation.
			remaining = append(remaining, cur)
			continue
		}
		consumed = append(consumed, cur)
	}

	if len(remaining) == 0 {
		delete(v.kvBySeq, seq)
	} else {
		v.kvBySeq[seq] = remaining
	}
	return consumed, len(consumed) > 0
}

// observationFilter describes which observations should be included in the
// validator's observations.
type observationFilter int

const (
	// observeAll includes all observations.
	observeAll observationFilter = iota
	// observeLocking includes only observations for operations that acquire locks
	// (i.e. writes and locking reads).
	observeLocking
)

type bufferingType byte

const (
	bufferingSingle bufferingType = iota
	bufferingBatchOrTxn
)

func transferLeaseResultIsIgnorable(res Result) bool {
	err := errorFromResult(res)
	if err == nil {
		return false
	}
	return kvserver.IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(err) ||
		kvserver.IsLeaseTransferRejectedBecauseTargetCannotReceiveLease(err) ||
		kvserver.IsLeaseTransferRejectedBecauseTargetHasSendQueueError(err) ||
		// A lease transfer is not permitted while a range merge is in its
		// critical phase.
		resultIsErrorStr(res, `cannot transfer lease while merge in progress`) ||
		// If the existing leaseholder has not yet heard about the transfer
		// target's liveness record through gossip, it will return an error.
		exceptLivenessCacheMiss(errorFromResult(res)) ||
		// Same as above, but matches cases where ErrRecordCacheMiss is
		// passed through a LeaseRejectedError. This is necessary until
		// LeaseRejectedErrors works with errors.Cause.
		resultIsErrorStr(res, liveness.ErrRecordCacheMiss.Error())
}

// processOp turns the result of an operation into its observations (which are
// later checked against the MVCC history). The boolean parameter indicates
// whether the operation is its own atomic unit or whether it's happening as
// part of a surrounding transaction or batch (in which case the caller is
// itself processOp, with the operation to handle being the batch or txn).
// Whenever it is `false`, processOp invokes the validator's checkAtomic method
// for the operation.
func (v *validator) processOp(op Operation) {
	// We will validate the presence of execution timestamps below, i.e. we verify
	// that the KV API will tell the caller at which MVCC timestamp operations have
	// executed. It won't do this in all situations (such as non-mvcc ops such as
	// splits, etc) but should do so for all MVCC ops.
	//
	// To start, we don't need an execution timestamp when buffering (the caller will need
	// an execution timestamp for the combined operation, though), or when the operation
	// didn't succeed.
	execTimestampStrictlyOptional := v.buffering == bufferingBatchOrTxn || op.Result().Error() != nil
	switch t := op.GetValue().(type) {
	case *GetOperation:
		if _, isErr := v.checkError(op, t.Result); isErr {
			break
		}
		if t.Result.ResumeSpan != nil {
			break
		}
		read := &observedRead{
			Key:        t.Key,
			SkipLocked: t.SkipLocked,
			Value:      roachpb.Value{RawBytes: t.Result.Value},
		}
		var observe bool
		switch v.observationFilter {
		case observeAll:
			observe = true
		case observeLocking:
			// NOTE: even if t.ForUpdate || t.ForShare, we only consider the read to
			// be locking if it has a guaranteed durability. Furthermore, we only
			// consider the read as an observation if it found and returned a value,
			// otherwise no lock would have been acquired on the non-existent key.
			// Gets do not acquire gap locks.
			observe = t.GuaranteedDurability && read.Value.IsPresent()
			read.DoNotObserveOnSavepointRollback = true
		default:
			panic("unexpected")
		}
		if observe {
			v.curObservations = append(v.curObservations, read)
		}

		if v.buffering == bufferingSingle {
			v.checkAtomic(`get`, t.Result)
		}
	case *PutOperation:
		if v.checkNonAmbError(op, t.Result) {
			break
		}
		if t.Result.ResumeSpan != nil {
			break
		}
		// Accumulate all the writes for this transaction.
		write := &observedWrite{
			Key:   t.Key,
			Seq:   t.Seq,
			Value: roachpb.MakeValueFromString(t.Value()),
		}
		if sv, ok := v.tryConsumeWrite(t.Key, t.Seq); ok {
			write.Timestamp = sv.Timestamp
		}
		v.curObservations = append(v.curObservations, write)

		if v.buffering == bufferingSingle {
			v.checkAtomic(`put`, t.Result)
		}
	case *CPutOperation:
		if v.checkNonAmbError(op, t.Result) {
			break
		}
		readObservation := &observedRead{Key: t.Key}
		var shouldObserveRead bool
		writeObservation := &observedWrite{
			Key:   t.Key,
			Seq:   t.Seq,
			Value: roachpb.MakeValueFromString(t.Value()),
		}
		var shouldObserveWrite bool
		// Consider two cases based on whether the CPut hit a ConditionFailedError.
		err := errorFromResult(t.Result)
		if e := (*kvpb.ConditionFailedError)(nil); errors.As(err, &e) {
			// If the CPut failed, the actual value (in the ConditionFailedError) is
			// observed, and the CPut's write is not observed.
			observedVal := roachpb.Value{}
			if e.ActualValue != nil {
				observedVal.RawBytes = e.ActualValue.RawBytes
			}
			readObservation.Value = observedVal
			shouldObserveRead = true
		} else {
			// If the CPut succeeded, the expected value is observed, and the CPut's
			// write is also observed.
			if !t.AllowIfDoesNotExist {
				// If AllowIfDoesNotExist == true, we don't know if the read found the
				// expected value or no value, so we can't add a read observation.
				// Otherwise, it must have observed the expected value.
				observedVal := roachpb.Value{}
				if t.ExpVal != nil {
					observedVal = roachpb.MakeValueFromBytes(t.ExpVal)
				}
				readObservation.Value = observedVal
				shouldObserveRead = true
			}
			if sv, ok := v.tryConsumeWrite(t.Key, t.Seq); ok {
				writeObservation.Timestamp = sv.Timestamp
			}
			shouldObserveWrite = true
		}
		// The read observation should be added before the write observation, since
		// that's the order in which the CPut executed. Moreover, the CPut read is
		// always non-locking, so if the observation filter is observeLocking, we
		// won't be adding it.
		if shouldObserveRead && v.observationFilter != observeLocking {
			v.curObservations = append(v.curObservations, readObservation)
		}
		if shouldObserveWrite {
			v.curObservations = append(v.curObservations, writeObservation)
		}
		if v.buffering == bufferingSingle {
			v.checkAtomic(`cput`, t.Result)
		}
	case *DeleteOperation:
		if v.checkNonAmbError(op, t.Result) {
			break
		}
		if t.Result.ResumeSpan != nil {
			break
		}
		sv, _ := v.tryConsumeWrite(t.Key, t.Seq)
		write := &observedWrite{
			Key:       t.Key,
			Seq:       t.Seq,
			Timestamp: sv.Timestamp,
		}
		v.curObservations = append(v.curObservations, write)

		if v.buffering == bufferingSingle {
			v.checkAtomic(`delete`, t.Result)
		}
	case *DeleteRangeOperation:
		if v.checkNonAmbError(op, t.Result) {
			break
		}
		// We express DeleteRange as point deletions on all of the keys it claimed
		// to have deleted and (atomically post-ceding the deletions) a scan that
		// sees an empty span. If DeleteRange places a tombstone it didn't report,
		// validation will fail with an unclaimed write. If it fails to delete a
		// key, the scan will not validate. If it reports that it deleted a key
		// that didn't have a non-nil value (i.e. didn't get a new tombstone),
		// then validation will fail with a missing write. If it reports & places
		// a tombstone that wasn't necessary (i.e. a combination of the above),
		// validation will succeed. This is arguably incorrect; we had code in
		// the past that handled this at the expense of additional complexity[^1].
		// See the `one deleterange after write with spurious deletion` test case
		// in TestValidate.
		//
		// [^1]: https://github.com/cockroachdb/cockroach/pull/68003/files#diff-804b6fefcb2b7ae68fab388e6dcbaf7dbc3937a266b14b79c330b703ea9d0d95R382-R388
		deleteOps := make([]observedOp, len(t.Result.Keys))
		for i, key := range t.Result.Keys {
			sv, _ := v.tryConsumeWrite(key, t.Seq)
			write := &observedWrite{
				Key:           key,
				Seq:           t.Seq,
				Value:         roachpb.Value{},
				IsDeleteRange: true, // only for String(), no semantics attached
				Timestamp:     sv.Timestamp,
			}
			deleteOps[i] = write
		}
		v.curObservations = append(v.curObservations, deleteOps...)
		// Adding the scan to the current observations should follow the same
		// conditions as a regular scan: add it ony if there are no errors.
		_, isErr := v.checkError(op, t.Result)
		// The span ought to be empty right after the DeleteRange.
		//
		// However, we do not add this observation if the observation filter is
		// observeLocking because the DeleteRange's read is not locking. This means
		// that for isolation levels that permit write skew, the DeleteRange does
		// not prevent new keys from being inserted in the deletion span between the
		// transaction's read and write timestamps.
		if v.observationFilter != observeLocking && !isErr {
			endKey := t.EndKey
			if t.Result.ResumeSpan != nil {
				endKey = t.Result.ResumeSpan.Key
			}
			v.curObservations = append(v.curObservations, &observedScan{
				Span: roachpb.Span{
					Key:    t.Key,
					EndKey: endKey,
				},
				IsDeleteRange: true, // just for printing
				KVs:           nil,
			})
		}

		if v.buffering == bufferingSingle {
			v.checkAtomic(`deleteRange`, t.Result)
		}
	case *DeleteRangeUsingTombstoneOperation:
		if v.checkNonAmbError(op, t.Result, exceptUnhandledRetry) {
			// If there was an error and it's not an ambiguous result, since we don't
			// allow this operation to span ranges (which might otherwise allow for
			// partial execution, since it's not atomic across ranges), we know none
			// of the writes must have become visible. Check this by not emitting them
			// in the first place. (Otherwise, we could get an error but the write
			// would still be there and kvnemesis would pass).
			break
		}
		// NB: MVCC range deletions aren't allowed in transactions (and can't be
		// overwritten in the same non-txn'al batch), so we currently will only
		// ever see one write to consume. With transactions (or self-overlapping
		// batches) we could get the following:
		//
		//   txn.DelRangeUsingTombstone(a, c)
		//   txn.Put(b, v)
		//   txn.Commit
		//
		// The resulting atomic unit would emit two MVCC range deletions. [a,b)
		// and [b\x00, c).
		//
		// The code here handles this, and it is unit tested, so that if and when
		// we do support rangedels in transactions, kvnemesis will be ready.
		//
		// However, DeleteRangeUsingTombstone is a ranged non-txnal request type
		// that will be split in DistSender, and so it is *not* atomic[^1]. An
		// earlier attempt at letting `kvnemesis` handle this fact by treating each
		// individual written piece that we see as an atomic unit led to too much
		// complexity (in particular, we have to validate/tolerate partial
		// executions). Instead, we *disable* DistSender's splitting of
		// DeleteRangeUsingTombstone when run with kvnemesis, and attempt to create
		// only operations for it that respect the likely range splits.
		//
		// In theory this code here supports any kind of atomic batched or
		// transactional MVCC range deletions, assuming the KV API started to
		// support them as well.
		//
		// [^1]: https://github.com/cockroachdb/cockroach/issues/46081
		svs, _ := v.tryConsumeRangedWrite(t.Seq, t.Key, t.EndKey)
		var unobserved roachpb.SpanGroup
		unobserved.Add(roachpb.Span{Key: t.Key, EndKey: t.EndKey})
		for _, sv := range svs {
			unobserved.Sub(sv.Span)
			write := &observedWrite{
				Key:       sv.Key,
				EndKey:    sv.EndKey,
				Seq:       t.Seq,
				Timestamp: sv.Timestamp,
			}
			v.curObservations = append(v.curObservations, write)
		}
		// Add unmaterialized versions of the write for any gaps. If !atomicAcrossSplits,
		// the batch might've partially succeeded (and so there might be gaps), but in
		// this case we ought to have received an error.
		for _, sp := range unobserved.Slice() {
			write := &observedWrite{
				Key:    sp.Key,
				EndKey: sp.EndKey,
				Seq:    t.Seq,
			}
			v.curObservations = append(v.curObservations, write)
		}

		// Adding the scan to the current observations should follow the same
		// conditions as a regular scan: add it ony if there are no errors.
		_, isErr := v.checkError(op, t.Result)
		// The span ought to be empty right after the DeleteRange, even if parts of
		// the DeleteRange that didn't materialize due to a shadowing operation.
		//
		// See above for why we do not add this observation if the observation
		// filter is observeLocking.
		if v.observationFilter != observeLocking && !isErr {
			v.curObservations = append(v.curObservations, &observedScan{
				Span: roachpb.Span{
					Key:    t.Key,
					EndKey: t.EndKey,
				},
			})
		}

		if v.buffering == bufferingSingle {
			v.checkAtomic(`deleteRangeUsingTombstone`, t.Result)
		}
	case *AddSSTableOperation:
		if resultHasErrorType(t.Result, &kvpb.RangeKeyMismatchError{}) {
			// The AddSSTable may race with a range split. It's not possible to ingest
			// an SST spanning multiple ranges, but the generator will optimistically
			// try to fit the SST inside one of the current ranges, so we ignore the
			// error and try again later.
		} else if v.checkNonAmbError(op, t.Result, exceptUnhandledRetry) {
			// Fail or retry on other errors, depending on type.
			break
		}
		err := func() error {
			iter, err := storage.NewMemSSTIterator(t.Data, false /* verify */, storage.IterOptions{
				KeyTypes:   storage.IterKeyTypePointsAndRanges,
				LowerBound: keys.MinKey,
				UpperBound: keys.MaxKey,
			})
			if err != nil {
				return err
			}
			defer iter.Close()
			for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
				if ok, err := iter.Valid(); !ok {
					return err
				}
				if iter.RangeKeyChanged() {
					hasPoint, hasRange := iter.HasPointAndRange()
					if hasRange {
						rangeKeys := iter.RangeKeys().Clone()
						// AddSSTable can only write at a single timestamp, so there
						// can't be overlapping range keys. Assert this.
						if rangeKeys.Len() != 1 {
							return errors.AssertionFailedf("got AddSSTable with overlapping range keys: %s",
								rangeKeys)
						}
						rangeKey := rangeKeys.AsRangeKey(rangeKeys.Versions[0])
						mvccValue, err := storage.DecodeMVCCValue(rangeKeys.Versions[0].Value)
						if err != nil {
							return err
						}
						seq := mvccValue.KVNemesisSeq.Get()
						svs, _ := v.tryConsumeRangedWrite(seq, rangeKey.StartKey, rangeKey.EndKey)
						var unobserved roachpb.SpanGroup
						unobserved.Add(roachpb.Span{Key: rangeKey.StartKey, EndKey: rangeKey.EndKey})
						for _, sv := range svs {
							unobserved.Sub(sv.Span)
							write := &observedWrite{
								Key:       sv.Key,
								EndKey:    sv.EndKey,
								Seq:       seq,
								Timestamp: sv.Timestamp,
							}
							v.curObservations = append(v.curObservations, write)
						}
						// Add unmaterialized versions of the write for any gaps.
						for _, sp := range unobserved.Slice() {
							write := &observedWrite{
								Key:    sp.Key,
								EndKey: sp.EndKey,
								Seq:    t.Seq,
							}
							v.curObservations = append(v.curObservations, write)
						}
					}
					if !hasPoint { // can only happen at range key start bounds
						continue
					}
				}

				key := iter.UnsafeKey().Clone().Key
				rawValue, err := iter.Value()
				if err != nil {
					return err
				}
				mvccValue, err := storage.DecodeMVCCValue(rawValue)
				if err != nil {
					return err
				}
				seq := mvccValue.KVNemesisSeq.Get()
				write := &observedWrite{
					Key:   key,
					Seq:   seq,
					Value: mvccValue.Value,
				}
				if sv, ok := v.tryConsumeWrite(key, seq); ok {
					write.Timestamp = sv.Timestamp
				}
				v.curObservations = append(v.curObservations, write)
			}
		}()
		if err != nil {
			v.failures = append(v.failures, err)
		}
		if v.buffering == bufferingSingle {
			v.checkAtomic(`addSSTable`, t.Result)
		}
	case *BarrierOperation:
		execTimestampStrictlyOptional = true
		if op.Barrier.WithLeaseAppliedIndex &&
			resultHasErrorType(t.Result, &kvpb.RangeKeyMismatchError{}) {
			// Barriers requesting LAIs can't span ranges. The generator will
			// optimistically try to fit the barrier inside one of the current ranges,
			// but this may race with a split, so we ignore the error in this case and
			// try again later.
		} else {
			// Fail or retry on other errors, depending on type.
			v.checkNonAmbError(op, t.Result, exceptUnhandledRetry)
		}
		// We don't yet actually check the barrier guarantees here, i.e. that all
		// concurrent writes are applied by the time it completes. Maybe later.
	case *FlushLockTableOperation:
		execTimestampStrictlyOptional = true
		if resultHasErrorType(t.Result, &kvpb.RangeKeyMismatchError{}) {
			// FlushLockTableOperation may race with a split.
		} else {
			// Fail or retry on other errors, depending on type.
			v.checkNonAmbError(op, t.Result, exceptUnhandledRetry)
		}
	case *ScanOperation:
		if _, isErr := v.checkError(op, t.Result); isErr {
			break
		}
		readSpan := roachpb.Span{Key: t.Key, EndKey: t.EndKey}
		// If the ResumeSpan equals the original request span, the scan wasn't
		// processed at all.
		if t.Result.ResumeSpan != nil && t.Result.ResumeSpan.Equal(readSpan) {
			break
		}

		switch v.observationFilter {
		case observeAll:
			if t.Result.ResumeSpan != nil {
				if op.Scan.Reverse {
					readSpan.Key = t.Result.ResumeSpan.EndKey
				} else {
					readSpan.EndKey = t.Result.ResumeSpan.Key
				}
			}
			scan := &observedScan{
				Span:       readSpan,
				Reverse:    t.Reverse,
				SkipLocked: t.SkipLocked,
				KVs:        make([]roachpb.KeyValue, len(t.Result.Values)),
			}
			for i, kv := range t.Result.Values {
				scan.KVs[i] = roachpb.KeyValue{
					Key:   kv.Key,
					Value: roachpb.Value{RawBytes: kv.Value},
				}
			}
			v.curObservations = append(v.curObservations, scan)
		case observeLocking:
			// If we are only observing locking operations then we only want to
			// consider the scan to be locking if it has a guaranteed durability.
			// Furthermore, we only consider the individual keys that were returned to
			// be locked, not the entire span that was scanned. Scans do not acquire
			// gap locks.
			if t.GuaranteedDurability {
				for _, kv := range t.Result.Values {
					read := &observedRead{
						Key:                             kv.Key,
						SkipLocked:                      t.SkipLocked,
						Value:                           roachpb.Value{RawBytes: kv.Value},
						DoNotObserveOnSavepointRollback: true,
					}
					v.curObservations = append(v.curObservations, read)
				}
			}
		default:
			panic("unexpected")
		}

		if v.buffering == bufferingSingle {
			atomicScanType := `scan`
			if t.Reverse {
				atomicScanType = `reverse scan`
			}
			v.checkAtomic(atomicScanType, t.Result)
		}
	case *BatchOperation:
		// Intentionally don't check the error here. An error on the Batch becomes
		// that error on each individual operation.

		// Only call checkAtomic if we're in bufferingSingle here. We could have
		// been a batch inside a txn.
		wasBuffering := v.buffering
		v.buffering = bufferingBatchOrTxn
		for _, op := range t.Ops {
			v.processOp(op)
		}
		if wasBuffering == bufferingSingle {
			v.checkAtomic(`batch`, t.Result)
		}
	case *ClosureTxnOperation:
		// A txn can only fail with an intentional rollback or ambiguous result.
		// Retry and omitted errors can only happen inside of the txn, but not
		// inform its result.
		// For ambiguous results, we must continue validating the txn (since writes
		// may be there). For all other errors, it still makes sense to do so in case
		// since a problem at the txn level is likely due to something weird in an
		// individual operation, so it makes sense to try to emit more failures.
		//
		// So we ignore the results of failIfError, calling it only for its side
		// effect of perhaps registering a failure with the validator.
		v.failIfError(op, t.Result, exceptRollback, exceptAmbiguous)

		ops := t.Ops
		if t.CommitInBatch != nil {
			ops = append(ops, t.CommitInBatch.Ops...)
		}
		if t.IsoLevel.ToleratesWriteSkew() {
			// If the transaction ran under an isolation level that permits write skew
			// then we only validate the atomicity of locking operations (writes and
			// locking reads). Non-locking reads may be inconsistent with the commit
			// timestamp of the transaction.
			v.observationFilter = observeLocking
		}
		v.buffering = bufferingBatchOrTxn
		for _, op := range ops {
			v.processOp(op)
		}
		atomicTxnType := fmt.Sprintf(`%s txn`, t.IsoLevel.StringLower())
		v.checkAtomic(atomicTxnType, t.Result)
	case *SplitOperation:
		execTimestampStrictlyOptional = true
		v.failIfError(op, t.Result) // splits should never return *any* error
	case *MergeOperation:
		execTimestampStrictlyOptional = true
		if resultIsErrorStr(t.Result, `cannot merge final range`) {
			// Because of some non-determinism, it is not worth it (or maybe not
			// possible) to prevent these usage errors. Additionally, I (dan) think
			// this hints at some unnecessary friction in the AdminMerge api. There is
			// a similar inconsistency when a race condition means that AdminMerge is
			// called on something that is not a split point. I propose that the
			// AdminMerge contract should be that it can be called on any key, split
			// point or not, and after a successful operation, the guarantee is that
			// there is no split at that key. #44378
			//
			// In the meantime, no-op.
		} else if resultIsErrorStr(t.Result, `merge failed: unexpected value`) {
			// TODO(dan): If this error is going to remain a part of the kv API, we
			// should make it sniffable with errors.As. Currently this seems to be
			// broken by wrapping it with `kvpb.NewErrorf("merge failed: %s",
			// err)`.
			//
			// However, I think the right thing to do is sniff this inside the
			// AdminMerge code and retry so the client never sees it. In the meantime,
			// no-op. #44377
		} else if resultIsErrorStr(t.Result, `merge failed: cannot merge ranges when (lhs|rhs) is in a joint state or has learners`) {
			// This operation executed concurrently with one that was changing
			// replicas.
		} else if resultIsErrorStr(t.Result, `merge failed: ranges not collocated`) {
			// A merge requires that the two ranges have replicas on the same nodes,
			// but Generator intentiontally does not try to avoid this so that this
			// edge case is exercised.
		} else if resultIsErrorStr(t.Result, `merge failed: waiting for all (left|right)-hand replicas to (initialize|catch up)`) {
			// Probably should be transparently retried.
		} else if resultIsErrorStr(t.Result, `merge failed: non-deletion intent on local range descriptor`) {
			// Probably should be transparently retried.
		} else if resultIsErrorStr(t.Result, `merge failed: range missing intent on its local descriptor`) {
			// Probably should be transparently retried.
		} else if resultIsErrorStr(t.Result, `merge failed: RHS range bounds do not match`) {
			// Probably should be transparently retried.
		} else {
			v.failIfError(op, t.Result) // fail on all other errors
		}
	case *ChangeReplicasOperation:
		execTimestampStrictlyOptional = true
		var ignore bool
		if err := errorFromResult(t.Result); err != nil {
			ignore = kvserver.IsRetriableReplicationChangeError(err) ||
				kvserver.IsIllegalReplicationChangeError(err) ||
				kvserver.IsReplicationChangeInProgressError(err) ||
				transferLeaseResultIsIgnorable(t.Result) // replication changes can transfer leases
		}
		if !ignore {
			v.failIfError(op, t.Result) // fail on all other errors
		}
	case *TransferLeaseOperation:
		execTimestampStrictlyOptional = true
		if !transferLeaseResultIsIgnorable(t.Result) {
			v.failIfError(op, t.Result) // fail on all other errors
		}
	case *ChangeSettingOperation:
		execTimestampStrictlyOptional = true
		// It's possible that reading the modified setting times out. Ignore these
		// errors for now, at least until we do some validation that depends on the
		// cluster settings being fully propagated.
		if !resultIsErrorStr(t.Result, `setting updated but timed out waiting to read new value`) {
			v.failIfError(op, t.Result)
		}
	case *ChangeZoneOperation:
		execTimestampStrictlyOptional = true
		v.failIfError(op, t.Result) // fail on all errors
	case *SavepointCreateOperation:
		sp := &observedSavepoint{ID: int(t.ID), Type: create}
		v.curObservations = append(v.curObservations, sp)
		// Don't fail on all errors because savepoints can be labeled with
		// errOmitted if a previous op in the txn failed.
		v.checkError(op, t.Result)
	case *SavepointReleaseOperation:
		sp := &observedSavepoint{ID: int(t.ID), Type: release}
		v.curObservations = append(v.curObservations, sp)
		// Don't fail on all errors because savepoints can be labeled with
		// errOmitted if a previous op in the txn failed.
		v.checkError(op, t.Result)
	case *SavepointRollbackOperation:
		sp := &observedSavepoint{ID: int(t.ID), Type: rollback}
		v.curObservations = append(v.curObservations, sp)
		// Don't fail on all errors because savepoints can be labeled with
		// errOmitted if a previous op in the txn failed.
		v.checkError(op, t.Result)
	case *MutateBatchHeaderOperation:
		execTimestampStrictlyOptional = true
		v.checkError(op, t.Result)
	case *AddNetworkPartitionOperation, *RemoveNetworkPartitionOperation:
		execTimestampStrictlyOptional = true
		// Ignore any errors due to the generator trying to add/remove a partition
		// that doesn't exist or from a node to itself.
	case *StopNodeOperation:
		execTimestampStrictlyOptional = true
		v.checkError(op, t.Result)
	case *RestartNodeOperation:
		execTimestampStrictlyOptional = true
		v.checkError(op, t.Result)
	case *CrashNodeOperation:
		execTimestampStrictlyOptional = true
		v.checkError(op, t.Result)
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, t, t))
	}

	// If the current operation is expected to have an operation timestamp but
	// didn't have one, emit a failure.
	if !execTimestampStrictlyOptional && op.Result().OptionalTimestamp.IsEmpty() {
		v.failures = append(v.failures, errors.Errorf("execution timestamp missing for %s", op))
	}
}

// checkAtomic verifies a set of operations that should be atomic by trying to find
// a timestamp at which the observed reads and writes of the operations (as executed
// in the order in which they appear in the arguments) match the MVCC history.
func (v *validator) checkAtomic(atomicType string, result Result) {
	observations := v.curObservations
	v.curObservations = nil
	v.observationFilter = observeAll
	v.buffering = bufferingSingle

	// Only known-uncommitted results may come without a timestamp. Whenever we
	// actually tried to commit, there is a timestamp.
	if result.Type != ResultType_Error {
		// The timestamp is not optional in this case. Note however that at the time
		// of writing, checkAtomicCommitted doesn't capitalize on this unconditional
		// presence yet, and most unit tests don't specify it for reads.
		if !result.OptionalTimestamp.IsSet() {
			err := errors.AssertionFailedf("operation has no execution timestamp: %v", result)
			v.failures = append(v.failures, err)
		}
		v.checkAtomicCommitted(`committed `+atomicType, observations, result.OptionalTimestamp)
	} else if resultIsAmbiguous(result) {
		// An ambiguous result shouldn't have an execution timestamp.
		if result.OptionalTimestamp.IsSet() {
			err := errors.AssertionFailedf("OptionalTimestamp set for ambiguous result: %v", result)
			v.failures = append(v.failures, err)
		}
		v.checkAtomicAmbiguous(`ambiguous `+atomicType, observations)
	} else {
		v.checkAtomicUncommitted(`uncommitted `+atomicType, observations)
	}
}

// checkAtomicCommitted verifies an atomic unit (i.e. single cmd, batch, or txn) that
// was successful. Its writes thus must be present, and (as is always the case, no
// matter the outcome) its reads must have been valid.
//
// The execution timestamp optOptsTimestamp is always present for operations that
// succeeded in a "normal" way. However, for ambiguous results, it is not always
// present. This limitation could be lifted, see checkAtomicAmbiguous.
func (v *validator) checkAtomicCommitted(
	atomicType string, txnObservations []observedOp, execTimestamp hlc.Timestamp,
) {
	// The following works by verifying that there is at least one time at which
	// it was valid to see all the reads and writes that we saw in this
	// transaction.
	//
	// Concretely a transaction:
	// - Write k1@t2 -> v1
	// - Read k2 -> v2
	// - Scan [k3,k5) -> [v3,v4]
	// - Delete k5@t2 -> <nil> (MVCC delete writes tombstone value)
	// - Read k5 -> <nil>
	//
	// And what was present in KV after this and some other transactions:
	// - k1@t2, v1
	// - k1@t3, v5
	// - k2@t1, v2
	// - k2@t3, v6
	// - k3@t0, v3
	// - k4@t2, v4
	// - k5@t1, v7
	// - k5@t2, <nil>
	//
	// Each of the operations in the transaction, if taken individually, has some
	// window at which it was valid. The Write was only valid for a commit exactly
	// at t2: [t2,t2). This is because each Write's mvcc timestamp is the timestamp
	// of the txn commit. The Read would have been valid for [t1,t3) because v2 was
	// written at t1 and overwritten at t3. The scan would have been valid for
	// [t2,∞) because v3 was written at t0 and v4 was written at t2 and neither were
	// overwritten.  The Delete, same as the Write, is valid at the timestamp of
	// the txn commit, as it is simply a Write with an empty (nil) value.  The
	// final Read, it is worth noting, could be valid at two disjoint timespans
	// that must be considered: [-∞, t1), and [t2,∞).
	//
	//
	// As long as there is at least one time span in which all operations
	// overlap validly, we're good. However, if another write had a timestamp of
	// t3, then there is no timestamp at which the transaction could have
	// committed, which is a violation of our consistency guarantees.
	// Similarly if there was some read that was only valid from [t1,t2).
	// While Reads of concrete values only ever have one valid timespan, with
	// the introduction of Deletes, a Read that returns <nil> may have multiple
	// valid timespans (i.e. before the key existed, after it was deleted).
	//
	// Listen up, this is where it gets tricky. Within a transaction, if the same
	// key is written more than once, only the last one will ever be materialized
	// in KV (and be sent out over the RangeFeed). However, any reads in between
	// will see the overwritten values. This means that each transaction needs its
	// own view of KV when determining what is and is not valid.
	//
	// Concretely:
	// - Read k -> <nil>
	// - Write k -> v1
	// - Read k -> v1
	// - Write k -> v2
	// - Read k -> v2
	//
	// This is okay, but if we only use the writes that come out of RangeFeed to
	// compute our read validities, then there would be no time at which v1 could
	// have been read. So, we have to "layer" the k -> v1 write on top of our
	// RangeFeed output. At the same time, it would not have been valid for the
	// first or second read to see v2 because when they ran that value hadn't
	// been written yet.
	//
	// So, what we do to compute the read validity time windows is first hide all
	// the writes the transaction eventually did in some "view". Then step through
	// it, un-hiding each of them as we encounter each write, and using the
	// current state of the view as we encounter each read. Luckily this is easy
	// to do by with a pebble.Batch "view".
	firstBatch := v.kvs.kvs.NewIndexedBatch()
	defer func() { _ = firstBatch.Close() }()

	var failure string
	// writeTS is populated with the timestamp of the materialized observed writes
	// (if there are any). We'll use it below to maintain the "view" of prefixes
	// of atomic unit.
	var writeTS hlc.Timestamp
	// First, hide all of our writes from the view. Remember the index of the last
	// ('most recent') write to each key so that we can check below whether any
	// shadowed writes erroneously materialized. Recall that writes can be ranged
	// (mvcc range deletions), but these writes cannot be transactional. At the
	// time of writing, we also don't do non-transactional batches (see
	// DefaultConfig) which means in effect we'll only ever see ranged operations
	// alone in an atomic unit. This code still handles these cases, and they are
	// unit tested.
	lastWritesByIdx := map[int]struct{}{}
	var lastWrites roachpb.SpanGroup
	// We will iterate over the observations in reverse order to find the last
	// write; so we will encounter a savepoint rollback before the corresponding
	// savepoint create. Assuming a rollback is preceded by a matching create
	// (guaranteed by the generator), to identify the last write we need to ignore
	// all writes that occur between a savepoint rollback and a corresponding
	// savepoint create.
	// rollbackSp keeps track of the current active rollback.
	// rollbackSp = nil if no savepoint rollback has been encountered yet or any
	// encountered rollback has been matched by a rollback create.
	// rollbackSp = observedSavepoint{...} when the observedSavepoint object
	// contains a rollback for which we haven't encountered a matching create yet.
	var rollbackSp *observedSavepoint = nil
	batch := firstBatch
	for idx := len(txnObservations) - 1; idx >= 0; idx-- {
		observation := txnObservations[idx]
		switch o := observation.(type) {
		case *observedWrite:
			sp := roachpb.Span{Key: o.Key, EndKey: o.EndKey}
			// Check if the last writes set already covers the current write.
			//
			// Writes are fragmented in the sense that they are either fully the
			// last write or not, since all (materialized) writes happened at the
			// same MVCC timestamp, at least in the absence of bugs.
			//
			// For example, a Put A that gets shadowed by an MVCC rangedel B that
			// then gets overlaid by a Put C and then intersected by another
			// rangedel D should give an "incremental history" (as we construct it
			// further down below)
			//                      [-----D----)
			//              C
			// [----------B------------)
			//       A
			//
			// and lastWrites will be
			//
			// [-----B-----)C[--B--)[-----D----)
			//
			// In particular, when we constructed the observedWrite for our rangedels,
			// we construct them for the actual spans from the rangefeed, not the span
			// of the operation.
			var lastWrite bool
			{
				var g roachpb.SpanGroup
				g.Add(lastWrites.Slice()...)
				// If subtracting did nothing and there are no active rollbacks, it's a
				// most recent write.
				lastWrite = !g.Sub(sp) && rollbackSp == nil
			}
			if lastWrite {
				lastWritesByIdx[idx] = struct{}{}
				lastWrites.Add(sp)
			}

			if o.Timestamp.IsEmpty() {
				// This write didn't materialize (say a superseded write in
				// a txn), so it's not present here.
				continue
			}

			// NB: we allow writeTS to change here, since that will be caught by
			// validation below anyway, and then we can produce better errors since
			// read timestamps will be filled in.
			if writeTS.IsEmpty() {
				writeTS = o.Timestamp
			}

			if len(o.EndKey) == 0 { // point write
				mvccKey := storage.MVCCKey{Key: o.Key, Timestamp: o.Timestamp}
				if err := batch.Delete(storage.EncodeMVCCKey(mvccKey), nil); err != nil {
					panic(err)
				}
			} else { // ranged write
				key := storage.EngineKey{Key: o.Key}.Encode()
				endKey := storage.EngineKey{Key: o.EndKey}.Encode()
				suffix := mvccencoding.EncodeMVCCTimestampSuffix(o.Timestamp)
				if err := batch.RangeKeyUnset(key, endKey, suffix, nil); err != nil {
					panic(err)
				}
			}
		case *observedRead:
			// If this read should not be observed on savepoint rollback, and there is
			// a savepoint being rolled back, mark the read as valid at all times.
			if rollbackSp != nil && o.DoNotObserveOnSavepointRollback {
				o.AlwaysValid = true
			}
		case *observedSavepoint:
			switch o.Type {
			case create:
				// Set rollbackSp to nil if this savepoint create matches the rolled
				// back one recorded in rollbackSp.
				if rollbackSp != nil && rollbackSp.ID == o.ID {
					rollbackSp = nil
				}
			case release:
				// Savepoint releases don't affect the last write.
			case rollback:
				// Update rollbackSp only if there is no active rollback (i.e. a
				// rollback for which we haven't found a matching create). Otherwise,
				// the active rollback recorded in rollbackSp subsumes the one we're
				// seeing now.
				if rollbackSp == nil {
					rollbackSp = o
				}
			}
		}
	}

	// A map from a savepoint id to a batch that represents the state of the
	// txn at the time of the savepoint creation.
	spIDToBatch := make(map[int]*pebble.Batch)

	// Iterate through the observations, building up the snapshot visible at each
	// point in the atomic unit and filling in the valid read times (validating
	// them later, in a separate loop, for better errors). We also check that only
	// the most recent writes materialized (i.e. showed up in MVCC). Check if any
	// key that was written twice in the txn had the overwritten writes
	// materialize in kv.
	for idx, observation := range txnObservations {
		if failure != `` {
			break
		}
		switch o := observation.(type) {
		case *observedWrite:
			// Only the most recent write between overlapping mutations makes it into MVCC.
			// writeTS was populated above as the unique timestamp at which the writes became
			// visible. We know the operation had writes (we're looking at one now) and so
			// this operation has either materialized or is covered by a later one that did,
			// and so we must have a timestamp here.
			// The only exception is when all writes were rolled back by a savepoint
			// rollback. In that case, we still need a writeTS to ensure reads within the txn
			// can see those writes; the execTimestamp serves that purpose.
			if writeTS.IsEmpty() {
				writeTS = execTimestamp
			}

			_, isLastWrite := lastWritesByIdx[idx]
			if !isLastWrite && o.Timestamp.IsSet() {
				failure = `committed txn overwritten key had write`
				break
			}

			// Make this write visible (at writeTS, regardless of whether it's the
			// last write or not, since that's the snapshot at which our operation
			// wrote).
			if len(o.EndKey) == 0 {
				if err := batch.Set(storage.EncodeMVCCKey(storage.MVCCKey{Key: o.Key, Timestamp: writeTS}), o.Value.RawBytes, nil); err != nil {
					panic(err)
				}
			} else {
				key := storage.EngineKey{Key: o.Key}.Encode()
				endKey := storage.EngineKey{Key: o.EndKey}.Encode()
				suffix := mvccencoding.EncodeMVCCTimestampSuffix(writeTS)
				if err := batch.RangeKeySet(key, endKey, suffix, o.Value.RawBytes, nil); err != nil {
					panic(err)
				}
			}
		case *observedRead:
			if o.AlwaysValid {
				o.ValidTimes = disjointTimeSpans{{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}}
			} else {
				o.ValidTimes = validReadTimes(batch, o.Key, o.Value.RawBytes, o.SkipLocked /* missingKeyValid */)
			}
		case *observedScan:
			// All kvs should be within scan boundary.
			for _, kv := range o.KVs {
				if !o.Span.ContainsKey(kv.Key) {
					opCode := "scan"
					if o.IsDeleteRange {
						opCode = "delete range"
					}
					failure = fmt.Sprintf(`key %s outside %s bounds`, kv.Key, opCode)
					break
				}
			}
			// All kvs should be in order.
			orderedKVs := sort.Interface(roachpb.KeyValueByKey(o.KVs))
			if o.Reverse {
				orderedKVs = sort.Reverse(orderedKVs)
			}
			if !sort.IsSorted(orderedKVs) {
				failure = `scan result not ordered correctly`
			}
			o.Valid = validScanTime(batch, o.Span, o.KVs, o.SkipLocked /* missingKeysValid */)
		case *observedSavepoint:
			switch o.Type {
			case create:
				// Clone the existing batch by creating a new batch and applying the
				// existing batch state to it.
				newBatch := v.kvs.kvs.NewIndexedBatch()
				_ = newBatch.Apply(batch, nil)
				if _, ok := spIDToBatch[o.ID]; ok {
					panic(errors.AssertionFailedf("validating a savepoint create op: ID %d already exists", o.ID))
				}
				spIDToBatch[o.ID] = newBatch
			case release:
				// Savepoint releases don't affect the validation.
			case rollback:
				if _, ok := spIDToBatch[o.ID]; !ok {
					panic(errors.AssertionFailedf("validating a savepoint rollback op: ID %d does not exist", o.ID))
				}
				// The new batch to use for validation.
				batch = spIDToBatch[o.ID]
			}
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observation, observation))
		}
	}

	// Close all batches in spIDToBatch.
	for _, spBatch := range spIDToBatch {
		_ = spBatch.Close()
	}

	validPossibilities := disjointTimeSpans{{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}}
	for idx, observation := range txnObservations {
		if failure != `` {
			break
		}
		var opValid disjointTimeSpans
		switch o := observation.(type) {
		case *observedWrite:
			_, isLastWrite := lastWritesByIdx[idx]
			if !isLastWrite {
				continue
			}
			if o.Timestamp.IsEmpty() {
				failure = atomicType + ` missing write at seq ` + o.Seq.String()
				continue
			}
			opValid = disjointTimeSpans{{Start: o.Timestamp, End: o.Timestamp.Next()}}
		case *observedRead:
			opValid = o.ValidTimes
		case *observedScan:
			opValid = o.Valid.Combined()
		case *observedSavepoint:
			// A savepoint is always valid.
			opValid = disjointTimeSpans{{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}}
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observation, observation))
		}
		newValidSpans := validPossibilities.validIntersections(opValid)
		if len(newValidSpans) == 0 {
			failure = atomicType + ` non-atomic timestamps`
		}
		validPossibilities = newValidSpans
	}

	// Finally, validate that the write timestamp of the transaction matches the
	// write timestamp of each write within that transaction.
	if failure == `` && writeTS.IsSet() && execTimestamp.IsSet() && writeTS != execTimestamp {
		failure = fmt.Sprintf(`mismatched write timestamp %s and exec timestamp %s`, writeTS, execTimestamp)
	}

	if failure != `` {
		err := errors.Errorf("%s: %s", failure, printObserved(txnObservations...))
		v.failures = append(v.failures, err)
	}
}

func (v *validator) checkAtomicAmbiguous(atomicType string, txnObservations []observedOp) {
	// If the atomic unit hasn't observed any writes (i.e. it's a read-only/admin
	// op) or any part of it has materialized, treat it as committed.
	//
	// TODO(tbg): even when there's no materialized write, we could treat the
	// prefix of pure reads as a committed operation. This is probably most
	// relevant for aborted txns, which must have still seen a consistent snapshot
	// before they realized they were aborted, and which had bugs in the past.
	var execTimestamp hlc.Timestamp
	var isRW bool
	for _, observation := range txnObservations {
		o, ok := observation.(*observedWrite)
		if !ok {
			continue
		}
		isRW = true
		if o.Timestamp.IsSet() {
			execTimestamp = o.Timestamp
			break
		}
	}

	if !isRW || execTimestamp.IsSet() {
		v.checkAtomicCommitted(atomicType, txnObservations, execTimestamp)
	} else {
		// This is a writing transaction but not a single one of its writes
		// showed up in KV, so verify that it is uncommitted.
		//
		// NB: if there's ever a way for a writing transaction to not leave
		// a trace on the rangefeed (DeleteRange comes to mind) then it's
		// fine to treat that transaction as uncommitted as well.
		v.checkAtomicUncommitted(atomicType, txnObservations)
	}
}

func (v *validator) checkAtomicUncommitted(atomicType string, txnObservations []observedOp) {
	var failure string
	for _, observed := range txnObservations {
		if failure != `` {
			break
		}
		switch o := observed.(type) {
		case *observedWrite:
			if o.Timestamp.IsSet() {
				failure = atomicType + ` had writes`
			}
			// NB: While we don't check deletes here, as we cannot uniquely identify
			// the particular delete operation that is responsible for a stored
			// tombstone value for a key, if an uncommitted delete actually
			// materialized in the storage engine, we will see an "extra write" error
			// upon final validation.
		case *observedRead:
			// TODO(dan): Figure out what we can assert about reads in an uncommitted
			// transaction.
		case *observedScan:
			// TODO(dan): Figure out what we can assert about reads in an uncommitted
			// transaction.
		case *observedSavepoint:
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observed, observed))
		}
	}

	if failure != `` {
		err := errors.Errorf("%s: %s", failure, printObserved(txnObservations...))
		v.failures = append(v.failures, err)
	}
}

// checkError returns true if the operation resulted in an error. It also registers a failure
// with the validator unless the error is an ambiguous result, an omitted error, or a retry
// error. Additional exceptions may be passed in.
// Exceptions don't influence the return value, which simply indicates whether there
// was *any* error. This is useful because any error usually means the rest of the validation
// for the command ought to be skipped.
//
// Writing operations usually want to call checkNonAmbError instead.
//
// Note that in a Batch each operation inherits the outcome of the Batch as a
// whole, which means that a read operation may result in an
// AmbiguousResultError (which it presumably didn't cause). Ideally Batch would
// track this more precisely but until it does we're just going to allow
// ambiguous results everywhere. We could also work around this in kvnemesis by
// tracking which context we're in, i.e. allow ambiguous results for, say, a Get
// but only if in a batched context. Similarly, we shouldn't allow retry errors
// when in a non-batched context, since these ought to be retried away
// internally.
func (v *validator) checkError(
	op Operation, r Result, extraExceptions ...func(err error) bool,
) (ambiguous, hadError bool) {
	sl := []func(error) bool{
		exceptAmbiguous, exceptOmitted, exceptRetry, exceptDelRangeUsingTombstoneStraddlesRangeBoundary,
	}
	sl = append(sl, extraExceptions...)
	return v.failIfError(op, r, sl...)
}

// checkNonAmbError returns true if the result has an error, but the error
// is not an ambiguous result. It applies the same exceptions as checkError,
// which won't cause a failure to be emitted to the validator for certain
// "excepted" error types. True will be returned for those nevertheless.
//
// This is typically called by operations that may write, since these always
// want to emit observedWrites in case the operation actually did commit.
func (v *validator) checkNonAmbError(
	op Operation, r Result, extraExceptions ...func(error) bool,
) bool {
	isAmb, isErr := v.checkError(op, r, extraExceptions...)
	return isErr && !isAmb
}

// failIfError is the lower-level version of checkError that requires all
// exceptions to be passed in. This includes exceptAmbiguous, if desired.
// The first bool will be true if the error is an ambiguous result. Note
// that for this ambiguous result to *not* also be registered as a failure,
// exceptAmbiguous must be passed in.
func (v *validator) failIfError(
	op Operation, r Result, exceptions ...func(err error) bool,
) (ambiguous, hasError bool) {
	exceptions = append(exceptions[:len(exceptions):len(exceptions)], func(err error) bool {
		return errors.Is(err, errInjected)
	})
	switch r.Type {
	case ResultType_Unknown:
		err := errors.AssertionFailedf(`unknown result %s`, op)
		v.failures = append(v.failures, err)
	case ResultType_Error:
		ctx := context.Background()
		err := errors.DecodeError(ctx, *r.Err)
		for _, fn := range exceptions {
			if fn(err) {
				return exceptAmbiguous(err), true
			}
		}
		err = errors.Wrapf(err, `error applying %s`, op)
		v.failures = append(v.failures, err)
	}
	return false, false
}

func errorFromResult(r Result) error {
	if r.Type != ResultType_Error {
		return nil
	}
	ctx := context.Background()
	return errors.DecodeError(ctx, *r.Err)
}

func exceptLivenessCacheMiss(err error) bool {
	return errors.Is(err, liveness.ErrRecordCacheMiss)
}

func resultIsAmbiguous(r Result) bool {
	resErr := errorFromResult(r)
	hasClientVisibleAE := errors.HasInterface(resErr, (*kvpb.ClientVisibleAmbiguousError)(nil))
	return hasClientVisibleAE
}

// TODO(dan): Checking errors using string containment is fragile at best and a
// security issue at worst. Unfortunately, some errors that currently make it
// out of our kv apis are created with `errors.New` and so do not have types
// that can be sniffed. Some of these may be removed or handled differently but
// the rest should graduate to documented parts of the public api. Remove this
// once it happens.
func resultIsErrorStr(r Result, msgRE string) bool {
	if err := errorFromResult(r); err != nil {
		return regexp.MustCompile(msgRE).MatchString(err.Error())
	}
	return false
}

func resultHasErrorType(r Result, reference error) bool {
	if err := errorFromResult(r); err != nil {
		return errors.HasType(err, reference)
	}
	return false
}

func mustGetStringValue(value []byte) string {
	v, err := storage.DecodeMVCCValue(value)
	if err != nil {
		panic(errors.Wrapf(err, "decoding %x", value))
	}
	if v.IsTombstone() {
		return `<nil>`
	}
	b, err := v.Value.GetBytes()
	if err != nil {
		panic(errors.Wrapf(err, "decoding %x", value))
	}
	return string(b)
}

func validReadTimes(
	b *pebble.Batch, key roachpb.Key, value []byte, missingKeyValid bool,
) disjointTimeSpans {
	if len(value) == 0 && missingKeyValid {
		// If no value was returned for the key and this is allowed, all times are
		// valid for this read.
		return disjointTimeSpans{{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}}
	}

	var hist []storage.MVCCValue
	lowerBound := storage.EncodeMVCCKey(storage.MVCCKey{Key: key})
	upperBound := storage.EncodeMVCCKey(storage.MVCCKey{Key: key.Next()})
	iter, err := b.NewIter(&pebble.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		panic(err)
	}
	defer func() { _ = iter.Close() }()

	iter.SeekGE(lowerBound)
	for ; iter.Valid(); iter.Next() {
		hasPoint, hasRange := iter.HasPointAndRange()
		if hasRange && iter.RangeKeyChanged() {
			encK, encEK := iter.RangeBounds()
			k, err := storage.DecodeMVCCKey(encK)
			if err != nil {
				panic(err)
			}
			ek, err := storage.DecodeMVCCKey(encEK)
			if err != nil {
				panic(err)
			}

			sp := roachpb.Span{Key: k.Key, EndKey: ek.Key}
			if !sp.ContainsKey(key) {
				// We used bounds that should make this impossible.
				panic(fmt.Sprintf("iterator for %s on non-overlapping range key %s", key, sp))
			}
			// Range key contains the key. Emit a point deletion on the key
			// at the tombstone's timestamp for each active range key.
			for _, rk := range iter.RangeKeys() {
				ts, err := mvccencoding.DecodeMVCCTimestampSuffix(rk.Suffix)
				if err != nil {
					panic(err)
				}
				hist = append(hist, storage.MVCCValue{Value: roachpb.Value{Timestamp: ts}})
			}
		}

		if !hasPoint {
			continue
		}

		mvccKey, err := storage.DecodeMVCCKey(iter.Key())
		if err != nil {
			panic(err)
		}

		if !mvccKey.Key.Equal(key) {
			// We used bounds that should make this impossible.
			panic("iterator on non-overlapping key")
		}

		// Handle a point key - put it into `hist`.
		valB, err := iter.ValueAndErr()
		valB = append([]byte{}, valB...)
		if err != nil {
			panic(err)
		}
		v, err := storage.DecodeMVCCValue(valB)
		if err != nil {
			panic(err)
		}
		v.Value.Timestamp = mvccKey.Timestamp
		hist = append(hist, v)
	}
	// The slice isn't sorted due to MVCC rangedels. Sort in descending order.
	slices.SortFunc(hist, func(a, b storage.MVCCValue) int {
		return -a.Value.Timestamp.Compare(b.Value.Timestamp)
	})

	sv := mustGetStringValue(value)
	var validTimes disjointTimeSpans
	end := hlc.MaxTimestamp
	for i := range hist {
		v := hist[i].Value
		if mustGetStringValue(v.RawBytes) == sv {
			validTimes = append(validTimes, timeSpan{Start: v.Timestamp, End: end})
		}
		end = v.Timestamp
	}

	if len(value) == 0 {
		validTimes = append(disjointTimeSpans{{Start: hlc.MinTimestamp, End: end}}, validTimes...)
	}

	return validTimes
}

func validScanTime(
	b *pebble.Batch, span roachpb.Span, kvs []roachpb.KeyValue, missingKeysValid bool,
) multiKeyTimeSpan {
	valid := multiKeyTimeSpan{
		Gaps: disjointTimeSpans{{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}},
	}

	// Find the valid time span for each kv returned.
	for _, kv := range kvs {
		// Since scan results don't include deleted keys, there should only ever
		// be 0 or 1 valid read time span for each `(key, specific-non-nil-value)`
		// returned, given that the values are guaranteed to be unique by the
		// Generator.
		//
		// NB: we use value uniqueness here, but we could also use seqnos, so this
		// is only a left-over of past times rather than an actual reliance on
		// unique values.
		validTimes := validReadTimes(b, kv.Key, kv.Value.RawBytes, missingKeysValid)
		if len(validTimes) > 1 {
			panic(errors.AssertionFailedf(
				`invalid number of read time spans for a (key,non-nil-value) pair in scan results: %s->%s: %v`,
				kv.Key, mustGetStringValue(kv.Value.RawBytes), validTimes))
		}
		if len(validTimes) == 0 {
			validTimes = append(validTimes, timeSpan{})
		}
		valid.Keys = append(valid.Keys, validTimes)
	}

	// Augment with the valid time span for any kv not observed but that
	// overlaps the scan span.
	keys := make(map[string]struct{}, len(kvs))
	for _, kv := range kvs {
		keys[string(kv.Key)] = struct{}{}
	}

	missingKeys := make(map[string]disjointTimeSpans)

	// Next, discover all of the keys that were *not* returned but overlap the
	// scan span and compute validReadTimes for them.
	//
	// Note that this iterator ignores MVCC range deletions. We use this iterator
	// only to *discover* point keys; we then invoke validReadTimes for each of
	// them which *does* take into account MVCC range deletions.
	iter, err := b.NewIter(nil)
	if err != nil {
		panic(err)
	}
	defer func() { _ = iter.Close() }()

	iter.SeekGE(storage.EncodeMVCCKey(storage.MVCCKey{Key: span.Key}))

	for ; iter.Valid(); iter.Next() {
		mvccKey, err := storage.DecodeMVCCKey(iter.Key())
		if err != nil {
			panic(err)
		}
		if mvccKey.Key.Compare(span.EndKey) >= 0 {
			// Past scan boundary.
			break
		}
		if _, ok := keys[string(mvccKey.Key)]; ok {
			// Key in scan response.
			continue
		}
		if _, ok := missingKeys[string(mvccKey.Key)]; !ok {
			// Key not in scan response. Only valid if scan was before key's time, or
			// at a time when the key was deleted.
			missingKeys[string(mvccKey.Key)] = validReadTimes(b, mvccKey.Key, nil, missingKeysValid)
		}
	}

	for _, nilValueReadTimes := range missingKeys {
		valid.Gaps = valid.Gaps.validIntersections(nilValueReadTimes)
	}

	return valid
}

func printObserved(observedOps ...observedOp) string {
	var buf strings.Builder
	for _, observed := range observedOps {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		switch o := observed.(type) {
		case *observedWrite:
			opCode := "w"
			if o.isDelete() {
				if o.IsDeleteRange {
					if len(o.EndKey) == 0 {
						opCode = "dr.d"
					} else {
						opCode = "rd" // mvcc range del
					}
				} else {
					opCode = "d"
				}
			}
			ts := `missing`
			if o.Timestamp.IsSet() {
				ts = o.Timestamp.String()
			}
			if len(o.EndKey) == 0 {
				fmt.Fprintf(&buf, "[%s]%s:%s->%s@%s",
					opCode, o.Key, ts, mustGetStringValue(o.Value.RawBytes), o.Seq)
			} else {
				fmt.Fprintf(&buf, "[%s][%s,%s):%s->%s@%s",
					opCode, o.Key, o.EndKey, ts, mustGetStringValue(o.Value.RawBytes), o.Seq)
			}
		case *observedRead:
			opCode := "r"
			if o.SkipLocked {
				opCode += "(skip-locked)"
			}
			fmt.Fprintf(&buf, "[%s]%s:", opCode, o.Key)
			validTimes := o.ValidTimes
			if len(validTimes) == 0 {
				validTimes = append(validTimes, timeSpan{})
			}
			for idx, validTime := range validTimes {
				if idx != 0 {
					fmt.Fprintf(&buf, ",")
				}
				fmt.Fprintf(&buf, "%s", validTime)
			}
			fmt.Fprintf(&buf, "->%s", mustGetStringValue(o.Value.RawBytes))
		case *observedScan:
			opCode := "s"
			if o.IsDeleteRange {
				opCode = "dr.s"
			}
			if o.Reverse {
				opCode = "rs"
			}
			if o.SkipLocked {
				opCode += "(skip-locked)"
			}
			var kvs strings.Builder
			for i, kv := range o.KVs {
				if i > 0 {
					kvs.WriteString(`, `)
				}
				kvs.WriteString(kv.Key.String())
				if !o.IsDeleteRange {
					kvs.WriteByte(':')
					kvs.WriteString(mustGetStringValue(kv.Value.RawBytes))
				}
			}
			fmt.Fprintf(&buf, "[%s]%s:%s->[%s]",
				opCode, o.Span, o.Valid, kvs.String())
		case *observedSavepoint:
			opCode := "sp"
			if o.Type == create {
				opCode += "(create)"
			} else if o.Type == release {
				opCode += "(release)"
			} else if o.Type == rollback {
				opCode += "(rollback)"
			}
			fmt.Fprintf(&buf, "[%s]id:%d", opCode, o.ID)
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observed, observed))
		}
	}
	return buf.String()
}
