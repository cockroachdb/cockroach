// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
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
func Validate(steps []Step, kvs *Engine) []error {
	v, err := makeValidator(kvs)
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
	sort.Slice(steps, func(i, j int) bool { return steps[i].After.Less(steps[j].After) })
	for _, s := range steps {
		v.processOp(notBuffering, s.Op)
	}

	var extraKVs []observedOp
	for _, kv := range v.kvByValue {
		kv := &observedWrite{
			Key:       kv.Key.Key,
			Value:     roachpb.Value{RawBytes: kv.Value},
			Timestamp: kv.Key.Timestamp,
		}
		extraKVs = append(extraKVs, kv)
	}
	for key, tsToMatched := range v.tombstonesByKey {
		for ts, matched := range tsToMatched {
			if matched {
				continue
			}
			kv := &observedWrite{
				Key:       roachpb.Key(key),
				Value:     roachpb.Value{},
				Timestamp: ts,
			}
			extraKVs = append(extraKVs, kv)
		}
	}

	// These are writes that we saw in MVCC, but they weren't matched up to any
	// operation kvnemesis ran.
	if len(extraKVs) > 0 {
		err := errors.Errorf(`extra writes: %s`, printObserved(extraKVs...))
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

type observedWrite struct {
	Key   roachpb.Key
	Value roachpb.Value
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
	Value      roachpb.Value
	ValidTimes disjointTimeSpans
}

func (*observedRead) observedMarker() {}

type observedScan struct {
	Span          roachpb.Span
	IsDeleteRange bool
	Reverse       bool
	KVs           []roachpb.KeyValue
	Valid         multiKeyTimeSpan
}

func (*observedScan) observedMarker() {}

type validator struct {
	kvs *Engine

	// Observations for the current atomic unit. This is reset between units, in
	// checkAtomic, which then calls processOp (which might recurse owing to the
	// existence of txn closures, batches, etc).
	curObservations []observedOp

	// NB: The Generator carefully ensures that each value written is unique
	// globally over a run, so there's a 1:1 relationship between a value that was
	// written and the operation that wrote it.
	kvByValue map[string]storage.MVCCKeyValue

	// Unfortunately, with tombstones there is no 1:1 relationship between the nil
	// value and the delete operation that wrote it, so we must store all tombstones
	// for a given key. When validating committed delete operations, we validate
	// that there is a tombstone with a timestamp that would be valid, similar
	// to how reads are evaluated.  At the end of validation, we also validate
	// that we have seen a correct number of materialized delete operations
	// given the number of tombstones for each key; thus, we can see if we have
	// any "missing" or "extra" writes at the end.
	// Each key has a map of all the tombstone timestamps, stored with a boolean
	// flag indicating if it has been matched to a transactional delete or not.
	tombstonesByKey        map[string]map[hlc.Timestamp]bool // map[key]map[ts]->used
	committedDeletesForKey map[string]int

	failures []error
}

func makeValidator(kvs *Engine) (*validator, error) {
	kvByValue := make(map[string]storage.MVCCKeyValue)
	tombstonesForKey := make(map[string]map[hlc.Timestamp]bool)
	var err error
	kvs.Iterate(func(key storage.MVCCKey, value []byte, iterErr error) {
		if iterErr != nil {
			err = errors.CombineErrors(err, iterErr)
			return
		}
		v, decodeErr := storage.DecodeMVCCValue(value)
		if err != nil {
			err = errors.CombineErrors(err, decodeErr)
			return
		}
		if v.Value.GetTag() != roachpb.ValueType_UNKNOWN {
			valueStr := mustGetStringValue(value)
			if existing, ok := kvByValue[valueStr]; ok {
				// TODO(dan): This may be too strict. Some operations (db.Run on a
				// Batch) seem to be double-committing. See #46374.
				panic(errors.AssertionFailedf(
					`invariant violation: value %s was written by two operations %s and %s`,
					valueStr, existing.Key, key))
			}
			// NB: The Generator carefully ensures that each value written is unique
			// globally over a run, so there's a 1:1 relationship between a value that
			// was written and the operation that wrote it.
			kvByValue[valueStr] = storage.MVCCKeyValue{Key: key, Value: value}
		} else if !v.Value.IsPresent() {
			rawKey := string(key.Key)
			if _, ok := tombstonesForKey[rawKey]; !ok {
				tombstonesForKey[rawKey] = make(map[hlc.Timestamp]bool)
			}
			tombstonesForKey[rawKey][key.Timestamp] = false
		}
	})
	if err != nil {
		return nil, err
	}

	return &validator{
		kvs:                    kvs,
		kvByValue:              kvByValue,
		tombstonesByKey:        tombstonesForKey,
		committedDeletesForKey: make(map[string]int),
	}, nil
}

// getDeleteForKey looks up a stored tombstone for a given key (if it
// exists) from tombstonesByKey, returning the tombstone (i.e. MVCCKey) along
// with a `true` boolean value if found, or the empty key and `false` if not.
func (v *validator) getDeleteForKey(key string, ts hlc.Timestamp) (storage.MVCCKey, bool) {
	if used, ok := v.tombstonesByKey[key][ts]; !used && ok {
		v.tombstonesByKey[key][ts] = true
		return storage.MVCCKey{Key: []byte(key), Timestamp: ts}, true
	}

	return storage.MVCCKey{}, false
}

const (
	notBuffering = false
	isBuffering  = true
)

// processOp turns the result of an operation into its observations (which are
// later checked against the MVCC history). The boolean parameter indicates
// whether the operation is its own atomic unit or whether it's happening as
// part of a surrounding transaction or batch (in which case the caller is
// itself processOp, with the operation to handle being the batch or txn).
// Whenever it is `false`, processOp invokes the validator's checkAtomic method
// for the operation.
func (v *validator) processOp(buffering bool, op Operation) {
	// We don't need an execution timestamp when buffering (the caller will need
	// an execution timestamp for the combined operation, though). Additionally,
	// some operations supported by kvnemesis aren't MVCC-aware (splits, etc) and
	// thus also don't need an execution timestamp.
	execTimestampStrictlyOptional := buffering
	switch t := op.GetValue().(type) {
	case *GetOperation:
		v.failIfError(op, t.Result)
		if !buffering {
			v.checkAtomic(`get`, t.Result, op)
		} else {
			read := &observedRead{
				Key:   t.Key,
				Value: roachpb.Value{RawBytes: t.Result.Value},
			}
			v.curObservations = append(v.curObservations, read)
		}
	case *PutOperation:
		if !buffering {
			v.checkAtomic(`put`, t.Result, op)
		} else {
			// Accumulate all the writes for this transaction.
			kv, ok := v.kvByValue[string(t.Value)]
			delete(v.kvByValue, string(t.Value))
			write := &observedWrite{
				Key:   t.Key,
				Value: roachpb.MakeValueFromBytes(t.Value),
			}
			if ok {
				write.Timestamp = kv.Key.Timestamp
			}
			v.curObservations = append(v.curObservations, write)
		}
	case *DeleteOperation:
		if !buffering {
			v.checkAtomic(`delete`, t.Result, op)
		} else {
			// NB: While Put operations can be identified as having materialized
			// (or not) in the storage engine because the Generator guarantees each
			// value to be unique (and thus, if a MVCC key/value pair exists in the
			// storage engine with a value matching that of a write operation, it
			// materialized), the same cannot be done for Delete operations, which
			// all write the same tombstone value. Thus, Delete operations can only
			// be identified as materialized by determining if the final write
			// operation for a key in a given transaction was a Delete, and
			// validating that a potential tombstone for that key was stored.
			// This validation must be done at the end of the transaction;
			// specifically, in the function `checkAtomicCommitted(..)` where it looks
			// up a corresponding tombstone with `getDeleteForKey(..)`.
			write := &observedWrite{
				Key:   t.Key,
				Value: roachpb.Value{},
			}
			v.curObservations = append(v.curObservations, write)
		}
	case *DeleteRangeOperation:
		if !buffering {
			v.checkAtomic(`deleteRange`, t.Result, op)
		} else {
			// For the purposes of validation, DelRange operations decompose into
			// a specialized scan for keys with non-nil values, followed by
			// writes for each key, with a span to validate that the keys we are
			// deleting are within the proper bounds.  See above comment for how
			// the individual deletion tombstones for each key are validated.
			scan := &observedScan{
				Span: roachpb.Span{
					Key:    t.Key,
					EndKey: t.EndKey,
				},
				IsDeleteRange: true,
				KVs:           make([]roachpb.KeyValue, len(t.Result.Keys)),
			}
			deleteOps := make([]observedOp, len(t.Result.Keys))
			for i, key := range t.Result.Keys {
				scan.KVs[i] = roachpb.KeyValue{
					Key:   key,
					Value: roachpb.Value{},
				}
				write := &observedWrite{
					Key:           key,
					Value:         roachpb.Value{},
					IsDeleteRange: true,
				}
				deleteOps[i] = write
			}
			v.curObservations = append(v.curObservations, scan)
			v.curObservations = append(v.curObservations, deleteOps...)
		}
	case *ScanOperation:
		v.failIfError(op, t.Result)
		if !buffering {
			atomicScanType := `scan`
			if t.Reverse {
				atomicScanType = `reverse scan`
			}
			v.checkAtomic(atomicScanType, t.Result, op)
		} else {
			scan := &observedScan{
				Span: roachpb.Span{
					Key:    t.Key,
					EndKey: t.EndKey,
				},
				KVs:     make([]roachpb.KeyValue, len(t.Result.Values)),
				Reverse: t.Reverse,
			}
			for i, kv := range t.Result.Values {
				scan.KVs[i] = roachpb.KeyValue{
					Key:   kv.Key,
					Value: roachpb.Value{RawBytes: kv.Value},
				}
			}
			v.curObservations = append(v.curObservations, scan)
		}
	case *SplitOperation:
		execTimestampStrictlyOptional = true
		v.failIfError(op, t.Result)
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
			// broken by wrapping it with `roachpb.NewErrorf("merge failed: %s",
			// err)`.
			//
			// However, I think the right thing to do is sniff this inside the
			// AdminMerge code and retry so the client never sees it. In the meantime,
			// no-op. #44377
		} else if resultIsErrorStr(t.Result, `merge failed: cannot merge ranges when (rhs)|(lhs) is in a joint state or has learners`) {
			// This operation executed concurrently with one that was changing
			// replicas.
		} else if resultIsErrorStr(t.Result, `merge failed: ranges not collocated`) {
			// A merge requires that the two ranges have replicas on the same nodes,
			// but Generator intentiontally does not try to avoid this so that this
			// edge case is exercised.
		} else if resultIsErrorStr(t.Result, `merge failed: waiting for all left-hand replicas to initialize`) {
			// Probably should be transparently retried.
		} else if resultIsErrorStr(t.Result, `merge failed: waiting for all right-hand replicas to catch up`) {
			// Probably should be transparently retried.
		} else if resultIsErrorStr(t.Result, `merge failed: non-deletion intent on local range descriptor`) {
			// Probably should be transparently retried.
		} else if resultIsErrorStr(t.Result, `merge failed: range missing intent on its local descriptor`) {
			// Probably should be transparently retried.
		} else if resultIsErrorStr(t.Result, `merge failed: RHS range bounds do not match`) {
			// Probably should be transparently retried.
		} else {
			v.failIfError(op, t.Result)
		}
	case *ChangeReplicasOperation:
		execTimestampStrictlyOptional = true
		var ignore bool
		if err := errorFromResult(t.Result); err != nil {
			ignore = kvserver.IsRetriableReplicationChangeError(err) ||
				kvserver.IsIllegalReplicationChangeError(err) ||
				kvserver.IsReplicationChangeInProgressError(err)
		}
		if !ignore {
			v.failIfError(op, t.Result)
		}
	case *TransferLeaseOperation:
		execTimestampStrictlyOptional = true
		var ignore bool
		if err := errorFromResult(t.Result); err != nil {
			ignore = kvserver.IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(err) ||
				// Only VOTER (_FULL, _INCOMING, sometimes _OUTGOING) replicas can
				// hold a range lease. Attempts to transfer to lease to any other
				// replica type are rejected. See CheckCanReceiveLease.
				resultIsErrorStr(t.Result, `replica cannot hold lease`) ||
				// Only replicas that are part of the range can be given
				// the lease. This case is hit if a TransferLease op races
				// with a ChangeReplicas op.
				resultIsErrorStr(t.Result, `replica not found in RangeDescriptor`) ||
				// A lease transfer that races with a replica removal may find that
				// the store it was targeting is no longer part of the range.
				resultIsErrorStr(t.Result, `unable to find store \d+ in range`) ||
				// A lease transfer is not permitted while a range merge is in its
				// critical phase.
				resultIsErrorStr(t.Result, `cannot transfer lease while merge in progress`) ||
				// If the existing leaseholder has not yet heard about the transfer
				// target's liveness record through gossip, it will return an error.
				resultIsError(t.Result, liveness.ErrRecordCacheMiss) ||
				// Same as above, but matches cases where ErrRecordCacheMiss is
				// passed through a LeaseRejectedError. This is necessary until
				// LeaseRejectedErrors works with errors.Cause.
				resultIsErrorStr(t.Result, liveness.ErrRecordCacheMiss.Error())
		}
		if !ignore {
			v.failIfError(op, t.Result)
		}
	case *ChangeZoneOperation:
		execTimestampStrictlyOptional = true
		v.failIfError(op, t.Result)
	case *BatchOperation:
		if !resultIsRetryable(t.Result) {
			v.failIfError(op, t.Result)
			if !buffering {
				v.checkAtomic(`batch`, t.Result, t.Ops...)
			} else {
				for _, op := range t.Ops {
					v.processOp(buffering, op)
				}
			}
		}
	case *ClosureTxnOperation:
		ops := t.Ops
		if t.CommitInBatch != nil {
			ops = append(ops, t.CommitInBatch.Ops...)
		}
		v.checkAtomic(`txn`, t.Result, ops...)
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, t, t))
	}

	if !execTimestampStrictlyOptional && !buffering && op.Result().Type != ResultType_Error && op.Result().OptionalTimestamp.IsEmpty() {
		v.failures = append(v.failures, errors.Errorf("execution timestamp missing for %s", op))
	}
}

// checkAtomic verifies a set of operations that should be atomic by trying to find
// a timestamp at which the observed reads and writes of the operations (as executed
// in the order in which they appear in the arguments) match the MVCC history.
func (v *validator) checkAtomic(atomicType string, result Result, ops ...Operation) {
	for _, op := range ops {
		// NB: we're not really necessarily in a txn, but passing true here means that
		// we have an atomic unit, which is also the case if we are called here by a
		// non-transactional Put, for example.
		v.processOp(isBuffering, op)
	}
	observations := v.curObservations
	v.curObservations = nil

	// Only known-uncommitted results may come without a timestamp. Whenever we
	// actually tried to commit, there is a timestamp.
	if result.Type != ResultType_Error {
		// The timestamp is not optional in this case. Note however that at the time
		// of writing, checkAtomicCommitted doesn't capitalize on this unconditional
		// presence yet, and most unit tests don't specify it for reads.
		if !result.OptionalTimestamp.IsSet() {
			err := errors.AssertionFailedf("operation has no execution timestamp: %s", result)
			v.failures = append(v.failures, err)
		}
		v.checkAtomicCommitted(`committed `+atomicType, observations, result.OptionalTimestamp)
	} else if ambWriteTS, err := resultIsAmbiguous(result); err != nil {
		v.failures = append(v.failures, err)
	} else if ambWriteTS.IsSet() {
		if result.OptionalTimestamp.IsSet() {
			err := errors.AssertionFailedf("OptionalTimestamp set for ambiguous result: %s", result)
			v.failures = append(v.failures, err)
		}
		result.OptionalTimestamp.Forward(ambWriteTS)
		v.checkAtomicAmbiguous(`ambiguous `+atomicType, observations, result.OptionalTimestamp)
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
	batch := v.kvs.kvs.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	// If the same key is written multiple times in a transaction, only the last
	// one makes it to kv.
	lastWriteIdxByKey := make(map[string]int, len(txnObservations))
	for idx := len(txnObservations) - 1; idx >= 0; idx-- {
		observation := txnObservations[idx]
		switch o := observation.(type) {
		case *observedWrite:
			if _, ok := lastWriteIdxByKey[string(o.Key)]; !ok {
				lastWriteIdxByKey[string(o.Key)] = idx

				// Mark which deletes are materialized and match them with a stored
				// tombstone, since this cannot be done before the end of the txn.
				// This is because materialized deletes do not write unique values,
				// but must be the final write in a txn for that key.
				if o.isDelete() {
					key := string(o.Key)
					if storedDelete, ok := v.getDeleteForKey(key, execTimestamp); ok {
						o.Timestamp = storedDelete.Timestamp
					}
				}
			}
			mvccKey := storage.MVCCKey{Key: o.Key, Timestamp: o.Timestamp}
			if err := batch.Delete(storage.EncodeMVCCKey(mvccKey), nil); err != nil {
				panic(err)
			}
		}
	}

	// Check if any key that was written twice in the txn had the overwritten
	// writes materialize in kv. Also fill in all the read timestamps first so
	// they show up in the failure message.
	var failure string
	for idx, observation := range txnObservations {
		if failure != `` {
			break
		}
		switch o := observation.(type) {
		case *observedWrite:
			var mvccKey storage.MVCCKey
			if lastWriteIdx := lastWriteIdxByKey[string(o.Key)]; idx == lastWriteIdx {
				// The last write of a given key in the txn wins and should have made it
				// to kv.
				mvccKey = storage.MVCCKey{Key: o.Key, Timestamp: o.Timestamp}
			} else {
				if o.Timestamp.IsSet() {
					failure = `committed txn overwritten key had write`
				}
				// This write was never materialized in KV because the key got
				// overwritten later in the txn. But reads in the txn could have seen
				// it, so we put in the batch being maintained for validReadTimes using
				// the timestamp of the write for this key that eventually "won".
				mvccKey = storage.MVCCKey{
					Key:       o.Key,
					Timestamp: txnObservations[lastWriteIdx].(*observedWrite).Timestamp,
				}
			}
			if err := batch.Set(storage.EncodeMVCCKey(mvccKey), o.Value.RawBytes, nil); err != nil {
				panic(err)
			}
		case *observedRead:
			o.ValidTimes = validReadTimes(batch, o.Key, o.Value.RawBytes, false)
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
			o.Valid = validScanTime(batch, o.Span, o.KVs, o.IsDeleteRange)
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observation, observation))
		}
	}

	validPossibilities := disjointTimeSpans{{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}}
	for idx, observation := range txnObservations {
		if failure != `` {
			break
		}
		var opValid disjointTimeSpans
		switch o := observation.(type) {
		case *observedWrite:
			isLastWriteForKey := idx == lastWriteIdxByKey[string(o.Key)]
			if !isLastWriteForKey {
				continue
			}
			if o.Timestamp.IsEmpty() {
				failure = atomicType + ` missing write`
				continue
			}

			if o.isDelete() && len(txnObservations) == 1 {
				// For delete operations outside of transactions, it is not possible to
				// identify the precise tombstone, so we skip timestamp validation.
				continue
			}

			opValid = disjointTimeSpans{{Start: o.Timestamp, End: o.Timestamp.Next()}}
		case *observedRead:
			opValid = o.ValidTimes
		case *observedScan:
			opValid = o.Valid.Combined()
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
	for _, observation := range txnObservations {
		if failure != `` {
			break
		}
		switch o := observation.(type) {
		case *observedWrite:
			if o.Timestamp.IsSet() && o.Timestamp != execTimestamp {
				failure = fmt.Sprintf(`mismatched write timestamp %s`, execTimestamp)
			}
		}
	}

	if failure != `` {
		err := errors.Errorf("%s: %s", failure, printObserved(txnObservations...))
		v.failures = append(v.failures, err)
	}
}

func (v *validator) checkAtomicAmbiguous(
	atomicType string, txnObservations []observedOp, writeTimestamp hlc.Timestamp,
) {
	var committed bool
	var isRW bool
	for _, observation := range txnObservations {
		switch o := observation.(type) {
		case *observedWrite:
			isRW = true
			if o.Timestamp.IsSet() {
				committed = true
				break
			}
		}
	}

	if !isRW || committed {
		v.checkAtomicCommitted(atomicType, txnObservations, writeTimestamp)
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
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observed, observed))
		}
	}

	if failure != `` {
		err := errors.Errorf("%s: %s", failure, printObserved(txnObservations...))
		v.failures = append(v.failures, err)
	}
}

func (v *validator) failIfError(op Operation, r Result) {
	switch r.Type {
	case ResultType_Unknown:
		err := errors.AssertionFailedf(`unknown result %s`, op)
		v.failures = append(v.failures, err)
	case ResultType_Error:
		ctx := context.Background()
		err := errors.DecodeError(ctx, *r.Err)
		err = errors.Wrapf(err, `error applying %s`, op)
		v.failures = append(v.failures, err)
	}
}

func errorFromResult(r Result) error {
	if r.Type != ResultType_Error {
		return nil
	}
	ctx := context.Background()
	return errors.DecodeError(ctx, *r.Err)
}

func resultIsError(r Result, reference error) bool {
	return errors.Is(errorFromResult(r), reference)
}

func resultIsRetryable(r Result) bool {
	return errors.HasInterface(errorFromResult(r), (*roachpb.ClientVisibleRetryError)(nil))
}

func resultIsAmbiguous(r Result) (writeTS hlc.Timestamp, _ error) {
	resErr := errorFromResult(r)
	hasClientVisibleAE := errors.HasInterface(resErr, (*roachpb.ClientVisibleAmbiguousError)(nil))
	var ae *roachpb.AmbiguousResultError
	hasAE := errors.As(resErr, &ae)
	if hasClientVisibleAE != hasAE {
		return hlc.Timestamp{}, errors.AssertionFailedf(
			"client visible ambiguous result and AmbiguousResultError differ: %t != %t",
			hasClientVisibleAE, ae,
		)
	}
	if !hasAE {
		return hlc.Timestamp{}, nil
	}
	if ae.WriteTimestamp.IsEmpty() {
		return hlc.Timestamp{}, errors.AssertionFailedf("AmbiguousResultError without WriteTimestamp: %+v", ae)
	}
	return ae.WriteTimestamp, nil
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
	b *pebble.Batch, key roachpb.Key, value []byte, anyValueAccepted bool,
) disjointTimeSpans {
	var validTimes disjointTimeSpans
	end := hlc.MaxTimestamp

	iter := b.NewIter(nil)
	defer func() { _ = iter.Close() }()
	iter.SeekGE(storage.EncodeMVCCKey(storage.MVCCKey{Key: key}))
	for ; iter.Valid(); iter.Next() {
		mvccKey, err := storage.DecodeMVCCKey(iter.Key())
		if err != nil {
			panic(err)
		}
		if !mvccKey.Key.Equal(key) {
			break
		}
		if (anyValueAccepted && len(iter.Value()) > 0) ||
			(!anyValueAccepted && mustGetStringValue(iter.Value()) == mustGetStringValue(value)) {
			validTimes = append(validTimes, timeSpan{Start: mvccKey.Timestamp, End: end})
		}
		end = mvccKey.Timestamp
	}
	if !anyValueAccepted && len(value) == 0 {
		validTimes = append(disjointTimeSpans{{Start: hlc.MinTimestamp, End: end}}, validTimes...)
	}

	// NB: With the exception of deletes, the "only write each value once"
	// property of the generator means that we have a 1:1 mapping between some
	// `(key, non-nil-value)` observation and a time span in which it was valid.
	// With deletes, there multiple disjoint spans for a `(key, nil-value)`
	// observation (i.e. before the key existed, after it was deleted).
	// This means that for each read, we must consider all possibly valid times.
	return validTimes
}

func validScanTime(
	b *pebble.Batch, span roachpb.Span, kvs []roachpb.KeyValue, isDeleteRange bool,
) multiKeyTimeSpan {
	valid := multiKeyTimeSpan{
		Gaps: disjointTimeSpans{{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}},
	}

	// Find the valid time span for each kv returned.
	for _, kv := range kvs {
		// Since scan results don't include deleted keys, there should only ever
		// be 0 or 1 valid read time span for each `(key, specific-non-nil-value)`
		// returned, given that the values are guaranteed to be unique by the
		// Generator. However, in the DeleteRange case where we are looking for
		// `(key, any-non-nil-value)`, it is of course valid for there to be
		// multiple disjoint time spans.
		validTimes := validReadTimes(b, kv.Key, kv.Value.RawBytes, isDeleteRange)
		if !isDeleteRange && len(validTimes) > 1 {
			panic(errors.AssertionFailedf(
				`invalid number of read time spans for a (key,non-nil-value) pair in scan results: %s->%s`,
				kv.Key, mustGetStringValue(kv.Value.RawBytes)))
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
	iter := b.NewIter(nil)
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
			missingKeys[string(mvccKey.Key)] = validReadTimes(b, mvccKey.Key, nil, false)
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
					opCode = "dr.d"
				} else {
					opCode = "d"
				}
			}
			ts := `missing`
			if o.Timestamp.IsSet() {
				ts = o.Timestamp.String()
			}
			fmt.Fprintf(&buf, "[%s]%s:%s->%s",
				opCode, o.Key, ts, mustGetStringValue(o.Value.RawBytes))
		case *observedRead:
			fmt.Fprintf(&buf, "[r]%s:", o.Key)
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
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observed, observed))
		}
	}
	return buf.String()
}
