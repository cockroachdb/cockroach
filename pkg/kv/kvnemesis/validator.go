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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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
// TODO(dan): Verify that the results of reads match the data visible at the
// commit timestamp. We should be able to construct a validity timespan for each
// read in the transaction and fail if there isn't some timestamp that overlaps
// all of them.
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
		v.processOp(nil /* txnID */, s.Op)
	}

	var extraKVs []observedOp
	for _, kv := range v.kvByValue {
		kv := &observedWrite{
			Key:          kv.Key.Key,
			Value:        roachpb.Value{RawBytes: kv.Value},
			Timestamp:    kv.Key.Timestamp,
			Materialized: true,
		}
		extraKVs = append(extraKVs, kv)
	}
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
	// Timestamp will only be filled if Materialized is true.
	Timestamp    hlc.Timestamp
	Materialized bool
}

func (*observedWrite) observedMarker() {}

type observedRead struct {
	Key   roachpb.Key
	Value roachpb.Value
	Valid timeSpan
}

func (*observedRead) observedMarker() {}

type validator struct {
	kvs              *Engine
	observedOpsByTxn map[string][]observedOp

	// NB: The Generator carefully ensures that each value written is unique
	// globally over a run, so there's a 1:1 relationship between a value that was
	// written and the operation that wrote it.
	kvByValue map[string]storage.MVCCKeyValue

	failures []error
}

func makeValidator(kvs *Engine) (*validator, error) {
	kvByValue := make(map[string]storage.MVCCKeyValue)
	var err error
	kvs.Iterate(func(key storage.MVCCKey, value []byte, iterErr error) {
		if iterErr != nil {
			err = errors.CombineErrors(err, iterErr)
			return
		}
		v := roachpb.Value{RawBytes: value}
		if v.GetTag() != roachpb.ValueType_UNKNOWN {
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
		}
	})
	if err != nil {
		return nil, err
	}

	return &validator{
		kvs:              kvs,
		kvByValue:        kvByValue,
		observedOpsByTxn: make(map[string][]observedOp),
	}, nil
}

func (v *validator) processOp(txnID *string, op Operation) {
	switch t := op.GetValue().(type) {
	case *GetOperation:
		v.failIfError(op, t.Result)
		if txnID == nil {
			v.checkAtomic(`get`, t.Result, op)
		} else {
			read := &observedRead{
				Key:   t.Key,
				Value: roachpb.Value{RawBytes: t.Result.Value},
			}
			v.observedOpsByTxn[*txnID] = append(v.observedOpsByTxn[*txnID], read)
		}
	case *PutOperation:
		if txnID == nil {
			v.checkAtomic(`put`, t.Result, op)
		} else {
			// Accumulate all the writes for this transaction.
			kv, ok := v.kvByValue[string(t.Value)]
			delete(v.kvByValue, string(t.Value))
			write := &observedWrite{
				Key:          t.Key,
				Value:        roachpb.MakeValueFromBytes(t.Value),
				Materialized: ok,
			}
			if write.Materialized {
				write.Timestamp = kv.Key.Timestamp
			}
			v.observedOpsByTxn[*txnID] = append(v.observedOpsByTxn[*txnID], write)
		}
	case *SplitOperation:
		v.failIfError(op, t.Result)
	case *MergeOperation:
		if resultIsError(t.Result, `cannot merge final range`) {
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
		} else if resultIsError(t.Result, `merge failed: unexpected value`) {
			// TODO(dan): If this error is going to remain a part of the kv API, we
			// should make it sniffable with errors.As. Currently this seems to be
			// broken by wrapping it with `roachpb.NewErrorf("merge failed: %s",
			// err)`.
			//
			// However, I think the right thing to do is sniff this inside the
			// AdminMerge code and retry so the client never sees it. In the meantime,
			// no-op. #44377
		} else if resultIsError(t.Result, `merge failed: cannot merge range with non-voter replicas`) {
			// This operation executed concurrently with one that was changing
			// replicas.
		} else if resultIsError(t.Result, `merge failed: ranges not collocated`) {
			// A merge requires that the two ranges have replicas on the same nodes,
			// but Generator intentiontally does not try to avoid this so that this
			// edge case is exercised.
		} else if resultIsError(t.Result, `merge failed: waiting for all left-hand replicas to initialize`) {
			// Probably should be transparently retried.
		} else if resultIsError(t.Result, `merge failed: waiting for all right-hand replicas to catch up`) {
			// Probably should be transparently retried.
		} else if resultIsError(t.Result, `merge failed: non-deletion intent on local range descriptor`) {
			// Probably should be transparently retried.
		} else if resultIsError(t.Result, `merge failed: range missing intent on its local descriptor`) {
			// Probably should be transparently retried.
		} else {
			v.failIfError(op, t.Result)
		}
	case *ChangeReplicasOperation:
		if resultIsError(t.Result, `unable to add replica .* which is already present in`) {
			// Generator created this operations based on data about a range's
			// replicas that is now stale (because it raced with some other operation
			// created by that Generator): a replica is being added and in the
			// meantime, some other operation added the same replica.
		} else if resultIsError(t.Result, `unable to add replica .* which is already present as a learner`) {
			// Generator created this operations based on data about a range's
			// replicas that is now stale (because it raced with some other operation
			// created by that Generator): a replica is being added and in the
			// meantime, some other operation started (but did not finish) adding the
			// same replica.
		} else if resultIsError(t.Result, `descriptor changed`) {
			// Race between two operations being executed concurrently. Applier grabs
			// a range descriptor and then calls AdminChangeReplicas with it, but the
			// descriptor is changed by some other operation in between.
		} else if resultIsError(t.Result, `received invalid ChangeReplicasTrigger .* to remove self \(leaseholder\)`) {
			// Removing the leaseholder is invalid for technical reasons, but
			// Generator intentiontally does not try to avoid this so that this edge
			// case is exercised.
		} else if resultIsError(t.Result, `removing .* which is not in`) {
			// Generator created this operations based on data about a range's
			// replicas that is now stale (because it raced with some other operation
			// created by that Generator): a replica is being removed and in the
			// meantime, some other operation removed the same replica.
		} else if resultIsError(t.Result, `remote failed to apply snapshot for reason failed to apply snapshot: raft group deleted`) {
			// Probably should be transparently retried.
		} else if resultIsError(t.Result, `cannot apply snapshot: snapshot intersects existing range`) {
			// Probably should be transparently retried.
		} else if resultIsError(t.Result, `snapshot of type LEARNER was sent to .* which did not contain it as a replica`) {
			// Probably should be transparently retried.
		} else {
			v.failIfError(op, t.Result)
		}
	case *BatchOperation:
		if !resultIsRetryable(t.Result) {
			v.failIfError(op, t.Result)
			if txnID == nil {
				v.checkAtomic(`batch`, t.Result, t.Ops...)
			} else {
				for _, op := range t.Ops {
					v.processOp(txnID, op)
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
}

func (v *validator) checkAtomic(atomicType string, result Result, ops ...Operation) {
	fakeTxnID := uuid.MakeV4().String()
	for _, op := range ops {
		v.processOp(&fakeTxnID, op)
	}
	txnObservations := v.observedOpsByTxn[fakeTxnID]
	delete(v.observedOpsByTxn, fakeTxnID)
	if result.Type == ResultType_NoError {
		v.checkCommittedTxn(`committed `+atomicType, txnObservations)
	} else if resultIsAmbiguous(result) {
		v.checkAmbiguousTxn(`ambiguous `+atomicType, txnObservations)
	} else {
		v.checkUncommittedTxn(`uncommitted `+atomicType, txnObservations)
	}
}

func (v *validator) checkCommittedTxn(atomicType string, txnObservations []observedOp) {
	// The following works by verifying that there is at least one time at which
	// it was valid to see all the reads and writes that we saw in this
	// transaction.
	//
	// Concretely a transaction:
	// - Write k1@t2 -> v1
	// - Read k2 -> v2
	//
	// And what was present in KV after this and some other transactions:
	// - k1@t2, v1
	// - k1@t3, v3
	// - k2@t1, v2
	// - k2@t3, v4
	//
	// Each of the operations in the transaction, if taken individually, has some
	// window at which it was valid. The Write was only valid for a commit exactly
	// at t2: [t2,t2). This is because each Write's mvcc timestamp is the
	// timestamp of the txn commit. The Read would have been valid for [t1,t3)
	// because v2 was written at t1 and overwritten at t3.
	//
	// As long as these time spans overlap, we're good. However, if another write
	// had a timestamp of t3, then there is no timestamp at which the transaction
	// could have committed, which is a violation of our consistency guarantees.
	// Similarly if there was some read that was only valid from [t1,t2).
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
			}
			mvccKey := storage.MVCCKey{Key: o.Key, Timestamp: o.Timestamp}
			if err := batch.Delete(storage.EncodeKey(mvccKey), nil); err != nil {
				panic(err)
			}
		}
	}

	// Check if any key that was written twice in the txn had the overwritten
	// writes materialize in kv. Also fill in all the read timestamps first so
	// they show up in the failure message.
	var failure string
	for idx, observation := range txnObservations {
		switch o := observation.(type) {
		case *observedWrite:
			if lastWriteIdx := lastWriteIdxByKey[string(o.Key)]; idx == lastWriteIdx {
				// The last write of a given key in the txn wins and should have made it
				// to kv.
				mvccKey := storage.MVCCKey{Key: o.Key, Timestamp: o.Timestamp}
				if err := batch.Set(storage.EncodeKey(mvccKey), o.Value.RawBytes, nil); err != nil {
					panic(err)
				}
			} else {
				if o.Materialized {
					failure = `committed txn overwritten key had write`
				}
				// This write was never materialized in KV because the key got
				// overwritten later in the txn. But reads in the txn could have seen
				// it, so we put in the batch being maintained for validReadTime using
				// the timestamp of the write for this key that eventually "won".
				mvccKey := storage.MVCCKey{
					Key:       o.Key,
					Timestamp: txnObservations[lastWriteIdx].(*observedWrite).Timestamp,
				}
				if err := batch.Set(storage.EncodeKey(mvccKey), o.Value.RawBytes, nil); err != nil {
					panic(err)
				}
			}
		case *observedRead:
			o.Valid = validReadTime(batch, o.Key, o.Value.RawBytes)
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observation, observation))
		}
	}

	valid := timeSpan{Start: hlc.MinTimestamp, End: hlc.MaxTimestamp}
	for idx, observation := range txnObservations {
		if failure != `` {
			break
		}
		var opValid timeSpan
		switch o := observation.(type) {
		case *observedWrite:
			isLastWriteForKey := idx == lastWriteIdxByKey[string(o.Key)]
			if !isLastWriteForKey {
				continue
			}
			if !o.Materialized {
				failure = atomicType + ` missing write`
				continue
			}
			opValid = timeSpan{Start: o.Timestamp, End: o.Timestamp.Next()}
		case *observedRead:
			opValid = o.Valid
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observation, observation))
		}
		intersection := valid.Intersect(opValid)
		if intersection.IsEmpty() {
			failure = atomicType + ` non-atomic timestamps`
		}
		valid = intersection
	}

	if failure != `` {
		err := errors.Errorf("%s: %s", failure, printObserved(txnObservations...))
		v.failures = append(v.failures, err)
	}
}

func (v *validator) checkAmbiguousTxn(atomicType string, txnObservations []observedOp) {
	var somethingCommitted bool
	var hadWrite bool
	for _, observation := range txnObservations {
		switch o := observation.(type) {
		case *observedWrite:
			hadWrite = true
			if o.Materialized {
				somethingCommitted = true
				break
			}
		}
	}
	if !hadWrite {
		// TODO(dan): Is it possible to receive an ambiguous read-only txn? Assume
		// committed for now because the committed case has assertions about reads
		// but the uncommitted case doesn't and this seems to work.
		v.checkCommittedTxn(atomicType, txnObservations)
	} else if somethingCommitted {
		v.checkCommittedTxn(atomicType, txnObservations)
	} else {
		v.checkUncommittedTxn(atomicType, txnObservations)
	}
}

func (v *validator) checkUncommittedTxn(atomicType string, txnObservations []observedOp) {
	var failure string
	for _, observed := range txnObservations {
		if failure != `` {
			break
		}
		switch o := observed.(type) {
		case *observedWrite:
			if o.Materialized {
				failure = atomicType + ` had writes`
			}
		case *observedRead:
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

// TODO(dan): Checking errors using string containment is fragile at best and a
// security issue at worst. Unfortunately, some errors that currently make it
// out of our kv apis are created with `errors.New` and so do not have types
// that can be sniffed. Some of these may be removed or handled differently but
// the rest should graduate to documented parts of the public api. Remove this
// once it happens.
func resultIsError(r Result, msgRE string) bool {
	if r.Type != ResultType_Error {
		return false
	}
	ctx := context.Background()
	err := errors.DecodeError(ctx, *r.Err)
	return regexp.MustCompile(msgRE).MatchString(err.Error())
}

func resultIsRetryable(r Result) bool {
	if r.Type != ResultType_Error {
		return false
	}
	ctx := context.Background()
	err := errors.DecodeError(ctx, *r.Err)
	return errors.HasInterface(err, (*roachpb.ClientVisibleRetryError)(nil))
}

func resultIsAmbiguous(r Result) bool {
	if r.Type != ResultType_Error {
		return false
	}
	ctx := context.Background()
	err := errors.DecodeError(ctx, *r.Err)
	return errors.HasInterface(err, (*roachpb.ClientVisibleAmbiguousError)(nil))
}

func mustGetStringValue(value []byte) string {
	if len(value) == 0 {
		return `<nil>`
	}
	v, err := roachpb.Value{RawBytes: value}.GetBytes()
	if err != nil {
		panic(errors.Wrapf(err, "decoding %x", value))
	}
	return string(v)
}

func validReadTime(b *pebble.Batch, key roachpb.Key, value []byte) timeSpan {
	var validTime []timeSpan
	end := hlc.MaxTimestamp

	iter := b.NewIter(nil)
	defer func() { _ = iter.Close() }()
	iter.SeekGE(storage.EncodeKey(storage.MVCCKey{Key: key}))
	for ; iter.Valid(); iter.Next() {
		mvccKey, err := storage.DecodeMVCCKey(iter.Key())
		if err != nil {
			panic(err)
		}
		if !mvccKey.Key.Equal(key) {
			break
		}
		if mustGetStringValue(iter.Value()) == mustGetStringValue(value) {
			validTime = append(validTime, timeSpan{Start: mvccKey.Timestamp, End: end})
		}
		end = mvccKey.Timestamp
	}
	if len(value) == 0 {
		validTime = append(validTime, timeSpan{Start: hlc.MinTimestamp, End: end})
	}

	if len(validTime) == 0 {
		return timeSpan{}
	} else if len(validTime) == 1 {
		return validTime[0]
	} else {
		// TODO(dan): Until we add deletes, the "only write each value once"
		// property of the generator means that we have a 1:1 mapping between some
		// `(key, possibly-nil-value)` observation and a time span in which it was
		// valid. Once we add deletes, there will be multiple disjoint spans for the
		// `(key, nil)` case.
		panic(`unreachable`)
	}
}

func printObserved(observedOps ...observedOp) string {
	var buf strings.Builder
	for _, observed := range observedOps {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		switch o := observed.(type) {
		case *observedWrite:
			ts := `missing`
			if o.Materialized {
				ts = o.Timestamp.String()
			}
			fmt.Fprintf(&buf, "[w]%s:%s->%s", o.Key, ts, mustGetStringValue(o.Value.RawBytes))
		case *observedRead:
			var start string
			if o.Valid.Start == hlc.MinTimestamp {
				start = `<min>`
			} else {
				start = o.Valid.Start.String()
			}
			var end string
			if o.Valid.End == hlc.MaxTimestamp {
				end = `<max>`
			} else {
				end = o.Valid.End.String()
			}
			fmt.Fprintf(&buf, "[r]%s:[%s,%s)->%s",
				o.Key, start, end, mustGetStringValue(o.Value.RawBytes))
		default:
			panic(errors.AssertionFailedf(`unknown observedOp: %T %s`, observed, observed))
		}
	}
	return buf.String()
}
