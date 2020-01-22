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
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/google/btree"
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
// timestamp as given back by client.Txn. This doesn't currently work for
// nontransactional read-only ops (i.e. single read or batch of only reads) but
// that could be fixed by altering the API to communicating the timestamp back.
//
// Splits and merges are not verified for anything other than that they did not
// return an error.
func Validate(steps []Step, kvs *Engine) []error {
	v := makeValidator(kvs)

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

	var extraKVs []engine.MVCCKeyValue
	for _, kv := range v.kvByValue {
		extraKVs = append(extraKVs, kv)
	}
	if len(extraKVs) > 0 {
		err := errors.Errorf(`extra writes: %s`, errors.Safe(printKVs(extraKVs...)))
		v.failures = append(v.failures, err)
	}

	return v.failures
}

type validator struct {
	kvByValue map[string]engine.MVCCKeyValue
	kvsByTxn  map[string][]engine.MVCCKeyValue

	failures []error
}

func makeValidator(kvs *Engine) *validator {
	kvByValue := make(map[string]engine.MVCCKeyValue)
	kvs.kvs.Ascend(func(item btree.Item) bool {
		kv := item.(btreeItem)
		value := mustGetStringValue(kv.Value)
		if existing, ok := kvByValue[value]; ok {
			// TODO(dan): This may be too strict. Some operations (db.Run on a Batch)
			// seem to be double-committing.
			panic(errors.AssertionFailedf(`invariant violation: value %s was written by two operations %s and %s`,
				value, existing.Key, kv.Key))
		}
		kvByValue[value] = engine.MVCCKeyValue(kv)
		return true
	})
	return &validator{
		kvByValue: kvByValue,
		kvsByTxn:  make(map[string][]engine.MVCCKeyValue),
	}
}

func (v *validator) processOp(txnID *string, op Operation) {
	switch t := op.GetValue().(type) {
	case *GetOperation:
		if !resultIsRetryable(t.Result) {
			v.failIfError(op, t.Result)
		}
	case *PutOperation:
		if txnID == nil {
			v.checkAtomic(`put`, t.Result, op)
		} else {
			// Accumulate all the writes for this transaction.
			kv, ok := v.kvByValue[string(t.Value)]
			delete(v.kvByValue, string(t.Value))
			if !ok {
				kv.Key.Key = t.Key
				kv.Value = roachpb.MakeValueFromBytes(t.Value).RawBytes
			}
			v.kvsByTxn[*txnID] = append(v.kvsByTxn[*txnID], kv)
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
	txnKVs := v.kvsByTxn[fakeTxnID]
	delete(v.kvsByTxn, fakeTxnID)
	if result.Type == ResultType_NoError {
		v.checkCommittedTxn(`committed `+atomicType, txnKVs)
	} else if resultIsAmbiguous(result) {
		v.checkAmbiguousTxn(`ambiguous `+atomicType, txnKVs)
	} else {
		v.checkUncommittedTxn(`uncommitted `+atomicType, txnKVs)
	}
}

func (v *validator) checkCommittedTxn(atomicType string, txnKVs []engine.MVCCKeyValue) {
	// If the same key is written multiple times in a transaction, only the last
	// one makes it to kv.
	lastWriteIdxByKey := make(map[string]int, len(txnKVs))
	for idx := range txnKVs {
		lastWriteIdxByKey[string(txnKVs[idx].Key.Key)] = idx
	}

	var ts hlc.Timestamp
	var failure string
	for idx, kv := range txnKVs {
		if failure != `` {
			break
		}

		isLastWriteForKey := idx == lastWriteIdxByKey[string(kv.Key.Key)]
		if !isLastWriteForKey {
			if !kv.Key.Timestamp.IsEmpty() {
				failure = `committed txn overwritten key had write`
			}
			continue
		}
		if ts.IsEmpty() {
			ts = kv.Key.Timestamp
		}
		if kv.Key.Timestamp.IsEmpty() {
			failure = atomicType + ` missing write`
		} else if !ts.Equal(kv.Key.Timestamp) {
			failure = atomicType + ` different timestamps`
		}
	}

	if failure != `` {
		err := errors.Errorf("%s: %s", failure, errors.Safe(printKVs(txnKVs...)))
		v.failures = append(v.failures, err)
	}
}

func (v *validator) checkAmbiguousTxn(atomicType string, txnKVs []engine.MVCCKeyValue) {
	var somethingCommitted bool
	for _, kv := range txnKVs {
		if !kv.Key.Timestamp.IsEmpty() {
			somethingCommitted = true
			break
		}
	}
	if somethingCommitted {
		v.checkCommittedTxn(atomicType, txnKVs)
	} else {
		v.checkUncommittedTxn(atomicType, txnKVs)
	}
}

func (v *validator) checkUncommittedTxn(atomicType string, txnKVs []engine.MVCCKeyValue) {
	var failure string
	for _, kv := range txnKVs {
		if kv.Key.Timestamp.IsEmpty() {
			continue
		}
		failure = atomicType + ` had writes`
		break
	}
	if failure != `` {
		err := errors.Errorf("%s: %s", failure, errors.Safe(printKVs(txnKVs...)))
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
func resultIsError(r Result, msg string) bool {
	if r.Type != ResultType_Error {
		return false
	}
	ctx := context.Background()
	err := errors.DecodeError(ctx, *r.Err)
	return strings.Contains(err.Error(), msg)
}

func resultIsRetryable(r Result) bool {
	if r.Type != ResultType_Error {
		return false
	}
	ctx := context.Background()
	err := errors.DecodeError(ctx, *r.Err)
	_, isRetryable := err.(roachpb.ClientVisibleRetryError)
	return isRetryable
}

func resultIsAmbiguous(r Result) bool {
	if r.Type != ResultType_Error {
		return false
	}
	ctx := context.Background()
	err := errors.DecodeError(ctx, *r.Err)
	_, isAmbiguous := err.(roachpb.ClientVisibleAmbiguousError)
	return isAmbiguous
}

func mustGetStringValue(value []byte) string {
	if len(value) == 0 {
		return `<nil>`
	}
	value, err := roachpb.Value{RawBytes: value}.GetBytes()
	if err != nil {
		panic(errors.Wrapf(err, "decoding %x", value))
	}
	return string(value)
}

func printKVs(kvs ...engine.MVCCKeyValue) string {
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key.Less(kvs[j].Key) })

	var buf strings.Builder
	for _, kv := range kvs {
		if buf.Len() > 0 {
			buf.WriteString(" ")
		}
		if kv.Key.Timestamp.IsEmpty() {
			fmt.Fprintf(&buf, "%s:missing->%s", kv.Key.Key, mustGetStringValue(kv.Value))
		} else {
			fmt.Fprintf(&buf, "%s:%s->%s", kv.Key.Key, kv.Key.Timestamp, mustGetStringValue(kv.Value))
		}
	}
	return buf.String()
}
