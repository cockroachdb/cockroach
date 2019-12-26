// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

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
func Validate(steps []Step, kvs *Engine) []error {
	v := makeValidator(kvs)
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
	// TODO(dan): This is pretty convoluted, especially the PutOperation handling.
	switch t := op.GetValue().(type) {
	case *GetOperation:
		if !resultIsRetryable(t.Result) {
			v.failIfError(op, t.Result)
		}
	case *PutOperation:
		kv, ok := v.kvByValue[string(t.Value)]
		if ok {
			delete(v.kvByValue, string(t.Value))
		} else {
			kv.Key.Key = t.Key
		}
		if txnID != nil {
			// Accumulate all the writes for this transaction.
			v.kvsByTxn[*txnID] = append(v.kvsByTxn[*txnID], kv)
		} else {
			if t.Result.Type == ResultType_NoError {
				// All successful non-transactional writes must be present.
				if !ok {
					err := errors.Errorf(`missing write: %s->%s`,
						errors.Safe(roachpb.Key(t.Key)), errors.Safe(t.Value))
					v.failures = append(v.failures, err)
				}
			} else if resultIsAmbiguous(t.Result) {
				// Present or not are both okay.
			} else {
				if !resultIsRetryable(t.Result) {
					v.failIfError(op, t.Result)
				}
				if ok {
					err := errors.Errorf(`write from failed put: %s->%s`,
						errors.Safe(roachpb.Key(t.Key)), errors.Safe(t.Value))
					v.failures = append(v.failures, err)
				}
			}
		}
	case *SplitOperation:
		v.failIfError(op, t.Result)
	case *MergeOperation:
		v.failIfError(op, t.Result)
	case *BatchOperation:
		if !resultIsRetryable(t.Result) {
			v.failIfError(op, t.Result)
			for _, op := range t.Ops {
				v.processOp(txnID, op)
			}
		}
	case *ClosureTxnOperation:
		fakeTxnID := uuid.MakeV4().String()
		for _, op := range t.Ops {
			v.processOp(&fakeTxnID, op)
		}
		if t.Result.Type == ResultType_NoError {
			v.checkCommittedTxn(fakeTxnID)
		} else {
			v.checkUncommittedTxn(fakeTxnID)
		}
	default:
		panic(errors.AssertionFailedf(`unknown operation type: %T %v`, t, t))
	}
}

func (v *validator) checkCommittedTxn(txnID string) {
	txnKVs := v.kvsByTxn[txnID]
	delete(v.kvsByTxn, txnID)

	var ts hlc.Timestamp
	var failure string
	for _, kv := range txnKVs {
		if ts.IsEmpty() {
			ts = kv.Key.Timestamp
		}
		if len(kv.Value) == 0 {
			failure = `committed txn missing write`
		} else if !ts.Equal(kv.Key.Timestamp) {
			failure = `committed txn different timestamps`
		}
		if failure != `` {
			break
		}
	}

	if failure != `` {
		err := errors.Errorf("%s: %s", failure, errors.Safe(printKVs(txnKVs...)))
		v.failures = append(v.failures, err)
	}
}

func (v *validator) checkUncommittedTxn(txnID string) {
	txnKVs := v.kvsByTxn[txnID]
	delete(v.kvsByTxn, txnID)

	var failure string
	for _, kv := range txnKVs {
		if kv.Value == nil {
			continue
		}
		failure = `rolled back txn had writes`
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
		fmt.Fprintf(&buf, "%s:%s->%s", kv.Key.Key, kv.Key.Timestamp, mustGetStringValue(kv.Value))
	}
	return buf.String()
}
