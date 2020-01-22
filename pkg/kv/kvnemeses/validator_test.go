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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
)

var retryableError = roachpb.NewTransactionRetryWithProtoRefreshError(
	``, uuid.MakeV4(), roachpb.Transaction{})

func withResult(op Operation, err error) Operation {
	(*op.Result()) = resultError(context.Background(), err)
	return op
}

func TestValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	kv := func(key string, ts int, value string) engine.MVCCKeyValue {
		return engine.MVCCKeyValue{
			Key: engine.MVCCKey{
				Key:       []byte(key),
				Timestamp: hlc.Timestamp{WallTime: int64(ts)},
			},
			Value: roachpb.MakeValueFromString(value).RawBytes,
		}
	}
	kvs := func(kvs ...engine.MVCCKeyValue) []engine.MVCCKeyValue {
		return kvs
	}

	tests := []struct {
		name     string
		steps    []Step
		kvs      []engine.MVCCKeyValue
		expected []string
	}{
		{
			name:     "no ops and no kvs",
			steps:    nil,
			kvs:      nil,
			expected: nil,
		},
		{
			name:     "no ops with unexpected write",
			steps:    nil,
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: []string{`extra writes: "a":0.000000001,0->v1`},
		},
		{
			name:     "one put with expected write",
			steps:    []Step{step(withResult(put(`a`, `v1`), nil))},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name:     "one put with missing write",
			steps:    []Step{step(withResult(put(`a`, `v1`), nil))},
			kvs:      nil,
			expected: []string{`committed put missing write: "a":missing->v1`},
		},
		{
			name:     "one ambiguous put with successful write",
			steps:    []Step{step(withResult(put(`a`, `v1`), roachpb.NewAmbiguousResultError(``)))},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name:     "one ambiguous put with failed write",
			steps:    []Step{step(withResult(put(`a`, `v1`), roachpb.NewAmbiguousResultError(``)))},
			kvs:      nil,
			expected: nil,
		},
		{
			name:     "one retryable put with write (correctly) missing",
			steps:    []Step{step(withResult(put(`a`, `v1`), retryableError))},
			kvs:      nil,
			expected: nil,
		},
		{
			name:     "one retryable put with write (incorrectly) present",
			steps:    []Step{step(withResult(put(`a`, `v1`), retryableError))},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: []string{`uncommitted put had writes: "a":0.000000001,0->v1`},
		},
		{
			name:     "one batch put with successful write",
			steps:    []Step{step(withResult(batch(withResult(put(`a`, `v1`), nil)), nil))},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name:     "one batch put with missing write",
			steps:    []Step{step(withResult(batch(withResult(put(`a`, `v1`), nil)), nil))},
			kvs:      nil,
			expected: []string{`committed batch missing write: "a":missing->v1`},
		},
		{
			name: "one transactionally committed put with the correct writes",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name: "one transactionally committed with first write missing",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`b`, `v2`), nil),
				), nil)),
			},
			kvs:      kvs(kv(`b`, 1, `v2`)),
			expected: []string{`committed txn missing write: "a":missing->v1 "b":0.000000001,0->v2`},
		},
		{
			name: "one transactionally committed put with second write missing",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`b`, `v2`), nil),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: []string{`committed txn missing write: "a":0.000000001,0->v1 "b":missing->v2`},
		},
		{
			name: "one transactionally committed put with write timestamp disagreement",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`b`, `v2`), nil),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed txn different timestamps: "a":0.000000001,0->v1 "b":0.000000002,0->v2`,
			},
		},
		{
			name: "one transactionally rolled back put with write (correctly) missing",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Rollback,
					withResult(put(`a`, `v1`), nil),
				), errors.New(`rollback`))),
			},
			kvs:      nil,
			expected: nil,
		},
		{
			name: "one transactionally rolled back put with write (incorrectly) present",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Rollback,
					withResult(put(`a`, `v1`), nil),
				), errors.New(`rollback`))),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: []string{`uncommitted txn had writes: "a":0.000000001,0->v1`},
		},
		{
			name: "one transactionally rolled back batch with write (correctly) missing",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Rollback,
					withResult(batch(
						withResult(put(`a`, `v1`), nil),
					), nil),
				), errors.New(`rollback`))),
			},
			kvs:      nil,
			expected: nil,
		},
		{
			name: "two transactionally committed puts of the same key",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`a`, `v2`), nil),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v2`)),
			expected: nil,
		},
		{
			name: "two transactionally committed puts of the same key with extra write",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`a`, `v2`), nil),
				), nil)),
			},
			// HACK: These should be the same timestamp. See the TODO in
			// watcher.processEvents.
			kvs: kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`)),
			expected: []string{
				`committed txn overwritten key had write: "a":0.000000002,0->v2 "a":0.000000001,0->v1`,
			},
		},
		{
			name: "ambiguous transaction committed",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`b`, `v2`), nil),
				), roachpb.NewAmbiguousResultError(``))),
			},
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`b`, 1, `v2`)),
			expected: nil,
		},
		{
			name: "ambiguous transaction did not commit",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`b`, `v2`), nil),
				), roachpb.NewAmbiguousResultError(``))),
			},
			kvs:      nil,
			expected: nil,
		},
		{
			name: "ambiguous transaction committed but has validation error",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, `v1`), nil),
					withResult(put(`b`, `v2`), nil),
				), roachpb.NewAmbiguousResultError(``))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`ambiguous txn different timestamps: "a":0.000000001,0->v1 "b":0.000000002,0->v2`,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e := MakeEngine()
			for _, kv := range test.kvs {
				e.Put(kv.Key, kv.Value)
			}
			var actual []string
			if failures := Validate(test.steps, e); len(failures) > 0 {
				actual = make([]string, len(failures))
				for i := range failures {
					actual[i] = failures[i].Error()
				}
			}
			assert.Equal(t, test.expected, actual)
		})
	}
}
