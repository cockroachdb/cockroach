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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var retryableError = roachpb.NewTransactionRetryWithProtoRefreshError(
	``, uuid.MakeV4(), roachpb.Transaction{})

func withResult(op Operation, err error) Operation {
	(*op.Result()) = resultError(context.Background(), err)
	return op
}

func withReadResult(op Operation, value string) Operation {
	get := op.GetValue().(*GetOperation)
	get.Result = Result{
		Type: ResultType_Value,
	}
	if value != `` {
		get.Result.Value = roachpb.MakeValueFromString(value).RawBytes
	}
	return op
}

func withScanResult(op Operation, kvs ...KeyValue) Operation {
	scan := op.GetValue().(*ScanOperation)
	scan.Result = Result{
		Type:   ResultType_Values,
		Values: kvs,
	}
	return op
}

func TestValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	kv := func(key string, ts int, value string) storage.MVCCKeyValue {
		return storage.MVCCKeyValue{
			Key: storage.MVCCKey{
				Key:       []byte(key),
				Timestamp: hlc.Timestamp{WallTime: int64(ts)},
			},
			Value: roachpb.MakeValueFromString(value).RawBytes,
		}
	}
	kvs := func(kvs ...storage.MVCCKeyValue) []storage.MVCCKeyValue {
		return kvs
	}
	scanKV := func(key, value string) KeyValue {
		return KeyValue{
			Key:   []byte(key),
			Value: roachpb.MakeValueFromString(value).RawBytes,
		}
	}

	tests := []struct {
		name     string
		steps    []Step
		kvs      []storage.MVCCKeyValue
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
			expected: []string{`extra writes: [w]"a":0.000000001,0->v1`},
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
			expected: []string{`committed put missing write: [w]"a":missing->v1`},
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
			expected: []string{`uncommitted put had writes: [w]"a":0.000000001,0->v1`},
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
			expected: []string{`committed batch missing write: [w]"a":missing->v1`},
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
			expected: []string{`committed txn missing write: [w]"a":missing->v1 [w]"b":0.000000001,0->v2`},
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
			expected: []string{`committed txn missing write: [w]"a":0.000000001,0->v1 [w]"b":missing->v2`},
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
				`committed txn non-atomic timestamps: [w]"a":0.000000001,0->v1 [w]"b":0.000000002,0->v2`,
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
			expected: []string{`uncommitted txn had writes: [w]"a":0.000000001,0->v1`},
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
				`committed txn overwritten key had write: [w]"a":0.000000001,0->v1 [w]"a":0.000000002,0->v2`,
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
				`ambiguous txn non-atomic timestamps: [w]"a":0.000000001,0->v1 [w]"b":0.000000002,0->v2`,
			},
		},
		{
			name: "one read before write",
			steps: []Step{
				step(withReadResult(get(`a`), ``)),
				step(withResult(put(`a`, `v1`), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name: "one read before write returning wrong value",
			steps: []Step{
				step(withReadResult(get(`a`), `v2`)),
				step(withResult(put(`a`, `v1`), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed get non-atomic timestamps: [r]"a":[0,0, 0,0)->v2`,
			},
		},
		{
			name: "one read after write",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withReadResult(get(`a`), `v1`)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name: "one read after write returning wrong value",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withReadResult(get(`a`), `v2`)),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed get non-atomic timestamps: [r]"a":[0,0, 0,0)->v2`,
			},
		},
		{
			name: "one read in between writes",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withReadResult(get(`a`), `v1`)),
				step(withResult(put(`a`, `v2`), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`)),
			expected: nil,
		},
		{
			name: "batch of reads after writes",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(batch(
					withReadResult(get(`a`), `v1`),
					withReadResult(get(`b`), `v2`),
					withReadResult(get(`c`), ``),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: nil,
		},
		{
			name: "batch of reads after writes returning wrong values",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(batch(
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), `v1`),
					withReadResult(get(`c`), `v2`),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed batch non-atomic timestamps: ` +
					`[r]"a":[<min>, 0.000000001,0)-><nil> [r]"b":[0,0, 0,0)->v1 [r]"c":[0,0, 0,0)->v2`,
			},
		},
		{
			name: "batch of reads after writes with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(batch(
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), `v2`),
					withReadResult(get(`c`), ``),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed batch non-atomic timestamps: ` +
					`[r]"a":[<min>, 0.000000001,0)-><nil> [r]"b":[0.000000002,0, <max>)->v2 [r]"c":[<min>, <max>)-><nil>`,
			},
		},
		{
			name: "transactional reads with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(put(`b`, `v4`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), `v1`),
					withReadResult(get(`b`), `v3`),
				), nil)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid from 2-3: overlap 2-3
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 3, `v2`), kv(`b`, 2, `v3`), kv(`b`, 3, `v4`)),
			expected: nil,
		},
		{
			name: "transactional reads with empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(put(`b`, `v4`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), `v1`),
					withReadResult(get(`b`), `v3`),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 2-3: no overlap
			kvs: kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 2, `v3`), kv(`b`, 3, `v4`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[r]"a":[0.000000001,0, 0.000000002,0)->v1 [r]"b":[0.000000002,0, 0.000000003,0)->v3`,
			},
		},
		{
			name: "transactional reads one missing with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), `v1`),
					withReadResult(get(`b`), ``),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-2: overlap 1-2
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 2, `v3`)),
			expected: nil,
		},
		{
			name: "transactional reads one missing with empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), `v1`),
					withReadResult(get(`b`), ``),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-1: no overlap
			kvs: kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 1, `v3`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[r]"a":[0.000000001,0, 0.000000002,0)->v1 [r]"b":[<min>, 0.000000001,0)-><nil>`,
			},
		},
		{
			name: "transactional read and write with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), `v1`),
					withResult(put(`b`, `v3`), nil),
				), nil)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid at 2: overlap @2
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 3, `v2`), kv(`b`, 2, `v3`)),
			expected: nil,
		},
		{
			name: "transactional read and write with empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), `v1`),
					withResult(put(`b`, `v3`), nil),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid at 2: no overlap
			kvs: kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 2, `v3`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[r]"a":[0.000000001,0, 0.000000002,0)->v1 [w]"b":0.000000002,0->v3`,
			},
		},
		{
			name: "transaction with read before and after write",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, `v1`), nil),
					withReadResult(get(`a`), `v1`),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name: "transaction with incorrect read before write",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), `v1`),
					withResult(put(`a`, `v1`), nil),
					withReadResult(get(`a`), `v1`),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[r]"a":[0,0, 0,0)->v1 [w]"a":0.000000001,0->v1 [r]"a":[0.000000001,0, <max>)->v1`,
			},
		},
		{
			name: "transaction with incorrect read after write",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, `v1`), nil),
					withReadResult(get(`a`), ``),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[r]"a":[<min>, <max>)-><nil> [w]"a":0.000000001,0->v1 [r]"a":[<min>, 0.000000001,0)-><nil>`,
			},
		},
		{
			name: "two transactionally committed puts of the same key with reads",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, `v1`), nil),
					withReadResult(get(`a`), `v1`),
					withResult(put(`a`, `v2`), nil),
					withReadResult(get(`a`), `v2`),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v2`)),
			expected: nil,
		},
		{
			name: "one scan before write",
			steps: []Step{
				step(withScanResult(scan(`a`, `c`))),
				step(withResult(put(`a`, `v1`), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name: "one scan before write returning wrong value",
			steps: []Step{
				step(withScanResult(scan(`a`, `c`), scanKV(`a`, `v2`))),
				step(withResult(put(`a`, `v1`), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed scan non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0,0, 0,0), gap:[<min>, <max>)}->["a":v2]`,
			},
		},
		{
			name: "one scan after write",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`))),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name: "one scan after write returning wrong value",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`a`, `v2`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed scan non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0,0, 0,0), gap:[<min>, <max>)}->["a":v2]`,
			},
		},
		{
			name: "one scan after writes",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`), scanKV(`b`, `v2`))),
			},
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: nil,
		},
		{
			name: "one reverse scan after writes",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(reverseScan(`a`, `c`), scanKV(`b`, `v2`), scanKV(`a`, `v1`))),
			},
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: nil,
		},
		{
			name: "one scan after write returning extra key",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`), scanKV(`a2`, `v3`), scanKV(`b`, `v2`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed scan non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0.000000001,0, <max>), 1:[0,0, 0,0), 2:[0.000000002,0, <max>), gap:[<min>, <max>)}->["a":v1, "a2":v3, "b":v2]`,
			},
		},
		{
			name: "one reverse scan after write returning extra key",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(reverseScan(`a`, `c`), scanKV(`b`, `v2`), scanKV(`a2`, `v3`), scanKV(`a`, `v1`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed reverse scan non-atomic timestamps: ` +
					`[rs]{a-c}:{0:[0.000000002,0, <max>), 1:[0,0, 0,0), 2:[0.000000001,0, <max>), gap:[<min>, <max>)}->["b":v2, "a2":v3, "a":v1]`,
			},
		},
		{
			name: "one scan after write returning missing key",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`b`, `v2`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed scan non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0.000000002,0, <max>), gap:[<min>, 0.000000001,0)}->["b":v2]`,
			},
		},
		{
			name: "one reverse scan after write returning missing key",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(reverseScan(`a`, `c`), scanKV(`b`, `v2`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed reverse scan non-atomic timestamps: ` +
					`[rs]{a-c}:{0:[0.000000002,0, <max>), gap:[<min>, 0.000000001,0)}->["b":v2]`,
			},
		},
		{
			name: "one scan after writes returning results in wrong order",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`b`, `v2`), scanKV(`a`, `v1`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`scan result not ordered correctly: ` +
					`[s]{a-c}:{0:[0.000000002,0, <max>), 1:[0.000000001,0, <max>), gap:[<min>, <max>)}->["b":v2, "a":v1]`,
			},
		},
		{
			name: "one reverse scan after writes returning results in wrong order",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withScanResult(reverseScan(`a`, `c`), scanKV(`a`, `v1`), scanKV(`b`, `v2`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`scan result not ordered correctly: ` +
					`[rs]{a-c}:{0:[0.000000001,0, <max>), 1:[0.000000002,0, <max>), gap:[<min>, <max>)}->["a":v1, "b":v2]`,
			},
		},
		{
			name: "one scan after writes returning results outside scan boundary",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(put(`c`, `v3`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`), scanKV(`b`, `v2`), scanKV(`c`, `v3`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`), kv(`c`, 3, `v3`)),
			expected: []string{
				`key "c" outside scan bounds: ` +
					`[s]{a-c}:{0:[0.000000001,0, <max>), 1:[0.000000002,0, <max>), 2:[0.000000003,0, <max>), gap:[<min>, <max>)}->["a":v1, "b":v2, "c":v3]`,
			},
		},
		{
			name: "one reverse scan after writes returning results outside scan boundary",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(put(`c`, `v3`), nil)),
				step(withScanResult(reverseScan(`a`, `c`), scanKV(`c`, `v3`), scanKV(`b`, `v2`), scanKV(`a`, `v1`))),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`), kv(`c`, 3, `v3`)),
			expected: []string{
				`key "c" outside scan bounds: ` +
					`[rs]{a-c}:{0:[0.000000003,0, <max>), 1:[0.000000002,0, <max>), 2:[0.000000001,0, <max>), gap:[<min>, <max>)}->["c":v3, "b":v2, "a":v1]`,
			},
		},
		{
			name: "one scan in between writes",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`))),
				step(withResult(put(`a`, `v2`), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`)),
			expected: nil,
		},
		{
			name: "batch of scans after writes",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(batch(
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`), scanKV(`b`, `v2`)),
					withScanResult(scan(`b`, `d`), scanKV(`b`, `v2`)),
					withScanResult(scan(`c`, `e`)),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: nil,
		},
		{
			name: "batch of scans after writes returning wrong values",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(batch(
					withScanResult(scan(`a`, `c`)),
					withScanResult(scan(`b`, `d`), scanKV(`b`, `v1`)),
					withScanResult(scan(`c`, `e`), scanKV(`c`, `v2`)),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed batch non-atomic timestamps: ` +
					`[s]{a-c}:{gap:[<min>, 0.000000001,0)}->[] ` +
					`[s]{b-d}:{0:[0,0, 0,0), gap:[<min>, <max>)}->["b":v1] ` +
					`[s]{c-e}:{0:[0,0, 0,0), gap:[<min>, <max>)}->["c":v2]`,
			},
		},
		{
			name: "batch of scans after writes with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`b`, `v2`), nil)),
				step(withResult(batch(
					withScanResult(scan(`a`, `c`), scanKV(`b`, `v1`)),
					withScanResult(scan(`b`, `d`), scanKV(`b`, `v1`)),
					withScanResult(scan(`c`, `e`)),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`), kv(`b`, 2, `v2`)),
			expected: []string{
				`committed batch non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0,0, 0,0), gap:[<min>, 0.000000001,0)}->["b":v1] ` +
					`[s]{b-d}:{0:[0,0, 0,0), gap:[<min>, <max>)}->["b":v1] ` +
					`[s]{c-e}:{gap:[<min>, <max>)}->[]`,
			},
		},
		{
			name: "transactional scans with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(put(`b`, `v4`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`), scanKV(`b`, `v3`)),
					withScanResult(scan(`b`, `d`), scanKV(`b`, `v3`)),
				), nil)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid from 2-3: overlap 2-3
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 3, `v2`), kv(`b`, 2, `v3`), kv(`b`, 3, `v4`)),
			expected: nil,
		},
		{
			name: "transactional scans with empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(put(`b`, `v4`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`), scanKV(`b`, `v3`)),
					withScanResult(scan(`b`, `d`), scanKV(`b`, `v3`)),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 2-3: no overlap
			kvs: kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 2, `v3`), kv(`b`, 3, `v4`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0.000000001,0, 0.000000002,0), 1:[0.000000002,0, 0.000000003,0), gap:[<min>, <max>)}->["a":v1, "b":v3] ` +
					`[s]{b-d}:{0:[0.000000002,0, 0.000000003,0), gap:[<min>, <max>)}->["b":v3]`,
			},
		},
		{
			name: "transactional scans one missing with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
					withScanResult(scan(`b`, `d`)),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-2: overlap 1-2
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 2, `v3`)),
			expected: nil,
		},
		{
			name: "transactional scans one missing with empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(put(`b`, `v3`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
					withScanResult(scan(`b`, `d`)),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-1: no overlap
			kvs: kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 1, `v3`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0.000000001,0, 0.000000002,0), gap:[<min>, 0.000000001,0)}->["a":v1] ` +
					`[s]{b-d}:{gap:[<min>, 0.000000001,0)}->[]`,
			},
		},
		{
			name: "transactional scan and write with non-empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
					withResult(put(`b`, `v3`), nil),
				), nil)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid at 2: overlap @2
			kvs:      kvs(kv(`a`, 1, `v1`), kv(`a`, 3, `v2`), kv(`b`, 2, `v3`)),
			expected: nil,
		},
		{
			name: "transactional scan and write with empty time overlap",
			steps: []Step{
				step(withResult(put(`a`, `v1`), nil)),
				step(withResult(put(`a`, `v2`), nil)),
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
					withResult(put(`b`, `v3`), nil),
				), nil)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid at 2: no overlap
			kvs: kvs(kv(`a`, 1, `v1`), kv(`a`, 2, `v2`), kv(`b`, 2, `v3`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0.000000001,0, 0.000000002,0), gap:[<min>, <max>)}->["a":v1] [w]"b":0.000000002,0->v3`,
			},
		},
		{
			name: "transaction with scan before and after write",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`)),
					withResult(put(`a`, `v1`), nil),
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v1`)),
			expected: nil,
		},
		{
			name: "transaction with incorrect scan before write",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
					withResult(put(`a`, `v1`), nil),
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[s]{a-c}:{0:[0,0, 0,0), gap:[<min>, <max>)}->["a":v1] ` +
					`[w]"a":0.000000001,0->v1 ` +
					`[s]{a-c}:{0:[0.000000001,0, <max>), gap:[<min>, <max>)}->["a":v1]`,
			},
		},
		{
			name: "transaction with incorrect scan after write",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`)),
					withResult(put(`a`, `v1`), nil),
					withScanResult(scan(`a`, `c`)),
				), nil)),
			},
			kvs: kvs(kv(`a`, 1, `v1`)),
			expected: []string{
				`committed txn non-atomic timestamps: ` +
					`[s]{a-c}:{gap:[<min>, <max>)}->[] [w]"a":0.000000001,0->v1 [s]{a-c}:{gap:[<min>, 0.000000001,0)}->[]`,
			},
		},
		{
			name: "two transactionally committed puts of the same key with scans",
			steps: []Step{
				step(withResult(closureTxn(ClosureTxnType_Commit,
					withScanResult(scan(`a`, `c`)),
					withResult(put(`a`, `v1`), nil),
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v1`)),
					withResult(put(`a`, `v2`), nil),
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v2`)),
					withResult(put(`b`, `v3`), nil),
					withScanResult(scan(`a`, `c`), scanKV(`a`, `v2`), scanKV(`b`, `v3`)),
				), nil)),
			},
			kvs:      kvs(kv(`a`, 1, `v2`), kv(`b`, 1, `v3`)),
			expected: nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			e, err := MakeEngine()
			require.NoError(t, err)
			defer e.Close()
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
