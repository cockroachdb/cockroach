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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var retryableError = roachpb.NewTransactionRetryWithProtoRefreshError(
	``, uuid.MakeV4(), roachpb.Transaction{})

func withTimestamp(op Operation, ts int) Operation {
	op.Result().OptionalTimestamp = hlc.Timestamp{WallTime: int64(ts)}
	return op
}

func withResultTS(op Operation, ts int) Operation {
	return withTimestamp(withResultOK(op), ts)
}

func withResultOK(op Operation) Operation {
	return withResult(op)
}

func withResult(op Operation) Operation {
	return withResultErr(op, nil /* err */)
}

func withAmbResult(op Operation) Operation {
	err := roachpb.NewAmbiguousResultErrorf("boom")
	op = withResultErr(op, err)
	return op
}

func withResultErr(op Operation, err error) Operation {
	*op.Result() = resultInit(context.Background(), err)
	return op
}

func withReadResult(op Operation, value string) Operation {
	return withReadResultTS(op, value, 0)
}

func withReadResultTS(op Operation, value string, ts int) Operation {
	op = withResultTS(op, ts)
	get := op.GetValue().(*GetOperation)
	get.Result.Type = ResultType_Value
	if value != `` {
		get.Result.Value = roachpb.MakeValueFromString(value).RawBytes
	}
	return op
}

func withScanResultTS(op Operation, ts int, kvs ...KeyValue) Operation {
	op = withTimestamp(withResult(op), ts)
	scan := op.GetValue().(*ScanOperation)
	scan.Result.Type = ResultType_Values
	scan.Result.Values = kvs
	return op
}

func withDeleteRangeResult(op Operation, ts int, keys ...[]byte) Operation {
	op = withTimestamp(withResult(op), ts)
	delRange := op.GetValue().(*DeleteRangeOperation)
	delRange.Result.Type = ResultType_Keys
	delRange.Result.Keys = keys
	return op
}

func TestValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const (
		s1 = kvnemesisutil.Seq(1 + iota)
		s2
		s3
		s4
		s5
		s6
	)

	const (
		noTS = 0
		t1   = 1 + iota
		t2
		t3
		t4
		t5
	)

	vi := func(s kvnemesisutil.Seq) string {
		return PutOperation{Seq: s}.Value()
	}
	var (
		v1 = vi(s1)
		v2 = vi(s2)
		v3 = vi(s3)
	)

	type seqKV struct {
		key, endKey roachpb.Key
		val         []byte
		ts          hlc.Timestamp
		seq         kvnemesisutil.Seq
	}

	kv := func(key string, ts int, seq kvnemesisutil.Seq /* only for value, i.e. no semantics */) seqKV {
		return seqKV{
			key: roachpb.Key(key),
			ts:  hlc.Timestamp{WallTime: int64(ts)},
			val: roachpb.MakeValueFromString(PutOperation{Seq: seq}.Value()).RawBytes,
			seq: seq,
		}
	}
	tombstone := func(key string, ts int, seq kvnemesisutil.Seq) seqKV {
		r := kv(key, ts, seq)
		r.val = nil
		return r
	}
	kvs := func(kvs ...seqKV) []seqKV {
		return kvs
	}
	scanKV := func(key, value string) KeyValue {
		return KeyValue{
			Key:   []byte(key),
			Value: roachpb.MakeValueFromString(value).RawBytes,
		}
	}

	_ = scanKV

	tests := []struct {
		name  string
		steps []Step
		kvs   []seqKV
	}{
		{
			name:  "no ops and no kvs",
			steps: nil,
			kvs:   nil,
		},
		{
			name:  "no ops with unexpected write",
			steps: nil,
			kvs:   kvs(kv(`a`, t1, s1)),
		},
		{
			name:  "no ops with unexpected delete",
			steps: nil,
			kvs:   kvs(tombstone(`a`, t1, s1)),
		},
		{
			name:  "one put with expected write",
			steps: []Step{step(withResultTS(put(`a`, s1), t1))},
			kvs:   kvs(kv(`a`, t1, s1)),
		},
		{
			name:  "one delete with expected write",
			steps: []Step{step(withResultTS(del(`a`, s1), t1))},
			kvs:   kvs(tombstone(`a`, t1, s1)),
		},
		{
			name:  "one put with missing write",
			steps: []Step{step(withResultTS(put(`a`, s1), t1))},
			kvs:   nil,
		},
		{
			name:  "one delete with missing write",
			steps: []Step{step(withResultTS(del(`a`, s1), t1))},
			kvs:   nil,
		},
		{
			name:  "one ambiguous put with successful write",
			steps: []Step{step(withTimestamp(withAmbResult(put(`a`, s1)), t1))},
			kvs:   kvs(kv(`a`, t1, s1)),
		},
		{
			name:  "one ambiguous delete with successful write",
			steps: []Step{step(withAmbResult(del(`a`, s1)))},
			kvs:   kvs(tombstone(`a`, t1, s1)),
		},

		{
			name:  "one ambiguous put with failed write",
			steps: []Step{step(withAmbResult(put(`a`, s1)))},
			kvs:   nil,
		},
		{
			name:  "one ambiguous delete with failed write",
			steps: []Step{step(withAmbResult(del(`a`, s1)))},
			kvs:   nil,
		},
		{
			name: "one ambiguous delete with failed write before a later committed delete",
			steps: []Step{
				step(withAmbResult(del(`a`, s1))),
				step(withResultTS(del(`a`, s2), t2)),
			},
			kvs: kvs(tombstone(`a`, t2, s2)),
		},
		{
			name:  "one retryable put with write (correctly) missing",
			steps: []Step{step(withResultErr(put(`a`, s1), retryableError))},
			kvs:   nil,
		},
		{
			name:  "one retryable delete with write (correctly) missing",
			steps: []Step{step(withResultErr(del(`a`, s1), retryableError))},
			kvs:   nil,
		},
		{
			name:  "one retryable put with write (incorrectly) present",
			steps: []Step{step(withTimestamp(withResultErr(put(`a`, s1), retryableError), t1))},
			kvs:   kvs(kv(`a`, t1, s1)),
		},
		{
			name:  "one retryable delete with write (incorrectly) present",
			steps: []Step{step(withResultErr(del(`a`, s1), retryableError))},
			kvs:   kvs(tombstone(`a`, t1, s1)),
			// NB: Error messages are different because we can't match an uncommitted
			// delete op to a stored kv like above.

		},
		{
			name: "one delete with expected write after write transaction with shadowed delete",
			steps: []Step{
				step(withResultTS(del(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResultOK(put(`a`, s3)),
					withResultOK(del(`a`, s4)),
					withResultOK(put(`a`, s5)),
				), t3)),
				step(withResultTS(del(`a`, s6), t4)),
			},
			kvs: kvs(
				tombstone(`a`, t1, s1),
				kv(`a`, t2, s2),
				kv(`a`, t3, s5),
				tombstone(`a`, t4, s6)),
		},
		{
			name:  "one batch put with successful write",
			steps: []Step{step(withResultTS(batch(withResult(put(`a`, s1))), t1))},
			kvs:   kvs(kv(`a`, t1, s1)),
		},
		{
			name:  "one batch delete with successful write",
			steps: []Step{step(withResultTS(batch(del(`a`, s1)), t1))},
			kvs:   kvs(tombstone(`a`, t1, s1)),
		},
		{
			name:  "one batch put with missing write",
			steps: []Step{step(withResultTS(batch(withResult(put(`a`, s1))), t1))},
			kvs:   nil,
		},
		{
			name:  "one batch delete with missing write",
			steps: []Step{step(withResultTS(batch(withResult(del(`a`, s1))), t1))},
			kvs:   nil,
		},
		{
			name: "one transactionally committed put with the correct writes",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},

		{
			name: "one transactionally committed delete with the correct writes",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s1)),
		},
		{
			name: "one transactionally committed put with first write missing",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`b`, s2)),
				), t1)),
			},
			kvs: kvs(kv(`b`, t1, s2)),
		},
		{
			name: "one transactionally committed delete with first write missing",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(del(`b`, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(`b`, t1, s2)),
		},
		{
			name: "one transactionally committed put with second write missing",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`b`, s2)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one transactionally committed delete with second write missing",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(del(`b`, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s1)),
		},
		{
			name: "one transactionally committed put with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`b`, s2)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one transactionally committed delete with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(del(`b`, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s1), tombstone(`b`, t2, s2)),
			// NB: Error messages are different because we can't match an uncommitted
			// delete op to a stored kv like above.
		},
		{
			name: "one transactionally rolled back put with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxn(ClosureTxnType_Rollback,
					withResult(put(`a`, s1)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "one transactionally rolled back delete with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxn(ClosureTxnType_Rollback,
					withResult(del(`a`, s1)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "one transactionally rolled back put with write (incorrectly) present",
			steps: []Step{
				step(withResultErr(closureTxn(ClosureTxnType_Rollback,
					withResult(put(`a`, s1)),
				), errors.New(`rollback`))),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one transactionally rolled back delete with write (incorrectly) present",
			steps: []Step{
				step(withResultErr(closureTxn(ClosureTxnType_Rollback,
					withResult(del(`a`, s1)),
				), errors.New(`rollback`))),
			},
			kvs: kvs(tombstone(`a`, t1, s1)),
		},
		{
			name: "one transactionally rolled back batch put with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxn(ClosureTxnType_Rollback,
					withResult(batch(
						withResult(put(`a`, s1)),
					)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "one transactionally rolled back batch delete with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxn(ClosureTxnType_Rollback,
					withResult(batch(
						withResult(del(`a`, s1)),
					)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "two transactionally committed puts of the same key",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`a`, s2)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s2)),
		},
		{
			// NB: this can't happen in practice since KV would throw a WriteTooOldError.
			// But transactionally this works, see below.
			name: "batch with two deletes of same key",
			steps: []Step{
				step(withResultTS(batch(
					withResult(del(`a`, s1)),
					withResult(del(`a`, s2)),
				), t1)),
			},
		},
		{
			name: "two transactionally committed deletes of the same key",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(del(`a`, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s2)),
		},
		{
			name: "two transactionally committed writes (put, delete) of the same key",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(del(`a`, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s2)),
		},
		{
			name: "two transactionally committed writes (delete, put) of the same key",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(put(`a`, s2)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s2)),
		},
		{
			name: "two transactionally committed puts of the same key with extra write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`a`, s2)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2)),
		},
		{
			name: "two transactionally committed deletes of the same key with extra write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(del(`a`, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "two transactionally committed writes (put, delete) of the same key with extra write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResultOK(put(`a`, s1)),
					withResultOK(del(`a`, s2)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "ambiguous put-put transaction committed",
			steps: []Step{
				step(withAmbResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`b`, s2)),
				))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t1, s2)),
		},
		{
			name: "ambiguous put-del transaction committed",
			steps: []Step{
				step(withAmbResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(del(`b`, s2)),
				))),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`b`, t1, s2)),
		},
		{
			// NB: this case is a tough nut to crack if we rely on timestamps since we
			// don't have a single timestamp result here and no unique values. But we
			// use sequence numbers so no problem! We learn the commit timestamp from
			// them if any of the writes show up.
			name: "ambiguous del-del transaction committed",
			steps: []Step{
				step(withAmbResult(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(del(`a`, s2)),
				))),
			},
			kvs: kvs(tombstone(`a`, t1, s2)),
		},
		{
			name: "ambiguous del-del transaction committed but wrong seq",
			steps: []Step{
				step(withAmbResult(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s1)),
					withResult(del(`a`, s2)),
				))),
			},
			kvs: kvs(tombstone(`a`, t1, s1)),
		},
		{
			name: "ambiguous put-put transaction did not commit",
			steps: []Step{
				step(withAmbResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`b`, s2)),
				))),
			},
			kvs: nil,
		},
		{
			name: "ambiguous put-del transaction did not commit",
			steps: []Step{
				step(withAmbResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(del(`b`, s2)),
				))),
			},
			kvs: nil,
		},
		{
			name: "ambiguous put-put transaction committed but has validation error",
			steps: []Step{
				step(withAmbResult(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`b`, s2)),
				))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "ambiguous put-del transaction committed but has validation error",
			steps: []Step{
				step(withAmbResult(withTimestamp(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(del(`b`, s2)),
				), t2))),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`b`, t2, s2)),
		},
		{
			name: "one read before write",
			steps: []Step{
				step(withReadResultTS(get(`a`), ``, t1)),
				step(withResultTS(put(`a`, s1), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one read before delete",
			steps: []Step{
				step(withReadResultTS(get(`a`), ``, t1)),
				step(withResultTS(del(`a`, s1), t2)),
			},
			kvs: kvs(tombstone(`a`, t2, s1)),
		},
		{
			name: "one read before write and delete",
			steps: []Step{
				step(withReadResultTS(get(`a`), ``, t1)),
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(del(`a`, s2), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "one read before write returning wrong value",
			steps: []Step{
				step(withReadResultTS(get(`a`), v1, t1)),
				step(withResultTS(put(`a`, s1), t2)),
			},
			kvs: kvs(kv(`a`, t2, s1)),
		},
		{
			name: "one read after write",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withReadResultTS(get(`a`), v1, t2)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one read after write and delete",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(withTimestamp(del(`a`, s2), t2), t2)),
				step(withReadResultTS(get(`a`), v1, t1)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "one read after write and delete returning tombstone",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(del(`a`, s2), t2)),
				step(withReadResultTS(get(`a`), ``, t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "one read after write returning wrong value",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withReadResultTS(get(`a`), v2, t2)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one read in between writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withReadResultTS(get(`a`), v1, t2)),
				step(withResultTS(put(`a`, s2), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t3, s2)),
		},
		{
			name: "one read in between write and delete",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withReadResultTS(get(`a`), v1, t2)),
				step(withResultTS(del(`a`, s2), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t3, s2)),
		},
		{
			name: "batch of reads after writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(batch(
					withReadResult(get(`a`), v1),
					withReadResult(get(`b`), v2),
					withReadResult(get(`c`), ``),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "batch of reads after writes and deletes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(del(`a`, s3), t3)),
				step(withResultTS(del(`b`, s4), t4)),
				step(withResultTS(batch(
					withReadResult(get(`a`), v1),
					withReadResult(get(`b`), v2),
					withReadResult(get(`c`), ``),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s3), tombstone(`b`, t4, s4)),
		},
		{
			name: "batch of reads after writes and deletes returning tombstones",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(del(`a`, s3), t3)),
				step(withResultTS(del(`b`, s3), t4)),
				step(withResultTS(batch(
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), ``),
					withReadResult(get(`c`), ``),
				), t5)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s3), tombstone(`b`, t4, s4)),
		},
		{
			name: "batch of reads after writes returning wrong values",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(batch(
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), v1),
					withReadResult(get(`c`), v2),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "batch of reads after writes and deletes returning wrong values",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(del(`a`, s3), t3)),
				step(withResultTS(del(`b`, s4), t4)),
				step(withResultTS(batch(
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), v1),
					withReadResult(get(`c`), v2),
				), t5)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s3), tombstone(`b`, t4, s4)),
		},
		{
			name: "batch of reads after writes with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(batch(
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), v2),
					withReadResult(get(`c`), ``),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "batch of reads after writes and deletes with valid time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(del(`a`, s3), t3)),
				step(withResultTS(del(`b`, s4), t4)),
				step(withResultTS(batch(
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), v2),
					withReadResult(get(`c`), ``),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s3), tombstone(`b`, t4, s4)),
		},
		{
			name: "transactional reads with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t3)),
				step(withResultTS(put(`b`, s3), t2)),
				step(withResultTS(put(`b`, s4), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withReadResult(get(`b`), v3),
				), t3)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid from 2-3: overlap 2-3
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t3, s2), kv(`b`, t2, s3), kv(`b`, t3, s4)),
		},
		{
			name: "transactional reads after writes and deletes with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(del(`a`, s3), t3)),
				step(withResultTS(del(`b`, s4), t4)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), v2),
					withReadResult(get(`c`), ``),
				), t4)),
			},
			// Reading (a, <nil>) is valid from min-1 or 3-max, and (b, v2) is valid from 2-4: overlap 3-4
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s3), tombstone(`b`, t4, s4)),
		},
		{
			name: "transactional reads with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(put(`b`, s3), t2)),
				step(withResultTS(put(`b`, s4), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withReadResult(get(`b`), v3),
				), t3)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 2-3: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t2, s3), kv(`b`, t3, s4)),
		},
		{
			name: "transactional reads after writes and deletes with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResultOK(del(`a`, s3)),
					withResultOK(del(`b`, s4)),
				), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withReadResult(get(`b`), v2),
					withReadResult(get(`c`), ``),
				), t4)),
			},
			// Reading (a, <nil>) is valid from min-1 or 3-max, and (b, v2) is valid from 2-3: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s3), tombstone(`b`, t3, s4)),
		},
		{
			name: "transactional reads and deletes after write with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withResult(del(`a`, s2)),
					withReadResult(get(`a`), ``),
				), t2)),
				step(withResultTS(put(`a`, s3), t3)),
				step(withResultTS(del(`a`, s4), t4)),
			},
			// Reading (a, v1) is valid from 1-2, reading (a, <nil>) is valid from min-1, 2-3, or 4-max: overlap in txn view at 2
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2), kv(`a`, t3, s3), tombstone(`a`, t4, s4)),
		},
		{
			name: "transactional reads and deletes after write with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(del(`a`, s2)),
					withReadResult(get(`a`), ``),
				), t2)),
				step(withResultTS(put(`a`, s3), t3)),
				step(withResultTS(del(`a`, s4), t4)),
			},
			// First read of (a, <nil>) is valid from min-1 or 4-max, delete is valid at 2: no overlap
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2), kv(`a`, t3, s3), tombstone(`a`, t4, s4)),
		},
		{
			name: "transactional reads one missing with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(put(`b`, s3), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withReadResult(get(`b`), ``),
				), t1)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-2: overlap 1-2
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t2, s3)),
		},
		{
			name: "transactional reads one missing with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(put(`b`, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withReadResult(get(`b`), ``),
				), t1)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-1: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t1, s3)),
		},
		{
			name: "transactional read and write with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withResult(put(`b`, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid at 2: overlap @2
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t3, s2), kv(`b`, t2, s3)),
		},
		{
			name: "transactional read and write with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withResultOK(put(`b`, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid at 2: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t2, s3)),
		},
		{
			name: "transaction with read before and after write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, s1)),
					withReadResult(get(`a`), v1),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "transaction with read before and after delete",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withResult(del(`a`, s2)),
					withReadResult(get(`a`), ``),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "transaction with incorrect read before write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withResult(put(`a`, s1)),
					withReadResult(get(`a`), v1),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "transaction with incorrect read before delete",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(del(`a`, s2)),
					withReadResult(get(`a`), ``),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "transaction with incorrect read after write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, s1)),
					withReadResult(get(`a`), ``),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "transaction with incorrect read after delete",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), v1),
					withResultOK(del(`a`, s2)),
					withReadResult(get(`a`), v1),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "two transactionally committed puts of the same key with reads",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, s1)),
					withReadResult(get(`a`), v1),
					withResult(put(`a`, s2)),
					withReadResult(get(`a`), v2),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s2)),
		},
		{
			name: "two transactionally committed put/delete ops of the same key with reads",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, s1)),
					withReadResult(get(`a`), v1),
					withResult(del(`a`, s2)),
					withReadResult(get(`a`), ``),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s2)),
		},
		{
			name: "two transactionally committed put/delete ops of the same key with incorrect read",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withReadResult(get(`a`), ``),
					withResult(put(`a`, s1)),
					withReadResult(get(`a`), v1),
					withResult(del(`a`, s2)),
					withReadResult(get(`a`), v1),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s2)),
		},
		{
			name: "one transactional put with correct commit time",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one transactional put with incorrect commit time",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t2, s1)),
		},
		{
			name: "one transactional delete with write on another key after delete",
			steps: []Step{
				// NB: this Delete comes first in operation order, but the write is delayed.
				step(withResultTS(del(`a`, s1), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`b`, s2)),
					withResult(del(`a`, s3)),
				), t2)),
			},
			kvs: kvs(tombstone(`a`, t2, s3), tombstone(`a`, t3, s1), kv(`b`, t2, s2)),
		},
		{
			name: "two transactional deletes with out of order commit times",
			steps: []Step{
				step(withResultTS(del(`a`, s1), t2)),
				step(withResultTS(del(`b`, s2), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(del(`a`, s3)),
					withResult(del(`b`, s4)),
				), t1)),
			},
			kvs: kvs(tombstone(`a`, t1, s3), tombstone(`a`, t2, s1), tombstone(`b`, t1, s4), tombstone(`b`, t3, s2)),
		},
		{
			name: "one transactional scan followed by delete within time range",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withResult(del(`a`, s2)),
				), t2)),
				step(withResultTS(put(`b`, s3), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2), kv(`b`, t3, s3)),
		},
		{
			name: "one transactional scan followed by delete outside time range",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withResult(del(`a`, s2)),
				), t4)),
				step(withResultTS(put(`b`, s3), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t4, s2), kv(`b`, t3, s3)),
		},
		{
			name: "one scan before write",
			steps: []Step{
				step(withScanResultTS(scan(`a`, `c`), t1)),
				step(withResultTS(put(`a`, s1), t2)),
			},
			kvs: kvs(kv(`a`, t2, s1)),
		},
		{
			name: "one scan before write returning wrong value",
			steps: []Step{
				step(withScanResultTS(scan(`a`, `c`), t1, scanKV(`a`, v2))),
				step(withResultTS(put(`a`, s1), t2)),
			},
			kvs: kvs(kv(`a`, t2, s1)),
		},
		{
			name: "one scan after write",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withScanResultTS(scan(`a`, `c`), t2, scanKV(`a`, v1))),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one scan after write returning wrong value",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withScanResultTS(scan(`a`, `c`), t2, scanKV(`a`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one scan after writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(scan(`a`, `c`), t3, scanKV(`a`, v1), scanKV(`b`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one reverse scan after writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(reverseScan(`a`, `c`), t3, scanKV(`b`, v2), scanKV(`a`, v1))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one scan after writes and delete",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(del(`a`, s3), t3)),
				step(withResultTS(put(`a`, s4), t4)),
				step(withScanResultTS(scan(`a`, `c`), t5, scanKV(`b`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s3), kv(`a`, t4, s4)),
		},
		{
			name: "one scan after write returning extra key",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(scan(`a`, `c`), t3, scanKV(`a`, v1), scanKV(`a2`, v3), scanKV(`b`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one tranactional scan after write and delete returning extra key",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`b`, s2)),
					withResult(del(`a`, s3)),
				), t2)),
				step(withScanResultTS(scan(`a`, `c`), t3, scanKV(`a`, v1), scanKV(`b`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s3), kv(`b`, t2, s2)),
		},
		{
			name: "one reverse scan after write returning extra key",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(reverseScan(`a`, `c`), t3, scanKV(`b`, v2), scanKV(`a2`, v3), scanKV(`a`, v1))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one scan after write returning missing key",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(scan(`a`, `c`), t3, scanKV(`b`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one scan after writes and delete returning missing key",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`a`, s1)),
					withResult(put(`b`, s2)),
				), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`b`, v2)),
					withResult(del(`a`, s3)),
				), t2)),
				step(withResultTS(put(`a`, s4), t3)),
				step(withResultTS(del(`a`, s5), t4)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t1, s2), tombstone(`a`, t2, s3), kv(`a`, t3, s4), tombstone(`a`, t4, s5)),
		},
		{
			name: "one reverse scan after write returning missing key",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(reverseScan(`a`, `c`), t3, scanKV(`b`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one scan after writes returning results in wrong order",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(scan(`a`, `c`), t3, scanKV(`b`, v2), scanKV(`a`, v1))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one reverse scan after writes returning results in wrong order",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withScanResultTS(reverseScan(`a`, `c`), t3, scanKV(`a`, v1), scanKV(`b`, v2))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "one scan after writes returning results outside scan boundary",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(put(`c`, s3), t3)),
				step(withScanResultTS(scan(`a`, `c`), t4, scanKV(`a`, v1), scanKV(`b`, v2), scanKV(`c`, v3))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), kv(`c`, t3, s3)),
		},
		{
			name: "one reverse scan after writes returning results outside scan boundary",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(put(`c`, s3), t3)),
				step(withScanResultTS(reverseScan(`a`, `c`), t4, scanKV(`c`, v3), scanKV(`b`, v2), scanKV(`a`, v1))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), kv(`c`, t3, s3)),
		},
		{
			name: "one scan in between writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withScanResultTS(scan(`a`, `c`), t2, scanKV(`a`, v1))),
				step(withResultTS(put(`a`, s2), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t3, s2)),
		},
		{
			name: "batch of scans after writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(batch(
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1), scanKV(`b`, v2)),
					withScanResultTS(scan(`b`, `d`), noTS, scanKV(`b`, v2)),
					withScanResultTS(scan(`c`, `e`), noTS),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "batch of scans after writes returning wrong values",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(batch(
					withScanResultTS(scan(`a`, `c`), noTS),
					withScanResultTS(scan(`b`, `d`), noTS, scanKV(`b`, v1)),
					withScanResultTS(scan(`c`, `e`), noTS, scanKV(`c`, v2)),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "batch of scans after writes with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(batch(
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`b`, v1)),
					withScanResultTS(scan(`b`, `d`), noTS, scanKV(`b`, v1)),
					withScanResultTS(scan(`c`, `e`), noTS),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2)),
		},
		{
			name: "transactional scans with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t3)),
				step(withResultTS(put(`b`, s3), t2)),
				step(withResultTS(put(`b`, s4), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1), scanKV(`b`, v3)),
					withScanResultTS(scan(`b`, `d`), noTS, scanKV(`b`, v3)),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid from 2-3: overlap 2-3
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t3, s2), kv(`b`, t2, s3), kv(`b`, t3, s4)),
		},
		{
			name: "transactional scans after delete with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t3)),
				step(withResultTS(put(`b`, s3), t1)),
				step(withResultTS(del(`b`, s4), t2)),
				step(withResultTS(put(`b`, s5), t4)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withScanResultTS(scan(`b`, `d`), noTS),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and <nil> for `b` is valid <min>-1 and 2-4: overlap 2-3
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t3, s2), kv(`b`, t1, s3), tombstone(`b`, t2, s4), kv(`b`, t4, s5)),
		},
		{
			name: "transactional scans with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(put(`b`, s3), t2)),
				step(withResultTS(put(`b`, s4), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1), scanKV(`b`, v3)),
					withScanResultTS(scan(`b`, `d`), noTS, scanKV(`b`, v3)),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 2-3: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t2, s3), kv(`b`, t3, s4)),
		},
		{
			name: "transactional scans after delete with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(put(`b`, s3), t1)),
				step(withResultTS(del(`b`, s4), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withScanResultTS(scan(`b`, `d`), noTS),
				), t3)),
			},
			// Reading v1 is valid from 1-2 and <nil> for `b` is valid from <min>-1, 3-<max>: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t1, s3), tombstone(`b`, t3, s4)),
		},
		{
			name: "transactional scans one missing with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(put(`b`, s3), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withScanResultTS(scan(`b`, `d`), noTS),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-2: overlap 1-2
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t2, s3)),
		},
		{
			name: "transactional scans one missing with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(put(`b`, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withScanResultTS(scan(`b`, `d`), noTS),
				), t1)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-1: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t1, s3)),
		},
		{
			name: "transactional scan and write with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withResult(put(`b`, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid at 2: overlap @2
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t3, s2), kv(`b`, t2, s3)),
		},
		{
			name: "transactional scan and write with empty time overlap",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withResult(put(`b`, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid at 2: no overlap
			kvs: kvs(kv(`a`, t1, s1), kv(`a`, t2, s2), kv(`b`, t2, s3)),
		},
		{
			name: "transaction with scan before and after write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS),
					withResult(put(`a`, s1)),
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "transaction with incorrect scan before write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withResult(put(`a`, s1)),
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "transaction with incorrect scan after write",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS),
					withResult(put(`a`, s1)),
					withScanResultTS(scan(`a`, `c`), noTS),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "two transactionally committed puts of the same key with scans",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withScanResultTS(scan(`a`, `c`), noTS),
					withResult(put(`a`, s1)),
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v1)),
					withResult(put(`a`, s2)),
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v2)),
					withResult(put(`b`, s3)),
					withScanResultTS(scan(`a`, `c`), noTS, scanKV(`a`, v2), scanKV(`b`, v3)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s2), kv(`b`, t1, s3)),
		},
		{
			name: "one deleterange before write",
			steps: []Step{
				step(withDeleteRangeResult(delRange(`a`, `c`, s1), t1)),
				step(withResultTS(put(`a`, s2), t2)),
			},
			kvs: kvs(kv(`a`, t2, s2)),
		},
		{
			name: "one deleterange before write returning wrong value",
			steps: []Step{
				step(withDeleteRangeResult(delRange(`a`, `c`, s1), t1, roachpb.Key(`a`))),
				step(withResultTS(put(`a`, s2), t2)),
			},
			kvs: kvs(kv(`a`, t2, s2)),
		},
		{
			name: "one deleterange after write",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s2), noTS, roachpb.Key(`a`)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "one deleterange after write returning wrong value",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s2), t2),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "one deleterange after write missing write",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s2), t2, roachpb.Key(`a`)),
				), t1)),
			},
			kvs: kvs(kv(`a`, t1, s1)),
		},
		{
			name: "one deleterange after write extra deletion",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s2), t2, roachpb.Key(`a`), roachpb.Key(`b`)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2)),
		},
		{
			name: "one deleterange after write with spurious deletion",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s2), t2, roachpb.Key(`a`), roachpb.Key(`b`)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2), tombstone(`b`, t2, s2)),
		},
		{
			name: "one deleterange after writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(put(`c`, s3), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s4), noTS, roachpb.Key(`a`), roachpb.Key(`b`)),
				), t4)),
				step(withScanResultTS(scan(`a`, `d`), t4, scanKV(`c`, v3))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), kv(`c`, t3, s3), tombstone(`a`, t4, s4), tombstone(`b`, t4, s4)),
		},
		{
			name: "one deleterange after writes with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(put(`c`, s3), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s4), noTS, roachpb.Key(`a`), roachpb.Key(`b`), roachpb.Key(`c`)),
				), t4)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), kv(`c`, t3, s3), tombstone(`a`, t3, s4), tombstone(`b`, t4, s4), tombstone(`c`, t4, s4)),
		},
		{
			name: "one deleterange after writes with missing write",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(put(`c`, s3), t3)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s4), noTS, roachpb.Key(`a`), roachpb.Key(`b`)),
				), t4)),
				step(withScanResultTS(scan(`a`, `d`), t5, scanKV(`c`, v3))),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), kv(`c`, t3, s3), tombstone(`a`, t4, s4)),
		},
		{
			name: "one deleterange after writes and delete",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`b`, s2), t2)),
				step(withResultTS(del(`a`, s3), t4)),
				step(withResultTS(put(`a`, s4), t5)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s5), noTS, roachpb.Key(`a`), roachpb.Key(`b`)),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), kv(`b`, t2, s2), tombstone(`a`, t3, s5), tombstone(`b`, t3, s5), tombstone(`a`, t4, s3), kv(`b`, t5, s4)),
		},
		{
			name: "one transactional deleterange followed by put after writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s2), noTS, roachpb.Key(`a`)),
					withResult(put(`b`, s3)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2), kv(`b`, t2, s3)),
		},
		{
			name: "one transactional deleterange followed by put after writes with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s2), noTS, roachpb.Key(`a`)),
					withResult(put(`b`, s3)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s2), kv(`b`, t3, s3)),
		},
		{
			name: "one transactional put shadowed by deleterange after writes",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`b`, s2)),
					withDeleteRangeResult(delRange(`a`, `c`, s3), noTS, roachpb.Key(`a`), roachpb.Key(`b`)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s3), tombstone(`b`, t2, s3)),
		},
		{
			name: "one transactional put shadowed by deleterange after writes with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withResult(put(`b`, s2)),
					withDeleteRangeResult(delRange(`a`, `c`, s3), noTS, roachpb.Key(`a`), roachpb.Key(`b`)),
				), t2)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t2, s3), tombstone(`b`, t3, s3)),
		},
		{
			name: "one deleterange after writes returning keys outside span boundary",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`d`, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s3), noTS, roachpb.Key(`a`), roachpb.Key(`d`)),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t3, s3), kv(`d`, t2, s2)),
		},
		{
			name: "one deleterange after writes incorrectly deleting keys outside span boundary",
			steps: []Step{
				step(withResultTS(put(`a`, s1), t1)),
				step(withResultTS(put(`d`, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(`a`, `c`, s3), noTS, roachpb.Key(`a`), roachpb.Key(`d`)),
				), t3)),
			},
			kvs: kvs(kv(`a`, t1, s1), tombstone(`a`, t3, s3), kv(`d`, t2, s2), tombstone(`d`, t3, s3)),
		},
	}

	// TODO(tbg): try to find a way to iterate through the tests in a way that allows Goland to link
	// back to the failing case. I think it looks for the loop with `t.Run` in it...
	w := echotest.Walk(t, testutils.TestDataPath(t, t.Name()))
	defer w.Check(t)
	for _, test := range tests {
		t.Run(test.name, w.Do(t, test.name, func(t *testing.T, path string) {
			e, err := MakeEngine()
			require.NoError(t, err)
			defer e.Close()
			for _, kv := range test.kvs {
				e.Put(storage.MVCCKey{Key: kv.key, Timestamp: kv.ts}, kv.val)
			}

			tr := &SeqTracker{}
			for _, kv := range test.kvs {
				tr.Add(kv.key, kv.endKey, kv.ts, kv.seq)
			}

			var buf strings.Builder
			for _, step := range test.steps {
				fmt.Fprintln(&buf, strings.TrimSpace(step.String()))
			}
			for _, kv := range test.kvs {
				if len(kv.endKey) == 0 {
					fmt.Fprintln(&buf, kv.key, "@", kv.seq, mustGetStringValue(kv.val))
				} else {
					fmt.Fprintln(&buf, kv.key, "-", kv.endKey, "@", kv.seq, mustGetStringValue(kv.val))
				}
			}

			if failures := Validate(test.steps, e, tr); len(failures) > 0 {
				for i := range failures {
					fmt.Fprintln(&buf, failures[i])
				}
			}
			echotest.Require(t, buf.String(), path)
		}))
	}
}
