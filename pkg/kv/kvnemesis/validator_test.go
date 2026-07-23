// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	kvpb "github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

var retryableError = kvpb.NewTransactionRetryWithProtoRefreshError(
	``, uuid.MakeV4(), 0 /* prevTxnEpoch */, roachpb.Transaction{})

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
	err := kvpb.NewAmbiguousResultErrorf("boom")
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

func withScanResult(op Operation, kvs ...KeyValue) Operation {
	return withScanResultTS(op, 0, kvs...)
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

type seqKV struct {
	key, endKey roachpb.Key
	val         []byte // contains seq
	ts          hlc.Timestamp
}

func (kv *seqKV) seq() kvnemesisutil.Seq {
	mvccV, err := storage.DecodeMVCCValue(kv.val)
	if err != nil {
		panic(err)
	}
	return mvccV.KVNemesisSeq.Get()
}

func TestValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	if !buildutil.CrdbTestBuild {
		// `kvpb.RequestHeader` and `MVCCValueHeader` have a KVNemesisSeq field
		// that is zero-sized outside test builds. We could revisit that should
		// a need arise to run kvnemesis against production binaries.
		skip.IgnoreLint(t, "kvnemesis must be run with the crdb_test build tag")
	}

	const (
		s1 = kvnemesisutil.Seq(1 + iota)
		s2
		s3
		s4
		s5
		s6
	)

	const (
		noTS = iota
		t1
		t2
		t3
		t4
		t5
	)

	ctx := context.Background()

	vi := func(s kvnemesisutil.Seq) string {
		return PutOperation{Seq: s}.Value()
	}
	var (
		v1 = vi(s1)
		v2 = vi(s2)
		v3 = vi(s3)
	)

	valWithSeq := func(seq kvnemesisutil.Seq, v roachpb.Value) []byte {
		var vh enginepb.MVCCValueHeader
		vh.KVNemesisSeq.Set(seq)
		sl, err := storage.EncodeMVCCValue(storage.MVCCValue{
			MVCCValueHeader: vh,
			Value:           v,
		})
		if err != nil {
			panic(err)
		}
		return sl
	}
	kv := func(key string, ts int, seq kvnemesisutil.Seq) seqKV {
		return seqKV{
			key: roachpb.Key(key),
			ts:  hlc.Timestamp{WallTime: int64(ts)},
			val: valWithSeq(seq, roachpb.MakeValueFromString(PutOperation{Seq: seq}.Value())),
		}
	}
	tombstone := func(key string, ts int, seq kvnemesisutil.Seq) seqKV {
		r := kv(key, ts, seq)
		r.val = valWithSeq(seq, roachpb.Value{})
		return r
	}
	rd := func(key, endKey string, ts int, seq kvnemesisutil.Seq) seqKV {
		return seqKV{
			key:    roachpb.Key(key),
			endKey: roachpb.Key(endKey),
			ts:     hlc.Timestamp{WallTime: int64(ts)},
			val:    valWithSeq(seq, roachpb.Value{}),
		}
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

	type sstKV struct {
		key       string
		endKey    string
		tombstone bool
	}

	makeAddSSTable := func(seq kvnemesisutil.Seq, kvs []sstKV) Operation {
		f := &storage.MemObject{}
		st := cluster.MakeTestingClusterSettings()
		w := storage.MakeIngestionSSTWriter(ctx, st, f)
		defer w.Close()

		var span roachpb.Span
		var vh enginepb.MVCCValueHeader
		vh.KVNemesisSeq.Set(seq)
		ts := hlc.Timestamp{WallTime: 1}

		for _, kv := range kvs {
			key := roachpb.Key(kv.key)
			endKey := roachpb.Key(kv.endKey)
			value := storage.MVCCValue{MVCCValueHeader: vh}
			if !kv.tombstone {
				value.Value = roachpb.MakeValueFromString(sv(seq))
			}

			if len(endKey) == 0 {
				require.NoError(t, w.PutMVCC(storage.MVCCKey{Key: key, Timestamp: ts}, value))
			} else {
				require.NoError(t, w.PutMVCCRangeKey(
					storage.MVCCRangeKey{StartKey: key, EndKey: endKey, Timestamp: ts}, value))
			}

			if len(span.Key) == 0 || key.Compare(span.Key) < 0 {
				span.Key = key.Clone()
			}
			if len(endKey) > 0 {
				if endKey.Compare(span.EndKey) > 0 {
					span.EndKey = endKey.Clone()
				}
			} else if ek := roachpb.Key(tk(fk(kv.key) + 1)); ek.Compare(span.EndKey) > 0 {
				span.EndKey = ek.Clone()
			}
		}
		require.NoError(t, w.Finish())

		return addSSTable(f.Data(), span, ts, seq, false /* asWrites */)
	}

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
			kvs:   kvs(kv(k1, t1, s1)),
		},
		{
			name:  "no ops with unexpected delete",
			steps: nil,
			kvs:   kvs(tombstone(k1, t1, s1)),
		},
		{
			name:  "one put with expected write",
			steps: []Step{step(withResultTS(put(k1, s1), t1))},
			kvs:   kvs(kv(k1, t1, s1)),
		},
		{
			name:  "one delete with expected write",
			steps: []Step{step(withResultTS(del(k1, s1), t1))},
			kvs:   kvs(tombstone(k1, t1, s1)),
		},
		{
			name:  "one put with missing write",
			steps: []Step{step(withResultTS(put(k1, s1), t1))},
			kvs:   nil,
		},
		{
			name:  "one delete with missing write",
			steps: []Step{step(withResultTS(del(k1, s1), t1))},
			kvs:   nil,
		},
		{
			name:  "one ambiguous put with successful write",
			steps: []Step{step(withAmbResult(put(k1, s1)))},
			kvs:   kvs(kv(k1, t1, s1)),
		},
		{
			name:  "one ambiguous delete with successful write",
			steps: []Step{step(withAmbResult(del(k1, s1)))},
			kvs:   kvs(tombstone(k1, t1, s1)),
		},

		{
			name:  "one ambiguous put with failed write",
			steps: []Step{step(withAmbResult(put(k1, s1)))},
			kvs:   nil,
		},
		{
			name:  "one ambiguous delete with failed write",
			steps: []Step{step(withAmbResult(del(k1, s1)))},
			kvs:   nil,
		},
		{
			name: "one ambiguous delete with failed write before a later committed delete",
			steps: []Step{
				step(withAmbResult(del(k1, s1))),
				step(withResultTS(del(k1, s2), t2)),
			},
			kvs: kvs(tombstone(k1, t2, s2)),
		},
		{
			name:  "one retryable put with write (correctly) missing",
			steps: []Step{step(withResultErr(put(k1, s1), retryableError))},
			kvs:   nil,
		},
		{
			name:  "one retryable delete with write (correctly) missing",
			steps: []Step{step(withResultErr(del(k1, s1), retryableError))},
			kvs:   nil,
		},
		{
			name:  "one retryable put with write (incorrectly) present",
			steps: []Step{step(withTimestamp(withResultErr(put(k1, s1), retryableError), t1))},
			kvs:   kvs(kv(k1, t1, s1)),
		},
		{
			name:  "one retryable delete with write (incorrectly) present",
			steps: []Step{step(withResultErr(del(k1, s1), retryableError))},
			kvs:   kvs(tombstone(k1, t1, s1)),
			// NB: Error messages are different because we can't match an uncommitted
			// delete op to a stored kv like above.

		},
		{
			name: "one delete with expected write after write transaction with shadowed delete",
			steps: []Step{
				step(withResultTS(del(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResultOK(put(k1, s3)),
					withResultOK(del(k1, s4)),
					withResultOK(put(k1, s5)),
				), t3)),
				step(withResultTS(del(k1, s6), t4)),
			},
			kvs: kvs(
				tombstone(k1, t1, s1),
				kv(k1, t2, s2),
				kv(k1, t3, s5),
				tombstone(k1, t4, s6)),
		},
		{
			name:  "one batch put with successful write",
			steps: []Step{step(withResultTS(batch(withResult(put(k1, s1))), t1))},
			kvs:   kvs(kv(k1, t1, s1)),
		},
		{
			name:  "one batch delete with successful write",
			steps: []Step{step(withResultTS(batch(withResult(del(k1, s1))), t1))},
			kvs:   kvs(tombstone(k1, t1, s1)),
		},
		{
			name:  "one batch put with missing write",
			steps: []Step{step(withResultTS(batch(withResult(put(k1, s1))), t1))},
			kvs:   nil,
		},
		{
			name:  "one batch delete with missing write",
			steps: []Step{step(withResultTS(batch(withResult(del(k1, s1))), t1))},
			kvs:   nil,
		},
		{
			name: "one transactionally committed put with the correct writes",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},

		{
			name: "one transactionally committed delete with the correct writes",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s1)),
		},
		{
			name: "one transactionally committed put with first write missing",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k2, s2)),
				), t1)),
			},
			kvs: kvs(kv(k2, t1, s2)),
		},
		{
			name: "one transactionally committed delete with first write missing",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(del(k2, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(k2, t1, s2)),
		},
		{
			name: "one transactionally committed put with second write missing",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k2, s2)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one transactionally committed delete with second write missing",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(del(k2, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s1)),
		},
		{
			name: "one transactionally committed put with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k2, s2)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one transactionally committed delete with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(del(k2, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s1), tombstone(k2, t2, s2)),
			// NB: Error messages are different because we can't match an uncommitted
			// delete op to a stored kv like above.
		},
		{
			name: "one transactionally rolled back put with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxnSSI(ClosureTxnType_Rollback,
					withResult(put(k1, s1)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "one transactionally rolled back delete with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxnSSI(ClosureTxnType_Rollback,
					withResult(del(k1, s1)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "one transactionally rolled back put with write (incorrectly) present",
			steps: []Step{
				step(withResultErr(closureTxnSSI(ClosureTxnType_Rollback,
					withResult(put(k1, s1)),
				), errors.New(`rollback`))),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one transactionally rolled back delete with write (incorrectly) present",
			steps: []Step{
				step(withResultErr(closureTxnSSI(ClosureTxnType_Rollback,
					withResult(del(k1, s1)),
				), errors.New(`rollback`))),
			},
			kvs: kvs(tombstone(k1, t1, s1)),
		},
		{
			name: "one transactionally rolled back batch put with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxnSSI(ClosureTxnType_Rollback,
					withResult(batch(
						withResult(put(k1, s1)),
					)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "one transactionally rolled back batch delete with write (correctly) missing",
			steps: []Step{
				step(withResultErr(closureTxnSSI(ClosureTxnType_Rollback,
					withResult(batch(
						withResult(del(k1, s1)),
					)),
				), errors.New(`rollback`))),
			},
			kvs: nil,
		},
		{
			name: "two transactionally committed puts of the same key",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k1, s2)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s2)),
		},
		{
			// NB: this can't happen in practice since KV would throw a WriteTooOldError.
			// But transactionally this works, see below.
			name: "batch with two deletes of same key",
			steps: []Step{
				step(withResultTS(batch(
					withResult(del(k1, s1)),
					withResult(del(k1, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s2)),
		},
		{
			name: "two transactionally committed deletes of the same key",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(del(k1, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s2)),
		},
		{
			name: "two transactionally committed writes (put, delete) of the same key",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(del(k1, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s2)),
		},
		{
			name: "two transactionally committed writes (delete, put) of the same key",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(put(k1, s2)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s2)),
		},
		{
			name: "two transactionally committed puts of the same key with extra write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k1, s2)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2)),
		},
		{
			name: "two transactionally committed deletes of the same key with extra write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(del(k1, s2)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "two transactionally committed writes (put, delete) of the same key with extra write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResultOK(put(k1, s1)),
					withResultOK(del(k1, s2)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "ambiguous put-put transaction committed",
			steps: []Step{
				step(withAmbResult(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k2, s2)),
				))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t1, s2)),
		},
		{
			name: "ambiguous put-del transaction committed",
			steps: []Step{
				step(withAmbResult(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(del(k2, s2)),
				))),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k2, t1, s2)),
		},
		{
			// NB: this case is a tough nut to crack if we rely on timestamps since we
			// don't have a single timestamp result here and no unique values. But we
			// use sequence numbers so no problem! We learn the commit timestamp from
			// them if any of the writes show up.
			name: "ambiguous del-del transaction committed",
			steps: []Step{
				step(withAmbResult(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(del(k1, s2)),
				))),
			},
			kvs: kvs(tombstone(k1, t1, s2)),
		},
		{
			name: "ambiguous del-del transaction committed but wrong seq",
			steps: []Step{
				step(withAmbResult(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s1)),
					withResult(del(k1, s2)),
				))),
			},
			kvs: kvs(tombstone(k1, t1, s1)),
		},
		{
			name: "ambiguous put-put transaction did not commit",
			steps: []Step{
				step(withAmbResult(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k2, s2)),
				))),
			},
			kvs: nil,
		},
		{
			name: "ambiguous put-del transaction did not commit",
			steps: []Step{
				step(withAmbResult(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(del(k2, s2)),
				))),
			},
			kvs: nil,
		},
		{
			name: "ambiguous put-put transaction committed but has validation error",
			steps: []Step{
				step(withAmbResult(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k2, s2)),
				))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "ambiguous put-del transaction committed but has validation error",
			steps: []Step{
				step(withAmbResult(withTimestamp(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(del(k2, s2)),
				), t2))),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k2, t2, s2)),
		},
		{
			name: "one read before write",
			steps: []Step{
				step(withReadResultTS(get(k1), ``, t1)),
				step(withResultTS(put(k1, s1), t2)),
			},
			kvs: kvs(kv(k1, t2, s1)),
		},
		{
			name: "one for update read before write",
			steps: []Step{
				step(withReadResultTS(getForUpdate(k1), ``, t1)),
				step(withResultTS(put(k1, s1), t2)),
			},
			kvs: kvs(kv(k1, t2, s1)),
		},
		{
			name: "one for share read before write",
			steps: []Step{
				step(withReadResultTS(getForShare(k1), ``, t1)),
				step(withResultTS(put(k2, s1), t2)),
			},
			kvs: kvs(kv(k2, t2, s1)),
		},
		{
			name: "one read before delete",
			steps: []Step{
				step(withReadResultTS(get(k1), ``, t1)),
				step(withResultTS(del(k1, s1), t2)),
			},
			kvs: kvs(tombstone(k1, t2, s1)),
		},
		{
			name: "one read before write and delete",
			steps: []Step{
				step(withReadResultTS(get(k1), ``, t1)),
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(del(k1, s2), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "one for update read before write and delete",
			steps: []Step{
				step(withReadResultTS(getForUpdate(k1), ``, t1)),
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(del(k1, s2), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "one read before write returning wrong value",
			steps: []Step{
				step(withReadResultTS(get(k1), v1, t1)),
				step(withResultTS(put(k1, s1), t2)),
			},
			kvs: kvs(kv(k1, t2, s1)),
		},
		{
			name: "one read after write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(get(k1), v1, t2)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one for update read after write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(getForUpdate(k1), v1, t2)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one for share read after write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(getForShare(k1), v1, t2)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one read after write and delete",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(withTimestamp(del(k1, s2), t2), t2)),
				step(withReadResultTS(get(k1), v1, t1)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "one read after write and delete returning tombstone",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(del(k1, s2), t2)),
				step(withReadResultTS(get(k1), ``, t3)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "one read after write returning wrong value",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(get(k1), v2, t2)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one read in between writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(get(k1), v1, t2)),
				step(withResultTS(put(k1, s2), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s2)),
		},
		{
			name: "one read in between write and delete",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(get(k1), v1, t2)),
				step(withResultTS(del(k1, s2), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t3, s2)),
		},
		{
			name: "batch of reads after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(batch(
					withReadResult(get(k1), v1),
					withReadResult(getForUpdate(k2), v2),
					withReadResult(getForShare(k3), ``),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "batch of reads after writes and deletes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(del(k1, s3), t3)),
				step(withResultTS(del(k2, s4), t4)),
				step(withResultTS(batch(
					withReadResult(get(k1), v1),
					withReadResult(get(k2), v2),
					withReadResult(get(k3), ``),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s3), tombstone(k2, t4, s4)),
		},
		{
			name: "batch of reads after writes and deletes returning tombstones",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(del(k1, s3), t3)),
				step(withResultTS(del(k2, s3), t4)),
				step(withResultTS(batch(
					withReadResult(get(k1), ``),
					withReadResult(get(k2), ``),
					withReadResult(get(k3), ``),
				), t5)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s3), tombstone(k2, t4, s4)),
		},
		{
			name: "batch of reads after writes returning wrong values",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(batch(
					withReadResult(get(k1), ``),
					withReadResult(get(k2), v1),
					withReadResult(get(k3), v2),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "batch of reads after writes and deletes returning wrong values",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(del(k1, s3), t3)),
				step(withResultTS(del(k2, s4), t4)),
				step(withResultTS(batch(
					withReadResult(get(k1), ``),
					withReadResult(get(k2), v1),
					withReadResult(get(k3), v2),
				), t5)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s3), tombstone(k2, t4, s4)),
		},
		{
			name: "batch of reads after writes with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(batch(
					withReadResult(get(k1), ``),
					withReadResult(get(k2), v2),
					withReadResult(get(k3), ``),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "batch of reads after writes and deletes with valid time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(del(k1, s3), t3)),
				step(withResultTS(del(k2, s4), t4)),
				step(withResultTS(batch(
					withReadResult(get(k1), ``),
					withReadResult(get(k2), v2),
					withReadResult(get(k3), ``),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s3), tombstone(k2, t4, s4)),
		},
		{
			name: "transactional reads with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t3)),
				step(withResultTS(put(k2, s3), t2)),
				step(withResultTS(put(k2, s4), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withReadResult(get(k2), v3),
				), t3)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid from 2-3: overlap 2-3
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s2), kv(k2, t2, s3), kv(k2, t3, s4)),
		},
		{
			name: "transactional reads after writes and deletes with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(del(k1, s3), t3)),
				step(withResultTS(del(k2, s4), t4)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withReadResult(get(k2), v2),
					withReadResult(get(k3), ``),
				), t4)),
			},
			// Reading (a, <nil>) is valid from min-1 or 3-max, and (b, v2) is valid from 2-4: overlap 3-4
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s3), tombstone(k2, t4, s4)),
		},
		{
			name: "transactional reads with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t2)),
				step(withResultTS(put(k2, s4), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withReadResult(get(k2), v3),
				), t3)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 2-3: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t2, s3), kv(k2, t3, s4)),
		},
		{
			name: "transactional reads after writes and deletes with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResultOK(del(k1, s3)),
					withResultOK(del(k2, s4)),
				), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withReadResult(get(k2), v2),
					withReadResult(get(k3), ``),
				), t4)),
			},
			// Reading (a, <nil>) is valid from min-1 or 3-max, and (b, v2) is valid from 2-3: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s3), tombstone(k2, t3, s4)),
		},
		{
			name: "transactional reads and deletes after write with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withResult(del(k1, s2)),
					withReadResult(get(k1), ``),
				), t2)),
				step(withResultTS(put(k1, s3), t3)),
				step(withResultTS(del(k1, s4), t4)),
			},
			// Reading (a, v1) is valid from 1-2, reading (a, <nil>) is valid from min-1, 2-3, or 4-max: overlap in txn view at 2
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2), kv(k1, t3, s3), tombstone(k1, t4, s4)),
		},
		{
			name: "transactional reads and deletes after write with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withResult(del(k1, s2)),
					withReadResult(get(k1), ``),
				), t2)),
				step(withResultTS(put(k1, s3), t3)),
				step(withResultTS(del(k1, s4), t4)),
			},
			// First read of (a, <nil>) is valid from min-1 or 4-max, delete is valid at 2: no overlap
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2), kv(k1, t3, s3), tombstone(k1, t4, s4)),
		},
		{
			name: "transactional reads one missing with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withReadResult(get(k2), ``),
				), t1)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-2: overlap 1-2
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t2, s3)),
		},
		{
			name: "transactional reads one missing with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withReadResult(get(k2), ``),
				), t1)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-1: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3)),
		},
		{
			name: "transactional read and write with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withResult(put(k2, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid at 2: overlap @2
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s2), kv(k2, t2, s3)),
		},
		{
			name: "transactional read and write with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withResultOK(put(k2, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid at 2: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t2, s3)),
		},
		{
			name: "transaction with read before and after write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withResult(put(k1, s1)),
					withReadResult(get(k1), v1),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "transaction with read before and after delete",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withResult(del(k1, s2)),
					withReadResult(get(k1), ``),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "transaction with incorrect read before write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withResult(put(k1, s1)),
					withReadResult(get(k1), v1),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "transaction with incorrect read before delete",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withResult(del(k1, s2)),
					withReadResult(get(k1), ``),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "transaction with incorrect read after write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withResult(put(k1, s1)),
					withReadResult(get(k1), ``),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "transaction with incorrect read after delete",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), v1),
					withResultOK(del(k1, s2)),
					withReadResult(get(k1), v1),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "two transactionally committed puts of the same key with reads",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withResult(put(k1, s1)),
					withReadResult(get(k1), v1),
					withResult(put(k1, s2)),
					withReadResult(get(k1), v2),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s2)),
		},
		{
			name: "two transactionally committed put/delete ops of the same key with reads",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withResult(put(k1, s1)),
					withReadResult(get(k1), v1),
					withResult(del(k1, s2)),
					withReadResult(get(k1), ``),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s2)),
		},
		{
			name: "two transactionally committed put/delete ops of the same key with incorrect read",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withReadResult(get(k1), ``),
					withResult(put(k1, s1)),
					withReadResult(get(k1), v1),
					withResult(del(k1, s2)),
					withReadResult(get(k1), v1),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s2)),
		},
		{
			name: "one transactional put with correct commit time",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one transactional put with incorrect commit time",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
				), t1)),
			},
			kvs: kvs(kv(k1, t2, s1)),
		},
		{
			name: "one transactional delete with write on another key after delete",
			steps: []Step{
				// NB: this Delete comes first in operation order, but the write is delayed.
				step(withResultTS(del(k1, s1), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k2, s2)),
					withResult(del(k1, s3)),
				), t2)),
			},
			kvs: kvs(tombstone(k1, t2, s3), tombstone(k1, t3, s1), kv(k2, t2, s2)),
		},
		{
			name: "two transactional deletes with out of order commit times",
			steps: []Step{
				step(withResultTS(del(k1, s1), t2)),
				step(withResultTS(del(k2, s2), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(del(k1, s3)),
					withResult(del(k2, s4)),
				), t1)),
			},
			kvs: kvs(tombstone(k1, t1, s3), tombstone(k1, t2, s1), tombstone(k2, t1, s4), tombstone(k2, t3, s2)),
		},
		{
			name: "one transactional scan followed by delete within time range",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withResult(del(k1, s2)),
				), t2)),
				step(withResultTS(put(k2, s3), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2), kv(k2, t3, s3)),
		},
		{
			name: "one transactional scan followed by delete outside time range",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withResult(del(k1, s2)),
				), t4)),
				step(withResultTS(put(k2, s3), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t4, s2), kv(k2, t3, s3)),
		},
		{
			name: "one scan before write",
			steps: []Step{
				step(withScanResultTS(scan(k1, k3), t1)),
				step(withResultTS(put(k1, s1), t2)),
			},
			kvs: kvs(kv(k1, t2, s1)),
		},
		{
			name: "one scan before write returning wrong value",
			steps: []Step{
				step(withScanResultTS(scan(k1, k3), t1, scanKV(k1, v2))),
				step(withResultTS(put(k1, s1), t2)),
			},
			kvs: kvs(kv(k1, t2, s1)),
		},
		{
			name: "one scan after write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withScanResultTS(scan(k1, k3), t2, scanKV(k1, v1))),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one scan after write returning wrong value",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withScanResultTS(scan(k1, k3), t2, scanKV(k1, v2))),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one scan after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(scan(k1, k3), t3, scanKV(k1, v1), scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one reverse scan after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(reverseScan(k1, k3), t3, scanKV(k2, v2), scanKV(k1, v1))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one scan after writes and delete",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(del(k1, s3), t3)),
				step(withResultTS(put(k1, s4), t4)),
				step(withScanResultTS(scan(k1, k3), t5, scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s3), kv(k1, t4, s4)),
		},
		{
			name: "one scan after write returning extra key",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k3, s2), t2)),
				step(withScanResultTS(scan(k1, k4), t3, scanKV(k1, v1), scanKV(k2, v3), scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k3, t2, s2)),
		},
		{
			name: "one tranactional scan after write and delete returning extra key",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k2, s2)),
					withResult(del(k1, s3)),
				), t2)),
				step(withScanResultTS(scan(k1, k3), t3, scanKV(k1, v1), scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s3), kv(k2, t2, s2)),
		},
		{
			name: "one reverse scan after write returning extra key",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k3, s2), t2)),
				step(withScanResultTS(reverseScan(k1, k4), t3,
					scanKV(k3, v2),
					scanKV(k2, v3),
					scanKV(k1, v1),
				)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k3, t2, s2)),
		},
		{
			name: "one scan after write returning missing key",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(scan(k1, k3), t3, scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one scan after writes and delete returning missing key",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(put(k2, s2)),
				), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k2, v2)),
					withResult(del(k1, s3)),
				), t2)),
				step(withResultTS(put(k1, s4), t3)),
				step(withResultTS(del(k1, s5), t4)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t1, s2), tombstone(k1, t2, s3), kv(k1, t3, s4), tombstone(k1, t4, s5)),
		},
		{
			name: "one reverse scan after write returning missing key",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(reverseScan(k1, k3), t3, scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one scan after writes returning results in wrong order",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(scan(k1, k3), t3, scanKV(k2, v2), scanKV(k1, v1))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one reverse scan after writes returning results in wrong order",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(reverseScan(k1, k3), t3, scanKV(k1, v1), scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one scan after writes returning results outside scan boundary",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(put(k3, s3), t3)),
				step(withScanResultTS(scan(k1, k3), t4, scanKV(k1, v1), scanKV(k2, v2), scanKV(k3, v3))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), kv(k3, t3, s3)),
		},
		{
			name: "one reverse scan after writes returning results outside scan boundary",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(put(k3, s3), t3)),
				step(withScanResultTS(reverseScan(k1, k3), t4, scanKV(k3, v3), scanKV(k2, v2), scanKV(k1, v1))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), kv(k3, t3, s3)),
		},
		{
			name: "one scan in between writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withScanResultTS(scan(k1, k3), t2, scanKV(k1, v1))),
				step(withResultTS(put(k1, s2), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s2)),
		},
		{
			name: "batch of scans after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(batch(
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1), scanKV(k2, v2)),
					withScanResultTS(scan(k2, k4), noTS, scanKV(k2, v2)),
					withScanResultTS(scan(k3, k5), noTS),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "batch of scans after writes returning wrong values",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(batch(
					withScanResultTS(scan(k1, k3), noTS),
					withScanResultTS(scan(k2, k4), noTS, scanKV(k2, v1)),
					withScanResultTS(scan(k3, k5), noTS, scanKV(k3, v2)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "batch of scans after writes with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(batch(
					withScanResultTS(scan(k1, k3), noTS, scanKV(k2, v1)),
					withScanResultTS(scan(k2, k4), noTS, scanKV(k2, v1)),
					withScanResultTS(scan(k3, k5), noTS),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "transactional scans with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t3)),
				step(withResultTS(put(k2, s3), t2)),
				step(withResultTS(put(k2, s4), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1), scanKV(k2, v3)),
					withScanResultTS(scan(k2, k4), noTS, scanKV(k2, v3)),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid from 2-3: overlap 2-3
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s2), kv(k2, t2, s3), kv(k2, t3, s4)),
		},
		{
			name: "transactional scans after delete with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t3)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(del(k2, s4), t2)),
				step(withResultTS(put(k2, s5), t4)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withScanResultTS(scan(k2, k4), noTS),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and <nil> for k2 is valid <min>-1 and 2-4: overlap 2-3
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s2), kv(k2, t1, s3), tombstone(k2, t2, s4), kv(k2, t4, s5)),
		},
		{
			name: "transactional scans with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t2)),
				step(withResultTS(put(k2, s4), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1), scanKV(k2, v3)),
					withScanResultTS(scan(k2, k4), noTS, scanKV(k2, v3)),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 2-3: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t2, s3), kv(k2, t3, s4)),
		},
		{
			name: "transactional scans after delete with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(del(k2, s4), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withScanResultTS(scan(k2, k4), noTS),
				), t3)),
			},
			// Reading v1 is valid from 1-2 and <nil> for k2 is valid from <min>-1, 3-<max>: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3), tombstone(k2, t3, s4)),
		},
		{
			name: "transactional scans one missing with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withScanResultTS(scan(k2, k4), noTS),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-2: overlap 1-2
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t2, s3)),
		},
		{
			name: "transactional scans one missing with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withScanResultTS(scan(k2, k4), noTS),
				), t1)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid from 0-1: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3)),
		},
		{
			name: "transactional scan and write with non-empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withResult(put(k2, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-3 and v3 is valid at 2: overlap @2
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s2), kv(k2, t2, s3)),
		},
		{
			name: "transactional scan and write with empty time overlap",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withResult(put(k2, s3)),
				), t2)),
			},
			// Reading v1 is valid from 1-2 and v3 is valid at 2: no overlap
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t2, s3)),
		},
		{
			name: "transaction with scan before and after write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS),
					withResult(put(k1, s1)),
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "transaction with incorrect scan before write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withResult(put(k1, s1)),
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "transaction with incorrect scan after write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS),
					withResult(put(k1, s1)),
					withScanResultTS(scan(k1, k3), noTS),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "two transactionally committed puts of the same key with scans",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withScanResultTS(scan(k1, k3), noTS),
					withResult(put(k1, s1)),
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v1)),
					withResult(put(k1, s2)),
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v2)),
					withResult(put(k2, s3)),
					withScanResultTS(scan(k1, k3), noTS, scanKV(k1, v2), scanKV(k2, v3)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s2), kv(k2, t1, s3)),
		},
		{
			name: "one deleterange before write",
			steps: []Step{
				step(withDeleteRangeResult(delRange(k1, k3, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
			},
			kvs: kvs(kv(k1, t2, s2)),
		},
		{
			name: "one deleterange before write returning wrong value",
			steps: []Step{
				step(withDeleteRangeResult(delRange(k1, k3, s1), t1, roachpb.Key(k1))),
				step(withResultTS(put(k1, s2), t2)),
			},
			kvs: kvs(kv(k1, t2, s2)),
		},
		{
			name: "one deleterange after write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s2), noTS, roachpb.Key(k1)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "one deleterange after write returning wrong value",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s2), t2),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "one deleterange after write missing write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s2), t2, roachpb.Key(k1)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one deleterange after write extra deletion",
			steps: []Step{
				step(withResultTS(put(k1, s1), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s2), t2, roachpb.Key(k1), roachpb.Key(k2)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2)),
		},
		{
			name: "one deleterange after write with spurious deletion",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s2), t2, roachpb.Key(k1), roachpb.Key(k2)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2), tombstone(k2, t2, s2)),
		},
		{
			name: "one deleterange after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(put(k3, s3), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s4), noTS, roachpb.Key(k1), roachpb.Key(k2)),
				), t4)),
				step(withScanResultTS(scan(k1, k4), t4, scanKV(k3, v3))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), kv(k3, t3, s3), tombstone(k1, t4, s4), tombstone(k2, t4, s4)),
		},
		{
			name: "one deleterange after writes with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(put(k3, s3), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s4), noTS, roachpb.Key(k1), roachpb.Key(k2), roachpb.Key(k3)),
				), t4)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), kv(k3, t3, s3), tombstone(k1, t3, s4), tombstone(k2, t4, s4), tombstone(k3, t4, s4)),
		},
		{
			name: "one deleterange after writes with missing write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(put(k3, s3), t3)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s4), noTS, roachpb.Key(k1), roachpb.Key(k2)),
				), t4)),
				step(withScanResultTS(scan(k1, k4), t5, scanKV(k3, v3))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), kv(k3, t3, s3), tombstone(k1, t4, s4)),
		},
		{
			name: "one deleterange after writes and delete",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(del(k1, s3), t4)),
				step(withResultTS(put(k1, s4), t5)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s5), noTS, roachpb.Key(k1), roachpb.Key(k2)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2), tombstone(k1, t3, s5), tombstone(k2, t3, s5), tombstone(k1, t4, s3), kv(k2, t5, s4)),
		},
		{
			name: "one transactional deleterange followed by put after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s2), noTS, roachpb.Key(k1)),
					withResult(put(k2, s3)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2), kv(k2, t2, s3)),
		},
		{
			name: "one transactional deleterange followed by put after writes with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s2), noTS, roachpb.Key(k1)),
					withResult(put(k2, s3)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s2), kv(k2, t3, s3)),
		},
		{
			name: "one transactional put shadowed by deleterange after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k2, s2)),
					withDeleteRangeResult(delRange(k1, k3, s3), noTS, roachpb.Key(k1), roachpb.Key(k2)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s3), tombstone(k2, t2, s3)),
		},
		{
			name: "one transactional put shadowed by deleterange after writes with write timestamp disagreement",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k2, s2)),
					withDeleteRangeResult(delRange(k1, k3, s3), noTS, roachpb.Key(k1), roachpb.Key(k2)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t2, s3), tombstone(k2, t3, s3)),
		},
		{
			name: "one deleterange after writes returning keys outside span boundary",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k4, s2), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s3), noTS, roachpb.Key(k1), roachpb.Key(k4)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t3, s3), kv(k4, t2, s2)),
		},
		{
			name: "one deleterange after writes incorrectly deleting keys outside span boundary",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k4, s2), t2)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withDeleteRangeResult(delRange(k1, k3, s3), noTS, roachpb.Key(k1), roachpb.Key(k4)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), tombstone(k1, t3, s3), kv(k4, t2, s2), tombstone(k4, t3, s3)),
		},
		{
			name: "single mvcc rangedel",
			steps: []Step{
				step(withResultTS(delRangeUsingTombstone(k1, k2, s1), t1)),
			},
			kvs: kvs(rd(k1, k2, t1, s1)),
		},
		{
			name: "single mvcc rangedel after put",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(delRangeUsingTombstone(k1, k2, s2), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), rd(k1, k2, t2, s2)),
		},
		{
			name: "single mvcc rangedel before put",
			steps: []Step{
				step(withResultTS(delRangeUsingTombstone(k1, k2, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
			},
			kvs: kvs(rd(k1, k2, t1, s1), kv(k1, t2, s2)),
		},
		{
			name: "two overlapping rangedels",
			steps: []Step{
				step(withResultTS(delRangeUsingTombstone(k1, k3, s1), t1)),
				step(withResultTS(delRangeUsingTombstone(k2, k4, s2), t2)),
			},
			// Note: you see rangedel fragmentation in action here, which has to
			// happen. Even if we decided to hand pebble overlapping rangedels, it
			// would fragment them for us, and we'd get what you see below back when
			// we read.
			kvs: kvs(
				rd(k1, k2, t1, s1),
				rd(k2, k3, t1, s1),
				rd(k2, k3, t2, s2),
				rd(k3, k4, t2, s2),
			),
		},
		{
			name: "batch of touching rangedels",
			steps: []Step{step(withResultTS(batch(
				withResult(delRangeUsingTombstone(k1, k2, s1)),
				withResult(delRangeUsingTombstone(k2, k4, s2)),
			), t1)),
			},
			// Note that the tombstones aren't merged. In fact, our use of sequence numbers
			// embedded in MVCCValueHeader implies that pebble can never merge adjacent
			// tombstones from the same batch/txn.
			kvs: kvs(
				rd(k1, k2, t1, s1),
				rd(k2, k4, t1, s2),
			),
		},
		{
			// Note also that self-overlapping batches or rangedels in txns aren't
			// allowed today, so this particular example exists in this unit test but
			// not in real CRDB. But we can have "touching" rangedels today, see
			// above.
			name: "batch of two overlapping rangedels",
			steps: []Step{step(withResultTS(batch(
				withResult(delRangeUsingTombstone(k1, k3, s1)),
				withResult(delRangeUsingTombstone(k2, k4, s2)),
			), t1)),
			},
			// Note that the tombstones aren't merged. In fact, our use of sequence numbers
			// embedded in MVCCValueHeader implies that pebble can never merge adjacent
			// tombstones from the same batch/txn.
			// Note also that self-overlapping batches or rangedels in txns aren't
			// allowed today, so this particular example exists in this unit test but
			// not in real CRDB. But we can have "touching" rangedels today.
			kvs: kvs(
				rd(k1, k2, t1, s1),
				rd(k2, k4, t1, s2),
			),
		},
		{
			name: "read before rangedel",
			steps: []Step{
				step(withResultTS(put(k2, s1), t1)),
				step(withReadResultTS(get(k2), v1, t2)),
				step(withResultTS(delRangeUsingTombstone(k1, k3, s3), t3)),
			},
			kvs: kvs(
				kv(k2, t1, s1),
				rd(k1, k3, t3, s3),
			),
		},
		{
			// MVCC range deletions are executed individually when the range is split,
			// and if this happens kvnemesis will report a failure since the writes
			// will in all likelihood have non-atomic timestamps.
			// In an actual run we avoid this by adding a test hook to DistSender to
			// avoid splitting MVCC rangedels across ranges, instead failing with a
			// hard error, and the generator attempts - imperfectly - to respect the
			// split points.
			name: "rangedel with range split",
			steps: []Step{
				step(withResultTS(delRangeUsingTombstone(k1, k3, s1), t2)),
			},
			kvs: kvs(
				rd(k1, k2, t2, s1),
				rd(k2, k3, t1, s1),
			),
		},
		{
			name: "rangedel shadowing scan",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(delRangeUsingTombstone(k1, k2, s2), t2)),
				step(withScanResultTS(scan(k1, k2), t3)), // no rows returned
			},
			kvs: kvs(
				kv(k1, t1, s1),
				rd(k1, k2, t2, s2),
			),
		},
		{
			name: "addsstable ingestion",
			steps: []Step{
				step(withResultTS(makeAddSSTable(s1, []sstKV{
					{key: k1, tombstone: false},
					{key: k2, tombstone: true},
					{key: k3, endKey: k4, tombstone: true},
				}), t1)),
			},
			kvs: kvs(
				kv(k1, t1, s1),
				tombstone(k2, t1, s1),
				rd(k3, k4, t1, s1),
			),
		},
		{
			name: "addsstable ingestion shadowing scan",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s1), t1)),
				step(withResultTS(put(k3, s1), t1)),
				step(withResultTS(put(k4, s1), t1)),
				step(withResultTS(makeAddSSTable(s2, []sstKV{
					{key: k1, tombstone: false},
					{key: k2, tombstone: true},
					{key: k3, endKey: k4, tombstone: true},
				}), t2)),
				step(withScanResultTS(scan(k1, k5), t3, scanKV(k1, v2), scanKV(k4, v1))),
			},
			kvs: kvs(
				kv(k1, t1, s1),
				kv(k1, t2, s2),
				kv(k2, t1, s1),
				tombstone(k2, t2, s2),
				rd(k3, k4, t2, s2),
				kv(k3, t1, s1),
				kv(k4, t1, s1),
			),
		},
		{
			name: "one read skip locked after write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(getSkipLocked(k1), v1, t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one read skip locked after write returning wrong value",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(getSkipLocked(k1), v2, t2)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one read skip locked after write returning no value",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withReadResultTS(getSkipLocked(k1), ``, t2)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one scan skip locked after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(scanSkipLocked(k1, k3), t4, scanKV(k1, v1), scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one scan skip locked after writes returning wrong value",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(scanSkipLocked(k1, k3), t3, scanKV(k1, v3), scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one scan skip locked after writes returning no values",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(scanSkipLocked(k1, k3), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one scan skip locked after writes returning some values",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(scanSkipLocked(k1, k3), t3, scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one reverse scan skip locked after writes",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(reverseScanSkipLocked(k1, k3), t3, scanKV(k2, v2), scanKV(k1, v1))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one reverse scan skip locked after writes returning wrong value",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(reverseScanSkipLocked(k1, k3), t3, scanKV(k2, v2), scanKV(k1, v3))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one reverse scan skip locked after writes returning no values",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(reverseScanSkipLocked(k1, k3), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "one reverse scan skip locked after writes returning some values",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withScanResultTS(reverseScanSkipLocked(k1, k3), t3, scanKV(k2, v2))),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "weak isolation transaction with non-atomic writes",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withResultOK(put(k1, s1)),
					withResultOK(put(k2, s2)),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k2, t2, s2)),
		},
		{
			name: "weak isolation transaction with atomic writes",
			steps: []Step{
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withResultOK(put(k1, s1)),
					withResultOK(put(k2, s2)),
				), t2)),
			},
			kvs: kvs(kv(k1, t2, s1), kv(k2, t2, s2)), // difference: t2 instead of t1
		},
		{
			name: "weak isolation transaction with non-atomic non-locking get",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withReadResult(get(k1), v1),
					withResult(put(k3, s3)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k3, t3, s3)),
		},
		{
			name: "weak isolation transaction with non-atomic locking (unreplicated) get",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withReadResult(getForShare(k1), v1),
					withResult(put(k3, s3)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k3, t3, s3)),
		},
		{
			name: "weak isolation transaction with non-atomic locking (replicated) get",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withReadResult(getForShareGuaranteedDurability(k1), v1),
					withResult(put(k3, s3)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k3, t3, s3)),
		},
		{
			name: "weak isolation transaction with atomic locking (replicated) get",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withReadResult(getForShareGuaranteedDurability(k1), v2), // difference: v2 instead of v1
					withResult(put(k3, s3)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k3, t3, s3)),
		},
		{
			name: "weak isolation transaction with atomic locking (replicated) get and missing key",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withReadResult(getForShareGuaranteedDurability(k1), ``), // difference: no value instead of v1
					withResult(put(k3, s3)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k3, t3, s3)),
		},
		{
			name: "weak isolation transaction with non-atomic non-locking scan",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withScanResult(scan(k1, k3), scanKV(k1, v1), scanKV(k2, v3)),
					withResult(put(k3, s4)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3), kv(k3, t3, s4)),
		},
		{
			name: "weak isolation transaction with non-atomic locking (unreplicated) scan",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withScanResult(scanForShare(k1, k3), scanKV(k1, v1), scanKV(k2, v3)),
					withResult(put(k3, s4)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3), kv(k3, t3, s4)),
		},
		{
			name: "weak isolation transaction with non-atomic locking (replicated) scan",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withScanResult(scanForShareGuaranteedDurability(k1, k3), scanKV(k1, v1), scanKV(k2, v3)),
					withResult(put(k3, s4)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3), kv(k3, t3, s4)),
		},
		{
			name: "weak isolation transaction with atomic locking (replicated) scan",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withScanResult(scanForShareGuaranteedDurability(k1, k3), scanKV(k1, v2), scanKV(k2, v3)), // difference: v2 instead of v1
					withResult(put(k3, s4)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3), kv(k3, t3, s4)),
		},
		{
			name: "weak isolation transaction with atomic locking (replicated) scan and missing key",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k1, s2), t2)),
				step(withResultTS(put(k2, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withScanResult(scanForShareGuaranteedDurability(k1, k3), scanKV(k1, v2)), // difference: k2 not returned
					withResult(put(k3, s4)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t2, s2), kv(k2, t1, s3), kv(k3, t3, s4)),
		},
		{
			name: "weak isolation transaction with non-atomic deleterange",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(put(k2, s2), t2)),
				step(withResultTS(put(k3, s3), t1)),
				step(withResultTS(closureTxn(ClosureTxnType_Commit, isolation.Snapshot,
					withDeleteRangeResult(delRange(k1, k4, s4), noTS, roachpb.Key(k1), roachpb.Key(k3)),
				), t3)),
			},
			kvs: kvs(kv(k1, t1, s1), kv(k1, t3, s4), kv(k2, t2, s2), kv(k3, t1, s3), kv(k3, t3, s4)),
		},
		{
			name: "one savepoint and one put",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(createSavepoint(2)),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "one savepoint and release",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(createSavepoint(2)),
					withResult(put(k1, s2)),
					withResult(releaseSavepoint(2)),
					withReadResult(get(k1), v2),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s2)),
		},
		{
			name: "nested savepoint release",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(createSavepoint(2)),
					withResult(put(k1, s2)),
					withResult(createSavepoint(4)),
					withResult(put(k1, s3)),
					withResult(releaseSavepoint(2)),
					withReadResult(get(k1), v3),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s3)),
		},
		{
			name: "one savepoint and rollback",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(createSavepoint(2)),
					withResult(put(k1, s2)),
					withReadResult(get(k1), v2),
					withResult(rollbackSavepoint(2)),
					withReadResult(get(k1), v1),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "two nested savepoint rollbacks",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(createSavepoint(2)),
					withResult(put(k1, s2)),
					withReadResult(get(k1), v2),
					withResult(createSavepoint(5)),
					withResult(put(k1, s3)),
					withReadResult(get(k1), v3),
					withResult(rollbackSavepoint(2)),
					withReadResult(get(k1), v1),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "two closed nested savepoint rollbacks",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(createSavepoint(2)),
					withResult(put(k1, s2)),
					withReadResult(get(k1), v2),
					withResult(createSavepoint(5)),
					withResult(put(k1, s3)),
					withReadResult(get(k1), v3),
					withResult(rollbackSavepoint(5)),
					withResult(rollbackSavepoint(2)),
					withReadResult(get(k1), v1),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "two non-nested savepoints rollbacks",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(createSavepoint(0)),
					withResult(put(k1, s1)),
					withReadResult(get(k1), v1),
					withResult(rollbackSavepoint(0)),
					withResult(put(k1, s2)),
					withResult(createSavepoint(5)),
					withResult(put(k1, s3)),
					withReadResult(get(k1), v3),
					withResult(rollbackSavepoint(5)),
					withReadResult(get(k1), v2),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s2)),
		},
		{
			name: "savepoint release and rollback",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(put(k1, s1)),
					withResult(createSavepoint(2)),
					withResult(put(k1, s2)),
					withReadResult(get(k1), v2),
					withResult(createSavepoint(5)),
					withResult(put(k1, s3)),
					withReadResult(get(k1), v3),
					withResult(releaseSavepoint(5)),
					withResult(rollbackSavepoint(2)),
					withReadResult(get(k1), v1),
				), t1)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
		{
			name: "savepoint with no last write",
			steps: []Step{
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(createSavepoint(0)),
					withResult(put(k1, s1)),
					withReadResult(get(k1), v1),
					withResult(rollbackSavepoint(0)),
				), t1)),
			},
			kvs: nil,
		},
		{
			name: "savepoint with no last write and existing write",
			steps: []Step{
				step(withResultTS(put(k1, s1), t1)),
				step(withResultTS(closureTxnSSI(ClosureTxnType_Commit,
					withResult(createSavepoint(1)),
					withResult(put(k1, s2)),
					withReadResult(get(k1), v2),
					withResult(rollbackSavepoint(1)),
					withReadResult(get(k1), v1),
				), t2)),
			},
			kvs: kvs(kv(k1, t1, s1)),
		},
	}

	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for _, test := range tests {
		t.Run(test.name, w.Run(t, test.name, func(t *testing.T) string {
			e, err := MakeEngine()
			require.NoError(t, err)
			defer e.Close()

			var buf strings.Builder
			for _, step := range test.steps {
				fmt.Fprintln(&buf, strings.TrimSpace(step.String()))
			}

			tr := &SeqTracker{}
			for _, kv := range test.kvs {
				seq := kv.seq()
				tr.Add(kv.key, kv.endKey, kv.ts, seq)
				// NB: we go a little beyond what is truly necessary by embedding the
				// sequence numbers (inside kv.val) unconditionally, as they would be in
				// a real run. But we *do* need to embed them in `e.DeleteRange`, for
				// otherwise pebble might start merging adjacent MVCC range dels (since
				// they could have the same timestamp and empty value, where the seqno
				// would really produce unique values).
				if len(kv.endKey) == 0 {
					k := storage.MVCCKey{
						Key:       kv.key,
						Timestamp: kv.ts,
					}
					e.Put(k, kv.val)
					fmt.Fprintln(&buf, k, "@", seq, mustGetStringValue(kv.val))
				} else {
					k := storage.MVCCRangeKey{
						StartKey:  kv.key,
						EndKey:    kv.endKey,
						Timestamp: kv.ts,
					}
					e.DeleteRange(kv.key, kv.endKey, kv.ts, kv.val)
					fmt.Fprintln(&buf, k, "@", seq, mustGetStringValue(kv.val))
				}
			}

			if failures := Validate(test.steps, e, tr); len(failures) > 0 {
				for i := range failures {
					fmt.Fprintln(&buf, failures[i])
				}
			}
			return buf.String()
		}))
	}
}
