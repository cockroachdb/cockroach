// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed

import (
	"context"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func makeKV(key, val string, ts int64) storage.MVCCKeyValue {
	return storage.MVCCKeyValue{
		Key: storage.MVCCKey{
			Key:       roachpb.Key(key),
			Timestamp: hlc.Timestamp{WallTime: ts},
		},
		Value: []byte(val),
	}
}

func makeProvisionalKV(key, val string, ts int64) storage.MVCCKeyValue {
	return makeKV(key, val, ts)
}

func makeMetaKV(key string, meta enginepb.MVCCMetadata) storage.MVCCKeyValue {
	b, err := protoutil.Marshal(&meta)
	if err != nil {
		panic(err)
	}
	return storage.MVCCKeyValue{
		Key: storage.MVCCKey{
			Key: roachpb.Key(key),
		},
		Value: b,
	}
}

func makeInline(key, val string) storage.MVCCKeyValue {
	return makeMetaKV(key, enginepb.MVCCMetadata{
		RawBytes: []byte(val),
	})
}

func makeIntent(key string, txnID uuid.UUID, txnKey string, txnTS int64) storage.MVCCKeyValue {
	return makeMetaKV(key, enginepb.MVCCMetadata{
		Txn: &enginepb.TxnMeta{
			ID:             txnID,
			Key:            []byte(txnKey),
			WriteTimestamp: hlc.Timestamp{WallTime: txnTS},
			MinTimestamp:   hlc.Timestamp{WallTime: txnTS},
		},
		Timestamp: hlc.LegacyTimestamp{WallTime: txnTS},
	})
}

type testIterator struct {
	kvs []storage.MVCCKeyValue
	cur int

	closed bool
	err    error
	block  chan struct{}
	done   chan struct{}
}

func newTestIterator(kvs []storage.MVCCKeyValue) *testIterator {
	// Ensure that the key-values are sorted.
	if !sort.SliceIsSorted(kvs, func(i, j int) bool {
		return kvs[i].Key.Less(kvs[j].Key)
	}) {
		panic("unsorted kvs")
	}

	// Ensure that every intent has a matching MVCCMetadata key
	// and provisional key-value pair.
	const missingErr = "missing provisional kv (makeProvisionalKV) for intent meta key (makeIntent)"
	var meta enginepb.MVCCMetadata
	for i := 0; i < len(kvs); i++ {
		kv := kvs[i]
		if !kv.Key.IsValue() {
			if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
				panic(err)
			}
			if !meta.IsInline() {
				i++
				if i == len(kvs) {
					panic(missingErr)
				}
				expNextKey := storage.MVCCKey{
					Key:       kv.Key.Key,
					Timestamp: hlc.Timestamp(meta.Timestamp),
				}
				if !kvs[i].Key.Equal(expNextKey) {
					panic(missingErr)
				}
			}
		}
	}

	return &testIterator{
		kvs:  kvs,
		cur:  -1,
		done: make(chan struct{}),
	}
}

func (s *testIterator) Close() {
	s.closed = true
	close(s.done)
}

func (s *testIterator) SeekGE(key storage.MVCCKey) {
	if s.closed {
		panic("testIterator closed")
	}
	if s.block != nil {
		<-s.block
	}
	if s.err != nil {
		return
	}
	if s.cur == -1 {
		s.cur++
	}
	for ; s.cur < len(s.kvs); s.cur++ {
		if !s.curKV().Key.Less(key) {
			break
		}
	}
}

func (s *testIterator) Valid() (bool, error) {
	if s.err != nil {
		return false, s.err
	}
	if s.cur == -1 || s.cur >= len(s.kvs) {
		return false, nil
	}
	return true, nil
}

func (s *testIterator) Next() { s.cur++ }

func (s *testIterator) NextKey() {
	if s.cur == -1 {
		s.cur = 0
		return
	}
	origKey := s.curKV().Key.Key
	for s.cur++; s.cur < len(s.kvs); s.cur++ {
		if !s.curKV().Key.Key.Equal(origKey) {
			break
		}
	}
}

func (s *testIterator) UnsafeKey() storage.MVCCKey {
	return s.curKV().Key
}

func (s *testIterator) UnsafeValue() []byte {
	return s.curKV().Value
}

func (s *testIterator) curKV() storage.MVCCKeyValue {
	return s.kvs[s.cur]
}

func TestInitResolvedTSScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Mock processor. We just needs its eventC.
	p := Processor{
		Config: Config{
			Span: roachpb.RSpan{
				Key:    roachpb.RKey("d"),
				EndKey: roachpb.RKey("w"),
			},
		},
		eventC: make(chan event, 100),
	}

	// Run an init rts scan over a test iterator with the following keys.
	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	iter := newTestIterator([]storage.MVCCKeyValue{
		makeKV("a", "val1", 10),
		makeInline("b", "val2"),
		makeIntent("c", txn1, "txnKey1", 15),
		makeProvisionalKV("c", "txnKey1", 15),
		makeKV("c", "val3", 11),
		makeKV("c", "val4", 9),
		makeIntent("d", txn2, "txnKey2", 21),
		makeProvisionalKV("d", "txnKey2", 21),
		makeKV("d", "val5", 20),
		makeKV("d", "val6", 19),
		makeInline("g", "val7"),
		makeKV("m", "val8", 1),
		makeIntent("n", txn1, "txnKey1", 12),
		makeProvisionalKV("n", "txnKey1", 12),
		makeIntent("r", txn1, "txnKey1", 19),
		makeProvisionalKV("r", "txnKey1", 19),
		makeKV("r", "val9", 4),
		makeIntent("w", txn1, "txnKey1", 3),
		makeProvisionalKV("w", "txnKey1", 3),
		makeInline("x", "val10"),
		makeIntent("z", txn2, "txnKey2", 21),
		makeProvisionalKV("z", "txnKey2", 21),
		makeKV("z", "val11", 4),
	})

	initScan := newInitResolvedTSScan(&p, iter)
	initScan.Run(context.Background())
	require.True(t, iter.closed)

	// Compare the event channel to the expected events.
	expEvents := []event{
		{ops: []enginepb.MVCCLogicalOp{
			writeIntentOpWithKey(txn2, []byte("txnKey2"), hlc.Timestamp{WallTime: 21}),
		}},
		{ops: []enginepb.MVCCLogicalOp{
			writeIntentOpWithKey(txn1, []byte("txnKey1"), hlc.Timestamp{WallTime: 12}),
		}},
		{ops: []enginepb.MVCCLogicalOp{
			writeIntentOpWithKey(txn1, []byte("txnKey1"), hlc.Timestamp{WallTime: 19}),
		}},
		{initRTS: true},
	}
	require.Equal(t, len(expEvents), len(p.eventC))
	for _, expEvent := range expEvents {
		require.Equal(t, expEvent, <-p.eventC)
	}
}

type testTxnPusher struct {
	pushTxnsFn               func([]enginepb.TxnMeta, hlc.Timestamp) ([]*roachpb.Transaction, error)
	cleanupTxnIntentsAsyncFn func([]*roachpb.Transaction) error
}

func (tp *testTxnPusher) PushTxns(
	ctx context.Context, txns []enginepb.TxnMeta, ts hlc.Timestamp,
) ([]*roachpb.Transaction, error) {
	return tp.pushTxnsFn(txns, ts)
}

func (tp *testTxnPusher) CleanupTxnIntentsAsync(
	ctx context.Context, txns []*roachpb.Transaction,
) error {
	return tp.cleanupTxnIntentsAsyncFn(txns)
}

func (tp *testTxnPusher) mockPushTxns(
	fn func([]enginepb.TxnMeta, hlc.Timestamp) ([]*roachpb.Transaction, error),
) {
	tp.pushTxnsFn = fn
}

func (tp *testTxnPusher) mockCleanupTxnIntentsAsync(fn func([]*roachpb.Transaction) error) {
	tp.cleanupTxnIntentsAsyncFn = fn
}

func TestTxnPushAttempt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a set of transactions.
	txn1, txn2, txn3 := uuid.MakeV4(), uuid.MakeV4(), uuid.MakeV4()
	ts1, ts2, ts3 := hlc.Timestamp{WallTime: 1}, hlc.Timestamp{WallTime: 2}, hlc.Timestamp{WallTime: 3}
	txn1Meta := enginepb.TxnMeta{ID: txn1, Key: keyA, WriteTimestamp: ts1, MinTimestamp: ts1}
	txn2Meta := enginepb.TxnMeta{ID: txn2, Key: keyB, WriteTimestamp: ts2, MinTimestamp: ts2}
	txn3Meta := enginepb.TxnMeta{ID: txn3, Key: keyC, WriteTimestamp: ts3, MinTimestamp: ts3}
	txn1Proto := &roachpb.Transaction{TxnMeta: txn1Meta, Status: roachpb.PENDING}
	txn2Proto := &roachpb.Transaction{TxnMeta: txn2Meta, Status: roachpb.COMMITTED}
	txn3Proto := &roachpb.Transaction{TxnMeta: txn3Meta, Status: roachpb.ABORTED}

	// Run a txnPushAttempt.
	var tp testTxnPusher
	tp.mockPushTxns(func(txns []enginepb.TxnMeta, ts hlc.Timestamp) ([]*roachpb.Transaction, error) {
		require.Equal(t, 3, len(txns))
		require.Equal(t, txn1Meta, txns[0])
		require.Equal(t, txn2Meta, txns[1])
		require.Equal(t, txn3Meta, txns[2])
		require.Equal(t, hlc.Timestamp{WallTime: 15}, ts)

		// Return all three protos. The PENDING txn is pushed.
		txn1ProtoPushed := txn1Proto.Clone()
		txn1ProtoPushed.WriteTimestamp = ts
		return []*roachpb.Transaction{txn1ProtoPushed, txn2Proto, txn3Proto}, nil
	})
	tp.mockCleanupTxnIntentsAsync(func(txns []*roachpb.Transaction) error {
		require.Equal(t, 2, len(txns))
		require.Equal(t, txn2Proto, txns[0])
		require.Equal(t, txn3Proto, txns[1])
		return nil
	})

	// Mock processor. We just needs its eventC.
	p := Processor{eventC: make(chan event, 100)}
	p.TxnPusher = &tp

	txns := []enginepb.TxnMeta{txn1Meta, txn2Meta, txn3Meta}
	doneC := make(chan struct{})
	pushAttempt := newTxnPushAttempt(&p, txns, hlc.Timestamp{WallTime: 15}, doneC)
	pushAttempt.Run(context.Background())
	<-doneC // check if closed

	// Compare the event channel to the expected events.
	expEvents := []event{
		{ops: []enginepb.MVCCLogicalOp{
			updateIntentOp(txn1, hlc.Timestamp{WallTime: 15}),
			updateIntentOp(txn2, hlc.Timestamp{WallTime: 2}),
			abortTxnOp(txn3),
		}},
	}
	require.Equal(t, len(expEvents), len(p.eventC))
	for _, expEvent := range expEvents {
		require.Equal(t, expEvent, <-p.eventC)
	}
}
