// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package rangefeed

import (
	"context"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func makeKV(key, val string, ts int64) engine.MVCCKeyValue {
	return engine.MVCCKeyValue{
		Key: engine.MVCCKey{
			Key:       roachpb.Key(key),
			Timestamp: hlc.Timestamp{WallTime: ts},
		},
		Value: []byte(val),
	}
}

func makeMetaKV(key string, meta enginepb.MVCCMetadata) engine.MVCCKeyValue {
	b, err := protoutil.Marshal(&meta)
	if err != nil {
		panic(err)
	}
	return engine.MVCCKeyValue{
		Key: engine.MVCCKey{
			Key: roachpb.Key(key),
		},
		Value: b,
	}
}

func makeInline(key, val string) engine.MVCCKeyValue {
	return makeMetaKV(key, enginepb.MVCCMetadata{
		RawBytes: []byte(val),
	})
}

func makeIntent(key string, txnID uuid.UUID, txnKey string, txnTS int64) engine.MVCCKeyValue {
	return makeMetaKV(key, enginepb.MVCCMetadata{Txn: &enginepb.TxnMeta{
		ID:        txnID,
		Key:       []byte(txnKey),
		Timestamp: hlc.Timestamp{WallTime: txnTS},
	}})
}

type testIterator struct {
	kvs []engine.MVCCKeyValue
	cur int

	// Simulate unsafe buffers.
	unsafeKeyBuf []byte
	unsafeValBuf []byte

	closed bool
	err    error
	block  chan struct{}
	done   chan struct{}
}

func newTestIterator(kvs []engine.MVCCKeyValue) *testIterator {
	if !sort.SliceIsSorted(kvs, func(i, j int) bool {
		return kvs[i].Key.Less(kvs[j].Key)
	}) {
		panic("unsorted kvs")
	}
	return &testIterator{
		kvs:  kvs,
		cur:  -1,
		done: make(chan struct{}),
	}
}

func newErrorIterator(err error) *testIterator {
	return &testIterator{
		err:  err,
		done: make(chan struct{}),
	}
}

func (s *testIterator) Close() {
	s.closed = true
	close(s.done)
}

func (s *testIterator) Seek(key engine.MVCCKey) {
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

func (s *testIterator) UnsafeKey() engine.MVCCKey {
	curKey := s.curKV().Key
	curKey.Key = copyToUnsafeBuf(&s.unsafeKeyBuf, curKey.Key)
	return curKey
}

func (s *testIterator) UnsafeValue() []byte {
	curVal := s.curKV().Value
	return copyToUnsafeBuf(&s.unsafeValBuf, curVal)
}

func (s *testIterator) curKV() engine.MVCCKeyValue {
	return s.kvs[s.cur]
}

func copyToUnsafeBuf(buf *[]byte, src []byte) []byte {
	if cap(*buf) < len(src) {
		*buf = append([]byte(nil), src...)
	} else {
		*buf = (*buf)[:len(src)]
		copy(*buf, src)
	}
	return *buf
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
	iter := newTestIterator([]engine.MVCCKeyValue{
		makeKV("a", "val1", 10),
		makeInline("b", "val2"),
		makeIntent("c", txn1, "txnKey1", 15),
		makeKV("c", "val3", 11),
		makeKV("c", "val4", 9),
		makeIntent("d", txn2, "txnKey2", 21),
		makeKV("d", "val5", 20),
		makeKV("d", "val6", 19),
		makeInline("g", "val7"),
		makeKV("m", "val8", 1),
		makeIntent("n", txn1, "txnKey1", 12),
		makeIntent("r", txn1, "txnKey1", 19),
		makeKV("r", "val9", 4),
		makeIntent("w", txn1, "txnKey1", 3),
		makeInline("x", "val10"),
		makeIntent("z", txn2, "txnKey2", 21),
		makeKV("z", "val11", 4),
	})

	initScan := makeInitResolvedTSScan(&p, iter)
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

func TestCatchUpScan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Mock processor. We just needs its catchUpC.
	p := Processor{catchUpC: make(chan catchUpResult, 1)}

	// Run a catch-up scan for a registration over a test
	// iterator with the following keys.
	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	iter := newTestIterator([]engine.MVCCKeyValue{
		makeKV("a", "val1", 10),
		makeInline("b", "val2"),
		makeIntent("c", txn1, "txnKey1", 15),
		makeKV("c", "val3", 11),
		makeKV("c", "val4", 9),
		makeIntent("d", txn2, "txnKey2", 21),
		makeKV("d", "val5", 20),
		makeKV("d", "val6", 19),
		makeInline("g", "val7"),
		makeKV("m", "val8", 1),
		makeIntent("n", txn1, "txnKey1", 12),
		makeIntent("r", txn1, "txnKey1", 19),
		makeKV("r", "val9", 4),
		makeIntent("w", txn1, "txnKey1", 3),
		makeInline("x", "val10"),
		makeIntent("z", txn2, "txnKey2", 21),
		makeKV("z", "val11", 4),
	})
	r := newTestRegistration(roachpb.Span{
		Key:    roachpb.Key("d"),
		EndKey: roachpb.Key("w"),
	})
	r.catchUpIter = iter
	r.startTS = hlc.Timestamp{WallTime: 4}

	catchUpScan := makeCatchUpScan(&p, &r.registration)
	catchUpScan.Run(context.Background())
	require.True(t, iter.closed)

	// Compare the events sent on the registration's Stream to the expected events.
	expEvents := []*roachpb.RangeFeedEvent{
		rangeFeedValue(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("val5"), Timestamp: hlc.Timestamp{WallTime: 20}},
		),
		rangeFeedValue(
			roachpb.Key("d"),
			roachpb.Value{RawBytes: []byte("val6"), Timestamp: hlc.Timestamp{WallTime: 19}},
		),
		rangeFeedValue(
			roachpb.Key("g"),
			roachpb.Value{RawBytes: []byte("val7"), Timestamp: hlc.Timestamp{WallTime: 0}},
		),
		rangeFeedValue(
			roachpb.Key("r"),
			roachpb.Value{RawBytes: []byte("val9"), Timestamp: hlc.Timestamp{WallTime: 4}},
		),
	}
	require.Equal(t, expEvents, r.Events())
	require.Equal(t, 1, len(p.catchUpC))
	require.Equal(t, catchUpResult{r: &r.registration}, <-p.catchUpC)
}
