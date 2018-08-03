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

type testSnapshot struct {
	kvs    []engine.MVCCKeyValue
	closed bool
	block  chan struct{}
	done   chan struct{}
}

func newTestSnapshot(kvs []engine.MVCCKeyValue) *testSnapshot {
	if !sort.SliceIsSorted(kvs, func(i, j int) bool {
		return kvs[i].Key.Less(kvs[j].Key)
	}) {
		panic("unsorted kvs")
	}
	return &testSnapshot{
		kvs:  kvs,
		done: make(chan struct{}),
	}
}

func (s *testSnapshot) Iterate(
	start, end roachpb.Key, f func(engine.MVCCKeyValue) (bool, error),
) error {
	if s.closed {
		panic("testSnapshot closed")
	}
	if s.block != nil {
		<-s.block
	}
	for _, kv := range s.kvs {
		if kv.Key.Key.Compare(start) < 0 || kv.Key.Key.Compare(end) >= 0 {
			continue
		}
		if stop, err := f(kv); err != nil {
			return err
		} else if stop {
			break
		}
	}
	return nil
}

func (s *testSnapshot) Close() {
	s.closed = true
	close(s.done)
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

	// Run an init rts scan over a test snapshot with the following keys.
	txn1, txn2 := uuid.MakeV4(), uuid.MakeV4()
	snap := newTestSnapshot([]engine.MVCCKeyValue{
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

	initScan := makeInitResolvedTSScan(&p, snap)
	initScan.Run(context.Background())
	require.True(t, snap.closed)

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
