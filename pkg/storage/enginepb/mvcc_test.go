// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package enginepb_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/assert"
)

func TestFormatMVCCMetadata(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	ts := hlc.Timestamp{Logical: 1}
	tmeta := &enginepb.TxnMeta{
		Key:            roachpb.Key("a"),
		ID:             txnID,
		Epoch:          1,
		WriteTimestamp: ts,
		MinTimestamp:   ts,
	}
	val1 := roachpb.Value{}
	val1.SetString("foo")
	val2 := roachpb.Value{}
	val2.SetString("bar")
	val3 := roachpb.Value{}
	val3.SetString("baz")
	meta := &enginepb.MVCCMetadata{
		Txn:       tmeta,
		Timestamp: ts.ToLegacyTimestamp(),
		KeyBytes:  123,
		ValBytes:  456,
		RawBytes:  val1.RawBytes,
		IntentHistory: []enginepb.MVCCMetadata_SequencedIntent{
			{Sequence: 11, Value: val2.RawBytes},
			{Sequence: 22, Value: val3.RawBytes},
		},
	}

	const expStr = `txn={id=d7aa0f5e key="a" pri=0.00000000 epo=1 ts=0,1 min=0,1 seq=0}` +
		` ts=0,1 del=false klen=123 vlen=456 rawlen=8 nih=2`

	if str := meta.String(); str != expStr {
		t.Errorf(
			"expected meta: %s\n"+
				"got:          %s",
			expStr, str)
	}

	const expV = `txn={id=d7aa0f5e key="a" pri=0.00000000 epo=1 ts=0,1 min=0,1 seq=0}` +
		` ts=0,1 del=false klen=123 vlen=456 raw=/BYTES/foo ih={{11 /BYTES/bar}{22 /BYTES/baz}}`

	if str := fmt.Sprintf("%+v", meta); str != expV {
		t.Errorf(
			"expected meta: %s\n"+
				"got:           %s",
			expV, str)
	}
}

func TestTxnSeqIsIgnored(t *testing.T) {
	type s = enginepb.TxnSeq
	type r = enginepb.IgnoredSeqNumRange
	mr := func(a, b s) r {
		return r{Start: a, End: b}
	}

	testData := []struct {
		list       []r
		ignored    []s
		notIgnored []s
	}{
		{[]r{}, nil, []s{0, 1, 10}},
		{[]r{mr(1, 1)}, []s{1}, []s{0, 2, 10}},
		{[]r{mr(1, 1), mr(2, 3)}, []s{1, 2, 3}, []s{0, 4, 10}},
		{[]r{mr(1, 2), mr(4, 8), mr(9, 10)}, []s{1, 2, 5, 10}, []s{0, 3, 11}},
		{[]r{mr(0, 10)}, []s{0, 1, 2, 3, 10}, []s{11, 100}},
	}

	for _, tc := range testData {
		for _, ign := range tc.ignored {
			assert.True(t, enginepb.TxnSeqIsIgnored(ign, tc.list))
		}
		for _, notIgn := range tc.notIgnored {
			assert.False(t, enginepb.TxnSeqIsIgnored(notIgn, tc.list))
		}
	}
}
