// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package enginepb_test

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFormatMVCCMetadata(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	ts := hlc.Timestamp{Logical: 1}
	txnDidNotUpdateMeta := true
	tmeta := &enginepb.TxnMeta{
		Key:               roachpb.Key("a"),
		ID:                txnID,
		IsoLevel:          isolation.ReadCommitted,
		Epoch:             1,
		WriteTimestamp:    ts,
		MinTimestamp:      ts,
		CoordinatorNodeID: 6,
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
		TxnDidNotUpdateMeta: &txnDidNotUpdateMeta,
	}

	const expStr = `txn={id=d7aa0f5e key="a" iso=ReadCommitted pri=0.00000000 epo=1 ts=0,1 min=0,1 seq=0}` +
		` ts=0,1 del=false klen=123 vlen=456 rawlen=8 nih=2 mergeTs=<nil> txnDidNotUpdateMeta=true`

	if str := meta.String(); str != expStr {
		t.Errorf(
			"expected meta: %s\n"+
				"got:          %s",
			expStr, str)
	}

	const expV = `txn={id=d7aa0f5e key=‹"a"› iso=ReadCommitted pri=0.00000000 epo=1 ts=0,1 min=0,1 seq=0}` +
		` ts=0,1 del=false klen=123 vlen=456 raw=‹/BYTES/foo› ih={{11 ‹/BYTES/bar›}{22 ‹/BYTES/baz›}}` +
		` mergeTs=<nil> txnDidNotUpdateMeta=true`

	if str := redact.Sprintf("%+v", meta); str != expV {
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

func TestTxnSeqListExtends(t *testing.T) {
	type s = enginepb.TxnSeq
	type r = enginepb.IgnoredSeqNumRange
	mr := func(a, b s) r {
		return r{Start: a, End: b}
	}

	testData := []struct {
		a        []r
		b        []r
		isEqual  bool
		expected bool
	}{
		{
			[]r{},
			[]r{},
			true,
			true,
		},
		{
			[]r{mr(1, 5)},
			[]r{mr(1, 1)},
			false,
			true,
		},
		{
			[]r{mr(2, 5)},
			[]r{mr(1, 5)},
			false,
			false,
		},
		{
			[]r{mr(1, 5)},
			[]r{mr(2, 4)},
			false,
			true,
		},
		{
			[]r{mr(2, 4)},
			[]r{mr(1, 5)},
			false,
			false,
		},
		{
			[]r{mr(3, 5), mr(8, 26)},
			[]r{mr(3, 5), mr(10, 20), mr(22, 24)},
			false,
			true,
		},
		{
			[]r{mr(3, 5), mr(10, 20), mr(22, 24), mr(28, 32)},
			[]r{mr(3, 5), mr(10, 20), mr(22, 24)},
			false,
			true,
		},
		{
			[]r{mr(3, 5), mr(10, 20), mr(22, 24)},
			[]r{mr(3, 5), mr(10, 20), mr(22, 24)},
			true,
			true,
		},
	}

	for i, tc := range testData {
		t.Run(fmt.Sprintf("case=%d", i), func(t *testing.T) {
			require.Equal(t, tc.expected, enginepb.TxnSeqListExtends(tc.a, tc.b))
			if !tc.isEqual {
				require.Equal(t, !tc.expected, enginepb.TxnSeqListExtends(tc.b, tc.a))
			} else {
				require.Equal(t, true, enginepb.TxnSeqListExtends(tc.b, tc.a))
			}
		})
	}
	t.Run("disjoint", func(t *testing.T) {
		a := []r{mr(1, 2)}
		b := []r{mr(4, 5)}
		require.Equal(t, false, enginepb.TxnSeqListExtends(a, b))
		require.Equal(t, false, enginepb.TxnSeqListExtends(b, a))
	})
}

func TestTxnSeqListAppend(t *testing.T) {
	type r = enginepb.IgnoredSeqNumRange

	mr := func(a, b enginepb.TxnSeq) r {
		return r{Start: a, End: b}
	}

	testData := []struct {
		list     []r
		newRange r
		exp      []r
	}{
		{
			[]r{},
			mr(1, 2),
			[]r{mr(1, 2)},
		},
		{
			[]r{mr(1, 2)},
			mr(1, 4),
			[]r{mr(1, 4)},
		},
		{
			[]r{mr(1, 2), mr(3, 6)},
			mr(8, 10),
			[]r{mr(1, 2), mr(3, 6), mr(8, 10)},
		},
		{
			[]r{mr(1, 2), mr(5, 6)},
			mr(3, 8),
			[]r{mr(1, 2), mr(3, 8)},
		},
		{
			[]r{mr(1, 2), mr(5, 6)},
			mr(1, 8),
			[]r{mr(1, 8)},
		},
	}

	for _, tc := range testData {
		require.Equal(t, tc.exp, enginepb.TxnSeqListAppend(tc.list, tc.newRange))
	}
}

func TestFormatBytesAsKeyAndValue(t *testing.T) {
	// Injected by roachpb
	require.Equal(t, string(enginepb.FormatBytesAsKey([]byte("foo"))), "‹\"foo\"›")
	require.Equal(t, string(enginepb.FormatBytesAsKey([]byte("foo")).Redact()), "‹×›")

	// Injected by storage
	encodedIntVal := []byte{0x0, 0x0, 0x0, 0x0, 0x1, 0xf}
	require.Equal(t, string(enginepb.FormatBytesAsValue(encodedIntVal)), "‹/INT/-8›")
	require.Equal(t, string(enginepb.FormatBytesAsValue(encodedIntVal).Redact()), "‹×›")
}
