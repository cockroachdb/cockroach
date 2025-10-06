// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb_test

import (
	"fmt"
	"testing"

	// Hook up the pretty printer.
	_ "github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestTransactionString(t *testing.T) {
	txnID, err := uuid.FromBytes([]byte("ת\x0f^\xe4-Fؽ\xf7\x16\xe4\xf9\xbe^\xbe"))
	if err != nil {
		t.Fatal(err)
	}
	txn := roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			Key:            roachpb.Key("foo"),
			ID:             txnID,
			Epoch:          2,
			WriteTimestamp: hlc.Timestamp{WallTime: 20, Logical: 21},
			MinTimestamp:   hlc.Timestamp{WallTime: 10, Logical: 11},
			Priority:       957356782,
			Sequence:       15,
		},
		Name:                   "name",
		Status:                 roachpb.COMMITTED,
		LastHeartbeat:          hlc.Timestamp{WallTime: 10, Logical: 11},
		ReadTimestamp:          hlc.Timestamp{WallTime: 30, Logical: 31},
		GlobalUncertaintyLimit: hlc.Timestamp{WallTime: 40, Logical: 41},
	}
	expStr := `"name" meta={id=d7aa0f5e key="foo" iso=Serializable pri=44.58039917 epo=2 ts=0.000000020,21 min=0.000000010,11 seq=15}` +
		` lock=true stat=COMMITTED rts=0.000000030,31 wto=false gul=0.000000040,41`

	if str := txn.String(); str != expStr {
		t.Errorf(
			"expected txn: %s\n"+
				"got:          %s",
			expStr, str)
	}
}

func TestKeyString(t *testing.T) {
	require.Equal(t,
		`/Table/53/42/"=\xbc ⌘"`,
		roachpb.Key("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98").String())
}

func TestRangeDescriptorStringRedact(t *testing.T) {
	desc := roachpb.RangeDescriptor{
		RangeID:  1,
		StartKey: roachpb.RKey("c"),
		EndKey:   roachpb.RKey("g"),
		InternalReplicas: []roachpb.ReplicaDescriptor{
			{NodeID: 1, StoreID: 1},
			{NodeID: 2, StoreID: 2},
			{NodeID: 3, StoreID: 3},
		},
	}

	require.EqualValues(t,
		`r1:‹{c-g}› [(n1,s1):?, (n2,s2):?, (n3,s3):?, next=0, gen=0]`,
		redact.Sprint(desc),
	)
}

func TestSpansString(t *testing.T) {
	for _, tc := range []struct {
		spans    roachpb.Spans
		expected string
	}{
		{
			spans:    roachpb.Spans{},
			expected: "",
		},
		{
			spans:    roachpb.Spans{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
			expected: "{a-b}",
		},
		{
			spans:    roachpb.Spans{{Key: roachpb.Key("a")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			expected: "a, {c-d}",
		},
	} {
		require.Equal(t, tc.expected, tc.spans.String())
	}
}

func TestSpansBoundedString(t *testing.T) {
	getSpans := func(numSpans int) roachpb.Spans {
		var spans roachpb.Spans
		for i := 1; i <= numSpans; i++ {
			spans = append(spans, roachpb.Span{
				Key: roachpb.Key(fmt.Sprintf("a%d", i)),
			})
		}
		return spans
	}
	for _, tc := range []struct {
		spans     roachpb.Spans
		bytesHint int
		expected  string
	}{
		{
			spans:     getSpans(6),
			bytesHint: 0,
			// At most 6 spans are always included.
			expected: "a1, a2, a3, a4, a5, a6",
		},
		{
			spans:     getSpans(7),
			bytesHint: 0,
			// 3 at the head and 3 at the tail are always included.
			expected: "a1, a2, a3 ... a5, a6, a7",
		},
		{
			spans:     getSpans(7),
			bytesHint: 10,
			// First 3 spans use up the bytes hint.
			expected: "a1, a2, a3 ... a5, a6, a7",
		},
		{
			spans:     getSpans(7),
			bytesHint: 11,
			// Bytes hint is exceeded after printing 4 spans, at which point
			// only the guaranteed 3 tail spans are left, which are always
			// included.
			expected: "a1, a2, a3, a4, a5, a6, a7",
		},
		{
			spans:     getSpans(15),
			bytesHint: 20,
			expected:  "a1, a2, a3, a4, a5, a6 ... a13, a14, a15",
		},
	} {
		require.Equal(t, tc.expected, tc.spans.BoundedString(tc.bytesHint))
	}
}
