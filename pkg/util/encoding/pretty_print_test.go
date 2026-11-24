// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package encoding_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func TestPrettyPrintValueEncoded(t *testing.T) {
	uuidStr := "63616665-6630-3064-6465-616462656562"
	u, err := uuid.FromString(uuidStr)
	if err != nil {
		t.Fatalf("Bad test case. Attempted uuid.FromString(%q) got err: %d", uuidStr, err)
	}
	ip := "192.168.0.1/10"
	var ipAddr ipaddr.IPAddr
	err = ipaddr.ParseINet(ip, &ipAddr)
	if err != nil {
		t.Fatalf("Bad test case. Attempted ipaddr.ParseINet(%q) got err: %d", ip, err)
	}
	ba := bitarray.MakeBitArrayFromInt64(6, 9, 5)
	jString := `{"x": 1, "y": "one"}`
	j, err := json.ParseJSON(jString)
	if err != nil {
		t.Fatal(err)
	}
	jEnc, err := json.EncodeJSON(nil /* appendTo */, j)
	if err != nil {
		t.Fatal(err)
	}
	arrString, arrExp := `{1,2,3}`, `ARRAY[1,2,3]`
	arr, _, err := tree.ParseDArrayFromString(nil /* ctx */, arrString, types.Int)
	if err != nil {
		t.Fatal(err)
	}
	arrEnc, err := valueside.Encode(nil, valueside.NoColumnID, arr)
	if err != nil {
		t.Fatal(err)
	}
	bitArrString, bitArrExp := `{001,010,100}`, `ARRAY[B'001',B'010',B'100']`
	bitArr, _, err := tree.ParseDArrayFromString(nil /* ctx */, bitArrString, types.VarBit)
	if err != nil {
		t.Fatal(err)
	}
	bitArrEnc, err := valueside.Encode(nil, valueside.NoColumnID, bitArr)
	if err != nil {
		t.Fatal(err)
	}
	tupTyp := types.MakeTuple([]*types.T{types.Int, types.Float, types.Json})
	i, f := tree.DInt(1), tree.DFloat(2.3)
	tup := tree.NewDTuple(tupTyp, &i, &f, tree.NewDJSON(j))
	tupEnc, err := valueside.Encode(nil, valueside.NoColumnID, tup)
	if err != nil {
		t.Fatal(err)
	}
	tupExp := fmt.Sprintf(`(1, 2.3, '%s')`, jString)
	tests := []struct {
		buf      []byte
		expected string
	}{
		{encoding.EncodeNullValue(nil, encoding.NoColumnID), "NULL"},
		{encoding.EncodeBoolValue(nil, encoding.NoColumnID, true), "true"},
		{encoding.EncodeBoolValue(nil, encoding.NoColumnID, false), "false"},
		{encoding.EncodeIntValue(nil, encoding.NoColumnID, 7), "7"},
		{encoding.EncodeFloatValue(nil, encoding.NoColumnID, 6.28), "6.28"},
		{encoding.EncodeDecimalValue(nil, encoding.NoColumnID, apd.New(628, -2)), "6.28"},
		{encoding.EncodeTimeValue(nil, encoding.NoColumnID,
			time.Date(2016, 6, 29, 16, 2, 50, 5, time.UTC)), "2016-06-29T16:02:50.000000005Z"},
		{encoding.EncodeTimeTZValue(nil, encoding.NoColumnID,
			timetz.MakeTimeTZ(timeofday.New(10, 11, 12, 0), 5*60*60+24)), "10:11:12-05:00:24"},
		{encoding.EncodeDurationValue(nil, encoding.NoColumnID,
			duration.DecodeDuration(1, 2, 3)), "1 mon 2 days 00:00:00+3ns"},
		{encoding.EncodeBytesValue(nil, encoding.NoColumnID, []byte{0x1, 0x2, 0xF, 0xFF}), "0x01020fff"},
		{encoding.EncodeBytesValue(nil, encoding.NoColumnID, []byte("foo")), "foo"}, // printable bytes
		{encoding.EncodeBytesValue(nil, encoding.NoColumnID, []byte{0x89}), "0x89"}, // non-printable bytes
		{encoding.EncodeIPAddrValue(nil, encoding.NoColumnID, ipAddr), ip},
		{encoding.EncodeUUIDValue(nil, encoding.NoColumnID, u), uuidStr},
		{encoding.EncodeBitArrayValue(nil, encoding.NoColumnID, ba), "B001001"},
		{encoding.EncodeJSONValue(nil, encoding.NoColumnID, jEnc), jString},
		{arrEnc, arrExp},
		{bitArrEnc, bitArrExp},
		{tupEnc, tupExp},
	}
	for i, test := range tests {
		remaining, str, err := encoding.PrettyPrintValueEncoded(test.buf)
		if err != nil {
			t.Fatal(err)
		}
		if len(remaining) != 0 {
			t.Errorf("%d: expected all bytes to be consumed but was left with %s", i, remaining)
		}
		if str != test.expected {
			t.Errorf("%d: got %q expected %q", i, str, test.expected)
		}
	}
}
