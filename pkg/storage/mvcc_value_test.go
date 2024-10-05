// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMVCCValueLocalTimestampNeeded(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts0 := hlc.Timestamp{Logical: 0}
	ts1 := hlc.Timestamp{Logical: 1}
	ts2 := hlc.Timestamp{Logical: 2}

	testcases := map[string]struct {
		localTs   hlc.Timestamp
		versionTs hlc.Timestamp
		expect    bool
	}{
		"no local timestamp":      {ts0, ts2, false},
		"smaller local timestamp": {ts1, ts2, true},
		"equal local timestamp":   {ts2, ts2, false},
		"larger local timestamp":  {ts2, ts1, false},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			mvccVal := MVCCValue{}
			mvccVal.LocalTimestamp = hlc.ClockTimestamp(tc.localTs)

			require.Equal(t, tc.expect, mvccVal.LocalTimestampNeeded(tc.versionTs))
		})
	}
}

func TestMVCCValueGetLocalTimestamp(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ts0 := hlc.Timestamp{Logical: 0}
	ts1 := hlc.Timestamp{Logical: 1}
	ts2 := hlc.Timestamp{Logical: 2}
	ts2S := hlc.Timestamp{Logical: 2, Synthetic: true}

	testcases := map[string]struct {
		localTs   hlc.Timestamp
		versionTs hlc.Timestamp
		expect    hlc.Timestamp
	}{
		"no local timestamp":                    {ts0, ts2, ts2},
		"no local timestamp, version synthetic": {ts0, ts2S, hlc.MinTimestamp},
		"smaller local timestamp":               {ts1, ts2, ts1},
		"equal local timestamp":                 {ts2, ts2, ts2},
		"larger local timestamp":                {ts2, ts1, ts2},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			mvccVal := MVCCValue{}
			mvccVal.LocalTimestamp = hlc.ClockTimestamp(tc.localTs)

			require.Equal(t, hlc.ClockTimestamp(tc.expect), mvccVal.GetLocalTimestamp(tc.versionTs))
		})
	}
}

func TestMVCCValueFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var strVal, intVal roachpb.Value
	strVal.SetString("foo")
	intVal.SetInt(17)

	valHeader := enginepb.MVCCValueHeader{}
	valHeader.LocalTimestamp = hlc.ClockTimestamp{WallTime: 9}

	testcases := map[string]struct {
		val    MVCCValue
		expect string
	}{
		"tombstone":        {val: MVCCValue{}, expect: "/<empty>"},
		"bytes":            {val: MVCCValue{Value: strVal}, expect: "/BYTES/foo"},
		"int":              {val: MVCCValue{Value: intVal}, expect: "/INT/17"},
		"header+tombstone": {val: MVCCValue{MVCCValueHeader: valHeader}, expect: "{localTs=0.000000009,0}/<empty>"},
		"header+bytes":     {val: MVCCValue{MVCCValueHeader: valHeader, Value: strVal}, expect: "{localTs=0.000000009,0}/BYTES/foo"},
		"header+int":       {val: MVCCValue{MVCCValueHeader: valHeader, Value: intVal}, expect: "{localTs=0.000000009,0}/INT/17"},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.val.String())
		})
	}
}

func TestEncodeDecodeMVCCValue(t *testing.T) {
	defer leaktest.AfterTest(t)()
	DisableMetamorphicSimpleValueEncoding(t)

	var strVal, intVal roachpb.Value
	strVal.SetString("foo")
	intVal.SetInt(17)

	valHeader := enginepb.MVCCValueHeader{}
	valHeader.LocalTimestamp = hlc.ClockTimestamp{WallTime: 9}

	testcases := map[string]struct {
		val MVCCValue
	}{
		"tombstone":        {val: MVCCValue{}},
		"bytes":            {val: MVCCValue{Value: strVal}},
		"int":              {val: MVCCValue{Value: intVal}},
		"header+tombstone": {val: MVCCValue{MVCCValueHeader: valHeader}},
		"header+bytes":     {val: MVCCValue{MVCCValueHeader: valHeader, Value: strVal}},
		"header+int":       {val: MVCCValue{MVCCValueHeader: valHeader, Value: intVal}},
	}
	w := echotest.NewWalker(t, datapathutils.TestDataPath(t, t.Name()))
	for name, tc := range testcases {
		t.Run(name, w.Run(t, name, func(t *testing.T) string {
			var buf strings.Builder
			enc, err := EncodeMVCCValue(tc.val)
			require.NoError(t, err)
			fmt.Fprintf(&buf, "encoded: %x", enc)
			assert.Equal(t, encodedMVCCValueSize(tc.val), len(enc))

			dec, err := DecodeMVCCValue(enc)
			require.NoError(t, err)

			if len(dec.Value.RawBytes) == 0 {
				dec.Value.RawBytes = nil // normalize
			}

			require.Equal(t, tc.val, dec)
			require.Equal(t, tc.val.IsTombstone(), dec.IsTombstone())
			isTombstone, err := EncodedMVCCValueIsTombstone(enc)
			require.NoError(t, err)
			require.Equal(t, tc.val.IsTombstone(), isTombstone)

			return buf.String()
		}))
	}
}

func TestDecodeMVCCValueErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testcases := map[string]struct {
		enc    []byte
		expect error
	}{
		"missing tag":    {enc: []byte{0x0}, expect: errMVCCValueMissingTag},
		"missing header": {enc: []byte{0x0, 0x0, 0x0, 0x1, extendedEncodingSentinel}, expect: errMVCCValueMissingHeader},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			dec, err := DecodeMVCCValue(tc.enc)
			require.Equal(t, tc.expect, err)
			require.Zero(t, dec)
			isTombstone, err := EncodedMVCCValueIsTombstone(tc.enc)
			require.Equal(t, tc.expect, err)
			require.False(t, isTombstone)
		})
	}
}

func mvccValueBenchmarkConfigs() (
	headers map[string]enginepb.MVCCValueHeader,
	values map[string]roachpb.Value,
) {
	headers = map[string]enginepb.MVCCValueHeader{
		"empty":                  {},
		"local walltime":         {LocalTimestamp: hlc.ClockTimestamp{WallTime: 1643550788737652545}},
		"local walltime+logical": {LocalTimestamp: hlc.ClockTimestamp{WallTime: 1643550788737652545, Logical: 4096}},
		"omit in rangefeeds":     {OmitInRangefeeds: true},
	}
	if testing.Short() {
		// Reduce the number of configurations in short mode.
		delete(headers, "local walltime")
		delete(headers, "omit in rangefeeds")
	}
	values = map[string]roachpb.Value{
		"tombstone": {},
		"short":     roachpb.MakeValueFromString("foo"),
		"long":      roachpb.MakeValueFromBytes(bytes.Repeat([]byte{1}, 4096)),
	}
	return headers, values
}

func BenchmarkEncodeMVCCValue(b *testing.B) {
	DisableMetamorphicSimpleValueEncoding(b)
	headers, values := mvccValueBenchmarkConfigs()
	for hDesc, h := range headers {
		for vDesc, v := range values {
			name := fmt.Sprintf("header=%s/value=%s", hDesc, vDesc)
			mvccValue := MVCCValue{MVCCValueHeader: h, Value: v}
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					res, err := EncodeMVCCValue(mvccValue)
					if err != nil { // for performance
						require.NoError(b, err)
					}
					_ = res
				}
			})
		}
	}
}

func BenchmarkDecodeMVCCValue(b *testing.B) {
	headers, values := mvccValueBenchmarkConfigs()
	for hDesc, h := range headers {
		for vDesc, v := range values {
			for _, inline := range []bool{false, true} {
				name := fmt.Sprintf("header=%s/value=%s/inline=%t", hDesc, vDesc, inline)
				mvccValue := MVCCValue{MVCCValueHeader: h, Value: v}
				buf, err := EncodeMVCCValue(mvccValue)
				require.NoError(b, err)
				b.Run(name, func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						var res MVCCValue
						var err error
						if inline {
							var ok bool
							res, ok, err = tryDecodeSimpleMVCCValue(buf)
							if !ok && err == nil {
								res, err = decodeExtendedMVCCValue(buf)
							}
						} else {
							res, err = DecodeMVCCValue(buf)
						}
						if err != nil { // for performance
							require.NoError(b, err)
						}
						_ = res
					}
				})
			}
		}
	}
}

func BenchmarkMVCCValueIsTombstone(b *testing.B) {
	headers, values := mvccValueBenchmarkConfigs()
	for hDesc, h := range headers {
		for vDesc, v := range values {
			name := fmt.Sprintf("header=%s/value=%s", hDesc, vDesc)
			mvccValue := MVCCValue{MVCCValueHeader: h, Value: v}
			buf, err := EncodeMVCCValue(mvccValue)
			require.NoError(b, err)
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					isTombstone, err := EncodedMVCCValueIsTombstone(buf)
					if err != nil { // for performance
						require.NoError(b, err)
					}
					_ = isTombstone
				}
			})
		}
	}
}

// TODO(erikgrinaker): Use testutils/storageutils instead when test code is
// moved to storage_test to avoid circular deps.
func stringValue(s string) MVCCValue {
	return MVCCValue{Value: roachpb.MakeValueFromString(s)}
}

func stringValueRaw(s string) []byte {
	b, err := EncodeMVCCValue(MVCCValue{Value: roachpb.MakeValueFromString(s)})
	if err != nil {
		panic(err)
	}
	return b
}

func tombstoneLocalTS(localTS int) MVCCValue {
	return MVCCValue{
		MVCCValueHeader: enginepb.MVCCValueHeader{
			LocalTimestamp: hlc.ClockTimestamp{WallTime: int64(localTS)},
		},
	}
}

func tombstoneLocalTSRaw(localTS int) []byte {
	b, err := EncodeMVCCValue(tombstoneLocalTS(localTS))
	if err != nil {
		panic(err)
	}
	return b
}
