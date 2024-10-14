// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"fmt"
	"math"
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

	testcases := map[string]struct {
		localTs   hlc.Timestamp
		versionTs hlc.Timestamp
		expect    hlc.Timestamp
	}{
		"no local timestamp":      {ts0, ts2, ts2},
		"smaller local timestamp": {ts1, ts2, ts1},
		"equal local timestamp":   {ts2, ts2, ts2},
		"larger local timestamp":  {ts2, ts1, ts2},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			mvccVal := MVCCValue{}
			mvccVal.LocalTimestamp = hlc.ClockTimestamp(tc.localTs)

			require.Equal(t, hlc.ClockTimestamp(tc.expect), mvccVal.GetLocalTimestamp(tc.versionTs))
		})
	}
}

func TestEncodeMVCCValueForExport(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var strVal, intVal roachpb.Value
	strVal.SetString("foo")
	intVal.SetInt(17)

	var importEpoch uint32 = 3
	tsHeader := enginepb.MVCCValueHeader{LocalTimestamp: hlc.ClockTimestamp{WallTime: 9}}

	valHeaderFull := tsHeader
	valHeaderFull.ImportEpoch = importEpoch

	jobIDHeader := enginepb.MVCCValueHeader{ImportEpoch: importEpoch}

	testcases := map[string]struct {
		val    MVCCValue
		expect MVCCValue
	}{
		"noHeader":   {val: MVCCValue{Value: intVal}, expect: MVCCValue{Value: intVal}},
		"tsHeader":   {val: MVCCValue{MVCCValueHeader: tsHeader, Value: intVal}, expect: MVCCValue{Value: intVal}},
		"jobIDOnly":  {val: MVCCValue{MVCCValueHeader: jobIDHeader, Value: intVal}, expect: MVCCValue{MVCCValueHeader: jobIDHeader, Value: intVal}},
		"fullHeader": {val: MVCCValue{MVCCValueHeader: valHeaderFull, Value: intVal}, expect: MVCCValue{MVCCValueHeader: jobIDHeader, Value: intVal}},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			encodedVal, _, err := EncodeMVCCValueForExport(tc.val, nil)
			require.NoError(t, err)
			strippedMVCCVal, err := DecodeMVCCValue(encodedVal)
			require.NoError(t, err)
			require.Equal(t, tc.expect, strippedMVCCVal)
		})
	}

}
func TestMVCCValueFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var strVal, intVal roachpb.Value
	strVal.SetString("foo")
	intVal.SetInt(17)
	var importEpoch uint32 = 3
	var originID uint32 = 1
	var originTs = hlc.Timestamp{WallTime: 1, Logical: 1}

	valHeader := enginepb.MVCCValueHeader{}
	valHeader.LocalTimestamp = hlc.ClockTimestamp{WallTime: 9}

	valHeaderFull := valHeader
	valHeaderFull.ImportEpoch = importEpoch
	valHeaderFull.OriginID = originID
	valHeaderFull.OriginTimestamp = originTs

	valHeaderWithJobIDOnly := enginepb.MVCCValueHeader{ImportEpoch: importEpoch}

	valHeaderWithOriginIDOnly := enginepb.MVCCValueHeader{OriginID: originID}

	valHeaderWithOriginTsOnly := enginepb.MVCCValueHeader{OriginTimestamp: originTs}

	testcases := map[string]struct {
		val    MVCCValue
		expect string
	}{
		"tombstone":            {val: MVCCValue{}, expect: "/<empty>"},
		"bytes":                {val: MVCCValue{Value: strVal}, expect: "/BYTES/foo"},
		"int":                  {val: MVCCValue{Value: intVal}, expect: "/INT/17"},
		"timestamp+tombstone":  {val: MVCCValue{MVCCValueHeader: valHeader}, expect: "{localTs=0.000000009,0}/<empty>"},
		"timestamp+bytes":      {val: MVCCValue{MVCCValueHeader: valHeader, Value: strVal}, expect: "{localTs=0.000000009,0}/BYTES/foo"},
		"timestamp+int":        {val: MVCCValue{MVCCValueHeader: valHeader, Value: intVal}, expect: "{localTs=0.000000009,0}/INT/17"},
		"jobid+tombstone":      {val: MVCCValue{MVCCValueHeader: valHeaderWithJobIDOnly}, expect: "{importEpoch=3}/<empty>"},
		"jobid+bytes":          {val: MVCCValue{MVCCValueHeader: valHeaderWithJobIDOnly, Value: strVal}, expect: "{importEpoch=3}/BYTES/foo"},
		"jobid+int":            {val: MVCCValue{MVCCValueHeader: valHeaderWithJobIDOnly, Value: intVal}, expect: "{importEpoch=3}/INT/17"},
		"originid+tombstone":   {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginIDOnly}, expect: "{originID=1}/<empty>"},
		"originid+bytes":       {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginIDOnly, Value: strVal}, expect: "{originID=1}/BYTES/foo"},
		"originid+int":         {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginIDOnly, Value: intVal}, expect: "{originID=1}/INT/17"},
		"fullheader+tombstone": {val: MVCCValue{MVCCValueHeader: valHeaderFull}, expect: "{localTs=0.000000009,0, importEpoch=3, originID=1, originTs=0.000000001,1}/<empty>"},
		"fullheader+bytes":     {val: MVCCValue{MVCCValueHeader: valHeaderFull, Value: strVal}, expect: "{localTs=0.000000009,0, importEpoch=3, originID=1, originTs=0.000000001,1}/BYTES/foo"},
		"fullheader+int":       {val: MVCCValue{MVCCValueHeader: valHeaderFull, Value: intVal}, expect: "{localTs=0.000000009,0, importEpoch=3, originID=1, originTs=0.000000001,1}/INT/17"},
		"origints+tombstone":   {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginTsOnly}, expect: "{originTs=0.000000001,1}/<empty>"},
		"origints+bytes":       {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginTsOnly, Value: strVal}, expect: "{originTs=0.000000001,1}/BYTES/foo"},
		"origints+int":         {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginTsOnly, Value: intVal}, expect: "{originTs=0.000000001,1}/INT/17"},
	}
	for name, tc := range testcases {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.expect, tc.val.String())
		})
	}
}

func TestEncodeDecodeMVCCValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Force the production fast path by deactivating the
	// `disableSimpleValueEncoding` test constant.
	DisableMetamorphicSimpleValueEncoding(t)

	var strVal, intVal roachpb.Value
	strVal.SetString("foo")
	intVal.SetInt(17)
	var importEpoch uint32 = 3
	var originID uint32 = 1
	var originTs = hlc.Timestamp{WallTime: math.MaxInt64, Logical: 1}

	valHeader := enginepb.MVCCValueHeader{}
	valHeader.LocalTimestamp = hlc.ClockTimestamp{WallTime: 9}

	valHeaderFull := valHeader
	valHeaderFull.ImportEpoch = importEpoch
	valHeaderFull.OriginID = originID
	valHeaderFull.OriginTimestamp = originTs

	valHeaderWithJobIDOnly := enginepb.MVCCValueHeader{ImportEpoch: importEpoch}
	valHeaderWithOriginIDOnly := enginepb.MVCCValueHeader{OriginID: originID}
	valHeaderWithOriginTsOnly := enginepb.MVCCValueHeader{OriginTimestamp: originTs}

	testcases := map[string]struct {
		val MVCCValue
	}{
		"tombstone":                    {val: MVCCValue{}},
		"bytes":                        {val: MVCCValue{Value: strVal}},
		"int":                          {val: MVCCValue{Value: intVal}},
		"header+tombstone":             {val: MVCCValue{MVCCValueHeader: valHeader}},
		"header+bytes":                 {val: MVCCValue{MVCCValueHeader: valHeader, Value: strVal}},
		"header+int":                   {val: MVCCValue{MVCCValueHeader: valHeader, Value: intVal}},
		"headerJobIDOnly+tombstone":    {val: MVCCValue{MVCCValueHeader: valHeaderWithJobIDOnly}},
		"headerJobIDOnly+bytes":        {val: MVCCValue{MVCCValueHeader: valHeaderWithJobIDOnly, Value: strVal}},
		"headerJobIDOnly+int":          {val: MVCCValue{MVCCValueHeader: valHeaderWithJobIDOnly, Value: intVal}},
		"headerOriginIDOnly+tombstone": {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginIDOnly}},
		"headerOriginIDOnly+bytes":     {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginIDOnly, Value: strVal}},
		"headerOriginIDOnly+int":       {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginIDOnly, Value: intVal}},
		"headerFull+tombstone":         {val: MVCCValue{MVCCValueHeader: valHeaderFull}},
		"headerFull+bytes":             {val: MVCCValue{MVCCValueHeader: valHeaderFull, Value: strVal}},
		"headerFull+int":               {val: MVCCValue{MVCCValueHeader: valHeaderFull, Value: intVal}},
		"headerOriginTsOnly+tombstone": {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginTsOnly}},
		"headerOriginTsOnly+bytes":     {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginTsOnly, Value: strVal}},
		"headerOriginTsOnly+int":       {val: MVCCValue{MVCCValueHeader: valHeaderWithOriginTsOnly, Value: intVal}},
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
		t.Run("DeocdeValueFromMVCCValue/"+name, func(t *testing.T) {
			enc, err := EncodeMVCCValue(tc.val)
			require.NoError(t, err)
			assert.Equal(t, encodedMVCCValueSize(tc.val), len(enc))

			dec, err := DecodeValueFromMVCCValue(enc)
			require.NoError(t, err)

			if len(dec.RawBytes) == 0 {
				dec.RawBytes = nil // normalize
			}

			require.Equal(t, tc.val.Value, dec)
			require.Equal(t, tc.val.IsTombstone(), len(dec.RawBytes) == 0)
			isTombstone, err := EncodedMVCCValueIsTombstone(enc)
			require.NoError(t, err)
			require.Equal(t, tc.val.IsTombstone(), isTombstone)
		})
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
		t.Run("DecodeValueFromMVCCValue/"+name, func(t *testing.T) {
			dec, err := DecodeValueFromMVCCValue(tc.enc)
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
		"empty":                        {},
		"jobID":                        {ImportEpoch: 3},
		"local walltime":               {LocalTimestamp: hlc.ClockTimestamp{WallTime: 1643550788737652545}},
		"local walltime+logical":       {LocalTimestamp: hlc.ClockTimestamp{WallTime: 1643550788737652545, Logical: 4096}},
		"omit in rangefeeds":           {OmitInRangefeeds: true},
		"local walltime+jobID":         {LocalTimestamp: hlc.ClockTimestamp{WallTime: 1643550788737652545}, ImportEpoch: 3},
		"local walltime+logical+jobID": {LocalTimestamp: hlc.ClockTimestamp{WallTime: 1643550788737652545, Logical: 4096}, ImportEpoch: 3},
	}
	if testing.Short() {
		// Reduce the number of configurations in short mode.
		delete(headers, "local walltime")
		delete(headers, "omit in rangefeeds")
		delete(headers, "local walltime+jobID")
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

func BenchmarkEncodeMVCCValueForExport(b *testing.B) {
	DisableMetamorphicSimpleValueEncoding(b)
	headers, values := mvccValueBenchmarkConfigs()
	for hDesc, h := range headers {
		for vDesc, v := range values {
			name := fmt.Sprintf("header=%s/value=%s", hDesc, vDesc)
			mvccValue := MVCCValue{MVCCValueHeader: h, Value: v}
			var buf []byte
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var err error
					buf, _, err = EncodeMVCCValueForExport(mvccValue, buf[:0])
					if err != nil { // for performance
						require.NoError(b, err)
					}
					_ = buf
				}
			})
		}
	}
}

func BenchmarkEncodeMVCCValueWithAllocator(b *testing.B) {
	DisableMetamorphicSimpleValueEncoding(b)
	headers, values := mvccValueBenchmarkConfigs()
	for hDesc, h := range headers {
		for vDesc, v := range values {
			name := fmt.Sprintf("header=%s/value=%s", hDesc, vDesc)
			mvccValue := MVCCValue{MVCCValueHeader: h, Value: v}
			var buf []byte
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					var err error
					buf, _, err = EncodeMVCCValueToBuf(mvccValue, buf[:0])
					if err != nil { // for performance
						require.NoError(b, err)
					}
					_ = buf
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
								res, err = decodeExtendedMVCCValue(buf, true)
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

func BenchmarkDecodeValueFromMVCCValue(b *testing.B) {
	headers, values := mvccValueBenchmarkConfigs()
	for hDesc, h := range headers {
		for vDesc, v := range values {
			name := fmt.Sprintf("header=%s/value=%s", hDesc, vDesc)
			mvccValue := MVCCValue{MVCCValueHeader: h, Value: v}
			buf, err := EncodeMVCCValue(mvccValue)
			require.NoError(b, err)
			b.Run(name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					res, err := DecodeValueFromMVCCValue(buf)
					if err != nil { // for performance
						require.NoError(b, err)
					}
					_ = res
				}
			})
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
