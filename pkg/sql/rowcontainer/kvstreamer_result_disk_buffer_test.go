// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rowcontainer

import (
	"math/rand"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvstreamer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestRoundTripResult verifies that we can serialize and deserialize a Result
// without any corruption. Note that fields that are kept in-memory
// ('Position', 'memoryTok', 'subRequestIdx', 'subRequestDone', and
// 'scanComplete') aren't set on the test Results.
func TestRoundTripResult(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	scratchRow := make(rowenc.EncDatumRow, len(inOrderResultsBufferSpillTypeSchema))
	var da tree.DatumAlloc

	assertRoundTrips := func(original kvstreamer.Result, reverse bool) {
		var actual kvstreamer.Result
		require.NoError(t, serialize(&original, scratchRow, &da))
		require.NoError(t, deserialize(&actual, scratchRow, &da, reverse))
		require.Equal(t, original, actual)
	}

	t.Run("get", func(t *testing.T) {
		r := makeResultWithGetResp(rng, false /* empty */)
		assertRoundTrips(r, false /* reverse */)
	})

	t.Run("empty get", func(t *testing.T) {
		r := makeResultWithGetResp(rng, true /* empty */)
		assertRoundTrips(r, false /* reverse */)
	})

	t.Run("scan", func(t *testing.T) {
		for _, reverse := range []bool{false, true} {
			t.Run("reverse="+strconv.FormatBool(reverse), func(t *testing.T) {
				r := makeResultWithScanResp(rng, reverse)
				assertRoundTrips(r, reverse)
			})
		}
	})
}

func makeResultWithGetResp(rng *rand.Rand, empty bool) kvstreamer.Result {
	var r kvstreamer.Result
	r.GetResp = &kvpb.GetResponse{}
	if !empty {
		rawBytes := make([]byte, rng.Intn(20)+1)
		rng.Read(rawBytes)
		r.GetResp.Value = &roachpb.Value{
			RawBytes: rawBytes,
			Timestamp: hlc.Timestamp{
				WallTime: rng.Int63(),
				Logical:  rng.Int31(),
			},
		}
	}
	return r
}

func makeResultWithScanResp(rng *rand.Rand, reverse bool) kvstreamer.Result {
	var r kvstreamer.Result
	// Sometimes generate zero-length batchResponses.
	batchResponses := make([][]byte, rng.Intn(20))
	for i := range batchResponses {
		batchResponse := make([]byte, rng.Intn(20)+1)
		rng.Read(batchResponse)
		batchResponses[i] = batchResponse
	}
	if reverse {
		r.ScanResp = &kvpb.ReverseScanResponse{
			BatchResponses: batchResponses,
		}
	} else {
		r.ScanResp = &kvpb.ScanResponse{
			BatchResponses: batchResponses,
		}
	}
	return r
}
