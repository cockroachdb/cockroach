// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package roachpb

import (
	"math/rand"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

func TestInternalSeriesDataMarshaller(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	for range 1000 {
		data := NewPopulatedInternalTimeSeriesData(rng, false /* easy */)
		expected, err := data.Marshal()
		require.NoError(t, err)
		require.Equal(t, len(expected), data.Size())
		marshaller := MakeInternalTimeSeriesDataMarshaller(data)
		require.Equal(t, len(expected), marshaller.Size())
		buf := make([]byte, len(expected))
		marshaller.MarshalToSizedBuffer(buf)
		require.Equal(t, expected, buf)
	}
}

func TestInternalSeriesDataUnmarshalReusingSlices(t *testing.T) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	var m InternalTimeSeriesData
	for range 1000 {
		data := NewPopulatedInternalTimeSeriesData(rng, false /* easy */)
		buf, err := data.Marshal()
		require.NoError(t, err)
		require.NoError(t, m.UnmarshalReusingSlices(buf))
		require.Truef(t, data.Equal(m), "expected:\n%s\nactual:\n%s", data.String(), m.String())
		m.ResetRetainingSlices()
		// Verify that m is equal to the empty struct.
		require.True(t, m.Equal(&InternalTimeSeriesData{}))
	}

	// Test that we don't allocate when unmarshalling.
	t.Run("allocs", func(t *testing.T) {
		// Generate some buffers.
		var bufs [100][]byte
		for i := range bufs {
			data := NewPopulatedInternalTimeSeriesData(rng, false /* easy */)
			// We only care about the columnar format.
			data.Samples = nil
			var err error
			bufs[i], err = data.Marshal()
			require.NoError(t, err)
		}

		var i int
		m = InternalTimeSeriesData{}
		require.Zero(t, int(testing.AllocsPerRun(1000, func() {
			require.NoError(t, m.UnmarshalReusingSlices(bufs[i%len(bufs)]))
			i++
			m.ResetRetainingSlices()
		})))
	})
}

func BenchmarkInternalSeriesDataUnmarshalReusingSlices(b *testing.B) {
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	var bufs [1024][]byte
	for i := range bufs[:] {
		data := NewPopulatedInternalTimeSeriesData(rng, false /* easy */)
		// We only care about the columnar format.
		data.Samples = nil
		var err error
		bufs[i], err = data.Marshal()
		require.NoError(b, err)
	}
	b.ResetTimer()
	b.ReportAllocs()
	var m InternalTimeSeriesData
	for i := range b.N {
		require.NoError(b, m.UnmarshalReusingSlices(bufs[i%1024]))
		m.ResetRetainingSlices()
	}
}
