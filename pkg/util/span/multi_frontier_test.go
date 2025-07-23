// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package span_test

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMultiFrontierBasic(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(
		testingThreeRangePartitioner, sp('a', 'd'), sp('d', 'f'), sp('f', 'k'))
	require.NoError(t, err)
	require.Equal(t, `1: {{a-d}@0} 2: {{d-f}@0} 3: {{f-k}@0}`, multiFrontierStr(f))

	forwarded, err := f.Forward(sp('a', 'b'), ts(2))
	require.NoError(t, err)
	require.False(t, forwarded)
	require.Equal(t, ts(0), f.Frontier())
	require.Equal(t, `1: {{a-b}@2 {b-d}@0} 2: {{d-f}@0} 3: {{f-k}@0}`, multiFrontierStr(f))

	_, err = f.Forward(sp('a', 'e'), ts(2))
	require.ErrorContains(t, err, "got partitioner error when attempting to forward: invalid range")

	forwarded, err = f.Forward(sp('b', 'd'), ts(2))
	require.NoError(t, err)
	require.False(t, forwarded)
	require.Equal(t, ts(0), f.Frontier())
	require.Equal(t, `1: {{a-d}@2} 2: {{d-f}@0} 3: {{f-k}@0}`, multiFrontierStr(f))

	forwarded, err = f.Forward(sp('f', 'k'), ts(2))
	require.NoError(t, err)
	require.False(t, forwarded)
	require.Equal(t, ts(0), f.Frontier())
	require.Equal(t, `1: {{a-d}@2} 2: {{d-f}@0} 3: {{f-k}@2}`, multiFrontierStr(f))

	forwarded, err = f.Forward(sp('d', 'f'), ts(2))
	require.NoError(t, err)
	require.True(t, forwarded)
	require.Equal(t, ts(2), f.Frontier())
	require.Equal(t, `1: {{a-d}@2} 2: {{d-f}@2} 3: {{f-k}@2}`, multiFrontierStr(f))
}

func TestMultiFrontier_AddSpansAt(t *testing.T) {
	f, err := span.NewMultiFrontier(testingThreeRangePartitioner)
	require.NoError(t, err)
	require.Equal(t, ``, multiFrontierStr(f))

	// Add a single span.
	require.NoError(t, f.AddSpansAt(ts(2), sp('a', 'b')))
	require.Equal(t, ts(2), f.Frontier())
	require.Equal(t, `1: {{a-b}@2}`, multiFrontierStr(f))

	// Add another span in same partition but different timestamp.
	require.NoError(t, f.AddSpansAt(ts(3), sp('c', 'd')))
	require.Equal(t, ts(2), f.Frontier())
	require.Equal(t, `1: {{a-b}@2 {c-d}@3}`, multiFrontierStr(f))

	// Add another span in a different partition with lower timestamp.
	require.NoError(t, f.AddSpansAt(ts(1), sp('f', 'g')))
	require.Equal(t, ts(1), f.Frontier())
	require.Equal(t, `1: {{a-b}@2 {c-d}@3} 3: {{f-g}@1}`, multiFrontierStr(f))
}

func TestMultiFrontier_Frontier(t *testing.T) {
	// Create an empty multi frontier.
	t.Run("empty multi frontier", func(t *testing.T) {
		f, err := span.NewMultiFrontier(testingThreeRangePartitioner)
		require.NoError(t, err)
		require.Equal(t, ts(0), f.Frontier())
		require.Equal(t, ``, multiFrontierStr(f))
	})

	// Create an empty multi frontier with an initial timestamp.
	// The frontier will still be zero because no spans are being tracked.
	t.Run("empty multi frontier with initial timestamp", func(t *testing.T) {
		f, err := span.NewMultiFrontierAt(testingThreeRangePartitioner, ts(2))
		require.NoError(t, err)
		require.Equal(t, ts(0), f.Frontier())
		require.Equal(t, ``, multiFrontierStr(f))
	})

	// Create a multi frontier tracking some spans and do various forwards.
	t.Run("non-empty multi frontier with forwards", func(t *testing.T) {
		f, err := span.NewMultiFrontierAt(testingThreeRangePartitioner, ts(3),
			sp('a', 'b'), sp('d', 'e'))
		require.NoError(t, err)
		require.Equal(t, ts(3), f.Frontier())
		require.Equal(t, `1: {{a-b}@3} 2: {{d-e}@3}`, multiFrontierStr(f))

		forwarded, err := f.Forward(sp('a', 'b'), ts(5))
		require.NoError(t, err)
		require.False(t, forwarded)
		require.Equal(t, ts(3), f.Frontier())
		require.Equal(t, `1: {{a-b}@5} 2: {{d-e}@3}`, multiFrontierStr(f))

		forwarded, err = f.Forward(sp('d', 'e'), ts(5))
		require.NoError(t, err)
		require.True(t, forwarded)
		require.Equal(t, ts(5), f.Frontier())
		require.Equal(t, `1: {{a-b}@5} 2: {{d-e}@5}`, multiFrontierStr(f))
	})
}

// testingThreeRangePartitioner partitions spans in the range [a, k) into:
// - 1: [a, d)
// - 2: [d, f)
// - 3: [f, k)
func testingThreeRangePartitioner(sp roachpb.Span) (byte, error) {
	// TODO maybe sort by the first character instead
	if len(sp.Key) != 1 || len(sp.EndKey) != 1 {
		return 0, errors.Newf("expected single character keys: %s", sp)
	}
	switch {
	case key('a'-1).Less(sp.Key) && sp.EndKey.Less(key('d'+1)):
		return 1, nil
	case key('d'-1).Less(sp.Key) && sp.EndKey.Less(key('f'+1)):
		return 2, nil
	case key('f'-1).Less(sp.Key) && sp.EndKey.Less(key('k'+1)):
		return 3, nil
	default:
		return 0, errors.Newf("invalid range: %s", sp)
	}
}

func multiFrontierStr[T cmp.Ordered](f *span.MultiFrontier[T]) string {
	if f == nil {
		return ""
	}
	var buf strings.Builder
	frontiers := maps.Collect(f.Frontiers())
	for _, partition := range slices.Sorted(maps.Keys(frontiers)) {
		if buf.Len() != 0 {
			buf.WriteByte(' ')
		}
		buf.WriteString(fmt.Sprintf("%v: {", partition))
		var frontierBuf strings.Builder
		for sp, ts := range frontiers[partition].Entries() {
			if frontierBuf.Len() != 0 {
				frontierBuf.WriteByte(' ')
			}
			frontierBuf.WriteString(fmt.Sprintf(`%s@%d`, sp, ts.WallTime))
		}
		buf.WriteString(frontierBuf.String())
		buf.WriteByte('}')
	}
	return buf.String()
}

func key(b byte) roachpb.Key { return []byte{b} }

func sp(start, end byte) roachpb.Span {
	return roachpb.Span{Key: key(start), EndKey: key(end)}
}

func ts(wt int) hlc.Timestamp {
	return hlc.Timestamp{WallTime: int64(wt)}
}

// TODO add more tests that test the utility functions
