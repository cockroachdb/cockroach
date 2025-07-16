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
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestMultiFrontier_AddSpansAt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(testingTripartitePartitioner)
	require.NoError(t, err)
	require.Equal(t, ``, multiFrontierStr(f))

	// Add a single span.
	require.NoError(t, f.AddSpansAt(ts(2), sp('a', 'b')))
	require.Equal(t, ts(2), f.Frontier())
	require.Equal(t, `1: {{a-b}@2}`, multiFrontierStr(f))

	// Add another span in same partition but later timestamp.
	require.NoError(t, f.AddSpansAt(ts(3), sp('c', 'd')))
	require.Equal(t, ts(2), f.Frontier())
	require.Equal(t, `1: {{a-b}@2 {c-d}@3}`, multiFrontierStr(f))

	// Add another span in a different partition with lower timestamp.
	require.NoError(t, f.AddSpansAt(ts(1), sp('f', 'g')))
	require.Equal(t, ts(1), f.Frontier())
	require.Equal(t, `1: {{a-b}@2 {c-d}@3} 3: {{f-g}@1}`, multiFrontierStr(f))

	// Add an invalid span.
	err = f.AddSpansAt(ts(2), sp('a', 'h'))
	require.ErrorContains(t, err,
		"got partitioner error when attempting to add spans: invalid range")

	// Add a span that overlaps existing spans.
	require.NoError(t, f.AddSpansAt(ts(1), sp('a', 'd')))
	require.Equal(t, ts(1), f.Frontier())
	require.Equal(t, `1: {{a-b}@2 {b-c}@1 {c-d}@3} 3: {{f-g}@1}`, multiFrontierStr(f))
}

func TestMultiFrontier_Frontier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create an empty multi frontier.
	t.Run("empty multi frontier", func(t *testing.T) {
		f, err := span.NewMultiFrontier(testingTripartitePartitioner)
		require.NoError(t, err)
		require.Equal(t, ts(0), f.Frontier())
		require.Equal(t, ``, multiFrontierStr(f))
	})

	// Create an empty multi frontier with an initial timestamp.
	// The frontier will still be zero because no spans are being tracked.
	t.Run("empty multi frontier with initial timestamp", func(t *testing.T) {
		f, err := span.NewMultiFrontierAt(testingTripartitePartitioner, ts(2))
		require.NoError(t, err)
		require.Equal(t, ts(0), f.Frontier())
		require.Equal(t, ``, multiFrontierStr(f))
	})

	// Create a multi frontier tracking some spans and do various forwards.
	t.Run("non-empty multi frontier with forwards", func(t *testing.T) {
		f, err := span.NewMultiFrontierAt(testingTripartitePartitioner, ts(3),
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

func TestMultiFrontier_PeekFrontierSpan(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(testingTripartitePartitioner)
	require.NoError(t, err)
	require.Equal(t, roachpb.Span{}, f.PeekFrontierSpan())

	require.NoError(t, f.AddSpansAt(ts(2), sp('a', 'b')))
	require.Equal(t, sp('a', 'b'), f.PeekFrontierSpan())

	require.NoError(t, f.AddSpansAt(ts(1), sp('c', 'd')))
	require.Equal(t, sp('c', 'd'), f.PeekFrontierSpan())
}

func TestMultiFrontier_Forward(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(
		testingTripartitePartitioner, sp('a', 'd'), sp('d', 'f'), sp('f', 'k'))
	require.NoError(t, err)
	require.Equal(t, `1: {{a-d}@0} 2: {{d-f}@0} 3: {{f-k}@0}`, multiFrontierStr(f))

	forwarded, err := f.Forward(sp('a', 'b'), ts(2))
	require.NoError(t, err)
	require.False(t, forwarded)
	require.Equal(t, ts(0), f.Frontier())
	require.Equal(t, `1: {{a-b}@2 {b-d}@0} 2: {{d-f}@0} 3: {{f-k}@0}`, multiFrontierStr(f))

	_, err = f.Forward(sp('a', 'e'), ts(2))
	require.ErrorContains(t, err,
		"got partitioner error when attempting to forward: invalid range")

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

func TestMultiFrontier_Release(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(testingTripartitePartitioner, sp('a', 'b'), sp('d', 'f'))
	require.NoError(t, err)
	require.Equal(t, 2, f.Len())
	require.Equal(t, `1: {{a-b}@0} 2: {{d-f}@0}`, multiFrontierStr(f))

	f.Release()
	require.Equal(t, 0, f.Len())
	require.Equal(t, ``, multiFrontierStr(f))
}

func TestMultiFrontier_Entries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontierAt(testingTripartitePartitioner,
		ts(2), sp('a', 'b'), sp('d', 'f'))
	require.NoError(t, err)

	expected := map[string]hlc.Timestamp{
		sp('a', 'b').String(): ts(2),
		sp('d', 'f').String(): ts(2),
	}

	actual := make(map[string]hlc.Timestamp)
	for sp, ts := range f.Entries() {
		actual[sp.String()] = ts
	}

	require.Equal(t, expected, actual)
}

func TestMultiFrontier_SpanEntries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontierAt(testingTripartitePartitioner, ts(2), sp('a', 'b'), sp('d', 'f'))
	require.NoError(t, err)

	t.Run("entire span space", func(t *testing.T) {
		actual := make(map[string]hlc.Timestamp)
		for sp, ts := range f.SpanEntries(sp('a', 'k')) {
			actual[sp.String()] = ts
		}

		expected := map[string]hlc.Timestamp{
			sp('a', 'b').String(): ts(2),
			sp('d', 'f').String(): ts(2),
		}

		require.Equal(t, expected, actual)
	})

	t.Run("partial span space", func(t *testing.T) {
		actual := make(map[string]hlc.Timestamp)
		for sp, ts := range f.SpanEntries(sp('a', 'e')) {
			actual[sp.String()] = ts
		}

		expected := map[string]hlc.Timestamp{
			sp('a', 'b').String(): ts(2),
			sp('d', 'e').String(): ts(2),
		}

		require.Equal(t, expected, actual)
	})
}

func TestMultiFrontier_Len(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(testingTripartitePartitioner)
	require.NoError(t, err)
	require.Equal(t, 0, f.Len())

	// Add a single span.
	require.NoError(t, f.AddSpansAt(ts(1), sp('a', 'b')))
	require.Equal(t, 1, f.Len())
	require.Equal(t, `1: {{a-b}@1}`, multiFrontierStr(f))

	// Add another span in a different partition.
	require.NoError(t, f.AddSpansAt(ts(2), sp('f', 'g')))
	require.Equal(t, 2, f.Len())
	require.Equal(t, `1: {{a-b}@1} 3: {{f-g}@2}`, multiFrontierStr(f))

	// Add a span that merges with an existing span.
	require.NoError(t, f.AddSpansAt(ts(1), sp('b', 'c')))
	require.Equal(t, 2, f.Len())
	require.Equal(t, `1: {{a-c}@1} 3: {{f-g}@2}`, multiFrontierStr(f))
}

func TestMultiFrontier_String(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(testingTripartitePartitioner)
	require.NoError(t, err)
	require.Equal(t, ``, f.String())

	require.NoError(t, f.AddSpansAt(ts(1), sp('a', 'b')))
	require.NoError(t, f.AddSpansAt(ts(4), sp('e', 'f')))
	require.NoError(t, f.AddSpansAt(ts(9), sp('i', 'j')))

	// The partitions can be returned in any order so we sort
	// the returned string before making any assertions.
	sortStr := func(s string) string {
		ss := strings.Split(s, ", ")
		slices.Sort(ss)
		return strings.Join(ss, ", ")
	}
	require.Equal(t,
		`1: {[{a-b}@0.000000001,0]}, 2: {[{e-f}@0.000000004,0]}, 3: {[{i-j}@0.000000009,0]}`,
		sortStr(f.String()))
}

func TestMultiFrontier_Frontiers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	f, err := span.NewMultiFrontier(testingTripartitePartitioner)
	require.NoError(t, err)

	require.NoError(t, f.AddSpansAt(ts(1), sp('a', 'b')))
	require.NoError(t, f.AddSpansAt(ts(4), sp('e', 'f')))
	require.NoError(t, f.AddSpansAt(ts(9), sp('i', 'j')))

	require.ElementsMatch(t, []int{1, 2, 3}, slices.Collect(iterutil.Keys(f.Frontiers())))

	for partition, frontier := range f.Frontiers() {
		switch partition {
		case 1:
			require.Equal(t, ts(1), frontier.Frontier())
			require.Equal(t, sp('a', 'b'), frontier.PeekFrontierSpan())
		case 2:
			require.Equal(t, ts(4), frontier.Frontier())
			require.Equal(t, sp('e', 'f'), frontier.PeekFrontierSpan())
		case 3:
			require.Equal(t, ts(9), frontier.Frontier())
			require.Equal(t, sp('i', 'j'), frontier.PeekFrontierSpan())
		default:
			t.Fatalf("unknown partition: %d", partition)
		}
	}
}

// testingTripartitePartitioner partitions spans in the range [a, k) into
// one of three partitions:
// - 1: [a, d)
// - 2: [d, f)
// - 3: [f, k)
func testingTripartitePartitioner(sp roachpb.Span) (int, error) {
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

var _ span.PartitionerFunc[int] = testingTripartitePartitioner

func multiFrontierStr[P cmp.Ordered](f *span.MultiFrontier[P]) string {
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
