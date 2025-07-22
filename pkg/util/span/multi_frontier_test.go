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

func TestMultiFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	key := func(b byte) roachpb.Key { return []byte{b} }
	sp := func(start, end byte) roachpb.Span {
		return roachpb.Span{Key: key(start), EndKey: key(end)}
	}
	ts := func(wt int) hlc.Timestamp {
		return hlc.Timestamp{WallTime: int64(wt)}
	}

	// In this test, we'll simulate a few different tables with the ranges:
	// - 1: [a, d)
	// - 2: [d, f)
	// - 3: [f, k)

	partitioner := func(sp roachpb.Span) (byte, error) {
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

	f, err := span.NewMultiFrontier(partitioner, sp('a', 'd'), sp('d', 'f'), sp('f', 'k'))
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

// TODO add more tests that test the utility functions
