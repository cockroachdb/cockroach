// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestReadAmplification(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	info := func(level int, size int64) SSTableInfo {
		return SSTableInfo{
			Level: level,
			Size:  size,
		}
	}

	tables1 := SSTableInfos{
		info(0, 0),
		info(0, 0),
		info(0, 0),
		info(1, 0),
	}
	if a, e := tables1.readAmplification(-1), int64(4); a != e {
		t.Errorf("got %d, expected %d", a, e)
	}

	tables2 := SSTableInfos{
		info(0, 0),
		info(1, 0),
		info(2, 0),
		info(3, 0),
	}
	if a, e := tables2.readAmplification(-1), int64(4); a != e {
		t.Errorf("got %d, expected %d", a, e)
	}

	tables3 := SSTableInfos{
		info(1, 0),
		info(0, 0),
		info(0, 0),
		info(0, 0),
		info(1, 0),
		info(1, 0),
		info(2, 0),
		info(3, 0),
		info(6, 0),
	}
	if a, e := tables3.readAmplification(-1), int64(7); a != e {
		t.Errorf("got %d, expected %d", a, e)
	}
	if a, e := tables3.readAmplification(2), int64(6); a != e {
		t.Errorf("got %d, expected %d", a, e)
	}
	if a, e := tables3.readAmplification(1), int64(5); a != e {
		t.Errorf("got %d, expected %d", a, e)
	}
}

func stringToKey(s string) MVCCKey {
	return MakeMVCCMetadataKey([]byte(s))
}

func createTestSSTableInfos() SSTableInfos {
	ssti := SSTableInfos{
		// Level 0.
		{Level: 0, Size: 20, Start: stringToKey("a"), End: stringToKey("z")},
		{Level: 0, Size: 15, Start: stringToKey("a"), End: stringToKey("k")},
		// Level 1.
		{Level: 1, Size: 200, Start: stringToKey("a"), End: stringToKey("j")},
		{Level: 1, Size: 100, Start: stringToKey("k"), End: stringToKey("o")},
		{Level: 1, Size: 100, Start: stringToKey("r"), End: stringToKey("t")},
		// Level 2.
		{Level: 2, Size: 201, Start: stringToKey("a"), End: stringToKey("c")},
		{Level: 2, Size: 200, Start: stringToKey("d"), End: stringToKey("f")},
		{Level: 2, Size: 300, Start: stringToKey("h"), End: stringToKey("r")},
		{Level: 2, Size: 405, Start: stringToKey("s"), End: stringToKey("z")},
		// Level 3.
		{Level: 3, Size: 667, Start: stringToKey("a"), End: stringToKey("c")},
		{Level: 3, Size: 230, Start: stringToKey("d"), End: stringToKey("f")},
		{Level: 3, Size: 332, Start: stringToKey("h"), End: stringToKey("i")},
		{Level: 3, Size: 923, Start: stringToKey("k"), End: stringToKey("n")},
		{Level: 3, Size: 143, Start: stringToKey("n"), End: stringToKey("o")},
		{Level: 3, Size: 621, Start: stringToKey("p"), End: stringToKey("s")},
		{Level: 3, Size: 411, Start: stringToKey("u"), End: stringToKey("x")},
		// Level 4.
		{Level: 4, Size: 215, Start: stringToKey("a"), End: stringToKey("b")},
		{Level: 4, Size: 211, Start: stringToKey("b"), End: stringToKey("d")},
		{Level: 4, Size: 632, Start: stringToKey("e"), End: stringToKey("f")},
		{Level: 4, Size: 813, Start: stringToKey("f"), End: stringToKey("h")},
		{Level: 4, Size: 346, Start: stringToKey("h"), End: stringToKey("j")},
		{Level: 4, Size: 621, Start: stringToKey("j"), End: stringToKey("l")},
		{Level: 4, Size: 681, Start: stringToKey("m"), End: stringToKey("o")},
		{Level: 4, Size: 521, Start: stringToKey("o"), End: stringToKey("r")},
		{Level: 4, Size: 135, Start: stringToKey("r"), End: stringToKey("t")},
		{Level: 4, Size: 622, Start: stringToKey("t"), End: stringToKey("v")},
		{Level: 4, Size: 672, Start: stringToKey("x"), End: stringToKey("z")},
	}
	sort.Sort(ssti)
	return ssti
}

func TestSSTableInfosByLevel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ssti := NewSSTableInfosByLevel(createTestSSTableInfos())

	// First, verify that each level is sorted by start key, not size.
	for level, l := range ssti.levels {
		if level == 0 {
			continue
		}
		lastInfo := l[0]
		for _, info := range l[1:] {
			if !lastInfo.Start.Less(info.Start) {
				t.Errorf("sort failed (%s >= %s) for level %d", lastInfo.Start, info.Start, level)
			}
		}
	}
	if a, e := ssti.MaxLevel(), 4; a != e {
		t.Errorf("expected MaxLevel() == %d; got %d", e, a)
	}

	// Next, verify various contiguous overlap scenarios.
	testCases := []struct {
		span        roachpb.Span
		expMaxLevel int
	}{
		// The full a-z span overlaps more than two SSTables at all levels L1-L4
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("z")}, expMaxLevel: 0},
		// The a-j span overlaps the first three SSTables in L2, so max level is L1.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("j")}, expMaxLevel: 1},
		// The k-o span overlaps only two adjacent L4 SSTs: j-l & m-o.
		{span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("o")}, expMaxLevel: 4},
		// The K0-o0 span hits three SSTs in L4: j-l, m-o, & o-r.
		{span: roachpb.Span{Key: roachpb.Key("k0"), EndKey: roachpb.Key("o0")}, expMaxLevel: 3},
		// The k-z span overlaps the last 4 SSTs in L3.
		{span: roachpb.Span{Key: roachpb.Key("k"), EndKey: roachpb.Key("z")}, expMaxLevel: 2},
		// The c-c0 span overlaps only the second L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("c0")}, expMaxLevel: 4},
		// The a-f span full overlaps the first three L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("f")}, expMaxLevel: 3},
		// The a-d0 span only overlaps the first two L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d0")}, expMaxLevel: 4},
		// The a-e span only overlaps the first two L4 SSTs. It only is adjacent to the 3rd.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("e")}, expMaxLevel: 4},
		// The a-d span overlaps fully the first two L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("d")}, expMaxLevel: 4},
		// The a-a0 span overlaps only the first L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("a0")}, expMaxLevel: 4},
		// The 0-1 span doesn't overlap any L4 SSTs.
		{span: roachpb.Span{Key: roachpb.Key("0"), EndKey: roachpb.Key("1")}, expMaxLevel: 4},
		// The Z-a span doesn't overlap any L4 SSTs, just touches the start of the first.
		{span: roachpb.Span{Key: roachpb.Key("Z"), EndKey: roachpb.Key("a")}, expMaxLevel: 4},
		// The Z-a0 span overlaps only the first L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("Z"), EndKey: roachpb.Key("a0")}, expMaxLevel: 4},
		// The z-z0 span doesn't overlap any L4 SSTs, just touches the end of the last.
		{span: roachpb.Span{Key: roachpb.Key("z"), EndKey: roachpb.Key("z0")}, expMaxLevel: 4},
		// The y-z0 span overlaps the last L4 SST.
		{span: roachpb.Span{Key: roachpb.Key("y"), EndKey: roachpb.Key("z0")}, expMaxLevel: 4},
	}

	for _, test := range testCases {
		t.Run(fmt.Sprintf("%s-%s", test.span.Key, test.span.EndKey), func(t *testing.T) {
			maxLevel := ssti.MaxLevelSpanOverlapsContiguousSSTables(test.span)
			if test.expMaxLevel != maxLevel {
				t.Errorf("expected max level %d; got %d", test.expMaxLevel, maxLevel)
			}
		})
	}
}
