// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/stretchr/testify/require"
)

// TestSchemaIsStable asserts that the storage schema in doc.go does not change.
// If it does (e.g. there is a new key), this test fails and nudges the author
// to think through the implications.
//
// When changing the schema, it is a good idea to read the comments in doc.go
// around the modified sections, and understand the lifecycle of the relevant
// keys. For example, look at the code where the related keys are read, written,
// modified, and removed. Note that it can be tricky: e.g. some code might be
// removing a whole section of keys (like unreplicated RangeID-local space),
// without explicitly calling out individual keys.
//
// Once this due diligence is done, it is ok to fix this test.
func TestSchemaIsStable(t *testing.T) {
	pos := make(map[string]struct {
		first int
		last  int
	})
	add := func(k string, v int) {
		p, found := pos[k]
		if found {
			p.last++
			require.Equal(t, p.last, v)
		} else {
			p.first = v
			p.last = v
		}
		pos[k] = p
	}
	for i, ref := range schema {
		switch ref := ref.(type) {
		case []byte:
			add(string(ref), i)
		case roachpb.Key:
			add(string(ref), i)
		}
	}

	section := func(from, to string, items int) {
		t.Helper()
		fromPos, ok := pos[from]
		require.True(t, ok)
		toPos, ok := pos[to]
		require.True(t, ok)
		require.Greater(t, toPos.first, fromPos.last)
		require.Equal(t, items, toPos.first-fromPos.last-1)
	}

	// The replicated RangeID-local keys section.
	section(string(LocalRangeIDReplicatedInfix), string(localRangeIDUnreplicatedInfix), 8)
	// The unreplicated RangeID-local keys section.
	section(string(localRangeIDUnreplicatedInfix), string(LocalRangePrefix), 6)
	// The replicated range-local keys section.
	section(string(LocalRangePrefix), string(LocalStorePrefix), 4)
	// The store-local keys section.
	section(string(LocalStorePrefix), string(LocalRangeLockTablePrefix), 12)
}
