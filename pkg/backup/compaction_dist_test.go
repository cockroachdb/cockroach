// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package backup

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cloud/cloudpb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestCreateCompactionCorePlacements(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	testcases := []struct {
		name                 string
		entries              []execinfrapb.RestoreSpanEntry
		topology             mockTopology
		expectedDistribution [][]roachpb.Span
	}{
		{
			name:     "single node, single span entry",
			entries:  entries(entry("a", "b", "")),
			topology: topology(instance(1, "")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b")},
			},
		},
		{
			name:     "single node, multiple span entries",
			entries:  entries(entry("a", "b", ""), entry("c", "d", ""), entry("e", "f", "")),
			topology: topology(instance(1, "")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b"), mockSpan("c", "d"), mockSpan("e", "f")},
			},
		},
		{
			name:     "single node, multiple overlapping span entries",
			entries:  entries(entry("a", "b", ""), entry("b", "d", ""), entry("e", "f", "")),
			topology: topology(instance(1, "")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "d"), mockSpan("e", "f")},
			},
		},
		{
			name:     "multiple nodes, single span entry",
			entries:  entries(entry("a", "b", "")),
			topology: topology(instance(1, ""), instance(2, ""), instance(3, "")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b")},
				nil,
				nil,
			},
		},
		{
			name: "multiple nodes, exact multiple of span entries",
			entries: entries(
				entry("a", "b", ""), entry("c", "d", ""),
				entry("e", "f", ""), entry("g", "h", ""),
				entry("i", "j", ""), entry("k", "l", ""),
			),
			topology: topology(instance(1, ""), instance(2, ""), instance(3, "")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b"), mockSpan("c", "d")},
				{mockSpan("e", "f"), mockSpan("g", "h")},
				{mockSpan("i", "j"), mockSpan("k", "l")},
			},
		},
		{
			name: "multiple nodes, exact multiple of overlapping span entries",
			entries: entries(
				entry("a", "b", ""), entry("b", "d", ""), entry("d", "f", ""),
				entry("f", "h", ""), entry("i", "j", ""), entry("j", "l", ""),
			),
			topology: topology(instance(1, ""), instance(2, "")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "f")},
				{mockSpan("f", "h"), mockSpan("i", "l")},
			},
		},
		{
			name: "multiple nodes, not exact multiple of span entries",
			entries: entries(
				entry("a", "b", ""), entry("c", "d", ""),
				entry("e", "f", ""), entry("f", "h", ""),
				entry("i", "j", ""),
			),
			topology: topology(instance(1, ""), instance(2, ""), instance(3, "")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b"), mockSpan("c", "d")},
				{mockSpan("e", "h")},
				{mockSpan("i", "j")},
			},
		},
		{
			name: "locality aware, fully matching data",
			entries: entries(
				entry("a", "b", "dc=dc1"), entry("c", "d", "dc=dc2"), entry("e", "f", "dc=dc3"),
			),
			topology: topology(instance(1, "dc=dc1"), instance(2, "dc=dc2"), instance(3, "dc=dc3")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b")},
				{mockSpan("c", "d")},
				{mockSpan("e", "f")},
			},
		},
		{
			name: "locality aware, some default data",
			entries: entries(
				entry("a", "b", "dc=dc1"), entry("c", "d", "dc=dc2"),
				entry("e", "f", "dc=dc3"), entry("g", "h", ""),
			),
			topology: topology(
				instance(1, "dc=dc1"), instance(2, "dc=dc2"), instance(3, "dc=dc3"), instance(4, "dc=dc4"),
			),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b")},
				{mockSpan("c", "d")},
				{mockSpan("e", "f")},
				{mockSpan("g", "h")},
			},
		},
		{
			name: "locality aware, multiple nodes per set",
			entries: entries(
				entry("a", "b", "dc=dc1"), entry("c", "d", "dc=dc1"),
				entry("e", "f", "dc=dc1"), entry("g", "h", "dc=dc2"),
				entry("i", "j", "dc=dc2"), entry("k", "l", "dc=dc2"),
			),
			topology: topology(instance(1, "dc=dc1"), instance(2, "dc=dc1"), instance(3, "dc=dc2")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b"), mockSpan("c", "d")},
				{mockSpan("e", "f")},
				{mockSpan("g", "h"), mockSpan("i", "j"), mockSpan("k", "l")},
			},
		},
		{
			name: "locality aware, some non-matching",
			entries: entries(
				entry("a", "b", "dc=dc1"), entry("c", "d", "dc=dc2"), entry("e", "f", "dc=dc3"),
			),
			topology: topology(instance(1, "dc=dc1"), instance(2, "dc=dc2")),
			expectedDistribution: [][]roachpb.Span{
				{mockSpan("a", "b"), mockSpan("e", "f")},
				{mockSpan("c", "d")},
			},
		},
		{
			name: "locality aware distribution",
			entries: entries(
				// We expect an exact even distribution of dc1 data, since our number of entries is a
				// multiple of the number of matching nodes.
				entry("a", "b", "dc=dc1"), entry("c", "d", "dc=dc1"),
				entry("e", "f", "dc=dc1"), entry("g", "h", "dc=dc1"),
				// We expect a slight uneven distribution of dc2 data, as our number of entries are not
				// a multiple of the number of matching nodes.
				entry("i", "j", "dc=dc2"), entry("k", "l", "dc=dc2"),
				entry("m", "n", "dc=dc2"), entry("o", "p", "dc=dc2"),
				entry("q", "r", "dc=dc2"),
			),
			topology: topology(
				instance(1, "dc=dc1"), instance(2, "dc=dc1"),
				instance(3, "dc=dc2"), instance(4, "dc=dc2"),
			),
			expectedDistribution: [][]roachpb.Span{
				// Each dc1 node should get two entries, in keyspace order.
				{mockSpan("a", "b"), mockSpan("c", "d")},
				{mockSpan("e", "f"), mockSpan("g", "h")},
				// The first dc2 node will have one extra entry,
				// but everything will still be in keyspace order.
				{mockSpan("i", "j"), mockSpan("k", "l"), mockSpan("m", "n")},
				{mockSpan("o", "p"), mockSpan("q", "r")},
			},
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			genSpan := func(_ context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
				for _, entry := range tc.entries {
					spanCh <- entry
				}
				return nil
			}

			localitySets, err := buildLocalitySets(
				ctx, tc.topology.ids, tc.topology.localities, false /* strict */, genSpan,
			)
			require.NoError(t, err)

			placements, err := createCompactionCorePlacements(
				ctx,
				0, /* jobID */
				username.RootUserName(),
				jobspb.BackupDetails{},
				execinfrapb.ElidePrefix_None,
				genSpan,
				nil, /* spansToCompact */
				tc.topology.ids,
				localitySets,
				0, /* targetSize */
				0, /* maxFiles */
			)
			require.NoError(t, err)
			require.Equal(t, len(tc.topology.ids), len(placements))

			for i, expected := range tc.expectedDistribution {
				id := base.SQLInstanceID(i + 1)
				// Placements are returned in no particular order, so we need to find the one with a
				// matching id.
				matched := false
				for _, placement := range placements {
					if placement.SQLInstanceID == id {
						require.NotNil(t, placement.Core.CompactBackups)
						require.Equal(t, expected, placement.Core.CompactBackups.AssignedSpans)
						matched = true
						break
					}
				}
				require.True(t, matched)
			}
		})
	}
}

func TestBuildLocalitySets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	testcases := []struct {
		name         string
		entries      []execinfrapb.RestoreSpanEntry
		topology     mockTopology
		expectedSets map[string]localitySet
	}{
		{
			name:     "non-locality-aware",
			entries:  entries(entry("a", "b", ""), entry("c", "d", ""), entry("e", "f", "")),
			topology: topology(instance(1, ""), instance(2, ""), instance(3, "")),
			expectedSets: map[string]localitySet{
				"default": {instanceIDs: []base.SQLInstanceID{1, 2, 3}, totalEntries: 3},
			},
		},
		{
			name: "fully-matching",
			entries: entries(
				entry("a", "b", "dc=dc1"), entry("c", "d", "dc=dc2"), entry("e", "f", "dc=dc3"),
			),
			topology: topology(instance(1, "dc=dc1"), instance(2, "dc=dc2"), instance(3, "dc=dc3")),
			expectedSets: map[string]localitySet{
				"dc=dc1": {instanceIDs: []base.SQLInstanceID{1}, totalEntries: 1},
				"dc=dc2": {instanceIDs: []base.SQLInstanceID{2}, totalEntries: 1},
				"dc=dc3": {instanceIDs: []base.SQLInstanceID{3}, totalEntries: 1},
			},
		},
		{
			name: "some-default",
			entries: entries(
				entry("a", "b", ""),
				entry("c", "d", "dc=dc1"), entry("e", "f", "dc=dc2"), entry("g", "h", "dc=dc3"),
			),
			topology: topology(
				instance(1, "dc=dc1"), instance(2, "dc=dc2"), instance(3, "dc=dc3"), instance(4, "dc=dc4"),
			),
			expectedSets: map[string]localitySet{
				// We assign nodes which do not match any of the locality-specific URIs to the default set.
				"default": {instanceIDs: []base.SQLInstanceID{4}, totalEntries: 1},
				"dc=dc1":  {instanceIDs: []base.SQLInstanceID{1}, totalEntries: 1},
				"dc=dc2":  {instanceIDs: []base.SQLInstanceID{2}, totalEntries: 1},
				"dc=dc3":  {instanceIDs: []base.SQLInstanceID{3}, totalEntries: 1},
			},
		},
		{
			name: "no-matches-for-locality",
			entries: entries(
				entry("a", "b", "dc=dc1"), entry("c", "d", "dc=dc2"), entry("e", "f", "dc=dc4"),
			),
			topology: topology(
				instance(1, "dc=dc1"), instance(2, "dc=dc2"), instance(3, "dc=dc3"),
			),
			expectedSets: map[string]localitySet{
				// Instances which don't match any locality-specific entries are assigned to the default set.
				"default": {instanceIDs: []base.SQLInstanceID{3}, totalEntries: 0},
				"dc=dc1":  {instanceIDs: []base.SQLInstanceID{1}, totalEntries: 1},
				"dc=dc2":  {instanceIDs: []base.SQLInstanceID{2}, totalEntries: 1},
				// Entries which don't have any matching instances are evenly distributed across all available nodes.
				"dc=dc4": {instanceIDs: []base.SQLInstanceID{1, 2, 3}, totalEntries: 1},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			genSpan := func(_ context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
				for _, entry := range tc.entries {
					spanCh <- entry
				}
				return nil
			}

			localitySets, err := buildLocalitySets(
				t.Context(), tc.topology.ids, tc.topology.localities, false /* strict */, genSpan,
			)
			require.NoError(t, err)
			require.Equal(t, len(tc.expectedSets), len(localitySets))

			for locality, actualSet := range localitySets {
				expectedSet, ok := tc.expectedSets[locality]
				require.True(t, ok)
				require.Equal(t, expectedSet, *actualSet)
			}
		})
	}
}

// Note that we only unit test buildLocalitySets for `WITH STRICT STORAGE LOCALITY` because,
// in the context of locality-aware compaction, this option simply indicates that if we have a
// localitySet which we have no nodes assigned to, rather than falling back to even assignment,
// we should instead error. This error occurs entirely in buildLocalitySets, and
// createCompactionCorePlacements is ignorant of whether or not the option is set.
func TestBuildLocalitySetsStrict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("no-matching-nodes", func(t *testing.T) {
		// This combination of entries and topology means that we will have data from a locality (dc=dc3)
		// which does not have any matching nodes. In a non strict setting, we expect this to fall back
		// to even assignment. In a strict setting, we expect this to trigger an error.
		entries := entries(
			entry("a", "b", "dc=dc1"),
			entry("c", "d", "dc=dc2"),
			entry("e", "f", "dc=dc3"),
		)
		topology := topology(
			instance(1, "dc=dc1"),
			instance(2, "dc=dc2"),
			instance(3, "dc=dc4"),
		)
		genSpan := func(_ context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
			for _, entry := range entries {
				spanCh <- entry
			}
			return nil
		}

		// Validate that even assignment occurs when strict is not set.
		expectedSets := map[string]*localitySet{
			"dc=dc1": {instanceIDs: []base.SQLInstanceID{1}, totalEntries: 1},
			"dc=dc2": {instanceIDs: []base.SQLInstanceID{2}, totalEntries: 1},
			// Since no nodes match dc=dc3, we fall back to even assignment.
			"dc=dc3": {instanceIDs: []base.SQLInstanceID{1, 2, 3}, totalEntries: 1},
			// Since node 3 has locality dc=dc4, which does not match any of our entries,
			// it is assigned to the defaul set, though this set has no data to process.
			"default": {instanceIDs: []base.SQLInstanceID{3}, totalEntries: 0},
		}
		sets, err := buildLocalitySets(
			t.Context(), topology.ids, topology.localities, false /* strict */, genSpan,
		)
		require.NoError(t, err)
		require.Equal(t, len(expectedSets), len(sets))
		for locality, actualSet := range sets {
			expectedSet, ok := expectedSets[locality]
			require.True(t, ok)
			require.Equal(t, expectedSet, actualSet)
		}

		// Validate that the same inputs produce an error when strict is set.
		_, err = buildLocalitySets(
			t.Context(), topology.ids, topology.localities, true /* strict */, genSpan,
		)
		require.Error(t, err)
	})

	// The same behavior as the above subtest is expected for the default set as well.
	t.Run("fully-matching-nodes", func(t *testing.T) {
		// Since all nodes match a specific locality, no nodes will be assigned to the default set.
		// This means entry ("g", "h") will have no nodes to process it, creating the same fallback
		// vs error scenario as above.
		entries := entries(
			entry("a", "b", "dc=dc1"),
			entry("c", "d", "dc=dc2"),
			entry("e", "f", "dc=dc3"),
			entry("g", "h", ""),
		)
		topology := topology(
			instance(1, "dc=dc1"),
			instance(2, "dc=dc2"),
			instance(3, "dc=dc3"),
		)
		genSpan := func(_ context.Context, spanCh chan execinfrapb.RestoreSpanEntry) error {
			for _, entry := range entries {
				spanCh <- entry
			}
			return nil
		}

		expectedSets := map[string]*localitySet{
			"dc=dc1": {instanceIDs: []base.SQLInstanceID{1}, totalEntries: 1},
			"dc=dc2": {instanceIDs: []base.SQLInstanceID{2}, totalEntries: 1},
			"dc=dc3": {instanceIDs: []base.SQLInstanceID{3}, totalEntries: 1},
			// Since all of our nodes matched a specific locality, we fall back to even assignment
			// for the default set in a non strict setting.
			"default": {instanceIDs: []base.SQLInstanceID{1, 2, 3}, totalEntries: 1},
		}
		sets, err := buildLocalitySets(
			t.Context(), topology.ids, topology.localities, false /* strict */, genSpan,
		)
		require.NoError(t, err)
		require.Equal(t, len(expectedSets), len(sets))
		for locality, actualSet := range sets {
			expectedSet, ok := expectedSets[locality]
			require.True(t, ok)
			require.Equal(t, expectedSet, actualSet)
		}

		_, err = buildLocalitySets(
			t.Context(), topology.ids, topology.localities, true /* strict */, genSpan,
		)
		require.Error(t, err)
	})

}

type mockEntry struct {
	span     roachpb.Span
	locality string
}
type mockInstance struct {
	id       base.SQLInstanceID
	locality string
}
type mockTopology struct {
	ids        []base.SQLInstanceID
	localities []roachpb.Locality
}

func mockSpan(start, end string) roachpb.Span {
	return roachpb.Span{Key: roachpb.Key(start), EndKey: roachpb.Key(end)}
}

func topology(instances ...mockInstance) mockTopology {
	ids := make([]base.SQLInstanceID, len(instances))
	localities := make([]roachpb.Locality, len(instances))
	for i, instance := range instances {
		ids[i] = instance.id

		locality := roachpb.Locality{}
		if instance.locality != "" && instance.locality != "default" {
			tier := roachpb.Tier{}
			_ = tier.FromString(instance.locality)
			locality = roachpb.Locality{Tiers: []roachpb.Tier{tier}}
		}
		localities[i] = locality
	}

	return mockTopology{ids: ids, localities: localities}
}
func instance(id base.SQLInstanceID, locality string) mockInstance {
	return mockInstance{id: id, locality: locality}
}

func entries(specs ...mockEntry) []execinfrapb.RestoreSpanEntry {
	var entries []execinfrapb.RestoreSpanEntry
	for _, s := range specs {
		var dir cloudpb.ExternalStorage
		if s.locality == "" {
			dir = cloudpb.ExternalStorage{}
		} else {
			dir = cloudpb.ExternalStorage{URI: "nodelocal://1/test?COCKROACH_LOCALITY=" + s.locality}
		}
		entries = append(entries,
			execinfrapb.RestoreSpanEntry{
				Span:  s.span,
				Files: []execinfrapb.RestoreFileSpec{{Dir: dir}},
			},
		)
	}
	return entries
}
func entry(start, end string, locality string) mockEntry {
	return mockEntry{
		span:     mockSpan(start, end),
		locality: locality,
	}

}
