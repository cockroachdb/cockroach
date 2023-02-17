// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigstore

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestingApplyInternal exports an internal method for testing purposes.
func (s *Store) TestingApplyInternal(
	ctx context.Context, dryrun bool, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record, err error) {
	return s.applyInternal(ctx, dryrun, updates...)
}

// TestingSplitKeys returns the computed list of range split points between
// [start, end).
func (s *Store) TestingSplitKeys(tb testing.TB, start, end roachpb.RKey) []roachpb.RKey {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.spanConfigStore.TestingSplitKeys(tb, start, end)
}

// TestingSplitKeys returns the computed list of range split points between
// [start, end).
func (s *spanConfigStore) TestingSplitKeys(tb testing.TB, start, end roachpb.RKey) []roachpb.RKey {
	var splitKeys []roachpb.RKey
	computeStart := start
	for {
		splitKey, err := s.computeSplitKey(computeStart, end)
		require.NoError(tb, err)
		if splitKey == nil {
			break
		}

		splitKeys = append(splitKeys, splitKey)
		computeStart = splitKey
	}

	return splitKeys
}

// TestDataDriven runs datadriven tests against the Store interface.
// The syntax is as follows:
//
//	apply
//	delete [a,c)
//	set [c,h):X
//	set {entire-keyspace}:X
//	set {source=1,target=1}:Y
//	----
//	deleted [b,d)
//	deleted [e,g)
//	added [c,h):X
//	added {entire-keyspace}:X
//	added {source=1,target=1}:Y
//
//	get key=b
//	----
//	conf=A # or conf=FALLBACK if the key is not present
//
//	needs-split span=[b,h)
//	----
//	true
//
//	compute-split span=[b,h)
//	----
//	key=c
//
//	split-keys span=[b,h)
//	----
//	key=c
//
//	overlapping span=[b,h)
//	----
//	[b,d):A
//	[d,f):B
//	[f,h):A
//
//	interned
//	----
//	A (refs = 2)
//	B (refs = 1)
//
// Text of the form [a,b), {entire-keyspace}, {source=1,target=20}, and [a,b):C
// correspond to targets {spans, system targets} and span config records; see
// spanconfigtestutils.Parse{Target,Config,SpanConfigRecord} for more details.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		store := New(
			spanconfigtestutils.ParseConfig(t, "FALLBACK"),
			cluster.MakeTestingClusterSettings(),
			&spanconfig.TestingKnobs{
				StoreIgnoreCoalesceAdjacentExceptions: true,
				StoreInternConfigsInDryRuns:           true,
			},
		)
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var spanStr, keyStr string
			switch d.Cmd {
			case "apply":
				updates := spanconfigtestutils.ParseStoreApplyArguments(t, d.Input)
				dryrun := d.HasArg("dryrun")
				deleted, added, err := store.TestingApplyInternal(ctx, dryrun, updates...)
				if err != nil {
					return fmt.Sprintf("err: %v", err)
				}

				sort.Sort(spanconfig.Targets(deleted))
				sort.Slice(added, func(i, j int) bool {
					return added[i].GetTarget().Less(added[j].GetTarget())
				})

				var b strings.Builder
				for _, target := range deleted {
					b.WriteString(fmt.Sprintf("deleted %s\n", spanconfigtestutils.PrintTarget(t, target)))
				}
				for _, ent := range added {
					b.WriteString(fmt.Sprintf("added %s\n", spanconfigtestutils.PrintSpanConfigRecord(t, ent)))
				}
				return b.String()

			case "get":
				d.ScanArgs(t, "key", &keyStr)
				config, err := store.GetSpanConfigForKey(ctx, roachpb.RKey(keyStr))
				require.NoError(t, err)
				return fmt.Sprintf("conf=%s", spanconfigtestutils.PrintSpanConfig(config))

			case "needs-split":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)
				start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
				result := store.NeedsSplit(ctx, start, end)
				return fmt.Sprintf("%t", result)

			case "compute-split":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)
				start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
				splitKey := store.ComputeSplitKey(ctx, start, end)
				if splitKey == nil {
					return "n/a"
				}
				return fmt.Sprintf("key=%s", string(splitKey))

			case "split-keys":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)

				start, end := roachpb.RKey(span.Key), roachpb.RKey(span.EndKey)
				splitKeys := store.TestingSplitKeys(t, start, end)
				var b strings.Builder
				for _, splitKey := range splitKeys {
					b.WriteString(fmt.Sprintf("key=%s\n", string(splitKey)))
				}
				return b.String()

			case "overlapping":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)

				var results []string
				_ = store.ForEachOverlappingSpanConfig(ctx, span,
					func(sp roachpb.Span, conf roachpb.SpanConfig) error {
						record, err := spanconfig.MakeRecord(spanconfig.MakeTargetFromSpan(sp), conf)
						if err != nil {
							return err
						}
						results = append(results, spanconfigtestutils.PrintSpanConfigRecord(t, record))
						return nil
					},
				)
				return strings.Join(results, "\n")

			case "interned":
				var b strings.Builder
				for _, i := range store.testingInterned() {
					b.WriteString(fmt.Sprintf("%s (refs = %d)\n",
						spanconfigtestutils.PrintSpanConfig(i.SpanConfig), i.RefCount))
				}
				return b.String()

			default:
				t.Fatalf("unknown command: %s", d.Cmd)
			}

			return ""
		})
	})
}

// TestStoreClone verifies that a cloned store contains the same contents as the
// original.
func TestStoreClone(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()

	makeSpanConfigAddition := func(target spanconfig.Target, conf roachpb.SpanConfig) spanconfig.Update {
		addition, err := spanconfig.Addition(target, conf)
		require.NoError(t, err)
		return addition
	}
	updates := []spanconfig.Update{
		makeSpanConfigAddition(
			spanconfig.MakeTargetFromSpan(spanconfigtestutils.ParseSpan(t, "[a, b)")),
			spanconfigtestutils.ParseConfig(t, "A"),
		),
		makeSpanConfigAddition(
			spanconfig.MakeTargetFromSpan(spanconfigtestutils.ParseSpan(t, "[c, d)")),
			spanconfigtestutils.ParseConfig(t, "C"),
		),
		makeSpanConfigAddition(
			spanconfig.MakeTargetFromSpan(spanconfigtestutils.ParseSpan(t, "[e, f)")),
			spanconfigtestutils.ParseConfig(t, "E"),
		),
		makeSpanConfigAddition(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.MakeEntireKeyspaceTarget()),
			spanconfigtestutils.ParseConfig(t, "G"),
		),
		makeSpanConfigAddition(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.SystemTenantID, roachpb.MustMakeTenantID(10),
			)),
			spanconfigtestutils.ParseConfig(t, "H"),
		),
		makeSpanConfigAddition(
			spanconfig.MakeTargetFromSystemTarget(spanconfig.TestingMakeTenantKeyspaceTargetOrFatal(
				t, roachpb.MustMakeTenantID(10), roachpb.MustMakeTenantID(10),
			)),
			spanconfigtestutils.ParseConfig(t, "I"),
		),
	}

	original := New(roachpb.TestingDefaultSpanConfig(), cluster.MakeClusterSettings(), nil)
	original.Apply(ctx, false, updates...)
	clone := original.Clone()

	var originalRecords, clonedRecords []spanconfig.Record
	_ = original.Iterate(func(rec spanconfig.Record) error {
		originalRecords = append(originalRecords, rec)
		return nil
	})

	_ = clone.Iterate(func(rec spanconfig.Record) error {
		clonedRecords = append(clonedRecords, rec)
		return nil
	})

	require.Equal(t, len(updates), len(originalRecords))
	require.Equal(t, len(originalRecords), len(clonedRecords))
	for i := 0; i < len(originalRecords); i++ {
		require.True(
			t, originalRecords[i].GetTarget().Equal(clonedRecords[i].GetTarget()),
		)
		originalConfig := originalRecords[i].GetConfig()
		require.True(t, originalConfig.Equal(clonedRecords[i].GetConfig()))
	}
}

// BenchmarkStoreComputeSplitKey measures how long it takes to compute the split
// key while varying the total number of span config entries we have to sift
// through for each computation.
func BenchmarkStoreComputeSplitKey(b *testing.B) {
	ctx := context.Background()
	for _, numEntries := range []int{10_000, 100_000, 1_000_000} {
		b.Run(fmt.Sprintf("num-entries=%d", numEntries), func(b *testing.B) {
			store := New(
				roachpb.SpanConfig{},
				cluster.MakeClusterSettings(),
				&spanconfig.TestingKnobs{
					StoreIgnoreCoalesceAdjacentExceptions: true,
				},
			)
			var updates []spanconfig.Update
			for i := 0; i < numEntries; i++ {
				updates = append(updates, spanconfigtestutils.ParseStoreApplyArguments(b,
					fmt.Sprintf("set [%08d,%08d):X", i, i+1))...)
			}
			deleted, added := store.Apply(ctx, false, updates...)
			require.Len(b, deleted, 0)
			require.Len(b, added, numEntries)

			query := spanconfigtestutils.ParseSpan(b,
				fmt.Sprintf("[%08d, %08d)", 0, numEntries))

			overlapping := 0
			require.NoError(b, store.ForEachOverlappingSpanConfig(ctx, query,
				func(_ roachpb.Span, _ roachpb.SpanConfig) error {
					overlapping++
					return nil
				},
			))
			require.Equal(b, overlapping, numEntries)

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = store.ComputeSplitKey(ctx, roachpb.RKey(query.Key), roachpb.RKey(query.EndKey))
			}
		})
	}
}

type interned struct {
	roachpb.SpanConfig
	RefCount uint64
}

func (s *Store) testingInterned() []interned {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.mu.spanConfigStore.testingInterned()
}

func (s *spanConfigStore) testingInterned() []interned {
	var is []interned
	for canonical, refs := range s.interner.refCounts {
		is = append(is, interned{
			SpanConfig: *canonical,
			RefCount:   refs,
		})
	}
	sort.Slice(is, func(i, j int) bool {
		return spanconfigtestutils.PrintSpanConfig(is[i].SpanConfig) < spanconfigtestutils.PrintSpanConfig(is[j].SpanConfig)
	})
	return is
}
