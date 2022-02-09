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
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/spanconfig/spanconfigtestutils"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

// TestingApplyInternal exports an internal method for testing purposes.
func (s *Store) TestingApplyInternal(
	dryrun bool, updates ...spanconfig.Update,
) (deleted []spanconfig.Target, added []spanconfig.Record, err error) {
	return s.applyInternal(dryrun, updates...)
}

func (s *Store) TestingSpanConfigStoreForEachOverlapping(
	span roachpb.Span, f func(spanConfigEntry) error,
) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.mu.spanConfigStore.forEachOverlapping(span, f)
}

// TestDataDriven runs datadriven tests against the Store interface.
// The syntax is as follows:
//
// 		apply
// 		delete [a,c)
// 		set [c,h):X
// 		----
// 		deleted [b,d)
// 		deleted [e,g)
// 		added [c,h):X
//
// 		get key=b
// 		----
// 		conf=A # or conf=FALLBACK if the key is not present
//
// 		needs-split span=[b,h)
// 		----
// 		true
//
// 		compute-split span=[b,h)
// 		----
// 		key=c
//
// 		overlapping span=[b,h)
// 		----
// 		[b,d):A
// 		[d,f):B
// 		[f,h):A
//
//
// Text of the form [a,b) and [a,b):C correspond to spans and span config
// entries; see spanconfigtestutils.Parse{Span,Config,SpanConfigEntry} for more
// details.
func TestDataDriven(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	datadriven.Walk(t, testutils.TestDataPath(t), func(t *testing.T, path string) {
		store := New(spanconfigtestutils.ParseConfig(t, "FALLBACK"))
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			var spanStr, keyStr string
			switch d.Cmd {
			case "apply":
				updates := spanconfigtestutils.ParseStoreApplyArguments(t, d.Input)
				dryrun := d.HasArg("dryrun")
				deleted, added, err := store.TestingApplyInternal(dryrun, updates...)
				if err != nil {
					return fmt.Sprintf("err: %v", err)
				}

				sort.Sort(spanconfig.Targets(deleted))
				sort.Slice(added, func(i, j int) bool {
					return added[i].Target.Less(added[j].Target)
				})

				var b strings.Builder
				for _, target := range deleted {
					b.WriteString(fmt.Sprintf("deleted %s\n", spanconfigtestutils.PrintTarget(target)))
				}
				for _, ent := range added {
					b.WriteString(fmt.Sprintf("added %s\n", spanconfigtestutils.PrintSpanConfigRecord(ent)))
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
				return fmt.Sprintf("key=%s", string(splitKey))

			case "overlapping":
				d.ScanArgs(t, "span", &spanStr)
				span := spanconfigtestutils.ParseSpan(t, spanStr)

				var results []string
				_ = store.TestingSpanConfigStoreForEachOverlapping(span,
					func(entry spanConfigEntry) error {
						results = append(results,
							spanconfigtestutils.PrintSpanConfigRecord(spanconfig.Record{
								Target: spanconfig.MakeTargetFromSpan(entry.span),
								Config: entry.config,
							}),
						)
						return nil
					},
				)
				return strings.Join(results, "\n")

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

	updates := []spanconfig.Update{
		spanconfig.Addition(
			spanconfig.MakeTargetFromSpan(spanconfigtestutils.ParseSpan(t, "[a, b)")),
			spanconfigtestutils.ParseConfig(t, "A"),
		),
		spanconfig.Addition(
			spanconfig.MakeTargetFromSpan(spanconfigtestutils.ParseSpan(t, "[c, d)")),
			spanconfigtestutils.ParseConfig(t, "C"),
		),
		spanconfig.Addition(
			spanconfig.MakeTargetFromSpan(spanconfigtestutils.ParseSpan(t, "[e, f)")),
			spanconfigtestutils.ParseConfig(t, "E"),
		),
	}

	original := New(roachpb.TestingDefaultSpanConfig())
	original.Apply(ctx, false, updates...)
	clone := original.Copy(ctx)

	var originalRecords, clonedRecords []spanconfig.Record
	_ = original.Iterate(func(rec spanconfig.Record) error {
		originalRecords = append(originalRecords, rec)
		return nil
	})

	_ = clone.Iterate(func(rec spanconfig.Record) error {
		clonedRecords = append(clonedRecords, rec)
		return nil
	})

	require.Equal(t, len(originalRecords), len(clonedRecords))
	for i := 0; i < len(originalRecords); i++ {
		require.True(
			t, originalRecords[i].Target.Equal(clonedRecords[i].Target),
		)
		require.True(t, originalRecords[i].Config.Equal(clonedRecords[i].Config))
	}
}
