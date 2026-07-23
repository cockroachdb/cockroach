// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/interval"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/stretchr/testify/require"
)

// TestConstructReadsTreeForLeaf is a unit test for constructReadsTreeForLeaf.
// It simulates different sets of reads performed by processors and then asserts
// that the reads tree correctly distinguishes overlapping and non-overlapping
// keys.
func TestConstructReadsTreeForLeaf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	rng, _ := randutil.NewTestRand()
	evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	tableA, tableB, tableC := descpb.ID(100), descpb.ID(200), descpb.ID(300)
	makeKey := func(tableID descpb.ID, indexID descpb.IndexID, key string) roachpb.Key {
		keyPrefix := rowenc.MakeIndexKeyPrefix(evalCtx.Codec, tableID, indexID)
		return append(keyPrefix, []byte(key)...)
	}
	externalTenantID := roachpb.MustMakeTenantID(42)
	externalCodec := keys.MakeSQLCodec(externalTenantID)
	maybeAdjustKeyForExternal := func(key roachpb.Key, external bool) roachpb.Key {
		if len(key) == 0 || !external {
			return key
		}
		// Strip the tenant prefix of the original tenant and prepend the prefix
		// of the external one.
		var err error
		key, err = evalCtx.Codec.StripTenantPrefix(key)
		require.NoError(t, err)
		prefix := externalCodec.TenantPrefix()
		return append(prefix[:len(prefix):len(prefix)], key...)
	}

	type index struct {
		tableID descpb.ID
		indexID descpb.IndexID
	}
	for _, external := range []bool{false, true} {
		for _, tc := range []struct {
			readSpans      []roachpb.Span
			readIndexes    []index
			overlapping    []roachpb.Key
			nonOverlapping []roachpb.Key
		}{
			{
				// Will read:
				//   [TableA/1/1, TableA/1/5)
				readSpans: []roachpb.Span{
					{Key: makeKey(tableA, 1, "1"), EndKey: makeKey(tableA, 1, "5")},
				},
				overlapping: []roachpb.Key{
					makeKey(tableA, 1, "1"),
					makeKey(tableA, 1, "2"),
					makeKey(tableA, 1, "4"),
				},
				nonOverlapping: []roachpb.Key{
					makeKey(tableA, 1, "0"),
					makeKey(tableA, 1, "5"),
					makeKey(tableA, 2, "1"),
					makeKey(tableB, 1, "1"),
				},
			},
			{
				// Will read:
				//   TableA/1/1, TableA/1/3, TableA/1/5
				readSpans: []roachpb.Span{
					{Key: makeKey(tableA, 1, "1"), EndKey: nil},
					{Key: makeKey(tableA, 1, "3"), EndKey: nil},
					{Key: makeKey(tableA, 1, "5"), EndKey: nil},
				},
				overlapping: []roachpb.Key{
					makeKey(tableA, 1, "1"),
					makeKey(tableA, 1, "3"),
					makeKey(tableA, 1, "5"),
				},
				nonOverlapping: []roachpb.Key{
					makeKey(tableA, 1, "0"),
					makeKey(tableA, 1, "2"),
					makeKey(tableA, 1, "4"),
					makeKey(tableA, 2, "1"),
					makeKey(tableB, 1, "1"),
				},
			},
			{
				// Will read:
				//   indexes TableA/1 and TableB/2
				// Any key within these two indexes should overlap while any
				// keys outside shouldn't.
				readIndexes: []index{
					{tableID: tableA, indexID: 1},
					{tableID: tableB, indexID: 2},
				},
				overlapping: []roachpb.Key{
					makeKey(tableA, 1, strconv.Itoa(rng.Int())),
					makeKey(tableB, 2, strconv.Itoa(rng.Int())),
				},
				nonOverlapping: []roachpb.Key{
					makeKey(tableA, 2, strconv.Itoa(rng.Int())),
					makeKey(tableB, 1, strconv.Itoa(rng.Int())),
					makeKey(tableC, 1, strconv.Itoa(rng.Int())),
				},
			},
			{
				// Same as above but also add some random span- and point-reads
				// within the same indexes.
				readSpans: []roachpb.Span{
					{Key: makeKey(tableA, 1, strconv.Itoa(rng.Int())), EndKey: makeKey(tableA, 1, strconv.Itoa(rng.Int()))},
					{Key: makeKey(tableA, 1, strconv.Itoa(rng.Int())), EndKey: nil},
					{Key: makeKey(tableB, 2, strconv.Itoa(rng.Int())), EndKey: makeKey(tableB, 2, strconv.Itoa(rng.Int()))},
					{Key: makeKey(tableB, 2, strconv.Itoa(rng.Int())), EndKey: nil},
				},
				readIndexes: []index{
					{tableID: tableA, indexID: 1},
					{tableID: tableB, indexID: 2},
				},
				overlapping: []roachpb.Key{
					makeKey(tableA, 1, strconv.Itoa(rng.Int())),
					makeKey(tableB, 2, strconv.Itoa(rng.Int())),
				},
				nonOverlapping: []roachpb.Key{
					makeKey(tableA, 2, strconv.Itoa(rng.Int())),
					makeKey(tableB, 1, strconv.Itoa(rng.Int())),
					makeKey(tableC, 1, strconv.Itoa(rng.Int())),
				},
			},
		} {
			var processors []physicalplan.Processor
			if len(tc.readSpans) > 0 {
				for _, span := range tc.readSpans {
					if len(processors) == 0 || rng.Float64() < 0.5 {
						// With 50% probability include the next span into a
						// separate processor.
						processors = append(processors, physicalplan.Processor{
							Spec: execinfrapb.ProcessorSpec{
								Core: execinfrapb.ProcessorCoreUnion{
									TableReader: &execinfrapb.TableReaderSpec{},
								},
							},
						})
					}
					if span.EndKey != nil && span.EndKey.Less(span.Key) {
						// Due to randomly generated keys, we might create an
						// inverted span which is illegal.
						span.Key, span.EndKey = span.EndKey, span.Key
					}
					span.Key = maybeAdjustKeyForExternal(span.Key, external)
					span.EndKey = maybeAdjustKeyForExternal(span.EndKey, external)
					tr := processors[len(processors)-1].Spec.Core.TableReader
					tr.Spans = append(tr.Spans, span)
				}
			}
			for _, readIndex := range tc.readIndexes {
				// We could've used InvertedJoinerSpec or ZigzagJoinerSpec, but
				// it doesn't really matter.
				var jr execinfrapb.JoinReaderSpec
				jr.FetchSpec.TableID = readIndex.tableID
				jr.FetchSpec.IndexID = readIndex.indexID
				if external {
					jr.FetchSpec.External = &fetchpb.IndexFetchSpec_ExternalRowData{
						TenantID: externalTenantID,
						TableID:  readIndex.tableID,
					}
				}
				processors = append(processors, physicalplan.Processor{
					Spec: execinfrapb.ProcessorSpec{
						Core: execinfrapb.ProcessorCoreUnion{
							JoinReader: &jr,
						},
					},
				})
			}
			readsTree := constructReadsTreeForLeaf(&evalCtx, processors)
			require.NotNil(t, readsTree)
			overlaps := func(key roachpb.Key) bool {
				var sp roachpb.Span
				sp.Key = key
				sp.EndKey = key.Next()
				return readsTree.DoMatching(func(interval.Interface) (done bool) {
					return true
				}, sp.AsRange())
			}
			for _, k := range tc.overlapping {
				require.True(t, overlaps(maybeAdjustKeyForExternal(k, external)))
			}
			for _, k := range tc.nonOverlapping {
				require.False(t, overlaps(maybeAdjustKeyForExternal(k, external)))
			}
		}
	}
}

// TestPlanSafeForReducedLeaf ensures that we don't use the reads tree if
// we have an ineligible element somewhere in the plan.
func TestPlanSafeForReducedLeaf(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// This builtin has an overload that can execute almost any query via the
	// txn, so we use it as an example of an unsafe expression.
	//
	// If the heuristics in reducedLeafExprVisitor ever change, we simply need
	// an expression that is considered "unsafe" by the visitor for this test.
	expr, err := parser.ParseExpr("crdb_internal.execute_internally('SELECT 1')")
	require.NoError(t, err)
	semaCtx := tree.MakeSemaContext(nil /* resolver */)
	tExpr, err := expr.TypeCheck(context.Background(), &semaCtx, types.AnyElement)
	require.NoError(t, err)
	unsafeExpr := execinfrapb.Expression{LocalExpr: tExpr}

	for _, tc := range []struct {
		core execinfrapb.ProcessorCoreUnion
		post execinfrapb.PostProcessSpec
	}{
		{
			core: execinfrapb.ProcessorCoreUnion{Noop: &execinfrapb.NoopCoreSpec{}},
			post: execinfrapb.PostProcessSpec{RenderExprs: []execinfrapb.Expression{unsafeExpr}},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				JoinReader: &execinfrapb.JoinReaderSpec{
					LookupExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				JoinReader: &execinfrapb.JoinReaderSpec{
					RemoteLookupExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				JoinReader: &execinfrapb.JoinReaderSpec{
					OnExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				MergeJoiner: &execinfrapb.MergeJoinerSpec{
					OnExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				HashJoiner: &execinfrapb.HashJoinerSpec{
					OnExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				ZigzagJoiner: &execinfrapb.ZigzagJoinerSpec{
					OnExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				ProjectSet: &execinfrapb.ProjectSetSpec{
					Exprs: []execinfrapb.Expression{unsafeExpr},
				},
			},
		},
		// CDC processors are marked as "unoptimizedProcessor", but we include
		// this test case for completeness.
		{
			core: execinfrapb.ProcessorCoreUnion{
				ChangeAggregator: &execinfrapb.ChangeAggregatorSpec{
					Select: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				InvertedFilterer: &execinfrapb.InvertedFiltererSpec{
					PreFiltererSpec: &execinfrapb.InvertedFiltererSpec_PreFiltererSpec{
						Expression: unsafeExpr,
					},
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				InvertedJoiner: &execinfrapb.InvertedJoinerSpec{
					InvertedExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				InvertedJoiner: &execinfrapb.InvertedJoinerSpec{
					OnExpr: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				Filterer: &execinfrapb.FiltererSpec{
					Filter: unsafeExpr,
				},
			},
		},
		{
			core: execinfrapb.ProcessorCoreUnion{
				LocalPlanNode: &execinfrapb.LocalPlanNodeSpec{
					Name: "unsafe",
				},
			},
		},
	} {
		require.False(t, planSafeForReducedLeaf(
			[]physicalplan.Processor{{
				Spec: execinfrapb.ProcessorSpec{
					Core: tc.core,
					Post: tc.post,
				}},
			},
		))
	}
}
