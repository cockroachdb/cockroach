// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package stats

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRehydrateEnumStats(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Physical representations are keyed by the first letter of the logical
	// name so that renamed values keep theirs.
	physRep := map[byte][]byte{'a': {32}, 'b': {64}, 'c': {128}, 'd': {192}}
	mockEnum := func(version uint32, logical ...string) *types.T {
		physical := make([][]byte, len(logical))
		for i, v := range logical {
			physical[i] = physRep[v[0]]
		}
		enum := types.MakeEnum(catid.TypeIDToOID(200), catid.TypeIDToOID(201))
		enum.TypeMeta = types.UserDefinedTypeMetadata{
			Name:    &types.UserDefinedTypeName{Name: "e"},
			Version: version,
		}
		enum.TypeMeta.EnumData = &types.EnumMetadata{
			LogicalRepresentations:  logical,
			PhysicalRepresentations: physical,
			IsMemberReadOnly:        make([]bool, len(logical)),
		}
		return enum
	}
	histStat := func(typ *types.T, bounds ...tree.Datum) *TableStatistic {
		stat := &TableStatistic{TableStatisticProto: TableStatisticProto{
			TableID: 53, StatisticID: 1, ColumnIDs: []descpb.ColumnID{1}, RowCount: 10,
			HistogramData: &HistogramData{ColumnType: typ},
		}}
		stat.Histogram = append(stat.Histogram, cat.HistogramBucket{NumEq: 1, UpperBound: tree.DNull})
		for _, b := range bounds {
			stat.Histogram = append(stat.Histogram, cat.HistogramBucket{NumEq: 3, UpperBound: b})
		}
		return stat
	}
	enumStat := func(typ *types.T, logical ...string) *TableStatistic {
		bounds := make([]tree.Datum, len(logical))
		for i, l := range logical {
			d, err := tree.MakeDEnumFromLogicalRepresentation(typ, l)
			require.NoError(t, err)
			bounds[i] = &d
		}
		return histStat(typ, bounds...)
	}
	// stamped asserts every bound is pinned to typ and returns their names.
	stamped := func(stat *TableStatistic, typ *types.T) []string {
		require.Same(t, typ, stat.HistogramData.ColumnType)
		require.Equal(t, tree.DNull, stat.Histogram[0].UpperBound)
		var names []string
		for _, b := range stat.Histogram[1:] {
			d := b.UpperBound.(*tree.DEnum)
			require.Same(t, typ, d.EnumTyp)
			names = append(names, d.LogicalRep)
		}
		return names
	}

	memo := func() map[*TableStatistic]*TableStatistic { return map[*TableStatistic]*TableStatistic{} }
	col1 := func(typ *types.T) map[descpb.ColumnID]*types.T { return map[descpb.ColumnID]*types.T{1: typ} }

	v1 := mockEnum(1, "a", "b", "c")
	multiCol := &TableStatistic{TableStatisticProto: TableStatisticProto{
		TableID: 53, StatisticID: 2, ColumnIDs: []descpb.ColumnID{1, 2}, RowCount: 10,
	}}

	t.Run("no-op", func(t *testing.T) {
		stats := []*TableStatistic{enumStat(v1, "a", "b", "c"), multiCol}
		for _, newTypes := range []map[descpb.ColumnID]*types.T{nil, {2: mockEnum(2, "a")}} {
			res, ok := rehydrateEnumStatsInList(stats, newTypes, memo())
			require.True(t, ok)
			require.Same(t, &stats[0], &res[0], "unchanged input must be returned as-is")
		}
	})

	t.Run("added and renamed values restamp every bound", func(t *testing.T) {
		v2 := mockEnum(2, "a", "bee", "c", "d")
		orig := enumStat(v1, "a", "b", "c")
		rehydrated := memo()
		res, ok := rehydrateEnumStatsInList([]*TableStatistic{orig, multiCol}, col1(v2), rehydrated)
		require.True(t, ok)
		resStable, ok := rehydrateEnumStatsInList([]*TableStatistic{orig}, col1(v2), rehydrated)
		require.True(t, ok)

		require.Equal(t, []string{"a", "b", "c"}, stamped(orig, v1), "input must be untouched")
		require.Equal(t, []string{"a", "bee", "c"}, stamped(res[0], v2))
		require.Same(t, multiCol, res[1])
		evalCtx := eval.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
		bee := enumStat(v2, "bee").Histogram[1].UpperBound
		cmp, err := res[0].Histogram[2].UpperBound.Compare(context.Background(), &evalCtx, bee)
		require.NoError(t, err)
		require.Equal(t, 0, cmp)
		require.Same(t, res[0], resStable[0])
		require.Len(t, rehydrated, 1)
	})

	t.Run("unrepresentable bound fails", func(t *testing.T) {
		v2 := mockEnum(2, "a", "b", "c", "d")
		other := mockEnum(2, "a", "b", "c", "d")
		other.InternalType.Oid = catid.TypeIDToOID(300)
		for name, tc := range map[string]struct {
			stat *TableStatistic
			typ  *types.T
		}{
			"dropped value":    {enumStat(v1, "a", "b", "c"), mockEnum(2, "a", "c")},
			"older type":       {enumStat(v2, "a", "d"), v1},
			"different type":   {enumStat(v1, "a"), other},
			"unexpected bound": {histStat(v1, tree.NewDInt(1)), v2},
		} {
			_, ok := rehydrateEnumStatsInList([]*TableStatistic{tc.stat}, col1(tc.typ), memo())
			require.False(t, ok, name)
		}
	})
}
