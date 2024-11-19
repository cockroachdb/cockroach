// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package insights

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	ctx := context.Background()
	st := cluster.MakeTestingClusterSettings()
	store := newStore(st)

	// With the ExecutionInsightsCapacity set to 5, we retain the 5 most recently-seen statement insights.
	ExecutionInsightsCapacity.Override(ctx, &st.SV, 5)
	for id := 0; id < 10; id++ {
		addInsight(store, []uint64{uint64(id)})
	}
	assertInsightStatementIDs(t, store, []uint64{9, 8, 7, 6, 5})

	// Lowering the ExecutionInsightsCapacity requires having a new insight to evict the others.
	ExecutionInsightsCapacity.Override(ctx, &st.SV, 2)
	assertInsightStatementIDs(t, store, []uint64{9, 8, 7, 6, 5})
	addInsight(store, []uint64{10})
	assertInsightStatementIDs(t, store, []uint64{10, 9})

	ExecutionInsightsCapacity.Override(ctx, &st.SV, 5)
	// Insert a new insight with multiple statements to ensure the
	// eviction policy is enforced using the statement count.
	addInsight(store, []uint64{11, 12, 13, 14})
	assertInsightStatementIDs(t, store, []uint64{14, 13, 12, 11, 10})

	addInsight(store, []uint64{15, 16})
	// The last insight should be evicted to make room for the new one.
	assertInsightStatementIDs(t, store, []uint64{16, 15})
}

func addInsight(store *LockingStore, statementIDs []uint64) {
	stmts := make([]*Statement, len(statementIDs))
	for i, id := range statementIDs {
		stmts[i] = &Statement{ID: clusterunique.ID{Uint128: uint128.FromInts(0, id)}}
	}
	store.addInsight(&Insight{
		Transaction: &Transaction{
			ID: uuid.MakeV4(),
		},
		Statements: stmts,
	})
}

func assertInsightStatementIDs(t *testing.T, store *LockingStore, expected []uint64) {
	var actual []uint64
	store.IterateInsights(context.Background(), func(ctx context.Context, insight *Insight) {
		for _, s := range insight.Statements {
			actual = append(actual, s.ID.Lo)
		}
	})
	require.ElementsMatch(t, expected, actual)
}
