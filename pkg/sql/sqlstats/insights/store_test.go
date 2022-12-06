// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

	// With the ExecutionInsightsCapacity set to 5, we retain the 5 most recently-seen insights.
	ExecutionInsightsCapacity.Override(ctx, &st.SV, 5)
	for id := 0; id < 10; id++ {
		addInsight(store, uint64(id))
	}
	assertInsightStatementIDs(t, store, []uint64{9, 8, 7, 6, 5})

	// Lowering the ExecutionInsightsCapacity requires having a new insight to evict the others.
	ExecutionInsightsCapacity.Override(ctx, &st.SV, 2)
	assertInsightStatementIDs(t, store, []uint64{9, 8, 7, 6, 5})
	addInsight(store, 10)
	assertInsightStatementIDs(t, store, []uint64{10, 9})
}

func addInsight(store *lockingStore, idBase uint64) {
	store.AddInsight(&Insight{
		Transaction: &Transaction{
			ID: uuid.FastMakeV4(),
		},
		Statements: []*Statement{{ID: clusterunique.ID{Uint128: uint128.FromInts(0, idBase)}}},
	})
}

func assertInsightStatementIDs(t *testing.T, store *lockingStore, expected []uint64) {
	var actual []uint64
	store.IterateInsights(context.Background(), func(ctx context.Context, insight *Insight) {
		for _, s := range insight.Statements {
			actual = append(actual, s.ID.Lo)
		}
	})
	require.ElementsMatch(t, expected, actual)
}
