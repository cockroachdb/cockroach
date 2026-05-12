// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"context"
	"math/rand/v2"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestRandomSubDAG(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	t.Run("simple_pair", func(t *testing.T) {
		s := discoverSchemaFromDDL(t, srv, sqlDB, "rsd_pair", simplePairDDL)
		graphs := BuildFKGraphs(s)

		for seed := uint64(0); seed < 50; seed++ {
			rng := rand.New(rand.NewPCG(seed, 0))
			for _, g := range graphs {
				sorted, sub, dropped, err := RandomSubDAG(rng, g)
				require.NoError(t, err, "seed=%d", seed)
				require.Empty(t, dropped, "seed=%d: acyclic graph", seed)
				verifySubDAGInvariants(t, g, sub, sorted, seed)
			}
		}
	})

	t.Run("chain", func(t *testing.T) {
		s := discoverSchemaFromDDL(t, srv, sqlDB, "rsd_chain", chainDDL)
		graphs := BuildFKGraphs(s)

		for seed := uint64(0); seed < 50; seed++ {
			rng := rand.New(rand.NewPCG(seed, 0))
			for _, g := range graphs {
				sorted, sub, dropped, err := RandomSubDAG(rng, g)
				require.NoError(t, err, "seed=%d", seed)
				require.Empty(t, dropped, "seed=%d", seed)
				verifySubDAGInvariants(t, g, sub, sorted, seed)
			}
		}
	})

	t.Run("diamond", func(t *testing.T) {
		s := discoverSchemaFromDDL(t, srv, sqlDB, "rsd_diamond", diamondCompositeDDL)
		graphs := BuildFKGraphs(s)
		require.Len(t, graphs, 1)

		for seed := uint64(0); seed < 50; seed++ {
			rng := rand.New(rand.NewPCG(seed, 0))
			sorted, sub, dropped, err := RandomSubDAG(rng, graphs[0])
			require.NoError(t, err, "seed=%d", seed)
			require.Empty(t, dropped, "seed=%d", seed)
			verifySubDAGInvariants(t, graphs[0], sub, sorted, seed)
		}
	})

	t.Run("self_ref", func(t *testing.T) {
		// Self-ref edges don't create multi-table cycles. They don't affect
		// topological ordering and are not dropped by breakCycles. The
		// transaction generator handles self-refs separately (NULL + backfill).
		s := discoverSchemaFromDDL(t, srv, sqlDB, "rsd_selfref", selfRefDDL)
		graphs := BuildFKGraphs(s)
		require.Len(t, graphs, 1)

		for seed := uint64(0); seed < 20; seed++ {
			rng := rand.New(rand.NewPCG(seed, 0))
			sorted, sub, dropped, err := RandomSubDAG(rng, graphs[0])
			require.NoError(t, err, "seed=%d", seed)
			require.Empty(t, dropped, "seed=%d: self-ref is not a multi-table cycle", seed)
			verifySubDAGInvariants(t, graphs[0], sub, sorted, seed)
			hasSelfRef := false
			for _, e := range sub.Edges {
				if e.ReferencingTable == e.ReferencedTable {
					hasSelfRef = true
					break
				}
			}
			require.True(t, hasSelfRef, "seed=%d: self-ref edge should be preserved", seed)
		}
	})

	t.Run("topological_order_correct", func(t *testing.T) {
		s := discoverSchemaFromDDL(t, srv, sqlDB, "rsd_topo", transitiveOverlapDDL)
		graphs := BuildFKGraphs(s)
		require.Len(t, graphs, 1)

		for seed := uint64(0); seed < 50; seed++ {
			rng := rand.New(rand.NewPCG(seed, 0))
			sorted, sub, _, err := RandomSubDAG(rng, graphs[0])
			require.NoError(t, err, "seed=%d", seed)
			verifyTopologicalOrder(t, sub, sorted, seed)
		}
	})

	t.Run("varies_across_seeds", func(t *testing.T) {
		s := discoverSchemaFromDDL(t, srv, sqlDB, "rsd_varies", fanOut3DDL)
		graphs := BuildFKGraphs(s)
		require.Len(t, graphs, 1)

		sizes := make(map[int]bool)
		for seed := uint64(0); seed < 100; seed++ {
			rng := rand.New(rand.NewPCG(seed, 0))
			sorted, _, _, err := RandomSubDAG(rng, graphs[0])
			require.NoError(t, err)
			sizes[len(sorted)] = true
		}
		require.Greater(t, len(sizes), 1, "expected varying sub-DAG sizes")
	})
}

// verifySubDAGInvariants checks that a sub-DAG is a valid acyclic subset of
// the original graph.
func verifySubDAGInvariants(
	t *testing.T, original *FKGraph, sub *FKGraph, sorted []*Table, seed uint64,
) {
	t.Helper()

	require.GreaterOrEqual(t, len(sub.Tables), 1, "seed=%d: empty sub-DAG", seed)
	require.Equal(t, len(sub.Tables), len(sorted), "seed=%d: sorted length mismatch", seed)

	// All tables in sub must be in original.
	for name := range sub.Tables {
		_, ok := original.Tables[name]
		require.True(t, ok, "seed=%d: table %s not in original", seed, name)
	}

	// All edges in sub must reference tables in sub.
	for _, e := range sub.Edges {
		_, ok := sub.Tables[e.ReferencingTable]
		require.True(t, ok, "seed=%d: edge %s child %s missing", seed, e.Name, e.ReferencingTable)
		_, ok = sub.Tables[e.ReferencedTable]
		require.True(t, ok, "seed=%d: edge %s parent %s missing", seed, e.Name, e.ReferencedTable)
	}

	// Sorted output covers all sub-DAG tables.
	sortedNames := make([]string, len(sorted))
	for i, tbl := range sorted {
		sortedNames[i] = tbl.Name
	}
	sort.Strings(sortedNames)
	subNames := make([]string, 0, len(sub.Tables))
	for name := range sub.Tables {
		subNames = append(subNames, name)
	}
	sort.Strings(subNames)
	require.Equal(t, subNames, sortedNames, "seed=%d", seed)

	verifyTopologicalOrder(t, sub, sorted, seed)
	verifyConnected(t, sub, seed)
}

// verifyConnected checks that the sub-DAG is weakly connected: every table is
// reachable from every other table when edge direction is ignored.
func verifyConnected(t *testing.T, sub *FKGraph, seed uint64) {
	t.Helper()
	if len(sub.Tables) <= 1 {
		return
	}

	adj := make(map[string]map[string]bool)
	for name := range sub.Tables {
		adj[name] = make(map[string]bool)
	}
	for _, e := range sub.Edges {
		if e.ReferencingTable == e.ReferencedTable {
			continue
		}
		adj[e.ReferencingTable][e.ReferencedTable] = true
		adj[e.ReferencedTable][e.ReferencingTable] = true
	}

	var start string
	for name := range sub.Tables {
		start = name
		break
	}
	visited := map[string]bool{start: true}
	queue := []string{start}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for neighbor := range adj[cur] {
			if !visited[neighbor] {
				visited[neighbor] = true
				queue = append(queue, neighbor)
			}
		}
	}
	require.Equal(t, len(sub.Tables), len(visited),
		"seed=%d: sub-DAG is not connected", seed)
}

// verifyTopologicalOrder checks that every FK edge in the sub-DAG goes from a
// table earlier in the sorted order (parent) to one later (child).
func verifyTopologicalOrder(t *testing.T, sub *FKGraph, sorted []*Table, seed uint64) {
	t.Helper()
	pos := make(map[string]int, len(sorted))
	for i, tbl := range sorted {
		pos[tbl.Name] = i
	}

	for _, e := range sub.Edges {
		if e.ReferencingTable == e.ReferencedTable {
			continue
		}
		parentPos, ok1 := pos[e.ReferencedTable]
		childPos, ok2 := pos[e.ReferencingTable]
		if !ok1 || !ok2 {
			continue
		}
		require.Less(t, parentPos, childPos,
			"seed=%d: parent %s (pos %d) should come before child %s (pos %d)",
			seed, e.ReferencedTable, parentPos, e.ReferencingTable, childPos)
	}
}
