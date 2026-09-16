// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"math/rand/v2"
	"sort"

	"github.com/cockroachdb/errors"
)

// RandomSubDAG produces a random acyclic subgraph of the given FKGraph and
// returns its tables in topological order (parents before children). It:
//
//  1. Breaks cycles by iteratively running Kahn's topological sort and
//     dropping a random edge among the stuck nodes until the sort completes.
//  2. Randomly trims leaf and root tables to vary the transaction size.
//  3. Returns the surviving tables in insertion order.
//
// droppedEdges contains cycle-breaking edges that were removed. The caller can
// use these to patch NULL'd FK columns with UPDATEs after the main walk.
func RandomSubDAG(
	rng *rand.Rand, graph *FKGraph,
) (sorted []*Table, sub *FKGraph, droppedEdges []FKEdge, err error) {
	edges, dropped := breakCycles(rng, graph)
	sub = &FKGraph{
		Tables: copyTableMap(graph.Tables),
		Edges:  edges,
	}
	trimDAG(rng, sub)
	sorted, err = topologicalSort(sub)
	if err != nil {
		return nil, nil, nil, err
	}
	return sorted, sub, dropped, nil
}

// breakCycles uses Kahn's algorithm to find nodes stuck in cycles, then drops
// a random edge among them. Repeats until the topological sort has no stuck
// nodes.
func breakCycles(rng *rand.Rand, graph *FKGraph) (kept []FKEdge, dropped []FKEdge) {
	edges := make([]FKEdge, len(graph.Edges))
	copy(edges, graph.Edges)

	for {
		stuck := kahnsStuckNodes(graph.Tables, edges)
		if len(stuck) == 0 {
			return edges, dropped
		}

		var candidates []int
		for i, e := range edges {
			if e.ReferencingTable == e.ReferencedTable {
				continue
			}
			if stuck[e.ReferencingTable] && stuck[e.ReferencedTable] {
				candidates = append(candidates, i)
			}
		}

		drop := candidates[rng.IntN(len(candidates))]
		dropped = append(dropped, edges[drop])
		edges = append(edges[:drop], edges[drop+1:]...)
	}
}

// kahnsStuckNodes runs Kahn's algorithm and returns the set of table names that
// could not be processed — the nodes involved in cycles. Self-ref edges are
// ignored since they don't affect inter-table ordering.
func kahnsStuckNodes(tables map[string]*Table, edges []FKEdge) map[string]bool {
	inDegree := make(map[string]int, len(tables))
	for name := range tables {
		inDegree[name] = 0
	}
	for _, e := range edges {
		if e.ReferencingTable == e.ReferencedTable {
			continue
		}
		if _, ok := tables[e.ReferencingTable]; !ok {
			continue
		}
		if _, ok := tables[e.ReferencedTable]; !ok {
			continue
		}
		inDegree[e.ReferencingTable]++
	}

	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}

	visited := make(map[string]bool)
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		visited[name] = true

		for _, e := range edges {
			if e.ReferencedTable != name || e.ReferencingTable == e.ReferencedTable {
				continue
			}
			if _, ok := tables[e.ReferencingTable]; !ok {
				continue
			}
			inDegree[e.ReferencingTable]--
			if inDegree[e.ReferencingTable] == 0 {
				queue = append(queue, e.ReferencingTable)
			}
		}
	}

	stuck := make(map[string]bool)
	for name := range tables {
		if !visited[name] {
			stuck[name] = true
		}
	}
	return stuck
}

// topologicalSort returns tables in insertion order (parents before children)
// using Kahn's algorithm. Self-ref edges are skipped. Returns an error if the
// graph contains a cycle.
func topologicalSort(g *FKGraph) ([]*Table, error) {
	inDegree := make(map[string]int, len(g.Tables))
	for name := range g.Tables {
		inDegree[name] = 0
	}
	for _, e := range g.Edges {
		if e.ReferencingTable == e.ReferencedTable {
			continue
		}
		if _, ok := g.Tables[e.ReferencingTable]; !ok {
			continue
		}
		if _, ok := g.Tables[e.ReferencedTable]; !ok {
			continue
		}
		inDegree[e.ReferencingTable]++
	}

	var queue []string
	for name, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, name)
		}
	}
	sort.Strings(queue)

	var result []*Table
	for len(queue) > 0 {
		name := queue[0]
		queue = queue[1:]
		result = append(result, g.Tables[name])

		for _, e := range g.Edges {
			if e.ReferencedTable != name || e.ReferencingTable == e.ReferencedTable {
				continue
			}
			if _, ok := g.Tables[e.ReferencingTable]; !ok {
				continue
			}
			inDegree[e.ReferencingTable]--
			if inDegree[e.ReferencingTable] == 0 {
				queue = append(queue, e.ReferencingTable)
				sort.Strings(queue)
			}
		}
	}

	if len(result) != len(g.Tables) {
		return nil, errors.New("cycle detected: topological sort incomplete")
	}
	return result, nil
}

// trimDAG randomly removes leaf and root tables to vary transaction size.
func trimDAG(rng *rand.Rand, sub *FKGraph) {
	trimLeaves(rng, sub)
	trimRoots(rng, sub)
}

// trimLeaves removes tables that no other table in the sub-DAG references as a
// parent (self-ref edges don't count). Each leaf is removed with 30%
// probability. At least one table is always kept.
func trimLeaves(rng *rand.Rand, sub *FKGraph) {
	referenced := make(map[string]bool)
	for _, e := range sub.Edges {
		if e.ReferencingTable != e.ReferencedTable {
			referenced[e.ReferencedTable] = true
		}
	}

	var leaves []string
	for name := range sub.Tables {
		if !referenced[name] {
			leaves = append(leaves, name)
		}
	}
	sort.Strings(leaves)

	for _, name := range leaves {
		if len(sub.Tables) <= 1 {
			break
		}
		if rng.Float64() < 0.3 {
			removeTable(sub, name)
		}
	}
}

// trimRoots removes tables that have no outbound FK edges (they don't reference
// any parent, self-ref edges don't count). A root is only eligible if no other
// table still references it. Each eligible root is removed with 30%
// probability.
func trimRoots(rng *rand.Rand, sub *FKGraph) {
	hasOutbound := make(map[string]bool)
	for _, e := range sub.Edges {
		if e.ReferencingTable != e.ReferencedTable {
			hasOutbound[e.ReferencingTable] = true
		}
	}

	var roots []string
	for name := range sub.Tables {
		if !hasOutbound[name] {
			roots = append(roots, name)
		}
	}
	sort.Strings(roots)

	for _, name := range roots {
		if len(sub.Tables) <= 1 {
			break
		}
		stillReferenced := false
		for _, e := range sub.Edges {
			if e.ReferencedTable == name && e.ReferencingTable != name {
				stillReferenced = true
				break
			}
		}
		if stillReferenced {
			continue
		}
		if rng.Float64() < 0.3 {
			removeTable(sub, name)
		}
	}
}

// removeTable deletes a table and all its edges from the sub-DAG.
func removeTable(sub *FKGraph, name string) {
	delete(sub.Tables, name)
	var surviving []FKEdge
	for _, e := range sub.Edges {
		if e.ReferencingTable == name || e.ReferencedTable == name {
			continue
		}
		surviving = append(surviving, e)
	}
	sub.Edges = surviving
}

func copyTableMap(m map[string]*Table) map[string]*Table {
	cp := make(map[string]*Table, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
