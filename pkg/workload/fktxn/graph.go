// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fktxn

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// Column represents a column in a table.
type Column struct {
	Name     string
	Nullable bool
	// Computed is true when the column is a stored or virtual generated
	// column. The txn generator skips computed columns in INSERT/UPSERT
	// column lists (the DB rejects writes to them) and reads them back via
	// SELECT after the parent UPSERT when downstream FKs depend on their
	// values.
	Computed bool
	// Type is the column's SQL type. Used by the txn generator to produce
	// type-appropriate random values via randgen.RandDatum. May be nil for
	// columns whose type could not be parsed (e.g. user-defined types); the
	// txn generator must skip such columns or treat them as opaque.
	Type *types.T
}

// UniqueConstraint represents a UNIQUE or PRIMARY KEY constraint.
type UniqueConstraint struct {
	Name      string
	Columns   []string
	IsPrimary bool
}

// FKEdge represents a foreign key constraint from a child table to a parent
// table. ReferencingTable/Columns are the child side; ReferencedTable/Columns
// are the parent side.
type FKEdge struct {
	Name                 string
	ReferencingTable     string
	ReferencingColumns   []string
	ReferencedTable      string
	ReferencedColumns    []string
	ReferencedConstraint string
	Nullable             bool
}

// Table represents a database table with its columns, unique constraints, and
// FK edges in both directions.
type Table struct {
	Name              string
	Columns           []Column
	UniqueConstraints []UniqueConstraint
	OutboundFKs       []FKEdge
	InboundFKs        []FKEdge
}

// Schema holds all tables keyed by name.
type Schema struct {
	Tables map[string]*Table
}

// NewSchema creates an empty Schema.
func NewSchema() *Schema {
	return &Schema{Tables: make(map[string]*Table)}
}

// AddTable adds a table to the schema.
func (s *Schema) AddTable(t *Table) {
	s.Tables[t.Name] = t
}

// String returns a deterministic text representation of the schema, sorted by
// table name. Useful for datadriven test output.
func (s *Schema) String() string {
	names := make([]string, 0, len(s.Tables))
	for name := range s.Tables {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for i, name := range names {
		if i > 0 {
			b.WriteByte('\n')
		}
		t := s.Tables[name]
		fmt.Fprintf(&b, "table %s\n", t.Name)

		// Columns.
		colStrs := make([]string, len(t.Columns))
		for j, c := range t.Columns {
			if c.Nullable {
				colStrs[j] = c.Name
			} else {
				colStrs[j] = c.Name + " (not null)"
			}
		}
		fmt.Fprintf(&b, "  columns: [%s]\n", strings.Join(colStrs, ", "))

		// Unique constraints, sorted by name.
		ucs := make([]UniqueConstraint, len(t.UniqueConstraints))
		copy(ucs, t.UniqueConstraints)
		sort.Slice(ucs, func(a, b int) bool { return ucs[a].Name < ucs[b].Name })
		for _, uc := range ucs {
			kind := "unique"
			if uc.IsPrimary {
				kind = "primary"
			}
			fmt.Fprintf(&b, "  %s: %s [%s]\n", kind, uc.Name, strings.Join(uc.Columns, ", "))
		}

		// Outbound FKs, sorted by name.
		outFKs := make([]FKEdge, len(t.OutboundFKs))
		copy(outFKs, t.OutboundFKs)
		sort.Slice(outFKs, func(a, b int) bool { return outFKs[a].Name < outFKs[b].Name })
		for _, fk := range outFKs {
			nullable := ""
			if fk.Nullable {
				nullable = " (nullable)"
			}
			fmt.Fprintf(&b, "  fk: %s %s(%s) -> %s(%s)%s\n",
				fk.Name,
				fk.ReferencingTable, strings.Join(fk.ReferencingColumns, ", "),
				fk.ReferencedTable, strings.Join(fk.ReferencedColumns, ", "),
				nullable,
			)
		}

		// Inbound FKs, sorted by name.
		inFKs := make([]FKEdge, len(t.InboundFKs))
		copy(inFKs, t.InboundFKs)
		sort.Slice(inFKs, func(a, b int) bool { return inFKs[a].Name < inFKs[b].Name })
		for _, fk := range inFKs {
			fmt.Fprintf(&b, "  referenced by: %s %s(%s)\n",
				fk.Name,
				fk.ReferencingTable, strings.Join(fk.ReferencingColumns, ", "),
			)
		}
	}
	return b.String()
}

// FKGraph is a connected component of tables and FK edges determined by
// column-overlap transitivity. Nodes are tables, edges are FK constraints.
// A table can appear in multiple FKGraphs if it has UCs connecting to
// non-overlapping FK edge sets.
type FKGraph struct {
	Tables map[string]*Table
	Edges  []FKEdge
}

// Finalize cross-links InboundFKs on each parent table from the OutboundFKs
// on child tables. Call after all tables and FKs have been added.
func (s *Schema) Finalize() {
	for _, t := range s.Tables {
		t.InboundFKs = nil
	}
	for _, t := range s.Tables {
		for _, fk := range t.OutboundFKs {
			parent, ok := s.Tables[fk.ReferencedTable]
			if !ok {
				continue
			}
			parent.InboundFKs = append(parent.InboundFKs, fk)
		}
	}
}

// BuildFKGraphs computes connected components of tables and FK edges using
// column-overlap transitivity. Two FK edges are in the same component if they
// are connected through a UC on a shared table where one FK's referencing
// columns overlap with the UC's columns.
func BuildFKGraphs(s *Schema) []*FKGraph {
	// Collect all FK edges with integer IDs.
	var allEdges []FKEdge
	for _, t := range s.Tables {
		allEdges = append(allEdges, t.OutboundFKs...)
	}
	if len(allEdges) == 0 {
		return nil
	}

	// Build index: constraint name → edge index.
	edgeIdx := make(map[string]int, len(allEdges))
	for i, e := range allEdges {
		edgeIdx[e.Name] = i
	}

	uf := newUnionFind(len(allEdges))

	// For each (table, UC), union inbound FKs targeting this UC with outbound
	// FKs whose referencing columns overlap the UC's columns.
	for _, t := range s.Tables {
		for _, uc := range t.UniqueConstraints {
			var inboundToUC []int
			for _, fk := range t.InboundFKs {
				if fk.ReferencedConstraint == uc.Name {
					if idx, ok := edgeIdx[fk.Name]; ok {
						inboundToUC = append(inboundToUC, idx)
					}
				}
			}

			var outboundOverlapping []int
			for _, fk := range t.OutboundFKs {
				if columnsOverlap(fk.ReferencingColumns, uc.Columns) {
					if idx, ok := edgeIdx[fk.Name]; ok {
						outboundOverlapping = append(outboundOverlapping, idx)
					}
				}
			}

			// Union all edges in both sets.
			all := append(inboundToUC, outboundOverlapping...)
			for i := 1; i < len(all); i++ {
				uf.union(all[0], all[i])
			}
		}
	}

	// Group edges by union-find root.
	groups := make(map[int][]int)
	for i := range allEdges {
		root := uf.find(i)
		groups[root] = append(groups[root], i)
	}

	// Build FKGraphs from groups.
	var graphs []*FKGraph
	for _, indices := range groups {
		g := &FKGraph{Tables: make(map[string]*Table)}
		for _, i := range indices {
			e := allEdges[i]
			g.Edges = append(g.Edges, e)
			if t, ok := s.Tables[e.ReferencingTable]; ok {
				g.Tables[e.ReferencingTable] = t
			}
			if t, ok := s.Tables[e.ReferencedTable]; ok {
				g.Tables[e.ReferencedTable] = t
			}
		}
		graphs = append(graphs, g)
	}

	// Sort graphs deterministically by smallest table name.
	sort.Slice(graphs, func(i, j int) bool {
		return smallestTableName(graphs[i]) < smallestTableName(graphs[j])
	})

	return graphs
}

func smallestTableName(g *FKGraph) string {
	var smallest string
	for name := range g.Tables {
		if smallest == "" || name < smallest {
			smallest = name
		}
	}
	return smallest
}

func columnsOverlap(a, b []string) bool {
	for _, ca := range a {
		for _, cb := range b {
			if ca == cb {
				return true
			}
		}
	}
	return false
}

// String returns a deterministic text representation of the FKGraph.
func (g *FKGraph) String() string {
	names := make([]string, 0, len(g.Tables))
	for name := range g.Tables {
		names = append(names, name)
	}
	sort.Strings(names)

	edges := make([]FKEdge, len(g.Edges))
	copy(edges, g.Edges)
	sort.Slice(edges, func(i, j int) bool { return edges[i].Name < edges[j].Name })

	var b strings.Builder
	fmt.Fprintf(&b, "tables: [%s]\n", strings.Join(names, ", "))
	for _, e := range edges {
		fmt.Fprintf(&b, "  %s: %s(%s) -> %s(%s)\n",
			e.Name,
			e.ReferencingTable, strings.Join(e.ReferencingColumns, ", "),
			e.ReferencedTable, strings.Join(e.ReferencedColumns, ", "),
		)
	}
	return b.String()
}

type unionFind struct {
	parent []int
	rank   []int
}

func newUnionFind(n int) *unionFind {
	uf := &unionFind{
		parent: make([]int, n),
		rank:   make([]int, n),
	}
	for i := range uf.parent {
		uf.parent[i] = i
	}
	return uf
}

func (uf *unionFind) find(x int) int {
	for uf.parent[x] != x {
		uf.parent[x] = uf.parent[uf.parent[x]]
		x = uf.parent[x]
	}
	return x
}

func (uf *unionFind) union(x, y int) {
	rx, ry := uf.find(x), uf.find(y)
	if rx == ry {
		return
	}
	if uf.rank[rx] < uf.rank[ry] {
		rx, ry = ry, rx
	}
	uf.parent[ry] = rx
	if uf.rank[rx] == uf.rank[ry] {
		uf.rank[rx]++
	}
}
