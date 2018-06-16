// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package memo

import (
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// PrivateID identifies custom private data used by a memo expression and
// stored by the memo. Privates have numbers greater than 0; a PrivateID of 0
// indicates an unknown private.
type PrivateID uint32

// FuncOpDef defines the value of the Def private field of the Function
// operator. It provides the name and return type of the function, as well as a
// pointer to an already resolved builtin overload definition.
type FuncOpDef struct {
	Name       string
	Type       types.T
	Properties *tree.FunctionProperties
	Overload   *tree.Overload
}

func (f FuncOpDef) String() string {
	return f.Name
}

// ProjectionsOpDef defines the value of the Def private field of the
// Projections operator. It contains information about the projected columns.
type ProjectionsOpDef struct {
	// SynthesizedCols is a list of columns that matches 1-to-1 with Elems in
	// the ProjectionsOp.
	SynthesizedCols opt.ColList

	// PassthroughCols are columns that are projected unchanged. Passthrough
	// columns must be produced by the input (they can't be outer columns). Outer
	// column refs must be wrapped in VariableOp, with a new ColumnID in
	// SynthesizedCols.
	PassthroughCols opt.ColSet
}

// AllCols returns the set of all columns in the projection (synthesized and
// pass-through).
func (p ProjectionsOpDef) AllCols() opt.ColSet {
	if len(p.SynthesizedCols) == 0 {
		return p.PassthroughCols
	}
	s := p.PassthroughCols.Copy()
	for _, c := range p.SynthesizedCols {
		s.Add(int(c))
	}
	return s
}

// ScanOpDef defines the value of the Def private field of the Scan operator.
type ScanOpDef struct {
	// Table identifies the table to scan. It is an id that can be passed to
	// the Metadata.Table method in order to fetch opt.Table metadata.
	Table opt.TableID

	// Index identifies the index to scan (whether primary or secondary). It
	// can be passed to the opt.Table.Index(i int) method in order to fetch the
	// opt.Index metadata.
	Index int

	// Cols specifies the set of columns that the scan operator projects. This
	// may be a subset of the columns that the table/index contains.
	Cols opt.ColSet

	// If set, the scan is a constrained scan; the constraint contains the spans
	// that need to be scanned.
	Constraint *constraint.Constraint

	// HardLimit specifies the maximum number of rows that the scan can return
	// (after applying any constraint). This is a "hard" limit, meaning that
	// the scan operator must never return more than this number of rows, even
	// if more are available. If its value is zero, then the limit is
	// unknown, and the scan should return all available rows.
	HardLimit int64
}

// CanProvideOrdering returns true if the scan operator returns rows that
// satisfy the given required ordering.
func (s *ScanOpDef) CanProvideOrdering(md *opt.Metadata, required opt.Ordering) bool {
	// Scan naturally orders according to the order of the scanned index.
	index := md.Table(s.Table).Index(s.Index)

	// The index can provide the required ordering in either of these cases:
	// 1. The ordering columns are a prefix of the index columns.
	// 2. The index columns are a prefix of the ordering columns (this
	//    works because the columns are always a key, so any additional
	//    columns are unnecessary).
	// TODO(andyk): Use UniqueColumnCount when issues with nulls are solved,
	//              since unique index can still have duplicate nulls.
	cnt := index.ColumnCount()
	if s.Index == opt.PrimaryIndex {
		cnt = index.UniqueColumnCount()
	}
	if len(required) < cnt {
		cnt = len(required)
	}

	for i := 0; i < cnt; i++ {
		indexCol := index.Column(i)
		colID := md.TableColumn(s.Table, indexCol.Ordinal)
		orderingCol := opt.MakeOrderingColumn(colID, indexCol.Descending)
		if orderingCol != required[i] {
			return false
		}
	}
	return true
}

// GroupByDef defines the value of the Def private field of the GroupBy
// operator.
type GroupByDef struct {
	GroupingCols opt.ColSet
	Ordering     opt.Ordering
}

// LookupJoinDef defines the value of the Def private field of the LookupJoin
// operator.
//
// Example 1: join between two tables
//
//    CREATE TABLE abc (a INT, b INT, c INT)
//    CREATE TABLE xyz (x INT, y INT, z INT, PRIMARY KEY (x,y))
//    SELECT * FROM abc JOIN xyz ON (a=x) AND (b=y)
//
//    Input: scan from table abc.
//    Table: xyz
//    KeyCols: a, b
//    LookupCols: z
//
// Example 2: index join:
//
//    CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT, INDEX (b))
//    SELECT * FROM abc WHERE b=1
//
//    Input: scan on the index on b (returning columns a, b)
//    Table: abc
//    KeyCols: a
//    LookupCols: c
//
type LookupJoinDef struct {
	// Table identifies the table do to lookups in. The primary index is
	// currently the only index used.
	Table opt.TableID

	// Index identifies the index to do lookups in (whether primary or secondary).
	// It can be passed to the opt.Table.Index(i int) method in order to fetch the
	// opt.Index metadata.
	Index int

	// KeyCols are the columns (produced by the input) used to create lookup keys.
	// The key columns must be non-empty, and are listed in the same order as the
	// index columns (or a prefix of them). If this is an index join, then the key
	// column ids on both sides of the join will always be the same.
	KeyCols opt.ColList

	// LookupCols is the set of columns retrieved from the index. This set does
	// not include the key columns. The LookupJoin operator produces the columns
	// in its input plus these columns.
	LookupCols opt.ColSet
}

// IsIndexJoin is true if the lookup join is an index join, meaning that the
// input columns are from the same table as the lookup columns. In this special
// case, there is always a 1:1 relationship between input and output rows.
func (l *LookupJoinDef) IsIndexJoin(md *opt.Metadata) bool {
	// The input and index key column sets will be the same if this is an index
	// join, or always disjoint if not.
	ord := md.Table(l.Table).Index(l.Index).Column(0).Ordinal
	return md.TableColumn(l.Table, ord) == l.KeyCols[0]
}

// ExplainOpDef defines the value of the Def private field of the Explain operator.
type ExplainOpDef struct {
	Options tree.ExplainOptions

	// ColList stores the column IDs for the explain columns.
	ColList opt.ColList

	// Props stores the required physical properties for the enclosed expression.
	Props props.Physical
}

// ShowTraceOpDef defines the value of the Def private field of the Explain operator.
type ShowTraceOpDef struct {
	Type tree.ShowTraceType

	// Compact indicates that we output a smaller set of columns; set
	// when SHOW COMPACT [KV] TRACE is used.
	Compact bool

	// ColList stores the column IDs for the SHOW TRACE columns.
	ColList opt.ColList

	// Props stores the required physical properties for the enclosed expression.
	Props props.Physical
}

// RowNumberDef defines the value of the Def private field of the RowNumber
// operator.
type RowNumberDef struct {
	// Ordering denotes the required ordering of the input.
	Ordering opt.Ordering

	// ColID holds the id of the column introduced by this operator.
	ColID opt.ColumnID
}

// CanProvideOrdering returns true if the row number operator returns rows that
// can satisfy the given required ordering.
func (w *RowNumberDef) CanProvideOrdering(required opt.Ordering) bool {
	// RowNumber can provide the same ordering it requires from its input.

	// By construction, the ordinality is a key, and the output is always ordered
	// ascending by it, so any ordering columns after an ascending ordinality are
	// irrelevant.
	// TODO(justin): This could probably be generalized to some helper - when we
	// are checking if an ordering can be satisfied in this way, we can return
	// true early if the set of columns we have iterated over are a key.
	ordCol := opt.MakeOrderingColumn(w.ColID, false)
	for i, col := range required {
		if col == ordCol {
			return true
		}
		if i >= len(w.Ordering) || col != w.Ordering[i] {
			return false
		}
	}
	return true
}

// SetOpColMap defines the value of the ColMap private field of the set
// operators: Union, Intersect, Except, UnionAll, IntersectAll and ExceptAll.
// It matches columns from the left and right inputs of the operator
// with the output columns, since OutputCols are not ordered and may
// not correspond to each other.
//
// For example, consider the following query:
//   SELECT y, x FROM xy UNION SELECT b, a FROM ab
//
// Given:
//   col  index
//   x    1
//   y    2
//   a    3
//   b    4
//
// SetOpColMap will contain the following values:
//   Left:  [2, 1]
//   Right: [4, 3]
//   Out:   [5, 6]  <-- synthesized output columns
type SetOpColMap struct {
	Left  opt.ColList
	Right opt.ColList
	Out   opt.ColList
}
