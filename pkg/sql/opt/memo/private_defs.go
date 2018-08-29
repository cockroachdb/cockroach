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
	"fmt"

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

// TupleOrdinal is an ordinal index into an expression of type Tuple. It is
// used by the ColumnAccess scalar expression.
type TupleOrdinal uint32

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
	// (after applying any constraint), as well as the required scan direction.
	// This is a "hard" limit, meaning that the scan operator must never return
	// more than this number of rows, even if more are available. If its value is
	// zero, then the limit is unknown, and the scan should return all available
	// rows.
	HardLimit ScanLimit

	Flags ScanFlags
}

// ScanLimit is used for a limited table or index scan and stores the limit as
// well as the desired scan direction. A value of 0 means that there is no
// limit.
type ScanLimit int64

// MakeScanLimit initializes a ScanLimit with a number of rows and a direction.
func MakeScanLimit(rowCount int64, reverse bool) ScanLimit {
	if reverse {
		return ScanLimit(-rowCount)
	}
	return ScanLimit(rowCount)
}

// IsSet returns true if there is a limit.
func (sl ScanLimit) IsSet() bool {
	return sl != 0
}

// RowCount returns the number of rows in the limit.
func (sl ScanLimit) RowCount() int64 {
	if sl.Reverse() {
		return int64(-sl)
	}
	return int64(sl)
}

// Reverse returns true if the limit requires a reverse scan.
func (sl ScanLimit) Reverse() bool {
	return sl < 0
}

func (sl ScanLimit) String() string {
	if sl.Reverse() {
		return fmt.Sprintf("%d(rev)", -sl)
	}
	return fmt.Sprintf("%d", sl)
}

// ScanFlags stores any flags for the scan specified in the query (see
// tree.IndexFlags). These flags may be consulted by transformation rules or the
// coster.
type ScanFlags struct {
	// NoIndexJoin disallows use of non-covering indexes (index-join) for scanning
	// this table.
	NoIndexJoin bool

	// ForceIndex forces the use of a specific index (specified in Index).
	// ForceIndex and NoIndexJoin cannot both be set at the same time.
	ForceIndex bool
	Index      int
}

// Empty returns true if there are no flags set.
func (sf *ScanFlags) Empty() bool {
	return !sf.NoIndexJoin && !sf.ForceIndex
}

// VirtualScanOpDef defines the value of the Def private field of the
// VirtualScan operator.
type VirtualScanOpDef struct {
	// Table identifies the virtual table to synthesize and scan. It is an id
	// that can be passed to the Metadata.Table method in order to fetch
	// opt.Table metadata.
	Table opt.TableID

	// Cols specifies the set of columns that the VirtualScan operator projects.
	// This is always every column in the virtual table (i.e. never a subset even
	// if all columns are not needed).
	Cols opt.ColSet
}

// CanProvideOrdering returns true if the scan operator returns rows that
// satisfy the given required ordering; it also returns whether the scan needs
// to be in reverse order to match the required ordering.
func (s *ScanOpDef) CanProvideOrdering(
	md *opt.Metadata, required *props.OrderingChoice,
) (ok bool, reverse bool) {
	// Scan naturally orders according to scanned index's key columns. A scan can
	// be executed either as a forward or as a reverse scan (unless it has a row
	// limit, in which case the direction is fixed).
	//
	// The code below follows the structure of OrderingChoice.Implies. We go
	// through the columns and determine if the ordering matches with either scan
	// direction.

	// We start off as accepting either a forward or a reverse scan. Until then,
	// the reverse variable is unset. Once the direction is known, reverseSet is
	// true and reverse indicates whether we need to do a reverse scan.
	const (
		either = 0
		fwd    = 1
		rev    = 2
	)
	direction := either
	if s.HardLimit.IsSet() {
		// When we have a limit, the limit forces a certain scan direction (because
		// it affects the results, not just their ordering).
		direction = fwd
		if s.HardLimit.Reverse() {
			direction = rev
		}
	}
	index := md.Table(s.Table).Index(s.Index)
	for left, right := 0, 0; right < len(required.Columns); {
		if left >= index.KeyColumnCount() {
			return false, false
		}
		indexCol := index.Column(left)
		indexColID := s.Table.ColumnID(indexCol.Ordinal)
		if required.Optional.Contains(int(indexColID)) {
			left++
			continue
		}
		reqCol := &required.Columns[right]
		if !reqCol.Group.Contains(int(indexColID)) {
			return false, false
		}
		// The directions of the index column and the required column impose either
		// a forward or a reverse scan.
		required := fwd
		if indexCol.Descending != reqCol.Descending {
			required = rev
		}
		if direction == either {
			direction = required
		} else if direction != required {
			// We already determined the direction, and according to it, this column
			// has the wrong direction.
			return false, false
		}
		left, right = left+1, right+1
	}
	// If direction is either, we prefer forward scan.
	return true, direction == rev
}

// GroupByDef defines the value of the Def private field of the GroupBy and
// ScalarGroupBy operators. This struct is shared so that both operators can be
// treated polymorphically.
type GroupByDef struct {
	// GroupingCols partitions the GroupBy input rows into aggregation groups.
	// All rows sharing the same values for these columns are in the same group.
	// GroupingCols is always empty in the ScalarGroupBy case.
	GroupingCols opt.ColSet

	// Ordering specifies the sort order of values within each group. This is
	// only significant for order-sensitive aggregation operators, like ArrayAgg.
	Ordering props.OrderingChoice
}

// IndexJoinDef defines the value of the Def private field of the IndexJoin
// operator.
type IndexJoinDef struct {
	// Table identifies the table to do lookups in. The primary index is
	// currently the only index used.
	Table opt.TableID

	// Cols specifies the set of columns that the index join operator projects.
	// This may be a subset of the columns that the table contains.
	Cols opt.ColSet
}

// LookupJoinDef defines the value of the Def private field of the LookupJoin
// operators.
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
	// JoinType is InnerJoin or LeftJoin.
	// TODO(radu): support SemiJoin, AntiJoin.
	JoinType opt.Operator

	// Table identifies the table do to lookups in.
	Table opt.TableID

	// Index identifies the index to do lookups in (whether primary or secondary).
	// It can be passed to the opt.Table.Index(i int) method in order to fetch the
	// opt.Index metadata.
	Index int

	// KeyCols are the columns (produced by the input) used to create lookup keys.
	// The key columns must be non-empty, and are listed in the same order as the
	// index columns (or a prefix of them).
	KeyCols opt.ColList

	// LookupCols is the set of columns retrieved from the index. The LookupJoin
	// operator produces the columns in its input plus these columns. This set may
	// or may not contain the columns from the index we are using for the lookups
	// (which correspond to KeyCols).
	LookupCols opt.ColSet
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
}

// RowNumberDef defines the value of the Def private field of the RowNumber
// operator.
type RowNumberDef struct {
	// Ordering denotes the required ordering of the input.
	Ordering props.OrderingChoice

	// ColID holds the id of the column introduced by this operator.
	ColID opt.ColumnID
}

// CanProvideOrdering returns true if the row number operator returns rows that
// can satisfy the given required ordering.
func (w *RowNumberDef) CanProvideOrdering(required *props.OrderingChoice) bool {
	// By construction, any prefix of the ordering required of the input is also
	// ordered by the ordinality column. For example, if the required input
	// ordering is +a,+b, then any of these orderings can be provided:
	//
	//   +ord
	//   +a,+ord
	//   +a,+b,+ord
	//
	// As long as the optimizer is enabled, it will have already reduced the
	// ordering required of this operator to take into account that the ordinality
	// column is a key, so there will never be ordering columns after the
	// ordinality column in that case.
	ordCol := opt.MakeOrderingColumn(w.ColID, false)
	prefix := len(required.Columns)
	for i := range required.Columns {
		if required.MatchesAt(i, ordCol) {
			if i == 0 {
				return true
			}
			prefix = i
			break
		}
	}

	if prefix < len(required.Columns) {
		truncated := required.Copy()
		truncated.Truncate(prefix)
		return w.Ordering.Implies(&truncated)
	}

	return w.Ordering.Implies(required)
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

// MergeOnDef contains information on the equality columns we are doing a merge
// join on.
type MergeOnDef struct {
	// JoinType is one of the basic join operators: InnerJoin, LeftJoin,
	// RightJoin, FullJoin, SemiJoin, AntiJoin.
	JoinType opt.Operator

	// LeftEq and RightEq are orderings on equality columns. They have the same
	// length and LeftEq[i] is a column on the left side which is constrained to
	// be equal to RightEq[i] on the right side. The directions also have to
	// match.
	//
	// Examples of valid settings for abc JOIN def ON a=d,b=e:
	//   LeftEq: a+,b+   RightEq: d+,e+
	//   LeftEq: b-,a+   RightEq: e-,d+
	LeftEq  opt.Ordering
	RightEq opt.Ordering

	// LeftOrdering and RightOrdering are "simplified" versions of LeftEq/RightEq,
	// taking into account the functional dependencies of each side. We need both
	// versions because we need to configure execution with specific equality
	// columns and orderings.
	LeftOrdering  props.OrderingChoice
	RightOrdering props.OrderingChoice
}

// CanProvideOrdering returns true if the MergeJoin operator returns rows that
// satisfy the given required ordering.
func (m *MergeOnDef) CanProvideOrdering(required *props.OrderingChoice) bool {
	// TODO(radu): in principle, we could pass through an ordering that covers
	// more than the equality columns. For example, if we have a merge join
	// with left ordering a+,b+ and right ordering x+,y+ we could guarantee
	// a+,b+,c+ if we pass that requirement through to the left side. However,
	// this requires a specific contract on the execution side on which side's
	// ordering is preserved when multiple rows match on the equality columns.
	switch m.JoinType {
	case opt.InnerJoinOp:
		return m.LeftOrdering.Implies(required) || m.RightOrdering.Implies(required)

	case opt.LeftJoinOp, opt.SemiJoinOp, opt.AntiJoinOp:
		return m.LeftOrdering.Implies(required)

	case opt.RightJoinOp:
		return m.RightOrdering.Implies(required)

	default:
		return false
	}
}
