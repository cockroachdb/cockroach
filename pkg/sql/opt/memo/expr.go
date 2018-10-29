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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// RelExpr is implemented by all operators tagged as Relational. Relational
// expressions have a set of logical properties that describe the content and
// characteristics of their behavior and results. They are stored as part of a
// memo group that contains other logically equivalent expressions. Expressions
// in the same memo group are linked together in a list that can be traversed
// via calls to FirstExpr and NextExpr:
//
//      +--------------------------------------+
//      |  +---------------+                   |
//      |  |               |FirstExpr          |FirstExpr
//      v  v               |                   |
//    member #1 -------> member #2 --------> member #3 -------> nil
//              NextExpr           NextExpr            NextExpr
//
// A relational expression's physical properties and cost are defined once it
// has been optimized.
type RelExpr interface {
	opt.Expr

	// Memo is the memo which contains this relational expression.
	Memo() *Memo

	// Relational is the set of logical properties that describe the content and
	// characteristics of this expression's behavior and results.
	Relational() *props.Relational

	// Physical is the set of physical properties with respect to which this
	// expression was optimized. Enforcers may be added to the expression tree
	// to ensure the physical properties are provided.
	Physical() *props.Physical

	// FirstExpr returns the first member expression in the memo group (could be
	// this expression if it happens to be first in the group). Subsequent members
	// can be enumerated by then calling NextExpr. Note that enforcer operators
	// are not part of this list (but do maintain a link to it).
	FirstExpr() RelExpr

	// NextExpr returns the next member expression in the memo group, or nil if
	// there are no further members in the group.
	NextExpr() RelExpr

	// Cost is an estimate of the cost of executing this expression tree. Before
	// optimization, this cost will always be 0.
	Cost() Cost

	// group returns the memo group that contains this expression and any other
	// logically equivalent expressions. There is one group struct for each memo
	// group that stores the properties for the group, as well as the pointer to
	// the first member of the group.
	group() exprGroup

	// setNext sets this expression's next pointer to point to the given
	// expression. setNext will panic if the next pointer has already been set.
	setNext(e RelExpr)
}

// ScalarPropsExpr is implemented by scalar expressions which cache scalar
// properties, like FiltersExpr and ProjectionsExpr. The scalar properties are
// lazily populated only when requested by walking the expression subtree.
type ScalarPropsExpr interface {
	opt.ScalarExpr

	// ScalarProps returns the scalar properties associated with the expression.
	// These can be lazily calculated using the given memo as context.
	ScalarProps(mem *Memo) *props.Scalar
}

// TrueSingleton is a global instance of TrueExpr, to avoid allocations.
var TrueSingleton = &TrueExpr{}

// FalseSingleton is a global instance of FalseExpr, to avoid allocations.
var FalseSingleton = &FalseExpr{}

// NullSingleton is a global instance of NullExpr having the Unknown type (most
// common case), to avoid allocations.
var NullSingleton = &NullExpr{Typ: types.Unknown}

// CountRowsSingleton maintains a global instance of CountRowsExpr, to avoid
// allocations.
var CountRowsSingleton = &CountRowsExpr{}

// TrueFilter is a global instance of the empty FiltersExpr, used in situations
// where the filter should always evaluate to true:
//
//   SELECT * FROM a INNER JOIN b ON True
//
var TrueFilter = FiltersExpr{}

// FalseFilter is a global instance of a FiltersExpr that contains a single
// False expression, used in contradiction situations:
//
//   SELECT * FROM a WHERE 1=0
//
var FalseFilter = FiltersExpr{{Condition: FalseSingleton}}

// EmptyTuple is a global instance of a TupleExpr that contains no elements.
// While this cannot be created in SQL, it can be the created by normalizations.
var EmptyTuple = &TupleExpr{Typ: types.TTuple{}}

// ScalarListWithEmptyTuple is a global instance of a ScalarListExpr containing
// a TupleExpr that contains no elements. It's used when constructing an empty
// ValuesExpr:
//
//   SELECT 1
//
var ScalarListWithEmptyTuple = ScalarListExpr{EmptyTuple}

// EmptyGroupingPrivate is a global instance of a GroupingPrivate that has no
// grouping columns and no ordering.
var EmptyGroupingPrivate = GroupingPrivate{}

// MaxAggChildren is the maximum number of children that any aggregate operator
// can have.
const MaxAggChildren = 3

// LastGroupMember returns the last member in the same memo group of the given
// relational expression.
func LastGroupMember(e RelExpr) RelExpr {
	for {
		next := e.NextExpr()
		if next == nil {
			return e
		}
		e = next
	}
}

// IsTrue is true if the FiltersExpr always evaluates to true. This is the case
// when it has zero conditions.
func (n FiltersExpr) IsTrue() bool {
	return len(n) == 0
}

// IsFalse is true if the FiltersExpr always evaluates to false. The only case
// that's checked is the fully normalized case, when the list contains a single
// False condition.
func (n FiltersExpr) IsFalse() bool {
	return len(n) == 1 && n[0].Condition.Op() == opt.FalseOp
}

// OuterCols returns the set of outer columns needed by any of the filter
// condition expressions.
func (n FiltersExpr) OuterCols(mem *Memo) opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.UnionWith(n[i].ScalarProps(mem).OuterCols)
	}
	return colSet
}

// OutputCols returns the set of columns constructed by the Aggregations
// expression.
func (n AggregationsExpr) OutputCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.Add(int(n[i].Col))
	}
	return colSet
}

// TupleOrdinal is an ordinal index into an expression of type Tuple. It is
// used by the ColumnAccess scalar expression.
type TupleOrdinal uint32

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

// CanProvideOrdering returns true if the scan operator returns rows that
// satisfy the given required ordering; it also returns whether the scan needs
// to be in reverse order to match the required ordering.
func (s *ScanPrivate) CanProvideOrdering(
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

// CanProvideOrdering returns true if the row number operator returns rows that
// can satisfy the given required ordering.
func (r *RowNumberPrivate) CanProvideOrdering(required *props.OrderingChoice) bool {
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
	ordCol := opt.MakeOrderingColumn(r.ColID, false)
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
		return r.Ordering.Implies(&truncated)
	}

	return r.Ordering.Implies(required)
}

// CanProvideOrdering returns true if the MergeJoin operator returns rows that
// satisfy the given required ordering.
func (m *MergeJoinPrivate) CanProvideOrdering(required *props.OrderingChoice) bool {
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

// StreamingAggCols returns the subset of grouping columns that form a prefix of
// the ordering required of the input. These columns can be used to perform a
// streaming aggregation.
func (g *GroupingPrivate) StreamingAggCols(required *props.OrderingChoice) opt.ColSet {
	// The ordering required of the input is the intersection of the required
	// ordering on the grouping operator and the internal ordering. We use both
	// to determine the ordered grouping columns.
	var res opt.ColSet
	harvestCols := func(ord *props.OrderingChoice) {
		for i := range ord.Columns {
			cols := ord.Columns[i].Group.Intersection(g.GroupingCols)
			if cols.Empty() {
				// This group refers to a column that is not a grouping column.
				// The rest of the ordering is not useful.
				break
			}
			res.UnionWith(cols)
		}
	}
	harvestCols(required)
	harvestCols(&g.Ordering)
	return res
}
