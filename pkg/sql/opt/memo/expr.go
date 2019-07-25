// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package memo

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
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

	// RequiredPhysical is the set of required physical properties with respect to
	// which this expression was optimized. Enforcers may be added to the
	// expression tree to ensure the physical properties are provided.
	//
	// Set when optimization is complete, only for the expressions in the final
	// tree.
	RequiredPhysical() *physical.Required

	// ProvidedPhysical is the set of provided physical properties (which must be
	// compatible with the set of required physical properties).
	//
	// Set when optimization is complete, only for the expressions in the final
	// tree.
	ProvidedPhysical() *physical.Provided

	// Cost is an estimate of the cost of executing this expression tree. Set
	// when optimization is complete, only for the expressions in the final tree.
	Cost() Cost

	// FirstExpr returns the first member expression in the memo group (could be
	// this expression if it happens to be first in the group). Subsequent members
	// can be enumerated by then calling NextExpr. Note that enforcer operators
	// are not part of this list (but do maintain a link to it).
	FirstExpr() RelExpr

	// NextExpr returns the next member expression in the memo group, or nil if
	// there are no further members in the group.
	NextExpr() RelExpr

	// group returns the memo group that contains this expression and any other
	// logically equivalent expressions. There is one group struct for each memo
	// group that stores the properties for the group, as well as the pointer to
	// the first member of the group.
	group() exprGroup

	// bestProps returns the instance of bestProps associated with this
	// expression.
	bestProps() *bestProps

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

// TODO(justin): perhaps these should be auto-generated.

// RankSingleton is the global instance of RankExpr.
var RankSingleton = &RankExpr{}

// RowNumberSingleton is the global instance of RowNumber.
var RowNumberSingleton = &RowNumberExpr{}

// DenseRankSingleton is the global instance of DenseRankExpr.
var DenseRankSingleton = &DenseRankExpr{}

// PercentRankSingleton is the global instance of PercentRankExpr.
var PercentRankSingleton = &PercentRankExpr{}

// CumeDistSingleton is the global instance of CumeDistExpr.
var CumeDistSingleton = &CumeDistExpr{}

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
var EmptyTuple = &TupleExpr{Typ: types.EmptyTuple}

// ScalarListWithEmptyTuple is a global instance of a ScalarListExpr containing
// a TupleExpr that contains no elements. It's used when constructing an empty
// ValuesExpr:
//
//   SELECT 1
//
var ScalarListWithEmptyTuple = ScalarListExpr{EmptyTuple}

// EmptyGroupingPrivate is a global instance of a GroupingPrivate that has no
// grouping columns and no ordering.
var EmptyGroupingPrivate = &GroupingPrivate{}

// EmptyJoinPrivate is a global instance of a JoinPrivate that has no fields
// set.
var EmptyJoinPrivate = &JoinPrivate{}

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

// Sort sorts the FilterItems in n by the IDs of the expression.
func (n *FiltersExpr) Sort() {
	sort.Slice(*n, func(i, j int) bool {
		return (*n)[i].Condition.(opt.ScalarExpr).ID() < (*n)[j].Condition.(opt.ScalarExpr).ID()
	})
}

// Deduplicate removes all the duplicate filters from n.
func (n *FiltersExpr) Deduplicate() {
	dedup := (*n)[:0]

	// Only add it if it hasn't already been added.
	for i, filter := range *n {
		found := false
		for j := i - 1; j >= 0; j-- {
			previouslySeenFilter := (*n)[j]
			if previouslySeenFilter.Condition == filter.Condition {
				found = true
				break
			}
		}
		if !found {
			dedup = append(dedup, filter)
		}
	}

	*n = dedup
}

// RetainCommonFilters retains only the filters found in n and other.
func (n *FiltersExpr) RetainCommonFilters(other FiltersExpr) {
	// TODO(ridwanmsharif): Faster intersection using a map
	common := (*n)[:0]
	for _, filter := range *n {
		for _, otherFilter := range other {
			if filter.Condition == otherFilter.Condition {
				common = append(common, filter)
				break
			}
		}
	}
	*n = common
}

// RemoveCommonFilters removes the filters found in other from n.
func (n *FiltersExpr) RemoveCommonFilters(other FiltersExpr) {
	// TODO(ridwanmsharif): Faster intersection using a map
	common := (*n)[:0]
	for _, filter := range *n {
		found := false
		for _, otherFilter := range other {
			if filter.Condition == otherFilter.Condition {
				found = true
				break
			}
		}
		if !found {
			common = append(common, filter)
		}
	}
	*n = common
}

// OutputCols returns the set of columns constructed by the Aggregations
// expression.
func (n AggregationsExpr) OutputCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.Add(n[i].Col)
	}
	return colSet
}

// OuterCols returns the set of outer columns needed by any of the zip
// expressions.
func (n ZipExpr) OuterCols(mem *Memo) opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.UnionWith(n[i].ScalarProps(mem).OuterCols)
	}
	return colSet
}

// OutputCols returns the set of columns constructed by the Zip expression.
func (n ZipExpr) OutputCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		for _, col := range n[i].Cols {
			colSet.Add(col)
		}
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
	Direction  tree.Direction
	Index      int
}

// Empty returns true if there are no flags set.
func (sf *ScanFlags) Empty() bool {
	return !sf.NoIndexJoin && !sf.ForceIndex
}

// JoinFlags stores restrictions on the join execution method, derived from
// hints for a join specified in the query (see tree.JoinTableExpr).
type JoinFlags struct {
	DisallowLookupJoin bool
	DisallowMergeJoin  bool
	DisallowHashJoin   bool
}

// Empty returns true if there are no flags set.
func (jf JoinFlags) Empty() bool {
	return !jf.DisallowLookupJoin && !jf.DisallowMergeJoin && !jf.DisallowHashJoin
}

func (jf JoinFlags) String() string {
	if jf.Empty() {
		return "no flags"
	}
	var b strings.Builder
	if jf.DisallowLookupJoin {
		b.WriteString("no-lookup-join")
	}
	if jf.DisallowMergeJoin {
		if b.Len() > 0 {
			b.WriteByte(';')
		}
		b.WriteString("no-merge-join")
	}
	if jf.DisallowHashJoin {
		if b.Len() > 0 {
			b.WriteByte(';')
		}
		b.WriteString("no-hash-join")
	}
	return b.String()
}

// WindowFrame denotes the definition of a window frame for an individual
// window function, excluding the OFFSET expressions, if present.
type WindowFrame struct {
	Mode           tree.WindowFrameMode
	StartBoundType tree.WindowFrameBoundType
	EndBoundType   tree.WindowFrameBoundType
}

// NeedResults returns true if the mutation operator can return the rows that
// were mutated.
func (m *MutationPrivate) NeedResults() bool {
	return m.ReturnCols != nil
}

// IsColumnOutput returns true if the i-th ordinal column should be part of the
// mutation's output columns.
func (m *MutationPrivate) IsColumnOutput(i int) bool {
	return i < len(m.ReturnCols) && m.ReturnCols[i] != 0
}

// MapToInputID maps from the ID of a returned column to the ID of the
// corresponding input column that provides the value for it. If there is no
// matching input column ID, MapToInputID returns 0.
//
// NOTE: This can only be called if the mutation operator returns rows.
func (m *MutationPrivate) MapToInputID(tabColID opt.ColumnID) opt.ColumnID {
	if m.ReturnCols == nil {
		panic(errors.AssertionFailedf("MapToInputID cannot be called if ReturnCols is not defined"))
	}
	ord := m.Table.ColumnOrdinal(tabColID)
	return m.ReturnCols[ord]
}

// MapToInputCols maps the given set of table columns to a corresponding set of
// input columns using the MapToInputID function.
func (m *MutationPrivate) MapToInputCols(tabCols opt.ColSet) opt.ColSet {
	var inCols opt.ColSet
	tabCols.ForEach(func(t opt.ColumnID) {
		id := m.MapToInputID(t)
		if id == 0 {
			panic(errors.AssertionFailedf("could not find input column for %d", log.Safe(t)))
		}
		inCols.Add(id)
	})
	return inCols
}

// AddEquivTableCols adds an FD to the given set that declares an equivalence
// between each table column and its corresponding input column.
func (m *MutationPrivate) AddEquivTableCols(md *opt.Metadata, fdset *props.FuncDepSet) {
	for i, n := 0, md.Table(m.Table).DeletableColumnCount(); i < n; i++ {
		t := m.Table.ColumnID(i)
		id := m.MapToInputID(t)
		if id != 0 {
			fdset.AddEquivalency(t, id)
		}
	}
}

// ExprIsNeverNull makes a best-effort attempt to prove that the provided
// scalar is always non-NULL, given the set of outer columns that are known
// to be not null. This is particularly useful with check constraints.
// Check constraints are satisfied when the condition evaluates to NULL,
// whereas filters are not. For example consider the following check constraint:
//
// CHECK (col IN (1, 2, NULL))
//
// Any row evaluating this check constraint with any value for the column will
// satisfy this check constraint, as they would evaluate to true (in the case
// of 1 or 2) or NULL (in the case of everything else).
func ExprIsNeverNull(e opt.ScalarExpr, notNullCols opt.ColSet) bool {
	switch t := e.(type) {
	case *VariableExpr:
		return notNullCols.Contains(t.Col)

	case *TrueExpr, *FalseExpr, *ConstExpr, *IsExpr, *IsNotExpr:
		return true

	case *NullExpr:
		return false

	case *TupleExpr:
		// TODO(ridwanmsharif): Make this less conservative and instead update how
		// IN and NOT IN behave w.r.t tuples and how IndirectionExpr works with arrays.
		// Currently, the semantics of this function on Tuples are different
		// as it returns whether a NULL evaluation is possible given the composition of
		// the tuple. Changing this will require some additional logic in the IN cases.
		for i := range t.Elems {
			if !ExprIsNeverNull(t.Elems[i], notNullCols) {
				return false
			}
		}
		return true

	case *InExpr, *NotInExpr:
		// TODO(ridwanmsharif): If a tuple is found in either side, determine if the
		// expression is nullable based on the composition of the tuples.
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols) &&
			ExprIsNeverNull(t.Child(1).(opt.ScalarExpr), notNullCols)

	case *ArrayExpr:
		for i := range t.Elems {
			if !ExprIsNeverNull(t.Elems[i], notNullCols) {
				return false
			}
		}
		return true

	case *CaseExpr:
		for i := range t.Whens {
			if !ExprIsNeverNull(t.Whens[i], notNullCols) {
				return false
			}
		}
		return ExprIsNeverNull(t.Input, notNullCols) && ExprIsNeverNull(t.OrElse, notNullCols)

	case *CastExpr, *NotExpr, *RangeExpr:
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols)

	case *AndExpr, *OrExpr, *GeExpr, *GtExpr, *NeExpr, *EqExpr, *LeExpr, *LtExpr, *LikeExpr,
		*NotLikeExpr, *ILikeExpr, *NotILikeExpr, *SimilarToExpr, *NotSimilarToExpr, *RegMatchExpr,
		*NotRegMatchExpr, *RegIMatchExpr, *NotRegIMatchExpr, *ContainsExpr, *JsonExistsExpr,
		*JsonAllExistsExpr, *JsonSomeExistsExpr, *AnyScalarExpr, *BitandExpr, *BitorExpr, *BitxorExpr,
		*PlusExpr, *MinusExpr, *MultExpr, *DivExpr, *FloorDivExpr, *ModExpr, *PowExpr, *ConcatExpr,
		*LShiftExpr, *RShiftExpr, *WhenExpr:
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols) &&
			ExprIsNeverNull(t.Child(1).(opt.ScalarExpr), notNullCols)

	default:
		return false
	}
}
