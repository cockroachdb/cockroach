// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package memo

import (
	"context"
	"fmt"
	"math/bits"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// RelExpr is implemented by all operators tagged as Relational. Relational
// expressions have a set of logical properties that describe the content and
// characteristics of their behavior and results. They are stored as part of a
// memo group that contains other logically equivalent expressions. Expressions
// in the same memo group are linked together in a list that can be traversed
// via calls to FirstExpr and NextExpr:
//
//	  +--------------------------------------+
//	  |  +---------------+                   |
//	  |  |               |FirstExpr          |FirstExpr
//	  v  v               |                   |
//	member #1 -------> member #2 --------> member #3 -------> nil
//	          NextExpr           NextExpr            NextExpr
//
// A relational expression's physical properties and cost are defined once it
// has been optimized.
type RelExpr interface {
	opt.Expr

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
// properties, like FiltersExpr and ProjectionsExpr. These expressions are also
// tagged with the ScalarProps tag.
type ScalarPropsExpr interface {
	opt.ScalarExpr

	// ScalarProps returns the scalar properties associated with the expression.
	ScalarProps() *props.Scalar
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
//	SELECT * FROM a INNER JOIN b ON True
var TrueFilter = FiltersExpr{}

// EmptyTuple is a global instance of a TupleExpr that contains no elements.
// While this cannot be created in SQL, it can be the created by normalizations.
var EmptyTuple = &TupleExpr{Typ: types.EmptyTuple}

// ScalarListWithEmptyTuple is a global instance of a ScalarListExpr containing
// a TupleExpr that contains no elements. It's used when constructing an empty
// ValuesExpr:
//
//	SELECT 1
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
func (n FiltersExpr) OuterCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.UnionWith(n[i].ScalarProps().OuterCols)
	}
	return colSet
}

// Sort sorts the FilterItems in n by the ranks of the expressions.
func (n *FiltersExpr) Sort() {
	sort.Slice(*n, func(i, j int) bool {
		return (*n)[i].Condition.Rank() < (*n)[j].Condition.Rank()
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

// RemoveFiltersItem returns a new list that is a copy of the given list, except
// that it does not contain the given FiltersItem. If the list contains the item
// multiple times, then only the first instance is removed. If the list does not
// contain the item, then the method panics.
func (n FiltersExpr) RemoveFiltersItem(search *FiltersItem) FiltersExpr {
	newFilters := make(FiltersExpr, len(n)-1)
	for i := range n {
		if search == &n[i] {
			copy(newFilters, n[:i])
			copy(newFilters[i:], n[i+1:])
			return newFilters
		}
	}
	panic(errors.AssertionFailedf("item to remove is not in the list: %v", search))
}

// Difference returns a new list of filters containing the filters of n that are
// not in other. If other is empty, n is returned.
func (n FiltersExpr) Difference(other FiltersExpr) FiltersExpr {
	if len(other) == 0 {
		return n
	}
	newFilters := make(FiltersExpr, 0, len(n))
	for i := range n {
		found := false
		for j := range other {
			if n[i].Condition == other[j].Condition {
				found = true
				break
			}
		}
		if !found {
			newFilters = append(newFilters, n[i])
		}
	}
	return newFilters
}

// NoOpDistribution returns true if a DistributeExpr has the same distribution
// as its input.
func (e *DistributeExpr) NoOpDistribution() bool {
	if target, source, ok := e.GetDistributions(); ok {
		return target.Equals(source)
	}
	return false
}

// GetDistributions returns the target and source Distributions of this
// `DistributeExpr`, if the provided physical properties have been built.
func (e *DistributeExpr) GetDistributions() (target, source physical.Distribution, ok bool) {
	distributionProvidedPhysical := e.ProvidedPhysical()
	inputDistributionProvidedPhysical := e.Input.ProvidedPhysical()
	if distributionProvidedPhysical != nil && inputDistributionProvidedPhysical != nil {
		target = distributionProvidedPhysical.Distribution
		source = inputDistributionProvidedPhysical.Distribution
		return target, source, true
	}
	return physical.Distribution{}, physical.Distribution{}, false
}

// HasHomeRegion returns true if the operation tree being distributed has a home
// region.
func (e *DistributeExpr) HasHomeRegion() bool {
	inputDistributionProvidedPhysical := e.Input.ProvidedPhysical()

	if inputDistributionProvidedPhysical != nil {
		inputDistribution := inputDistributionProvidedPhysical.Distribution
		return len(inputDistribution.Regions) == 1
	}
	return false
}

// GetRegionOfDistribution returns the single region name of the provided
// distribution, if there is exactly one.
func (e *DistributeExpr) GetRegionOfDistribution() (region string, ok bool) {
	distributionProvidedPhysical := e.ProvidedPhysical()

	if distributionProvidedPhysical != nil {
		return distributionProvidedPhysical.Distribution.GetSingleRegion()
	}
	return "", false
}

// GetInputHomeRegion returns the single region name of the home region of the
// input expression tree to the Distribute operation, if there is exactly one.
func (e *DistributeExpr) GetInputHomeRegion() (inputHomeRegion string, ok bool) {
	inputDistributionProvidedPhysical := e.Input.ProvidedPhysical()

	if inputDistributionProvidedPhysical != nil {
		return inputDistributionProvidedPhysical.Distribution.GetSingleRegion()
	}
	return "", false
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
func (n ZipExpr) OuterCols() opt.ColSet {
	var colSet opt.ColSet
	for i := range n {
		colSet.UnionWith(n[i].ScalarProps().OuterCols)
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

	// NoZigzagJoin disallows use of a zigzag join for scanning this table.
	NoZigzagJoin bool

	// NoFullScan disallows use of a full scan for scanning this table.
	NoFullScan bool

	// AvoidFullScan avoids use of a full scan for scanning this table.
	AvoidFullScan bool

	// ForceIndex forces the use of a specific index (specified in Index).
	// ForceIndex and NoIndexJoin cannot both be set at the same time.
	ForceIndex bool
	// ForceInvertedIndex forces the use of an inverted index.
	ForceInvertedIndex bool
	ForceZigzag        bool

	// When the optimizer is performing unique constraint or foreign key
	// constraint check, we will temporarily disable the not visible index feature
	// and treat all indexes as they are visible. Default behaviour is to enable
	// the not visible feature. We will pass this flag, DisableNotVisibleIndex, to
	// the memo group when we are building a memo group for a ScanOp expression on
	// the given table. Later on, optimizer will enumerate all indexes on the
	// table to generate equivalent memo groups. If DisableNotVisibleIndex is
	// true, optimizer will also generate equivalent memo group using the
	// invisible index. Otherwise, optimizer will ignore the invisible indexes.
	DisableNotVisibleIndex bool

	Direction tree.Direction
	Index     int

	// ZigzagIndexes makes planner prefer a zigzag with particular indexes.
	// ForceZigzag must also be true.
	ZigzagIndexes intsets.Fast
}

// Empty returns true if there are no flags set.
func (sf *ScanFlags) Empty() bool {
	return *sf == ScanFlags{}
}

// onlyNotVisibleIndexFlagOn returns true if DisableNotVisibleIndex is the only
// flag set on.
func (sf *ScanFlags) onlyNotVisibleIndexFlagOn() bool {
	return *sf == ScanFlags{DisableNotVisibleIndex: true}
}

// JoinFlags stores restrictions on the join execution method, derived from
// hints for a join specified in the query (see tree.JoinTableExpr).  It is a
// bitfield where each bit indicates if a certain type of join is disallowed or
// preferred.
//
// The zero value indicates that any join is allowed and there are no special
// preferences.
type JoinFlags uint16

// Each flag indicates if a certain type of join is disallowed.
const (
	// DisallowHashJoinStoreLeft corresponds to a hash join where the left side
	// is stored into the hashtable.
	DisallowHashJoinStoreLeft JoinFlags = 1 << iota

	// DisallowHashJoinStoreRight corresponds to a hash join where the right
	// side is stored into the hashtable.
	DisallowHashJoinStoreRight

	// DisallowMergeJoin corresponds to a merge join.
	DisallowMergeJoin

	// DisallowLookupJoinIntoLeft corresponds to a lookup join where the lookup
	// table is on the left side.
	DisallowLookupJoinIntoLeft

	// DisallowLookupJoinIntoRight corresponds to a lookup join where the lookup
	// table is on the right side.
	DisallowLookupJoinIntoRight

	// DisallowInvertedJoinIntoLeft corresponds to an inverted join where the
	// inverted index is on the left side.
	DisallowInvertedJoinIntoLeft

	// DisallowInvertedJoinIntoRight corresponds to an inverted join where the
	// inverted index is on the right side.
	DisallowInvertedJoinIntoRight

	// PreferLookupJoinIntoLeft reduces the cost of a lookup join where the lookup
	// table is on the left side.
	PreferLookupJoinIntoLeft

	// PreferLookupJoinIntoRight reduces the cost of a lookup join where the
	// lookup table is on the right side.
	PreferLookupJoinIntoRight
)

const (
	disallowAll = DisallowHashJoinStoreLeft |
		DisallowHashJoinStoreRight |
		DisallowMergeJoin |
		DisallowLookupJoinIntoLeft |
		DisallowLookupJoinIntoRight |
		DisallowInvertedJoinIntoLeft |
		DisallowInvertedJoinIntoRight

	// AllowOnlyHashJoinStoreRight has all "disallow" flags set except
	// DisallowHashJoinStoreRight.
	AllowOnlyHashJoinStoreRight = disallowAll ^ DisallowHashJoinStoreRight

	// AllowOnlyLookupJoinIntoRight has all "disallow" flags set except
	// DisallowLookupJoinIntoRight.
	AllowOnlyLookupJoinIntoRight = disallowAll ^ DisallowLookupJoinIntoRight

	// AllowOnlyInvertedJoinIntoRight has all "disallow" flags set except
	// DisallowInvertedJoinIntoRight.
	AllowOnlyInvertedJoinIntoRight = disallowAll ^ DisallowInvertedJoinIntoRight

	// AllowOnlyMergeJoin has all "disallow" flags set except DisallowMergeJoin.
	AllowOnlyMergeJoin = disallowAll ^ DisallowMergeJoin

	// AllowAllJoinsIntoRight has all "disallow" flags set except
	// DisallowHashJoinStoreRight, DisallowLookupJoinIntoRight,
	// DisallowInvertedJoinIntoRight, and DisallowMergeJoin.
	AllowAllJoinsIntoRight = disallowAll ^ DisallowHashJoinStoreRight ^ DisallowLookupJoinIntoRight ^ DisallowInvertedJoinIntoRight ^ DisallowMergeJoin
)

var joinFlagStr = map[JoinFlags]string{
	DisallowHashJoinStoreLeft:     "hash join (store left side)",
	DisallowHashJoinStoreRight:    "hash join (store right side)",
	DisallowMergeJoin:             "merge join",
	DisallowLookupJoinIntoLeft:    "lookup join (into left side)",
	DisallowLookupJoinIntoRight:   "lookup join (into right side)",
	DisallowInvertedJoinIntoLeft:  "inverted join (into left side)",
	DisallowInvertedJoinIntoRight: "inverted join (into right side)",

	PreferLookupJoinIntoLeft:  "lookup join (into left side)",
	PreferLookupJoinIntoRight: "lookup join (into right side)",
}

// Empty returns true if this is the default value (where all join types are
// allowed).
func (jf JoinFlags) Empty() bool {
	return jf == 0
}

// Has returns true if all of the given flags are set in the receiver.
func (jf JoinFlags) Has(flags JoinFlags) bool {
	return jf&flags == flags
}

func (jf JoinFlags) String() string {
	if jf.Empty() {
		return "no flags"
	}

	prefer := jf & (PreferLookupJoinIntoLeft | PreferLookupJoinIntoRight)
	disallow := jf ^ prefer

	// Special cases with prettier results for common cases.
	var b strings.Builder
	switch disallow {
	case AllowOnlyHashJoinStoreRight:
		b.WriteString("force hash join (store right side)")
	case AllowOnlyLookupJoinIntoRight:
		b.WriteString("force lookup join (into right side)")
	case AllowOnlyInvertedJoinIntoRight:
		b.WriteString("force inverted join (into right side)")
	case AllowOnlyMergeJoin:
		b.WriteString("force merge join")

	default:
		for disallow != 0 {
			flag := JoinFlags(1 << uint8(bits.TrailingZeros8(uint8(disallow))))
			if b.Len() == 0 {
				b.WriteString("disallow ")
			} else {
				b.WriteString(" and ")
			}
			b.WriteString(joinFlagStr[flag])
			disallow ^= flag
		}
	}

	for prefer != 0 {
		flag := JoinFlags(1 << uint8(bits.TrailingZeros8(uint8(prefer))))
		if b.Len() > 0 {
			b.WriteString("; ")
		}
		b.WriteString("prefer ")
		b.WriteString(joinFlagStr[flag])
		prefer ^= flag
	}
	return b.String()
}

func (ij *InnerJoinExpr) initUnexportedFields(mem *Memo) {
	initJoinMultiplicity(mem, ij)
}

func (lj *LeftJoinExpr) initUnexportedFields(mem *Memo) {
	initJoinMultiplicity(mem, lj)
}

func (fj *FullJoinExpr) initUnexportedFields(mem *Memo) {
	initJoinMultiplicity(mem, fj)
}

func (sj *SemiJoinExpr) initUnexportedFields(mem *Memo) {
	initJoinMultiplicity(mem, sj)
}

func (lj *LookupJoinExpr) initUnexportedFields(mem *Memo) {
	// lookupProps are initialized as necessary by the logical props builder.
}

func (gj *InvertedJoinExpr) initUnexportedFields(mem *Memo) {
	// lookupProps are initialized as necessary by the logical props builder.
}

func (zj *ZigzagJoinExpr) initUnexportedFields(mem *Memo) {
	// leftProps and rightProps are initialized as necessary by the logical props
	// builder.
}

// joinWithMultiplicity allows join operators for which JoinMultiplicity is
// supported (currently InnerJoin, LeftJoin, and FullJoin) to be treated
// polymorphically.
type joinWithMultiplicity interface {
	setMultiplicity(props.JoinMultiplicity)
	getMultiplicity() props.JoinMultiplicity
}

var _ joinWithMultiplicity = &InnerJoinExpr{}
var _ joinWithMultiplicity = &LeftJoinExpr{}
var _ joinWithMultiplicity = &FullJoinExpr{}
var _ joinWithMultiplicity = &SemiJoinExpr{}

func (ij *InnerJoinExpr) setMultiplicity(multiplicity props.JoinMultiplicity) {
	ij.multiplicity = multiplicity
}

func (ij *InnerJoinExpr) getMultiplicity() props.JoinMultiplicity {
	return ij.multiplicity
}

func (lj *LeftJoinExpr) setMultiplicity(multiplicity props.JoinMultiplicity) {
	lj.multiplicity = multiplicity
}

func (lj *LeftJoinExpr) getMultiplicity() props.JoinMultiplicity {
	return lj.multiplicity
}

func (fj *FullJoinExpr) setMultiplicity(multiplicity props.JoinMultiplicity) {
	fj.multiplicity = multiplicity
}

func (fj *FullJoinExpr) getMultiplicity() props.JoinMultiplicity {
	return fj.multiplicity
}

func (sj *SemiJoinExpr) setMultiplicity(multiplicity props.JoinMultiplicity) {
	sj.multiplicity = multiplicity
}

func (sj *SemiJoinExpr) getMultiplicity() props.JoinMultiplicity {
	return sj.multiplicity
}

// UDFDefinition stores details about the SQL body of a UDF. It is stored
// separately from the call-site to allow different invocations of the same UDF
// to point to the same definition; this is necessary for recursive UDFs.
type UDFDefinition struct {
	// Name is the name of the function.
	Name string

	// Typ is the return type of the function.
	Typ *types.T

	// Volatility is the user-provided volatility of the function given during
	// CREATE FUNCTION.
	//
	// Volatility affects the visibility of mutations made by the statement
	// calling the function. A volatile function will see these mutations. Also,
	// statements within a volatile function's body will see changes made by
	// previous statements in the function body. In contrast, a stable,
	// immutable, or leakproof function will see a snapshot of the data as of the
	// start of the statement calling the function.
	Volatility volatility.V

	// SetReturning is true if the UDF has a SETOF return type.
	SetReturning bool

	// CalledOnNullInput is true if the function should be called when any of its
	// inputs are NULL. If false, the function will not be evaluated in the
	// presence of NULL inputs, and will instead evaluate directly to NULL.
	//
	// Note that this field only affects evaluation of UDFs within project-set
	// operators. Non-scalar UDFs are always in a project-set operator, while
	// scalar UDFs can be if used as a data source (e.g. SELECT * FROM udf()).
	CalledOnNullInput bool

	// MultiColDataSource is true if the function may return multiple columns.
	// This is only the case if the UDF returns a composite type and is used as a
	// data source.
	MultiColDataSource bool

	// IsRecursive indicates whether the routine recursively calls itself. This
	// applies to direct as well as indirect recursive calls (mutual recursion).
	IsRecursive bool

	// TriggerFunc indicates whether the routine is a trigger function. It is only
	// set for the outermost routine, and not for any sub-routines that implement
	// the PL/pgSQL body.
	TriggerFunc bool

	// BlockStart indicates whether the routine marks the start of a PL/pgSQL
	// block with an exception handler. This is used to determine when to
	// initialize the common state held between sub-routines within the same
	// block.
	BlockStart bool

	// RoutineType indicates whether this routine is a UDF, stored procedure, or
	// builtin function.
	RoutineType tree.RoutineType

	// RoutineLang indicates the language of the routine (SQL or PL/pgSQL).
	RoutineLang tree.RoutineLanguage

	// Params is the list of columns representing parameters of the function. The
	// i-th column in the list corresponds to the i-th parameter of the function.
	// During execution of the UDF, these columns are replaced with the arguments
	// of the function invocation.
	Params opt.ColList

	// Body contains a relational expression for each statement in the function
	// body. It is unset during construction of a recursive UDF.
	Body []RelExpr

	// BodyProps contains the physical properties with which each body statement
	// should be optimized if it is rebuilt. Each props corresponds to the RelExpr
	// at the same position in Body.
	BodyProps []*physical.Required

	// BodyStmts, if set, is the string representation of each statement in
	// Body. It is only populated when verbose tracing is enabled.
	BodyStmts []string

	// ExceptionBlock contains information needed for exception-handling when the
	// body of this routine returns an error. It can be unset.
	ExceptionBlock *ExceptionBlock

	// BlockState is shared between the routines that encapsulate a PLpgSQL block.
	// It is used to coordinate between the nested routines during exception
	// handling.
	BlockState *tree.BlockState

	// CursorDeclaration contains the information needed to open a SQL cursor with
	// the result of the *first* body statement. If it is set, there will be at
	// least two body statements - one to open the cursor, and one to evaluate the
	// result of the routine. This invariant is enforced when the PLpgSQL routine
	// is built. CursorDeclaration may be unset.
	CursorDeclaration *tree.RoutineOpenCursor
}

// ExceptionBlock contains the information needed to match and handle errors in
// the EXCEPTION block of a routine defined with PLpgSQL.
type ExceptionBlock struct {
	// Codes is a list of pgcode strings (see pgcode/codes.go). When the body of a
	// routine with an ExceptionBlock returns an error, the code of that error is
	// compared against the Codes slice for a match. As a special case, the code
	// may be "OTHERS", indicating that (almost) any error code should be matched.
	Codes []pgcode.Code

	// Actions contains routine definitions that represent exception handlers for
	// each code in the Codes slice.
	Actions []*UDFDefinition
}

// WindowFrame denotes the definition of a window frame for an individual
// window function, excluding the OFFSET expressions, if present.
type WindowFrame struct {
	Mode           treewindow.WindowFrameMode
	StartBoundType treewindow.WindowFrameBoundType
	EndBoundType   treewindow.WindowFrameBoundType
	FrameExclusion treewindow.WindowFrameExclusion
}

// HasOffset returns true if the WindowFrame contains a specific offset.
func (f *WindowFrame) HasOffset() bool {
	return f.StartBoundType.IsOffset() || f.EndBoundType.IsOffset()
}

func (f *WindowFrame) String() string {
	var bld strings.Builder
	switch f.Mode {
	case treewindow.GROUPS:
		fmt.Fprintf(&bld, "groups")
	case treewindow.ROWS:
		fmt.Fprintf(&bld, "rows")
	case treewindow.RANGE:
		fmt.Fprintf(&bld, "range")
	}

	frameBoundName := func(b treewindow.WindowFrameBoundType) string {
		switch b {
		case treewindow.UnboundedFollowing, treewindow.UnboundedPreceding:
			return "unbounded"
		case treewindow.CurrentRow:
			return "current-row"
		case treewindow.OffsetFollowing, treewindow.OffsetPreceding:
			return "offset"
		}
		panic(errors.AssertionFailedf("unexpected bound"))
	}
	fmt.Fprintf(&bld, " from %s to %s",
		frameBoundName(f.StartBoundType),
		frameBoundName(f.EndBoundType),
	)
	switch f.FrameExclusion {
	case treewindow.ExcludeCurrentRow:
		bld.WriteString(" exclude current row")
	case treewindow.ExcludeGroup:
		bld.WriteString(" exclude group")
	case treewindow.ExcludeTies:
		bld.WriteString(" exclude ties")
	}
	return bld.String()
}

// IsCanonical returns true if the ScanPrivate indicates an original unaltered
// primary index Scan operator (i.e. unconstrained and not limited).
// s.InvertedConstraint is implicitly nil because a primary index cannot
// be inverted.
func (s *ScanPrivate) IsCanonical() bool {
	return s.Index == cat.PrimaryIndex &&
		s.Constraint == nil &&
		s.HardLimit == 0 &&
		!s.LocalityOptimized
}

// IsUnfiltered returns true if the ScanPrivate will produce all rows in the
// table.
func (s *ScanPrivate) IsUnfiltered(md *opt.Metadata) bool {
	return (s.Constraint == nil || s.Constraint.IsUnconstrained()) &&
		s.InvertedConstraint == nil &&
		s.HardLimit == 0 &&
		s.PartialIndexPredicate(md) == nil &&
		s.Locking.WaitPolicy != tree.LockWaitSkipLocked
}

// IsFullIndexScan returns true if the ScanPrivate will produce all rows in the
// index.
func (s *ScanPrivate) IsFullIndexScan() bool {
	return (s.Constraint == nil || s.Constraint.IsUnconstrained()) &&
		s.InvertedConstraint == nil &&
		s.HardLimit == 0
}

// IsInvertedScan returns true if the index being scanned is an inverted
// index.
func (s *ScanPrivate) IsInvertedScan(md *opt.Metadata) bool {
	return md.Table(s.Table).Index(s.Index).Type() == idxtype.INVERTED
}

// IsVirtualTable returns true if the table being scanned is a virtual table.
func (s *ScanPrivate) IsVirtualTable(md *opt.Metadata) bool {
	tab := md.Table(s.Table)
	return tab.IsVirtualTable()
}

// IsNotVisibleIndexScan returns true if the index being scanned is not fully
// visible.
func (s *ScanPrivate) IsNotVisibleIndexScan(md *opt.Metadata) bool {
	index := md.Table(s.Table).Index(s.Index)
	return index.GetInvisibility() != 0.0
}

// showNotVisibleIndexInfo is a helper function that checks if we want to show
// invisible index info. We don't want to show the information is
// hideShowNotVisibleIndexInfo is true, and we are not scanning an invisible
// index. Otherwise, we want to show invisible index info.
func (s *ScanPrivate) showNotVisibleIndexInfo(md *opt.Metadata, hideNotVisibleIndexInfo bool) bool {
	return !(hideNotVisibleIndexInfo && !s.IsNotVisibleIndexScan(md))
}

// shouldPrintFlags is a helper function to check if we want to print the
// "flags:" for ScanFlags formatting.
func (s *ScanPrivate) shouldPrintFlags(md *opt.Metadata, hideNotVisibleIndexInfo bool) bool {
	if s.Flags.Empty() {
		return false
	}
	// If DisableNotVisibleIndex is the only flag on, we only want to print
	// "flags:" if we are actually going to show invisible index info.
	if s.Flags.onlyNotVisibleIndexFlagOn() && !s.showNotVisibleIndexInfo(md, hideNotVisibleIndexInfo) {
		return false
	}
	return true
}

// IsLocking returns true if the ScanPrivate is configured to use a row-level
// locking mode. This can be the case either because the Scan is in the scope of
// a SELECT .. FOR [KEY] UPDATE/SHARE clause or because the Scan was configured
// as part of the row retrieval of a DELETE or UPDATE statement.
func (s *ScanPrivate) IsLocking() bool {
	return s.Locking.IsLocking()
}

// PartialIndexPredicate returns the FiltersExpr representing the predicate of
// the partial index that the scan uses. If the scan does not use a partial
// index, nil is returned.
func (s *ScanPrivate) PartialIndexPredicate(md *opt.Metadata) FiltersExpr {
	tabMeta := md.TableMeta(s.Table)
	p, ok := tabMeta.PartialIndexPredicate(s.Index)
	if !ok {
		// The index is not a partial index.
		return nil
	}
	return *p.(*FiltersExpr)
}

// SetConstraint sets the constraint in the ScanPrivate and caches the exact
// prefix. This function should always be used instead of modifying the
// constraint directly.
func (s *ScanPrivate) SetConstraint(
	ctx context.Context, evalCtx *eval.Context, c *constraint.Constraint,
) {
	s.Constraint = c
	if c == nil {
		s.ExactPrefix = 0
	} else {
		s.ExactPrefix = c.ExactPrefix(ctx, evalCtx)
	}
}

// GetConstExprFromFilter finds the constant expression which is equated with
// the column with the given `colID` in the inverted join constant filters, if
// one exists. Otherwise, returns nil, ok=false.
func (ij *InvertedJoinPrivate) GetConstExprFromFilter(colID opt.ColumnID) (expr opt.Expr, ok bool) {

	for _, filter := range ij.ConstFilters {
		if filter.Condition.Op() == opt.EqOp {
			leftChild := filter.Condition.Child(0)
			if variableExpr, ok := leftChild.(*VariableExpr); ok {
				if variableExpr.Col == colID {
					return filter.Condition.Child(1), true
				}
			}
		}
	}
	return nil, false
}

// UsesPartialIndex returns true if the LookupJoinPrivate looks-up via a
// partial index.
func (lj *LookupJoinPrivate) UsesPartialIndex(md *opt.Metadata) bool {
	_, isPartialIndex := md.Table(lj.Table).Index(lj.Index).Predicate()
	return isPartialIndex
}

// GetConstPrefixFilter finds the position of the filter in the lookup join
// expression filters that constrains the first index column to one or more
// constant values. If such a filter is found, GetConstPrefixFilter returns the
// position of the filter and ok=true. Otherwise, returns ok=false.
func (lj *LookupJoinPrivate) GetConstPrefixFilter(md *opt.Metadata) (pos int, ok bool) {
	lookupTable := md.Table(lj.Table)
	lookupIndex := lookupTable.Index(lj.Index)

	idxCol := lj.Table.IndexColumnID(lookupIndex, 0)
	for i := range lj.LookupExpr {
		props := lj.LookupExpr[i].ScalarProps()
		if !props.TightConstraints {
			continue
		}
		if props.OuterCols.Len() != 1 {
			continue
		}
		col := props.OuterCols.SingleColumn()
		if col == idxCol {
			return i, true
		}
	}
	return 0, false
}

// LookupIndexPrefixIsEquatedWithColInColSet returns true if there is a term in
// `LookupExpr` equating the first column in the lookup index with a column in
// `colSet`.
func (lj *LookupJoinPrivate) LookupIndexPrefixIsEquatedWithColInColSet(
	md *opt.Metadata, colSet opt.ColSet,
) bool {
	lookupTable := md.Table(lj.Table)
	lookupIndex := lookupTable.Index(lj.Index)

	idxCol := lj.Table.IndexColumnID(lookupIndex, 0)

	for i := range lj.LookupExpr {
		props := lj.LookupExpr[i].ScalarProps()

		equivCols := props.FuncDeps.ComputeEquivGroup(idxCol)
		if colSet.Intersects(equivCols) {
			return true
		}
	}
	return false
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

// MapToInputCols maps the given set of columns to a corresponding set of
// input columns using the PassthroughCols list and MapToInputID function.
func (m *MutationPrivate) MapToInputCols(cols opt.ColSet) opt.ColSet {
	var inCols opt.ColSet

	// First see if any of the columns come from the passthrough columns.
	for _, c := range m.PassthroughCols {
		if cols.Contains(c) {
			inCols.Add(c)
		}
	}

	// The remaining columns must come from the table.
	tabCols := cols.Difference(inCols)
	tabCols.ForEach(func(t opt.ColumnID) {
		id := m.MapToInputID(t)
		if id == 0 {
			panic(errors.AssertionFailedf("could not find input column for %d", redact.Safe(t)))
		}
		inCols.Add(id)
	})

	return inCols
}

// AddEquivTableCols adds an FD to the given set that declares an equivalence
// between each table column and its corresponding input column.
func (m *MutationPrivate) AddEquivTableCols(md *opt.Metadata, fdset *props.FuncDepSet) {
	for i, n := 0, md.Table(m.Table).ColumnCount(); i < n; i++ {
		t := m.Table.ColumnID(i)
		id := m.MapToInputID(t)
		if id != 0 {
			fdset.AddEquivalency(t, id)
		}
	}
}

// WithBindingID is used by factory.Replace as a uniform way to get the with ID.
func (m *MutationPrivate) WithBindingID() opt.WithID {
	return m.WithID
}

// WithBindingID is used by factory.Replace as a uniform way to get the with ID.
func (w *WithExpr) WithBindingID() opt.WithID {
	return w.ID
}

// WithBindingID is used by factory.Replace as a uniform way to get the with ID.
func (r *RecursiveCTEExpr) WithBindingID() opt.WithID {
	return r.WithID
}

// initUnexportedFields is called when a project expression is created.
func (prj *ProjectExpr) initUnexportedFields(mem *Memo) {
	inputProps := prj.Input.Relational()
	// Determine the not-null columns.
	prj.notNullCols = inputProps.NotNullCols.Copy()
	for i := range prj.Projections {
		item := &prj.Projections[i]
		if ExprIsNeverNull(item.Element, inputProps.NotNullCols) {
			prj.notNullCols.Add(item.Col)
		}
	}

	// Determine the "internal" functional dependencies (for the union of input
	// columns and synthesized columns).
	prj.internalFuncDeps.CopyFrom(&inputProps.FuncDeps)
	for i := range prj.Projections {
		item := &prj.Projections[i]
		if v, ok := item.Element.(*VariableExpr); ok && inputProps.OutputCols.Contains(v.Col) {
			// Handle any column that is a direct reference to an input column. The
			// optimizer sometimes constructs these in order to generate different
			// column IDs; they can also show up after constant-folding e.g. an ORDER
			// BY expression.
			prj.internalFuncDeps.AddEquivalency(v.Col, item.Col)
			continue
		}

		if !item.scalar.VolatilitySet.HasVolatile() {
			from := item.scalar.OuterCols.Intersection(inputProps.OutputCols)

			// We want to set up the FD: from --> colID.
			// This does not necessarily hold for "composite" types like decimals or
			// collated strings. For example if d is a decimal, d::TEXT can have
			// different values for equal values of d, like 1 and 1.0.
			if !CanBeCompositeSensitive(item.Element) {
				prj.internalFuncDeps.AddSynthesizedCol(from, item.Col)
			}
		}
	}
	prj.internalFuncDeps.MakeNotNull(prj.notNullCols)
}

// InternalFDs returns the functional dependencies for the set of all input
// columns plus the synthesized columns.
func (prj *ProjectExpr) InternalFDs() *props.FuncDepSet {
	return &prj.internalFuncDeps
}

// GetProjectedEnumConstant looks for the projection with target colID in prj,
// and if it contains a constant enum, returns its string representation, or the
// empty string if not found.
func (prj *ProjectExpr) GetProjectedEnumConstant(colID opt.ColumnID) string {
	for _, projection := range prj.Projections {
		if projection.Col == colID {
			if projection.Element.Op() == opt.ConstOp {
				constExpr := projection.Element.(*ConstExpr)
				if enumValue, ok := constExpr.Value.(*tree.DEnum); ok {
					return enumValue.LogicalRep
				}
			} else {
				return ""
			}
		}
	}
	return ""
}

// FindInlinableConstants returns the set of input columns that are synthesized
// constant value expressions: ConstOp, TrueOp, FalseOp, or NullOp. Constant
// value expressions can often be inlined into referencing expressions. Only
// Project and Values operators synthesize constant value expressions.
func FindInlinableConstants(input RelExpr) opt.ColSet {
	var cols opt.ColSet
	if project, ok := input.(*ProjectExpr); ok {
		for i := range project.Projections {
			item := &project.Projections[i]
			if opt.IsConstValueOp(item.Element) {
				cols.Add(item.Col)
			}
		}
	} else if values, ok := input.(*ValuesExpr); ok && len(values.Rows) == 1 {
		tup := values.Rows[0].(*TupleExpr)
		for i, scalar := range tup.Elems {
			if opt.IsConstValueOp(scalar) {
				cols.Add(values.Cols[i])
			}
		}
	}
	return cols
}

// ExtractColumnFromProjectOrValues searches a Project or Values input
// expression for the column having the given id. It returns the expression for
// that column.
func ExtractColumnFromProjectOrValues(input RelExpr, col opt.ColumnID) opt.ScalarExpr {
	if project, ok := input.(*ProjectExpr); ok {
		for i := range project.Projections {
			item := &project.Projections[i]
			if item.Col == col {
				return item.Element
			}
		}
	} else if values, ok := input.(*ValuesExpr); ok && len(values.Rows) == 1 {
		tup := values.Rows[0].(*TupleExpr)
		for i, scalar := range tup.Elems {
			if values.Cols[i] == col {
				return scalar
			}
		}
	}
	panic(errors.AssertionFailedf("could not find column to extract"))
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

	case *TrueExpr, *FalseExpr, *ConstExpr, *IsExpr, *IsNotExpr, *IsTupleNullExpr, *IsTupleNotNullExpr:
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

	case *CastExpr, *AssignmentCastExpr, *NotExpr, *RangeExpr:
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols)

	case *AndExpr, *OrExpr, *GeExpr, *GtExpr, *NeExpr, *EqExpr, *LeExpr, *LtExpr, *LikeExpr,
		*NotLikeExpr, *ILikeExpr, *NotILikeExpr, *SimilarToExpr, *NotSimilarToExpr, *RegMatchExpr,
		*NotRegMatchExpr, *RegIMatchExpr, *NotRegIMatchExpr, *ContainsExpr, *ContainedByExpr, *JsonExistsExpr,
		*JsonAllExistsExpr, *JsonSomeExistsExpr, *AnyScalarExpr, *BitandExpr, *BitorExpr, *BitxorExpr,
		*PlusExpr, *MinusExpr, *MultExpr, *DivExpr, *FloorDivExpr, *ModExpr, *PowExpr, *ConcatExpr,
		*LShiftExpr, *RShiftExpr, *WhenExpr:
		return ExprIsNeverNull(t.Child(0).(opt.ScalarExpr), notNullCols) &&
			ExprIsNeverNull(t.Child(1).(opt.ScalarExpr), notNullCols)

	default:
		return false
	}
}

// OutputColumnIsAlwaysNull returns true if the expression produces only NULL
// values for the given column. Used to elide foreign key checks.
//
// This could be a logical property but we only care about simple cases (NULLs
// in Projections and Values).
func OutputColumnIsAlwaysNull(e RelExpr, col opt.ColumnID) bool {
	isNullScalar := func(scalar opt.ScalarExpr) bool {
		switch scalar.Op() {
		case opt.NullOp:
			return true
		case opt.CastOp:
			// Normally this cast should have been folded, but we want this to work
			// in "build" opttester mode (disabled normalization rules).
			return scalar.Child(0).Op() == opt.NullOp
		default:
			return false
		}
	}

	switch e.Op() {
	case opt.ProjectOp:
		p := e.(*ProjectExpr)
		if p.Passthrough.Contains(col) {
			return OutputColumnIsAlwaysNull(p.Input, col)
		}
		for i := range p.Projections {
			if p.Projections[i].Col == col {
				return isNullScalar(p.Projections[i].Element)
			}
		}

	case opt.ValuesOp:
		v := e.(*ValuesExpr)
		colOrdinal, ok := v.Cols.Find(col)
		if !ok {
			return false
		}
		for i := range v.Rows {
			if !isNullScalar(v.Rows[i].(*TupleExpr).Elems[colOrdinal]) {
				return false
			}
		}
		return true
	}

	return false
}

// CollectContiguousOrExprs finds all OrExprs in 'e' that are connected via
// a parent-child relationship, and returns them in an array of ScalarExprs.
func CollectContiguousOrExprs(e opt.ScalarExpr) []opt.ScalarExpr {
	var disjunctions = make([]opt.ScalarExpr, 0, 2)
	var collectDisjunctions func(e opt.ScalarExpr)
	collectDisjunctions = func(e opt.ScalarExpr) {
		if or, ok := e.(*OrExpr); ok {
			collectDisjunctions(or.Left)
			collectDisjunctions(or.Right)
		} else {
			disjunctions = append(disjunctions, e)
		}
	}
	collectDisjunctions(e)
	return disjunctions
}

// FKCascades stores metadata necessary for building cascading queries.
type FKCascades []FKCascade

// FKCascade stores metadata necessary for building a cascading query.
// Cascading queries are built as needed, after the original query is executed.
type FKCascade struct {
	FKConstraint cat.ForeignKeyConstraint

	// HasBeforeTriggers is true if the mutation that is planned for the cascade
	// will have BEFORE triggers.
	HasBeforeTriggers bool

	// Builder is an object that can be used as the "optbuilder" for the cascading
	// query.
	Builder PostQueryBuilder

	// WithID identifies the buffer for the mutation input in the original
	// expression tree. 0 if the cascade does not require input.
	WithID opt.WithID
}

// AfterTriggers stores metadata necessary for building a set of AFTER triggers.
// AFTER triggers are built as needed, after the original query is executed.
type AfterTriggers struct {
	Triggers []cat.Trigger

	// Builder is an object that can be used as the "optbuilder" for the cascading
	// query.
	Builder PostQueryBuilder

	// WithID identifies the buffer for the mutation input in the original
	// expression tree. It is always nonzero.
	WithID opt.WithID
}

// PostQueryBuilder is an interface used to construct either a cascading query
// for a specific FK relation, or an AFTER trigger action. For example: if we
// are deleting rows from a parent table, after deleting the rows from the
// parent table this interface will be used to build the corresponding deletion
// in the child table.
type PostQueryBuilder interface {
	// Build constructs the plan for the cascading query or AFTER trigger action.
	// The input is scanned using WithScan with the given WithID; colMap is
	// provided to map from columns in the original memo to those in the new memo
	// that is used for the With binding.
	//
	// The query does not need to be built in the same memo as the original query;
	// the only requirement is that the mutation input columns
	// (oldValues/newValues, canaryCol) are valid in the metadata and have entries
	// in the ColMap.
	//
	// The method does not mutate any captured state; it is ok to call Build
	// concurrently (e.g. if the plan it originates from is cached and reused).
	//
	// Some cascades (delete fast path) don't require an input binding. In that
	// case binding is 0, bindingProps is nil, and colMap is empty. Triggers
	// always require an input binding.
	//
	// Note: factory is always *norm.Factory; it is an interface{} only to avoid
	// circular package dependencies.
	Build(
		ctx context.Context,
		semaCtx *tree.SemaContext,
		evalCtx *eval.Context,
		catalog cat.Catalog,
		factory interface{},
		binding opt.WithID,
		bindingProps *props.Relational,
		colMap opt.ColMap,
	) (RelExpr, error)
}

// GroupingOrderType is the grouping column order type for group by and distinct
// operations in the memo.
type GroupingOrderType int

const (
	// NoStreaming means that the grouping columns have no useful order, so a
	// hash aggregator should be used.
	NoStreaming GroupingOrderType = iota
	// PartialStreaming means that the grouping columns are partially ordered,
	// so some optimizations can be done during aggregation.
	PartialStreaming
	// Streaming means that the grouping columns are fully ordered.
	Streaming
)

// GroupingOrderType calculates how many ordered columns that the grouping and
// input columns have in common and returns NoStreaming if there are none,
// Streaming if all columns match, and PartialStreaming if only some match. It
// is similar to StreamingGroupingColOrdering, but does not build an ordering.
func (g *GroupingPrivate) GroupingOrderType(required *props.OrderingChoice) GroupingOrderType {
	inputOrdering := required.Intersection(&g.Ordering)
	var orderedGroupingCols opt.ColSet
	for i := range inputOrdering.Columns {
		// Get any grouping column from the set. Normally there would be at most one
		// because we have rules that remove redundant grouping columns.
		cols := inputOrdering.Group(i).Intersection(g.GroupingCols)
		c, ok := cols.Next(0)
		if !ok {
			// This group refers to a column that is not a grouping column.
			// The rest of the ordering is not useful.
			break
		}
		orderedGroupingCols.Add(c)
	}
	if orderedGroupingCols.Len() == g.GroupingCols.Len() || g.GroupingCols.Len() == 0 {
		return Streaming
	} else if orderedGroupingCols.Len() == 0 {
		return NoStreaming
	}
	return PartialStreaming
}

// IsConstantsAndPlaceholders returns true if all values in the list are
// constant, placeholders or tuples containing constants, placeholders or other
// such nested tuples.
func (v *ValuesExpr) IsConstantsAndPlaceholders() bool {
	// Constant expressions should be leak-proof, so this is a quick test to
	// determine if the ValuesExpr even needs to be checked.
	if !v.Relational().VolatilitySet.IsLeakproof() {
		return false
	}
	// Another quick check for something other than constants
	if !v.Relational().OuterCols.Empty() {
		return false
	}
	// A safety check
	if len(v.Rows) == 0 {
		return false
	}
	if !v.Rows.IsConstantsAndPlaceholders() {
		return false
	}
	return true
}

// IsConstantsAndPlaceholders returns true if all scalar expressions in the list
// are constants, placeholders or tuples containing constants or placeholders.
// If a tuple nested within a tuple is found, false is returned.
func (e ScalarListExpr) IsConstantsAndPlaceholders() bool {
	return e.isConstantsAndPlaceholders(false /* insideTuple */)
}

func (e ScalarListExpr) isConstantsAndPlaceholders(insideTuple bool) bool {
	for _, scalarExpr := range e {
		if tupleExpr, ok := scalarExpr.(*TupleExpr); ok {
			if insideTuple {
				return false
			}
			if !tupleExpr.Elems.isConstantsAndPlaceholders(true) {
				return false
			}
		} else {
			if scalarExpr == nil {
				panic(errors.AssertionFailedf("nil scalar expression in list"))
			}
			if !opt.IsConstValueOp(scalarExpr) && scalarExpr.Op() != opt.PlaceholderOp {
				return false
			}
		}
	}
	return true
}

// IsConstantsAndPlaceholdersAndVariables returns true if all scalar expressions
// in the list are constants, placeholders, variables, or tuples containing
// constants, placeholders, or variables. If a tuple nested within a tuple is
// found, false is returned.
func (e ScalarListExpr) IsConstantsAndPlaceholdersAndVariables() bool {
	return e.isConstantsAndPlaceholdersAndVariables(false /* insideTuple */)
}

func (e ScalarListExpr) isConstantsAndPlaceholdersAndVariables(insideTuple bool) bool {
	for _, scalarExpr := range e {
		if tupleExpr, ok := scalarExpr.(*TupleExpr); ok {
			if insideTuple {
				return false
			}
			if !tupleExpr.Elems.isConstantsAndPlaceholdersAndVariables(true) {
				return false
			}
		} else {
			if scalarExpr == nil {
				panic(errors.AssertionFailedf("nil scalar expression in list"))
			}
			if !opt.IsConstValueOp(scalarExpr) && scalarExpr.Op() != opt.PlaceholderOp &&
				scalarExpr.Op() != opt.VariableOp {
				return false
			}
		}
	}
	return true
}
