// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/errors"
)

// OrderingChoice defines the set of possible row orderings that are provided or
// required by an operator. An OrderingChoice consists of two parts: an ordered
// sequence of equivalent column groups and a set of optional columns. Together,
// these parts specify a simple pattern that can match one or more candidate
// orderings. Here are some examples:
//
//   +1                  ORDER BY a
//   +1,-2               ORDER BY a,b DESC
//   +(1|2)              ORDER BY a        | ORDER BY b
//   +(1|2),+3           ORDER BY a,c      | ORDER BY b, c
//   -(3|4),+5 opt(1,2)  ORDER BY c DESC,e | ORDER BY a,d DESC,b DESC,e | ...
//
// Each column in the ordering sequence forms the corresponding column of the
// sort key, from most significant to least significant. Each column has a sort
// direction, either ascending or descending. The relation is ordered by the
// first column; rows that have the same value are then ordered by the second
// column; rows that still have the same value are ordered by the third column,
// and so on.
//
// Sometimes multiple columns in the relation have equivalent values. The
// OrderingChoiceColumn stores these columns in a group; any of the columns in
// the group can be used to form the corresponding column in the sort key. The
// equivalent group columns come from SQL expressions like:
//
//   a=b
//
// The optional column set contains columns that can appear anywhere (or
// nowhere) in the ordering. Optional columns come from SQL expressions like:
//
//   a=1
//
// Another case for optional columns is when we are grouping along a set of
// columns and only care about the intra-group ordering.
//
// The optional columns can be interleaved anywhere in the sequence of ordering
// columns, as they have no effect on the ordering.
type OrderingChoice struct {
	// Optional is the set of columns that can appear at any position in the
	// ordering. Columns in Optional must not appear in the Columns sequence.
	// In addition, if Columns is empty, then Optional must be as well.
	// After initial construction, Optional is immutable. To update, replace
	// with a different set containing the desired columns.
	Optional opt.ColSet

	// Columns is the sequence of equivalent column groups that can be used to
	// form each column in the sort key. Columns must not appear in the Optional
	// set. The array memory is owned by this struct, and should not be copied
	// to another OrderingChoice unless both are kept immutable.
	Columns []OrderingColumnChoice
}

// OrderingColumnChoice specifies the set of columns which can form one of the
// columns in the sort key, as well as the direction of that column (ascending
// or descending).
type OrderingColumnChoice struct {
	// Group is a set of equivalent columns, any of which can be used to form a
	// column in the sort key. After initial construction, Group is immutable.
	// To update, replace with a different set containing the desired columns.
	Group opt.ColSet

	// Descending is true if the sort key column is ordered from highest to
	// lowest. Otherwise, it's ordered from lowest to highest.
	Descending bool
}

const (
	colChoiceRegexStr = `(?:\((\d+(?:\|\d+)*)\))`
	ordColRegexStr    = `^(?:(?:\+|\-)(?:(\d+)|` + colChoiceRegexStr + `))$`
	colListRegexStr   = `(\d+(?:,\d+)*)`
	optRegexStr       = `^\s*([\S]+)?\s*(?:opt\(` + colListRegexStr + `\))?\s*$`
)

var once sync.Once
var optRegex, ordColRegex *regexp.Regexp

// ParseOrderingChoice parses the string representation of an OrderingChoice for
// testing purposes. Here are some examples of the string format:
//
//   +1
//   -(1|2),+3
//   +(1|2),+3 opt(5,6)
//
// The input string is expected to be valid; ParseOrderingChoice will panic if
// it is not.
func ParseOrderingChoice(s string) OrderingChoice {
	once.Do(func() {
		optRegex = regexp.MustCompile(optRegexStr)
		ordColRegex = regexp.MustCompile(ordColRegexStr)
	})

	var ordering OrderingChoice

	// Separate string into column sequence and optional column parts:
	//   +(1|2),+3 opt(5,6)
	//     matches[1]: +(1|2),+3
	//     matches[2]: opt(5,6)
	matches := optRegex.FindStringSubmatch(s)
	if matches == nil {
		panic(errors.AssertionFailedf("could not parse ordering choice: %s", s))
	}

	// Handle Any case.
	if len(matches[1]) == 0 {
		return OrderingChoice{}
	}

	// Split column sequence by comma:
	//   +(1|2),+3:
	//     +(1|2)
	//     +3
	for _, ordColStr := range strings.Split(matches[1], ",") {
		// Parse one item in the column sequence:
		//   +(1|2):
		//     matches[1]: <empty>
		//     matches[2]: 1|2
		//
		//   +3:
		//     matches[1]: 3
		//     matches[2]: <empty>
		ordColMatches := ordColRegex.FindStringSubmatch(ordColStr)

		// First character is the direction indicator.
		var colChoice OrderingColumnChoice
		colChoice.Descending = strings.HasPrefix(ordColStr, "-")

		if len(ordColMatches[1]) != 0 {
			// Single column in equivalence group.
			id, _ := strconv.Atoi(ordColMatches[1])
			colChoice.Group.Add(opt.ColumnID(id))
		} else {
			// Split multiple columns in equivalence group by pipe:
			//   1|2:
			//     1
			//     2
			for _, idStr := range strings.Split(ordColMatches[2], "|") {
				id, _ := strconv.Atoi(idStr)
				colChoice.Group.Add(opt.ColumnID(id))
			}
		}

		ordering.Columns = append(ordering.Columns, colChoice)
	}

	// Parse any optional columns by splitting by comma:
	//   opt(5,6):
	//     5
	//     6
	if len(matches[2]) != 0 {
		for _, idStr := range strings.Split(matches[2], ",") {
			id, _ := strconv.Atoi(idStr)
			ordering.Optional.Add(opt.ColumnID(id))
		}
	}

	return ordering
}

// ParseOrdering parses a simple opt.Ordering; for example: "+1,-3".
//
// The input string is expected to be valid; ParseOrdering will panic if it is
// not.
func ParseOrdering(str string) opt.Ordering {
	prov := ParseOrderingChoice(str)
	if !prov.Optional.Empty() {
		panic(errors.AssertionFailedf("invalid ordering %s", str))
	}
	for i := range prov.Columns {
		if prov.Columns[i].Group.Len() != 1 {
			panic(errors.AssertionFailedf("invalid ordering %s", str))
		}
	}
	return prov.ToOrdering()
}

// Any is true if this instance allows any ordering (any length, any columns).
func (oc *OrderingChoice) Any() bool {
	return len(oc.Columns) == 0
}

// FromOrdering sets this OrderingChoice to the given opt.Ordering.
func (oc *OrderingChoice) FromOrdering(ord opt.Ordering) {
	oc.Optional = opt.ColSet{}
	oc.Columns = make([]OrderingColumnChoice, len(ord))
	for i := range ord {
		oc.Columns[i].Group.Add(ord[i].ID())
		oc.Columns[i].Descending = ord[i].Descending()
	}
}

// FromOrderingWithOptCols sets this OrderingChoice to the given opt.Ordering
// and with the given optional columns. Any optional columns in the given
// ordering are ignored.
func (oc *OrderingChoice) FromOrderingWithOptCols(ord opt.Ordering, optCols opt.ColSet) {
	oc.Optional = optCols.Copy()
	oc.Columns = make([]OrderingColumnChoice, 0, len(ord))
	for i := range ord {
		if !oc.Optional.Contains(ord[i].ID()) {
			oc.Columns = append(oc.Columns, OrderingColumnChoice{
				Group:      opt.MakeColSet(ord[i].ID()),
				Descending: ord[i].Descending(),
			})
		}
	}
}

// ToOrdering returns an opt.Ordering instance composed of the shortest possible
// orderings that this instance allows. If there are several, then one is chosen
// arbitrarily.
func (oc *OrderingChoice) ToOrdering() opt.Ordering {
	ordering := make(opt.Ordering, len(oc.Columns))
	for i := range oc.Columns {
		col := &oc.Columns[i]
		ordering[i] = opt.MakeOrderingColumn(col.AnyID(), col.Descending)
	}
	return ordering
}

// ColSet returns the set of all non-optional columns that are part of this
// instance. For example, (1,2,3) will be returned if the OrderingChoice is:
//
//   +1,(2|3) opt(4,5)
//
func (oc *OrderingChoice) ColSet() opt.ColSet {
	var cs opt.ColSet
	for i := range oc.Columns {
		cs.UnionWith(oc.Columns[i].Group)
	}
	return cs
}

// Implies returns true if any ordering allowed by <oc> is also allowed by <other>.
//
// In the case of no optional or equivalent columns, Implies returns true when
// the given ordering is a prefix of this ordering.
//
// Examples:
//
//   <empty>           implies <empty>
//   +1                implies <empty>        (given set is prefix)
//   +1                implies +1
//   +1,-2             implies +1             (given set is prefix)
//   +1,-2             implies +1,-2
//   +1                implies +1 opt(2)      (unused optional col is ignored)
//   -2,+1             implies +1 opt(2)      (optional col is ignored)
//   +1                implies +(1|2)         (subset of choice)
//   +(1|2)            implies +(1|2|3)       (subset of choice)
//   +(1|2),-4         implies +(1|2|3),-(4|5)
//   +(1|2) opt(4)     implies +(1|2|3) opt(4)
//   +1,+2,+3          implies +(1|2),+3      (unused group columns become optional)
//
//   <empty>           !implies +1
//   +1                !implies -1            (direction mismatch)
//   +1                !implies +1,-2         (prefix matching not commutative)
//   +1 opt(2)         !implies +1            (extra optional cols not allowed)
//   +1 opt(2)         !implies +1 opt(3)
//   +(1|2)            !implies -(1|2)        (direction mismatch)
//   +(1|2)            !implies +(3|4)        (no intersection)
//   +(1|2)            !implies +(2|3)        (intersects, but not subset)
//   +(1|2|3)          !implies +(1|2)        (subset of choice not commutative)
//   +(1|2)            !implies +1 opt(2)
//
func (oc *OrderingChoice) Implies(other *OrderingChoice) bool {
	if !oc.Optional.SubsetOf(other.Optional) {
		return false
	}

	// Copy the optional columns so we can add to the set. All group columns
	// become optional after one column in the group is used.
	optional := colSetHelper{colSet: other.Optional}

	for left, right := 0, 0; right < len(other.Columns); {
		if left >= len(oc.Columns) {
			return false
		}

		leftCol, rightCol := &oc.Columns[left], &other.Columns[right]

		switch {
		case leftCol.Descending == rightCol.Descending && leftCol.Group.SubsetOf(rightCol.Group):
			// The columns match.
			optional.unionWith(rightCol.Group)
			left, right = left+1, right+1

		case optional.intersects(leftCol.Group):
			// Left column is optional in the right set.
			left++

		default:
			return false
		}
	}
	return true
}

// Intersects returns true if there are orderings that satisfy both
// OrderingChoices. See Intersection for more information.
func (oc *OrderingChoice) Intersects(other *OrderingChoice) bool {
	// Copy the optional columns so we can add to the sets. All group columns
	// become optional after one column in the group is used.
	leftOptional := colSetHelper{colSet: oc.Optional}
	rightOptional := colSetHelper{colSet: other.Optional}

	for left, right := 0, 0; left < len(oc.Columns) && right < len(other.Columns); {
		leftCol, rightCol := &oc.Columns[left], &other.Columns[right]
		switch {
		case leftCol.Descending == rightCol.Descending && leftCol.Group.Intersects(rightCol.Group):
			// The columns match.
			leftOptional.unionWith(leftCol.Group)
			rightOptional.unionWith(rightCol.Group)
			left, right = left+1, right+1

		case rightOptional.intersects(leftCol.Group):
			// Left column is optional in the right set.
			leftOptional.unionWith(leftCol.Group)
			left++

		case leftOptional.intersects(rightCol.Group):
			// Right column is optional in the left set.
			rightOptional.unionWith(rightCol.Group)
			right++

		default:
			return false
		}
	}
	return true
}

// Intersection returns an OrderingChoice that Implies both ordering choices.
// Can only be called if Intersects is true. Some examples:
//
//  +1           ∩ <empty> = +1
//  +1           ∩ +1,+2   = +1,+2
//  +1,+2 opt(3) ∩ +1,+3   = +1,+3,+2
//
// In general, OrderingChoice is not expressive enough to represent the
// intersection. In such cases, an OrderingChoice representing a subset of the
// intersection is returned. For example,
//  +1 opt(2) ∩ +2 opt(1)
// can be either +1,+2 or +2,+1; only one of these is returned. Note that
// the function may not be commutative in this case. In practice, such cases are
// unlikely.
//
// It is guaranteed that if one OrderingChoice Implies the other, it will also
// be the Intersection.
func (oc *OrderingChoice) Intersection(other *OrderingChoice) OrderingChoice {
	// We have to handle Any cases separately because an Any ordering choice has
	// no optional columns (even though semantically it should have all possible
	// columns as optional).
	if oc.Any() {
		return other.Copy()
	}
	if other.Any() {
		return oc.Copy()
	}

	result := make([]OrderingColumnChoice, 0, len(oc.Columns)+len(other.Columns))

	// Copy the optional columns so we can add to the sets. All group columns
	// become optional after one column in the group is used.
	leftOptional := colSetHelper{colSet: oc.Optional}
	rightOptional := colSetHelper{colSet: other.Optional}

	left, right := 0, 0
	for left < len(oc.Columns) && right < len(other.Columns) {
		leftCol, rightCol := &oc.Columns[left], &other.Columns[right]

		switch {
		case leftCol.Descending == rightCol.Descending && leftCol.Group.Intersects(rightCol.Group):
			// The columns match.
			result = append(result, OrderingColumnChoice{
				Group:      leftCol.Group.Intersection(rightCol.Group),
				Descending: leftCol.Descending,
			})
			leftOptional.unionWith(leftCol.Group)
			rightOptional.unionWith(rightCol.Group)
			left, right = left+1, right+1

		case rightOptional.intersects(leftCol.Group):
			// Left column is optional in the right set.
			result = append(result, OrderingColumnChoice{
				Group:      rightOptional.intersection(leftCol.Group),
				Descending: leftCol.Descending,
			})
			leftOptional.unionWith(leftCol.Group)
			left++

		case leftOptional.intersects(rightCol.Group):
			// Right column is optional in the left set.
			result = append(result, OrderingColumnChoice{
				Group:      leftOptional.intersection(rightCol.Group),
				Descending: rightCol.Descending,
			})
			rightOptional.unionWith(rightCol.Group)
			right++

		default:
			panic(errors.AssertionFailedf("non-intersecting sets"))
		}
	}
	// An ordering matched a prefix of the other. Append the tail of the other
	// ordering.
	for ; left < len(oc.Columns); left++ {
		result = append(result, oc.Columns[left])
	}
	for ; right < len(other.Columns); right++ {
		result = append(result, other.Columns[right])
	}
	return OrderingChoice{
		Optional: oc.Optional.Intersection(other.Optional),
		Columns:  result,
	}
}

// CommonPrefix is similar to Intersection, but it does not panic if the orderings
// are non-intersecting. Instead, it returns the longest prefix of intersecting
// columns. Some examples:
//
//  +1           common prefix <empty> = <empty>
//  +1           common prefix +1,+2   = +1
//  +1,+2 opt(3) common prefix +1,+3   = +1,+3
//
// Note that CommonPrefix is asymmetric: optional columns of oc will be used to
// match trailing columns of other, but the reverse is not true. For example:
//
//  +1 opt(2) common prefix +1,+2     = +1,+2
//  +1,+2     common prefix +1 opt(2) = +1
//
func (oc *OrderingChoice) CommonPrefix(other *OrderingChoice) OrderingChoice {
	if oc.Any() || other.Any() {
		return OrderingChoice{}
	}

	result := make([]OrderingColumnChoice, 0, len(oc.Columns))

	leftOptional := colSetHelper{colSet: oc.Optional}
	rightOptional := colSetHelper{colSet: other.Optional}

	left, right := 0, 0
	for left < len(oc.Columns) && right < len(other.Columns) {
		leftCol, rightCol := &oc.Columns[left], &other.Columns[right]

		switch {
		case leftCol.Descending == rightCol.Descending && leftCol.Group.Intersects(rightCol.Group):
			// The columns match.
			result = append(result, OrderingColumnChoice{
				Group:      leftCol.Group.Intersection(rightCol.Group),
				Descending: leftCol.Descending,
			})
			leftOptional.unionWith(leftCol.Group)
			rightOptional.unionWith(rightCol.Group)
			left, right = left+1, right+1

		case rightOptional.intersects(leftCol.Group):
			// Left column is optional in the right set.
			result = append(result, OrderingColumnChoice{
				Group:      rightOptional.intersection(leftCol.Group),
				Descending: leftCol.Descending,
			})
			leftOptional.unionWith(leftCol.Group)
			left++

		case leftOptional.intersects(rightCol.Group):
			// Right column is optional in the left set.
			result = append(result, OrderingColumnChoice{
				Group:      leftOptional.intersection(rightCol.Group),
				Descending: rightCol.Descending,
			})
			rightOptional.unionWith(rightCol.Group)
			right++

		default:
			return OrderingChoice{
				Optional: oc.Optional.Intersection(other.Optional),
				Columns:  result,
			}
		}
	}
	// oc matched a prefix of other. Append the tail of the other ordering that is
	// included in the optional columns of oc.
	for ; right < len(other.Columns); right++ {
		rightCol := &other.Columns[right]
		if !leftOptional.intersects(rightCol.Group) {
			break
		}
		result = append(result, OrderingColumnChoice{
			Group:      leftOptional.intersection(rightCol.Group),
			Descending: rightCol.Descending,
		})
	}
	return OrderingChoice{
		Optional: oc.Optional.Intersection(other.Optional),
		Columns:  result,
	}
}

// commonPrefixLength returns the length of the OrderingChoice that would be
// returned by CommonPrefix, along with a boolean indicating whether the common
// prefix between the receiver and 'other' implies 'other'.
func (oc *OrderingChoice) commonPrefixLength(other *OrderingChoice) (length int, implies bool) {
	if oc.Any() || other.Any() {
		return 0 /* length */, other.Any()
	}

	leftOptional := colSetHelper{colSet: oc.Optional}
	rightOptional := colSetHelper{colSet: other.Optional}

	left, right := 0, 0
	for left < len(oc.Columns) && right < len(other.Columns) {
		leftCol, rightCol := &oc.Columns[left], &other.Columns[right]

		switch {
		case leftCol.Descending == rightCol.Descending && leftCol.Group.Intersects(rightCol.Group):
			// The columns match.
			length++
			leftOptional.unionWith(leftCol.Group)
			rightOptional.unionWith(rightCol.Group)
			left, right = left+1, right+1

		case rightOptional.intersects(leftCol.Group):
			// Left column is optional in the right set.
			length++
			leftOptional.unionWith(leftCol.Group)
			left++

		case leftOptional.intersects(rightCol.Group):
			// Right column is optional in the left set.
			length++
			rightOptional.unionWith(rightCol.Group)
			right++

		default:
			// If we have reached this point, the common prefix will not include all
			// of the non-optional columns from the 'other' OrderingChoice, and
			// therefore will not imply 'other'.
			return length, false
		}
	}
	// oc matched a prefix of other. Append the tail of the other ordering that is
	// included in the optional columns of oc.
	for ; right < len(other.Columns); right++ {
		rightCol := &other.Columns[right]
		if !leftOptional.intersects(rightCol.Group) {
			break
		}
		length++
	}
	// The common prefix will imply 'other' if and only if it includes all of the
	// non-optional columns from 'other'.
	return length, right == len(other.Columns)
}

// SubsetOfCols is true if the OrderingChoice only references columns in the
// given set.
func (oc *OrderingChoice) SubsetOfCols(cs opt.ColSet) bool {
	if !oc.Optional.SubsetOf(cs) {
		return false
	}
	for i := range oc.Columns {
		if !oc.Columns[i].Group.SubsetOf(cs) {
			return false
		}
	}
	return true
}

// CanProjectCols is true if at least one column in each ordering column group is
// part of the given column set. For example, if the OrderingChoice is:
//
//   +1,-(2|3) opt(4,5)
//
// then CanProjectCols will behave as follows for these input sets:
//
//   (1,2)   => true
//   (1,3)   => true
//   (1,2,4) => true
//   (1)     => false
//   (3,4)   => false
//
func (oc *OrderingChoice) CanProjectCols(cs opt.ColSet) bool {
	for i := range oc.Columns {
		if !oc.Columns[i].Group.Intersects(cs) {
			return false
		}
	}
	return true
}

// MatchesAt returns true if the ordering column at the given index in this
// instance matches the given column. The column matches if its id is part of
// the equivalence group and if it has the same direction.
func (oc *OrderingChoice) MatchesAt(index int, col opt.OrderingColumn) bool {
	if oc.Optional.Contains(col.ID()) {
		return true
	}
	choice := &oc.Columns[index]
	if choice.Descending != col.Descending() {
		return false
	}
	if !choice.Group.Contains(col.ID()) {
		return false
	}
	return true
}

// AppendCol adds a new column to the end of the sequence of ordering columns
// maintained by this instance. The new column has the given ID and direction as
// the only ordering choice.
func (oc *OrderingChoice) AppendCol(id opt.ColumnID, descending bool) {
	ordCol := OrderingColumnChoice{Descending: descending}
	ordCol.Group.Add(id)
	oc.Optional.Remove(id)
	oc.Columns = append(oc.Columns, ordCol)
}

// Copy returns a complete copy of this instance, with a private version of the
// ordering column array.
func (oc *OrderingChoice) Copy() OrderingChoice {
	var other OrderingChoice
	other.Optional = oc.Optional.Copy()
	other.Columns = make([]OrderingColumnChoice, len(oc.Columns))
	copy(other.Columns, oc.Columns)
	return other
}

// CanSimplify returns true if a call to Simplify would result in any changes to
// the OrderingChoice. Changes include additional constant columns, removed
// groups, additional equivalent columns, or removed non-equivalent columns.
// This is used to quickly check whether Simplify needs to be called without
// requiring allocations in the common case. This logic should be changed in
// concert with the Simplify logic.
func (oc *OrderingChoice) CanSimplify(fdset *FuncDepSet) bool {
	if oc.Any() {
		// Any ordering allowed, so can't simplify further.
		return false
	}

	// Check whether optional columns can be added by the FD set.
	optional := fdset.ComputeClosure(oc.Optional)
	if !optional.Equals(oc.Optional) {
		return true
	}

	closure := optional
	for i := range oc.Columns {
		group := &oc.Columns[i]

		// If group contains an optional column, then group can be simplified
		// or removed entirely.
		if group.Group.Intersects(optional) {
			return true
		}

		// If group is functionally determined by previous groups, then it can
		// be removed entirely.
		if group.Group.SubsetOf(closure) {
			return true
		}

		// Check whether the equivalency group needs to change based on the FD.
		equiv := fdset.ComputeEquivGroup(group.AnyID())
		if !equiv.Equals(group.Group) {
			return true
		}

		// Add this group's columns and find closure with new columns.
		closure.UnionWith(equiv)
		closure = fdset.ComputeClosure(closure)
	}

	return false
}

// Simplify uses the given FD set to streamline the orderings allowed by this
// instance. It can both increase and decrease the number of allowed orderings:
//
//   1. Constant columns add additional optional column choices.
//
//   2. Equivalent columns allow additional choices within an ordering column
//      group.
//
//   3. Non-equivalent columns in an ordering column group are removed.
//
//   4. If the columns in a group are functionally determined by columns from
//      previous groups, the group can be dropped. This technique is described
//      in the "Reduce Order" section of this paper:
//
//        Simmen, David & Shekita, Eugene & Malkemus, Timothy. (1996).
//        Fundamental Techniques for Order Optimization.
//        Sigmod Record. Volume 25 Issue 2, June 1996. Pages 57-67.
//        https://cs.uwaterloo.ca/~gweddell/cs798/p57-simmen.pdf
//
// This logic should be changed in concert with the CanSimplify logic.
func (oc *OrderingChoice) Simplify(fdset *FuncDepSet) {
	oc.Optional = fdset.ComputeClosure(oc.Optional)

	closure := oc.Optional
	n := 0
	for i := range oc.Columns {
		group := &oc.Columns[i]

		// Constant columns from the FD set become optional ordering columns and
		// so can be removed.
		if group.Group.Intersects(oc.Optional) {
			if group.Group.SubsetOf(oc.Optional) {
				continue
			}
			group.Group = group.Group.Difference(oc.Optional)
		}

		// If this group is functionally determined from previous groups, then
		// discard it.
		if group.Group.SubsetOf(closure) {
			continue
		}

		// Set group to columns equivalent to an arbitrary column in the group
		// based on the FD set. This can both add and remove columns from the
		// group.
		group.Group = fdset.ComputeEquivGroup(group.AnyID())

		// Add this group's columns and find closure with the new columns.
		closure = closure.Union(group.Group)
		closure = fdset.ComputeClosure(closure)

		if n != i {
			oc.Columns[n] = oc.Columns[i]
		}
		n++
	}
	oc.Columns = oc.Columns[:n]

	if len(oc.Columns) == 0 {
		// Normalize Any case by dropping any optional columns.
		oc.Optional = opt.ColSet{}
	}
}

// Truncate removes all ordering columns beyond the given index. For example,
// +1,+(2|3),-4 opt(5,6) would be truncated to:
//
//   prefix=0  => opt(5,6)
//   prefix=1  => +1 opt(5,6)
//   prefix=2  => +1,+(2|3) opt(5,6)
//   prefix=3+ => +1,+(2|3),-4 opt(5,6)
//
func (oc *OrderingChoice) Truncate(prefix int) {
	if prefix < len(oc.Columns) {
		oc.Columns = oc.Columns[:prefix]
		if len(oc.Columns) == 0 {
			// Normalize Any case by dropping any optional columns.
			oc.Optional = opt.ColSet{}
		}
	}
}

// ProjectCols removes any references to columns that are not in the given
// set. This method can only be used when the OrderingChoice can be expressed
// with the given columns; i.e. all groups have at least one column in the set.
func (oc *OrderingChoice) ProjectCols(cols opt.ColSet) {
	startLen := len(oc.Columns)
	oc.RestrictToCols(cols)
	if startLen != len(oc.Columns) {
		panic(errors.AssertionFailedf("no columns left from group"))
	}
}

// RestrictToCols removes any references to columns that are not in the given
// set. If a group does not contain any columns in the given set, it removes
// that group and all following groups.
func (oc *OrderingChoice) RestrictToCols(cols opt.ColSet) {
	if !oc.Optional.SubsetOf(cols) {
		oc.Optional = oc.Optional.Intersection(cols)
	}
	for i := range oc.Columns {
		if !oc.Columns[i].Group.SubsetOf(cols) {
			oc.Columns[i].Group = oc.Columns[i].Group.Intersection(cols)
			if oc.Columns[i].Group.Empty() {
				oc.Columns = oc.Columns[:i]
				break
			}
		}
	}
}

// PrefixIntersection computes an OrderingChoice which:
//  - implies <oc> (this instance), and
//  - implies a "segmented ordering", which is any ordering which starts with a
//    permutation of all columns in <prefix> followed by the <suffix> ordering.
//
// Note that <prefix> and <suffix> cannot have any columns in common.
//
// Such an ordering can be computed via the following rules:
//
//  - if <prefix> and <suffix> are empty: return this instance.
//
//  - if <oc> is empty: generate an arbitrary segmented ordering.
//
//  - if the first column of <oc> is either in <prefix> or is the first column
//    of <suffix> while <prefix> is empty: this column is the first column of
//    the result; calculate the rest recursively.
//
func (oc OrderingChoice) PrefixIntersection(
	prefix opt.ColSet, suffix []OrderingColumnChoice,
) (_ OrderingChoice, ok bool) {
	var result OrderingChoice
	oc = oc.Copy()

	prefixHelper := colSetHelper{colSet: prefix}

	for {
		switch {
		case prefixHelper.empty() && len(suffix) == 0:
			// Any ordering is allowed by <prefix>+<suffix>, so use <oc> directly.
			result.Columns = append(result.Columns, oc.Columns...)
			return result, true
		case len(oc.Columns) == 0:
			// Any ordering is allowed by <oc>, so pick an arbitrary ordering of the
			// columns in <prefix> then append suffix.
			// TODO(justin): investigate picking an order more intelligently here.
			for col, ok := prefixHelper.next(0); ok; col, ok = prefixHelper.next(col + 1) {
				result.AppendCol(col, false /* descending */)
			}

			result.Columns = append(result.Columns, suffix...)
			return result, true
		case prefixHelper.empty() && len(oc.Columns) > 0 && len(suffix) > 0 &&
			oc.Columns[0].Group.Intersects(suffix[0].Group) &&
			oc.Columns[0].Descending == suffix[0].Descending:
			// <prefix> is empty, and <suffix> and <oc> agree on the first column, so
			// emit that column, remove it from both, and loop.
			newCol := oc.Columns[0]
			newCol.Group = oc.Columns[0].Group.Intersection(suffix[0].Group)
			result.Columns = append(result.Columns, newCol)

			oc.Columns = oc.Columns[1:]
			suffix = suffix[1:]
		case len(oc.Columns) > 0 && prefixHelper.intersects(oc.Columns[0].Group):
			// <prefix> contains the first column in <oc>, so emit it and remove it
			// from both.
			result.Columns = append(result.Columns, oc.Columns[0])

			prefixHelper.differenceWith(oc.Columns[0].Group)
			oc.Columns = oc.Columns[1:]
		default:
			// If no rule applied, fail.
			return OrderingChoice{}, false
		}
	}
}

// Equals returns true if the set of orderings matched by this instance is the
// same as the set matched by the given instance.
func (oc *OrderingChoice) Equals(rhs *OrderingChoice) bool {
	if len(oc.Columns) != len(rhs.Columns) {
		return false
	}
	if !oc.Optional.Equals(rhs.Optional) {
		return false
	}

	for i := range oc.Columns {
		left := &oc.Columns[i]
		y := &rhs.Columns[i]

		if left.Descending != y.Descending {
			return false
		}
		if !left.Group.Equals(y.Group) {
			return false
		}
	}
	return true
}

func (oc OrderingChoice) String() string {
	var buf bytes.Buffer
	oc.Format(&buf)
	return buf.String()
}

// Format writes the OrderingChoice to the given buffer in a human-readable
// string representation that can also be parsed by ParseOrderingChoice:
//
//   +1
//   +1,-2
//   +(1|2)
//   +(1|2),+3
//   -(3|4),+5 opt(1,2)
//
func (oc OrderingChoice) Format(buf *bytes.Buffer) {
	for g := range oc.Columns {
		group := &oc.Columns[g]
		count := group.Group.Len()

		if group.Descending {
			buf.WriteByte('-')
		} else {
			buf.WriteByte('+')
		}

		// Write equivalence group.
		if count > 1 {
			buf.WriteByte('(')
		}
		first := true
		for i, ok := group.Group.Next(0); ok; i, ok = group.Group.Next(i + 1) {
			if !first {
				buf.WriteByte('|')
			} else {
				first = false
			}
			fmt.Fprintf(buf, "%d", i)
		}
		if count > 1 {
			buf.WriteByte(')')
		}

		if g+1 != len(oc.Columns) {
			buf.WriteByte(',')
		}
	}

	// Write set of optional columns.
	if !oc.Optional.Empty() {
		if len(oc.Columns) != 0 {
			buf.WriteByte(' ')
		}
		fmt.Fprintf(buf, "opt%s", oc.Optional)
	}
}

// RemapColumns returns a copy of oc with all columns in from mapped to columns
// in to.
func (oc *OrderingChoice) RemapColumns(from, to opt.ColList) OrderingChoice {
	var other OrderingChoice
	other.Optional = opt.TranslateColSetStrict(oc.Optional, from, to)
	other.Columns = make([]OrderingColumnChoice, len(oc.Columns))
	for i := range oc.Columns {
		col := &oc.Columns[i]
		other.Columns[i] = OrderingColumnChoice{
			Group:      opt.TranslateColSetStrict(col.Group, from, to),
			Descending: col.Descending,
		}
	}
	return other
}

// AnyID returns the ID of an arbitrary member of the group of equivalent
// columns.
func (oc *OrderingColumnChoice) AnyID() opt.ColumnID {
	id, ok := oc.Group.Next(0)
	if !ok {
		panic(errors.AssertionFailedf("column choice group should have at least one column id"))
	}
	return id
}

// OrderingSet is a set of orderings, with the restriction that no ordering
// is a prefix of another ordering in the set.
type OrderingSet []OrderingChoice

// Copy returns a copy of the set which can be independently modified.
func (os OrderingSet) Copy() OrderingSet {
	res := make(OrderingSet, len(os))
	copy(res, os)
	return res
}

// Add an ordering to the list, checking whether it intersects another
// ordering (or vice-versa).
func (os *OrderingSet) Add(other *OrderingChoice) {
	if other.Any() {
		panic(errors.AssertionFailedf("empty ordering choice"))
	}
	for i := range *os {
		if (*os)[i].Intersects(other) {
			// Replace with an ordering that implies both.
			(*os)[i] = (*os)[i].Intersection(other)
			return
		}
	}
	*os = append(*os, *other)
}

// Simplify simplifies all the orderings based on the FDs.
func (os *OrderingSet) Simplify(fds *FuncDepSet) {
	old := *os
	*os = old[:0]
	for _, o := range old {
		newOrd := o
		if o.CanSimplify(fds) {
			newOrd = o.Copy()
			newOrd.Simplify(fds)
		}
		if !newOrd.Any() {
			// This function appends at most one element; it is ok to operate on
			// the same slice.
			os.Add(&newOrd)
		}
	}
}

// RestrictToImplies keeps only the orderings that imply the required ordering.
func (os *OrderingSet) RestrictToImplies(required *OrderingChoice) {
	res := (*os)[:0]
	for _, o := range *os {
		if o.Implies(required) {
			res = append(res, o)
		}
	}
	*os = res
}

// RestrictToCols keeps only the orderings (or prefixes of them) that refer to
// columns in the given set. The fds argument allows ordering columns that
// are not in the given set to be remapped to equivalent columns that are in the
// given set by calling Simplify.
func (os *OrderingSet) RestrictToCols(cols opt.ColSet, fds *FuncDepSet) {
	old := *os
	*os = old[:0]
	for _, o := range old {
		newOrd := o.Copy()
		if o.CanSimplify(fds) {
			newOrd.Simplify(fds)
		}
		newOrd.RestrictToCols(cols)
		if !newOrd.Any() {
			// This function appends at most one element; it is ok to operate on
			// the same slice.
			os.Add(&newOrd)
		}
	}
}

func (os OrderingSet) String() string {
	var buf bytes.Buffer
	for i, o := range os {
		if i > 0 {
			buf.WriteByte(' ')
		}
		buf.WriteByte('(')
		buf.WriteString(o.String())
		buf.WriteByte(')')
	}
	return buf.String()
}

// RemapColumns returns a copy of os with all columns in from mapped to columns
// in to.
func (os OrderingSet) RemapColumns(from, to opt.ColList) OrderingSet {
	res := make(OrderingSet, len(os))
	for i := range os {
		res[i] = os[i].RemapColumns(from, to)
	}
	return res
}

// LongestCommonPrefix returns the longest common prefix between the
// OrderingChoices within the receiver and the given OrderingChoice. However, if
// the longest common prefix implies the given OrderingChoice, nil is returned
// instead. This allows LongestCommonPrefix to avoid allocating in the common
// case where its result is just discarded by Optimizer.enforceProps.
func (os OrderingSet) LongestCommonPrefix(other *OrderingChoice) *OrderingChoice {
	var bestPrefixLength, bestPrefixIdx int
	for i, orderingChoice := range os {
		length, implies := orderingChoice.commonPrefixLength(other)
		if implies {
			// We have found a prefix that implies the required ordering. No order
			// needs to be enforced.
			return nil
		}
		if length > bestPrefixLength {
			bestPrefixLength = length
			bestPrefixIdx = i
		}
	}
	if bestPrefixLength == 0 {
		// No need to call CommonPrefix since no 'best' prefix was found.
		return &OrderingChoice{}
	}
	commonPrefix := os[bestPrefixIdx].CommonPrefix(other)
	return &commonPrefix
}

// colSetHelper is used to lazily copy the wrapped ColSet only when a mutating
// function is first called.
type colSetHelper struct {
	colSet opt.ColSet
	copied bool
}

// unionWith returns a colSetHelper reflecting a UnionWith operation performed
// on the receiver ColSet. If the original ColSet has not been copied, unionWith
// copies it now.
func (h *colSetHelper) unionWith(other opt.ColSet) {
	if !h.copied {
		h.colSet = h.colSet.Copy()
		h.copied = true
	}
	h.colSet.UnionWith(other)
}

// differenceWith returns a colSetHelper reflecting a DifferenceWith operation
// performed on the receiver ColSet. If the original ColSet has not been copied,
// differenceWith copies it now.
func (h *colSetHelper) differenceWith(other opt.ColSet) {
	if !h.copied {
		h.colSet = h.colSet.Copy()
		h.copied = true
	}
	h.colSet.DifferenceWith(other)
}

func (h *colSetHelper) intersects(other opt.ColSet) bool {
	// No need to copy, since intersects will not modify the ColSet.
	return h.colSet.Intersects(other)
}

func (h *colSetHelper) intersection(other opt.ColSet) opt.ColSet {
	// No need to copy, since intersection will not modify the ColSet.
	return h.colSet.Intersection(other)
}

func (h *colSetHelper) next(startVal opt.ColumnID) (opt.ColumnID, bool) {
	return h.colSet.Next(startVal)
}

func (h *colSetHelper) empty() bool {
	return h.colSet.Empty()
}
