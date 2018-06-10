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

package props

import (
	"bytes"
	"fmt"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// Logical properties describe the content and characteristics of data returned
// by all expression variants within a memo group. While each expression in the
// group may return rows or columns in a different order, or compute the result
// using different algorithms, the same set of data is returned and can then be
// transformed into whatever layout or presentation format that is desired,
// according to the required physical properties.
type Logical struct {
	// Relational contains the set of properties that describe relational
	// operators, like select, join, and project. It is nil for scalar
	// operators.
	Relational *Relational

	// Scalar contains the set of properties that describe scalar operators,
	// like And, Plus, and Const. It is nil for relational operators.
	Scalar *Scalar
}

// Relational properties are the subset of logical properties that are computed
// for relational expressions that return rows and columns rather than scalar
// values.
type Relational struct {
	// OutputCols is the set of columns that can be projected by the
	// expression. Ordering, naming, and duplication of columns is not
	// representable by this property; those are physical properties.
	OutputCols opt.ColSet

	// NotNullCols is the subset of output columns which cannot be NULL.
	// The NULL-ability of columns flows from the inputs and can also be
	// derived from filters that are NULL-intolerant.
	NotNullCols opt.ColSet

	// OuterCols is the set of columns that are referenced by variables within
	// this relational sub-expression, but are not bound within the scope of
	// the expression. For example:
	//
	//   SELECT *
	//   FROM a
	//   WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x AND b.y = 5)
	//
	// For the inner SELECT expression, a.x is an outer column, meaning that it
	// is defined "outside" the SELECT expression (hence the name "outer"). The
	// SELECT expression binds the b.x and b.y references, so they are not
	// part of the outer column set. The outer SELECT binds the a.x column, and
	// so its outer column set is empty.
	OuterCols opt.ColSet

	// WeakKeys are the column sets which form weak keys and are subsets of the
	// expression's output columns. A weak key set cannot contain any other weak
	// key set (it would be redundant).
	//
	// A column set is a key if no two rows are equal after projection onto that
	// set. This definition treats NULL as if were equal to NULL, so two rows
	// having duplicate NULL values would *not* qualify as key rows. Therefore,
	// in the usual case, the key columns are also not nullable. The simplest
	// example of a key is the primary key for a table (recall that all of the
	// columns of the primary key are defined to be NOT NULL).
	//
	// A weak key is similar to a key, with the difference that NULL values are
	// treated as *not equal* to other NULL values. Therefore, two rows having
	// duplicate NULL values could still qualify as weak key rows. A UNIQUE index
	// on a table is a weak key and possibly a key if all of the columns are NOT
	// NULL. A weak key is a key if "(WeakKeys[i] & NotNullCols) == WeakKeys[i]".
	//
	// An empty key is valid (an empty key implies there is at most one row). Note
	// that an empty key is always the only key in the set, since it's a subset of
	// every other key (i.e. every other key would be redundant).
	WeakKeys opt.WeakKeys

	// Cardinality is the number of rows that can be returned from this relational
	// expression. The number of rows will always be between the inclusive Min and
	// Max bounds. If Max=math.MaxUint32, then there is no limit to the number of
	// rows returned by the expression.
	Cardinality Cardinality

	// Stats is the set of statistics that apply to this relational expression.
	// See opt/statistics.go and statistics_builder.go for more details.
	Stats opt.Statistics

	// Rule encapsulates the set of properties that are maintained to assist
	// with specific sets of transformation rules. They are not intended to be
	// general purpose in nature. Typically, they're used by rules which need to
	// decide whether to push operators down into the tree. These properties
	// "bubble up" information about the subtree which can aid in that decision.
	//
	// Whereas the other logical relational properties are filled in by the memo
	// package upon creation of a new memo group, the rules properties are filled
	// in by one of the transformation packages, since deriving rule properties
	// is so closely tied with maintenance of the rules that depend upon them.
	// For example, the PruneCols set is connected to the PruneCols normalization
	// rules. The decision about which columns to add to PruneCols depends upon
	// what works best for those rules. Neither the rules nor their properties
	// can be considered in isolation, without considering the other.
	Rule struct {
		// PruneCols is the subset of output columns that can potentially be
		// eliminated by one of the PruneCols normalization rules. Those rules
		// operate by pushing a Project operator down the tree that discards
		// unused columns. For example:
		//
		//   SELECT y FROM xyz WHERE x=1 ORDER BY y LIMIT 1
		//
		// The z column is never referenced, either by the filter or by the
		// limit, and would be part of the PruneCols set for the Limit operator.
		// The final Project operator could then push down a pruning Project
		// operator that eliminated the z column from its subtree.
		//
		// PruneCols is built bottom-up. It typically starts out containing the
		// complete set of output columns in a leaf expression, but quickly
		// empties out at higher levels of the expression tree as the columns
		// are referenced. Drawing from the example above:
		//
		//   Limit PruneCols : [z]
		//   Select PruneCols: [y, z]
		//   Scan PruneCols  : [x, y, z]
		//
		// Only a small number of relational operators are capable of pruning
		// columns (e.g. Scan, Project). A pruning Project operator pushed down
		// the tree must journey downwards until it finds a pruning-capable
		// operator. If a column is part of PruneCols, then it is guaranteed that
		// such an operator exists at the end of the journey. Operators that are
		// not capable of filtering columns (like Explain) will not add any of
		// their columns to this set.
		PruneCols opt.ColSet
	}
}

// Scalar properties are the subset of logical properties that are computed for
// scalar expressions that return primitive-valued types.
type Scalar struct {
	// Type is the data type of the scalar expression (int, string, etc).
	Type types.T

	// OuterCols is the set of columns that are referenced by variables within
	// this scalar sub-expression, but are not bound within the scope of the
	// expression. For example:
	//
	//   SELECT *
	//   FROM a
	//   WHERE EXISTS(SELECT * FROM b WHERE b.x = a.x AND b.y = 5)
	//
	// For the EXISTS expression, only a.x is an outer column, meaning that
	// only it is defined "outside" the EXISTS expression (hence the name
	// "outer"). Note that what constitutes an "outer column" is dependent on
	// an expression's location in the query. For example, while the b.x and
	// b.y columns are not outer columns on the EXISTS expression, they *are*
	// outer columns on the inner WHERE condition.
	OuterCols opt.ColSet

	// Constraints is the set of constraints deduced from a boolean expression.
	// For the expression to be true, all constraints in the set must be
	// satisfied.
	// This field is populated lazily, as necessary.
	Constraints *constraint.Set

	// TightConstraints is true if the expression is exactly equivalent to the
	// constraints. If it is false, the constraints are weaker than the
	// expression.
	// This field is populated lazily, as necessary.
	TightConstraints bool

	// HasCorrelatedSubquery is true if the scalar expression tree contains a
	// subquery having one or more outer columns. The subquery can be a Subquery,
	// Exists, or Any operator. These operators need to be hoisted out of scalar
	// expression trees and turned into top-level apply joins. This property makes
	// detection fast and easy so that the hoister doesn't waste time searching
	// subtrees that don't contain subqueries.
	HasCorrelatedSubquery bool
}

// OuterCols is a helper method that returns either the relational or scalar
// OuterCols field, depending on the operator's type.
func (p *Logical) OuterCols() opt.ColSet {
	if p.Scalar != nil {
		return p.Scalar.OuterCols
	}
	return p.Relational.OuterCols
}

// FormatColSet outputs the specified set of columns using FormatCol to format
// the output.
func (p *Logical) FormatColSet(
	f *opt.ExprFmtCtx, tp treeprinter.Node, heading string, colSet opt.ColSet,
) {
	if !colSet.Empty() {
		var buf bytes.Buffer
		buf.WriteString(heading)
		colSet.ForEach(func(i int) {
			p.FormatCol(f, &buf, "", opt.ColumnID(i))
		})
		tp.Child(buf.String())
	}
}

// FormatColList outputs the specified list of columns using FormatCol to
// format the output.
func (p *Logical) FormatColList(
	f *opt.ExprFmtCtx, tp treeprinter.Node, heading string, colList opt.ColList,
) {
	if len(colList) > 0 {
		var buf bytes.Buffer
		buf.WriteString(heading)
		for _, col := range colList {
			p.FormatCol(f, &buf, "", col)
		}
		tp.Child(buf.String())
	}
}

// FormatCol outputs the specified column using the following format:
//   label:index(type)
//
// If the column is not nullable, then this is the format:
//   label:index(type!null)
//
// If a label is given, then it is used. Otherwise, a "best effort" label is
// used from query metadata.
func (p *Logical) FormatCol(f *opt.ExprFmtCtx, buf *bytes.Buffer, label string, id opt.ColumnID) {
	if label == "" {
		label = f.Metadata().ColumnLabel(id)
	}

	if !isSimpleColumnName(label) {
		// Add quotations around the column name if it appears to be an
		// expression. This also indicates that the column name is not eligible
		// to be shortened.
		label = "\"" + label + "\""
	} else if f.HasFlags(opt.ExprFmtHideQualifications) {
		// If the label is qualified, try to shorten it.
		if idx := strings.LastIndex(label, "."); idx != -1 {
			short := label[idx+1:]
			suffix := label[idx:] // includes the "."
			// Check if shortening the label could cause ambiguity: is there another
			// column that would be shortened to the same name?
			ambiguous := false
			for col := opt.ColumnID(1); int(col) <= f.Metadata().NumColumns(); col++ {
				if col != id {
					if l := f.Metadata().ColumnLabel(col); l == short || strings.HasSuffix(l, suffix) {
						ambiguous = true
						break
					}
				}
			}
			if !ambiguous {
				label = short
			}
		}
	}

	typ := f.Metadata().ColumnType(id)
	buf.WriteByte(' ')
	buf.WriteString(label)
	buf.WriteByte(':')
	fmt.Fprintf(buf, "%d", id)
	buf.WriteByte('(')
	buf.WriteString(typ.String())

	if !p.Relational.NotNullCols.SubsetOf(p.Relational.OutputCols) {
		panic(fmt.Sprintf("not null cols %s not a subset of output cols %s",
			p.Relational.NotNullCols, p.Relational.OutputCols))
	}
	if p.Relational.NotNullCols.Contains(int(id)) {
		buf.WriteString("!null")
	}
	buf.WriteByte(')')
}

// isSimpleColumnName returns true if the given label consists of only ASCII
// letters, numbers, underscores, quotation marks, and periods ("."). It is
// used to determine whether or not we can shorten a column label by removing
// the prefix up to the last ".". Although isSimpleColumnName excludes some
// valid table column names, it ensures that we don't shorten expressions such
// as "a.x + b.x" to "x". It is better to err on the side of not shortening
// than to incorrectly shorten a column name representing an expression.
func isSimpleColumnName(label string) bool {
	for i, r := range label {
		if r > unicode.MaxASCII {
			return false
		}

		if i == 0 {
			if r != '"' && !unicode.IsLetter(r) {
				// The first character must be a letter or quotation mark.
				return false
			}
		} else if r != '.' && r != '_' && r != '"' && !unicode.IsNumber(r) && !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}
