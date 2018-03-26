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
	"bytes"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/treeprinter"
)

// LogicalProps describe the content and characteristics of data returned by
// all expression variants within a memo group. While each expression in the
// group may return rows or columns in a different order, or compute the
// result using different algorithms, the complete set of data is returned
// and can be transformed into whatever layout or presentation format that is
// desired.
type LogicalProps struct {
	// Relational contains the set of properties that describe relational
	// operators, like select, join, and project. It is nil for scalar
	// operators.
	Relational *RelationalProps

	// Scalar contains the set of properties that describe scalar operators,
	// like And, Plus, and Const. It is nil for relational operators.
	Scalar *ScalarProps
}

// RelationalProps are the subset of logical properties that are computed for
// relational expressions that return rows and columns rather than scalar
// values.
type RelationalProps struct {
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
	//
	// TODO(andyk): populate this when we have subquery support
	OuterCols opt.ColSet

	// Stats is the set of statistics that apply to this relational expression.
	// See the comment for the Statistics struct for more details.
	Stats Statistics
}

// Statistics is a collection of measurements and statistics that is used by
// the coster to estimate the cost of expressions. Statistics are collected
// for tables and indexes and are exposed to the optimizer via opt.Catalog
// interfaces. As logical properties are derived bottom-up for each
// expression, statistics are derived for each relational expression, based
// on the characteristics of the expression's operator type, as well as the
// properties of its operands. For example:
//
//   SELECT * FROM a WHERE a=1
//
// Table and column statistics can be used to estimate the number of rows
// in table a, and then to determine the selectivity of the a=1 filter, in
// order to derive the statistics of the Select expression.
type Statistics struct {
	// RowCount is the estimated number of rows returned by the expression.
	RowCount uint64
}

// ScalarProps are the subset of logical properties that are computed for
// scalar expressions that return primitive-valued types.
type ScalarProps struct {
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
	// satisfied. Unset for non-boolean scalars.
	Constraints *constraint.Set

	// TightConstraints is true if the expression is exactly equivalent to the
	// constraints. If it is false, the constraints are weaker than the
	// expression.
	TightConstraints bool
}

// OuterCols is a helper method that returns either the relational or scalar
// OuterCols field, depending on the operator's type.
func (p *LogicalProps) OuterCols() opt.ColSet {
	if p.Scalar != nil {
		return p.Scalar.OuterCols
	}
	return p.Relational.OuterCols
}

// FormatColSet outputs the specified set of columns using FormatCol to format
// the output.
func (p *LogicalProps) FormatColSet(
	tp treeprinter.Node, md *opt.Metadata, heading string, colSet opt.ColSet,
) {
	if !colSet.Empty() {
		var buf bytes.Buffer
		buf.WriteString(heading)
		colSet.ForEach(func(i int) {
			p.FormatCol(&buf, md, "", opt.ColumnID(i))
		})
		tp.Child(buf.String())
	}
}

// FormatColList outputs the specified list of columns using FormatCol to
// format the output.
func (p *LogicalProps) FormatColList(
	tp treeprinter.Node, md *opt.Metadata, heading string, colList opt.ColList,
) {
	if len(colList) > 0 {
		var buf bytes.Buffer
		buf.WriteString(heading)
		for _, col := range colList {
			p.FormatCol(&buf, md, "", col)
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
func (p *LogicalProps) FormatCol(
	buf *bytes.Buffer, md *opt.Metadata, label string, id opt.ColumnID,
) {
	if label == "" {
		label = md.ColumnLabel(id)
	}
	typ := md.ColumnType(id)
	buf.WriteByte(' ')
	buf.WriteString(label)
	buf.WriteByte(':')
	fmt.Fprintf(buf, "%d", id)
	buf.WriteByte('(')
	buf.WriteString(typ.String())
	if p.Relational.NotNullCols.Contains(int(id)) {
		buf.WriteString("!null")
	}
	buf.WriteByte(')')
}
