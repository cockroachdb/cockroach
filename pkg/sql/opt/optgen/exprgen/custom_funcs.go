// Copyright 2019 The Cockroach Authors.
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

package exprgen

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

type customFuncs struct {
	f   *norm.Factory
	mem *memo.Memo
	cat cat.Catalog
}

// NewColumn creates a new column in the metadata.
func (c *customFuncs) NewColumn(name, typeStr string) opt.ColumnID {
	typ, err := ParseType(typeStr)
	if err != nil {
		panic(exprGenErr{err})
	}
	return c.f.Metadata().AddColumn(name, typ)
}

// LookupColumn looks up a column that was already specified in the expression
// so far (either via NewColumn or by using a table).
func (c *customFuncs) LookupColumn(name string) opt.ColumnID {
	md := c.f.Metadata()

	var res opt.ColumnID
	for colID := opt.ColumnID(1); int(colID) <= md.NumColumns(); colID++ {
		if md.ColumnMeta(colID).Alias == name {
			if res != 0 {
				panic(errorf("ambigous column %s", name))
			}
			res = colID
		}
	}
	if res == 0 {
		panic(errorf("unknown column %s", name))
	}
	return res
}

// ColList creates a ColList from a comma-separated list of column names,
// looking up each column.
func (c *customFuncs) ColList(cols string) opt.ColList {
	strs := strings.Split(cols, ",")
	res := make(opt.ColList, len(strs))
	for i, col := range strs {
		res[i] = c.LookupColumn(col)
	}
	return res
}

// ColSet creates a ColSet from a comma-separated list of column names, looking
// up each column.
func (c *customFuncs) ColSet(cols string) opt.ColSet {
	return c.ColList(cols).ToSet()
}

// Var creates a VariableOp for the given column. It allows (Var "name") as a
// shorthand for (Variable (LookupColumn "name")).
func (c *customFuncs) Var(colName string) opt.ScalarExpr {
	return c.f.ConstructVariable(c.LookupColumn(colName))
}

// ProjectionItem creates a ProjectionItem. A list of such items can be used as
// a ProjectionsExpr.
func (c *customFuncs) ProjectionItem(
	element opt.ScalarExpr, col opt.ColumnID,
) memo.ProjectionsItem {
	return memo.ProjectionsItem{
		Element:    element,
		ColPrivate: memo.ColPrivate{Col: col},
		Typ:        c.mem.Metadata().ColumnMeta(col).Type,
	}
}

// AggregationsItem creates an AggregationsItem. A list of such items can be used as
// an AggregationsExpr.
func (c *customFuncs) AggregationsItem(agg opt.ScalarExpr, col opt.ColumnID) memo.AggregationsItem {
	return memo.AggregationsItem{
		Agg:        agg,
		ColPrivate: memo.ColPrivate{Col: col},
		Typ:        c.mem.Metadata().ColumnMeta(col).Type,
	}
}

// Ordering parses a string like "+a,-b" into an Ordering.
func (c *customFuncs) Ordering(str string) opt.Ordering {
	defer func() {
		if r := recover(); r != nil {
			panic(errorf("could not parse Ordering \"%s\"", str))
		}
	}()
	return physical.ParseOrdering(c.substituteCols(str))
}

// OrderingChoice parses a string like "+a,-(b|c)" into an OrderingChoice.
func (c *customFuncs) OrderingChoice(str string) physical.OrderingChoice {
	defer func() {
		if r := recover(); r != nil {
			panic(errorf("could not parse OrderingChoice \"%s\"", str))
		}
	}()
	return physical.ParseOrderingChoice(c.substituteCols(str))
}

// substituteCols extracts every word (sequence of letters) from the string,
// looks up the column with that name, and replaces the string with the column
// ID. E.g.: "+a,+b" -> "+1,+2".
func (c *customFuncs) substituteCols(str string) string {
	var b strings.Builder
	lastPos := -1
	maybeEmit := func(curPos int) {
		if lastPos != -1 {
			col := str[lastPos:curPos]
			fmt.Fprintf(&b, "%d", c.LookupColumn(col))
		}
		lastPos = -1
	}
	for i, r := range str {
		if unicode.IsLetter(r) {
			if lastPos == -1 {
				lastPos = i
			}
			continue
		}
		maybeEmit(i)
		b.WriteRune(r)
	}
	maybeEmit(len(str))
	return b.String()
}

// MakeLookupJoin is a wrapper around ConstructLookupJoin that swaps the order
// of the private and the filters. This is useful because the expressions are
// evaluated in order, and we want to be able to refer to the lookup columns in
// the ON expression. For example:
//
//   (MakeLookupJoin
//     (Scan [ (Table "def") (Cols "d,e") ])
//     [ (JoinType "left-join") (Table "abc") (Index "abc@ab") (KeyCols "a") (Cols "a,b") ]
//     [ (Gt (Var "a") (Var "e")) ]
//   )
//
// If the order of the last two was swapped, we wouldn't be able to look up
// column a.
func (c *customFuncs) MakeLookupJoin(
	input memo.RelExpr, lookupJoinPrivate *memo.LookupJoinPrivate, on memo.FiltersExpr,
) memo.RelExpr {
	return c.f.ConstructLookupJoin(input, on, lookupJoinPrivate)
}

// Sort adds a sort enforcer which sorts according to the ordering that will be
// required by its parent.
func (c *customFuncs) Sort(input memo.RelExpr) memo.RelExpr {
	return &memo.SortExpr{Input: input}
}

// rootSentinel is used as the root value when Root is used.
type rootSentinel struct {
	expr     memo.RelExpr
	required *physical.Required
}

// Presentation converts a ColList to a Presentation.
func (c *customFuncs) Presentation(cols opt.ColList) physical.Presentation {
	res := make(physical.Presentation, len(cols))
	for i := range cols {
		res[i].ID = cols[i]
		res[i].Alias = c.mem.Metadata().ColumnMeta(cols[i]).Alias
	}
	return res
}

// NoOrdering returns the empty OrderingChoice.
func (c *customFuncs) NoOrdering() physical.OrderingChoice {
	return physical.OrderingChoice{}
}

// Root can be used only at the top level on an expression, to annotate the
// root with a presentation and/or required ordering. The operator must be able
// to provide the ordering. For example:
//   (Root
//     ( ... )
//     (Presentation "a,b")
//     (OrderingChoice "+a")
//   )
func (c *customFuncs) Root(
	root memo.RelExpr, presentation physical.Presentation, ordering physical.OrderingChoice,
) *rootSentinel {
	props := &physical.Required{
		Presentation: presentation,
		Ordering:     ordering,
	}
	return &rootSentinel{expr: root, required: props}
}
