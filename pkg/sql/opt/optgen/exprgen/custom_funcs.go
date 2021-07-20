// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package exprgen

import (
	"fmt"
	"strings"
	"unicode"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
				panic(errorf("ambiguous column %s", name))
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
	if cols == "" {
		return opt.ColList{}
	}
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

// MinPhysProps returns the singleton minimum set of physical properties.
func (c *customFuncs) MinPhysProps() *physical.Required {
	return physical.MinRequired
}

// MakePhysProps returns a set of physical properties corresponding to the
// input presentation and OrderingChoice.
func (c *customFuncs) MakePhysProps(
	p physical.Presentation, o props.OrderingChoice,
) *physical.Required {
	return c.mem.InternPhysicalProps(&physical.Required{
		Presentation: p,
		Ordering:     o,
	})
}

// ExplainOptions creates a tree.ExplainOptions from a comma-separated list of
// options.
func (c *customFuncs) ExplainOptions(opts string) tree.ExplainOptions {
	explain, err := tree.MakeExplain(strings.Split(opts, ","), &tree.Select{})
	if err != nil {
		panic(exprGenErr{err})
	}
	return explain.(*tree.Explain).ExplainOptions
}

// Var creates a VariableOp for the given column. It allows (Var "name") as a
// shorthand for (Variable (LookupColumn "name")).
func (c *customFuncs) Var(colName string) opt.ScalarExpr {
	return c.f.ConstructVariable(c.LookupColumn(colName))
}

// FindTable looks up a table in the metadata without creating it.
// This is required to construct operators like IndexJoin which must
// reference the same table multiple times.
func (c *customFuncs) FindTable(name string) opt.TableID {
	tables := c.mem.Metadata().AllTables()

	var res opt.TableID
	for i := range tables {
		if string(tables[i].Table.Name()) == name {
			if res != 0 {
				panic(errorf("ambiguous table %q", name))
			}
			res = tables[i].MetaID
		}
	}
	if res == 0 {
		panic(errorf("couldn't find table with name %q", name))
	}
	return res
}

// Ordering parses a string like "+a,-b" into an Ordering.
func (c *customFuncs) Ordering(str string) opt.Ordering {
	defer func() {
		if r := recover(); r != nil {
			panic(errorf("could not parse Ordering \"%s\"", str))
		}
	}()
	return props.ParseOrdering(c.substituteCols(str))
}

// OrderingChoice parses a string like "+a,-(b|c)" into an OrderingChoice.
func (c *customFuncs) OrderingChoice(str string) props.OrderingChoice {
	defer func() {
		if r := recover(); r != nil {
			panic(errorf("could not parse OrderingChoice \"%s\"", str))
		}
	}()
	return props.ParseOrderingChoice(c.substituteCols(str))
}

// substituteCols extracts every word (sequence of letters, numbers, and
// underscores) from the string, looks up the column with that name, and
// replaces the string with the column ID. E.g.: "+a,+b" -> "+1,+2".
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
		if unicode.IsLetter(r) || r == '_' || unicode.IsNumber(r) {
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
func (c *customFuncs) NoOrdering() props.OrderingChoice {
	return props.OrderingChoice{}
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
	root memo.RelExpr, presentation physical.Presentation, ordering props.OrderingChoice,
) *rootSentinel {
	props := &physical.Required{
		Presentation: presentation,
		Ordering:     ordering,
	}
	return &rootSentinel{expr: root, required: props}
}
