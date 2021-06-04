// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package reducesql

import (
	"bytes"
	"fmt"
	"go/constant"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	// Import builtins.
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/reduce"
)

// SQLPasses is a collection of reduce.Pass interfaces that reduce SQL
// statements.
var SQLPasses = []reduce.Pass{
	removeStatement,
	replaceStmt,
	removeWithCTEs,
	removeWith,
	removeCreateDefs,
	removeComputedColumn,
	removeValuesCols,
	removeWithSelectExprs,
	removeSelectAsExprs,
	removeIndexFlags,
	removeValuesRows,
	removeSelectExprs,
	nullExprs,
	nullifyFuncArgs,
	removeLimit,
	removeOrderBy,
	removeOrderByExprs,
	removeGroupBy,
	removeGroupByExprs,
	removeCreateNullDefs,
	removeIndexCols,
	removeIndexPredicate,
	removeWindowPartitions,
	removeDBSchema,
	removeFroms,
	removeJoins,
	removeWhere,
	removeHaving,
	removeDistinct,
	simplifyOnCond,
	simplifyVal,
	removeCTENames,
	removeCasts,
	removeAliases,
	unparenthesize,
}

type sqlWalker struct {
	topOnly bool
	match   func(int, interface{}) int
	replace func(int, interface{}) (int, tree.NodeFormatter)
}

// walkSQL walks SQL statements and allows for in-place transformations to be
// made directly to the nodes. match is a function that does both matching
// and transformation. It takes an int specifying the 0-based occurrence to
// transform. If the number of possible transformations is lower than that,
// nothing is mutated. It returns the number of transformations it could have
// performed. It is safe to mutate AST nodes directly because the string is
// reparsed into a new AST each time.
func walkSQL(name string, match func(transform int, node interface{}) (matched int)) reduce.Pass {
	w := sqlWalker{
		match: match,
	}
	return reduce.MakeIntPass(name, w.Transform)
}

// replaceStatement is like walkSQL, except if it returns a non-nil
// replacement, the top-level SQL statement is completely replaced with the
// return value.
func replaceStatement(
	name string,
	replace func(transform int, node interface{}) (matched int, replacement tree.NodeFormatter),
) reduce.Pass {
	w := sqlWalker{
		replace: replace,
	}
	return reduce.MakeIntPass(name, w.Transform)
}

// replaceTopStatement is like replaceStatement but only applies to top-level
// statements.
func replaceTopStatement(
	name string,
	replace func(transform int, node interface{}) (matched int, replacement tree.NodeFormatter),
) reduce.Pass {
	w := sqlWalker{
		replace: replace,
		topOnly: true,
	}
	return reduce.MakeIntPass(name, w.Transform)
}

var (
	// LogUnknown determines whether unknown types encountered during
	// statement walking.
	LogUnknown   bool
	unknownTypes = map[string]bool{}
)

func (w sqlWalker) Transform(s string, i int) (out string, ok bool, err error) {
	stmts, err := parser.Parse(s)
	if err != nil {
		return "", false, err
	}

	asts := make([]tree.NodeFormatter, len(stmts))
	for si, stmt := range stmts {
		asts[si] = stmt.AST
	}

	var replacement tree.NodeFormatter
	// nodeCount is incremented on each visited node per statement. It is
	// currently used to determine if walk is at the top-level statement
	// or not.
	var nodeCount int
	var walk func(...interface{})
	walk = func(nodes ...interface{}) {
		for _, node := range nodes {
			nodeCount++
			if w.topOnly && nodeCount > 1 {
				return
			}
			if i < 0 {
				return
			}
			var matches int
			if w.match != nil {
				matches = w.match(i, node)
			} else {
				matches, replacement = w.replace(i, node)
			}
			i -= matches

			if node == nil {
				continue
			}
			if _, ok := node.(tree.Datum); ok {
				continue
			}

			switch node := node.(type) {
			case *tree.AliasedTableExpr:
				walk(node.Expr)
			case *tree.AndExpr:
				walk(node.Left, node.Right)
			case *tree.AnnotateTypeExpr:
				walk(node.Expr)
			case *tree.Array:
				walk(node.Exprs)
			case *tree.BinaryExpr:
				walk(node.Left, node.Right)
			case *tree.CaseExpr:
				walk(node.Expr, node.Else)
				for _, w := range node.Whens {
					walk(w.Cond, w.Val)
				}
			case *tree.CastExpr:
				walk(node.Expr)
			case *tree.CoalesceExpr:
				for _, expr := range node.Exprs {
					walk(expr)
				}
			case *tree.ColumnTableDef:
				walk(node.Computed.Expr)
				for _, expr := range node.CheckExprs {
					walk(expr)
				}
			case *tree.ComparisonExpr:
				walk(node.Left, node.Right)
			case *tree.CreateTable:
				for _, def := range node.Defs {
					walk(def)
				}
				if node.AsSource != nil {
					walk(node.AsSource)
				}
			case *tree.CTE:
				walk(node.Stmt)
			case *tree.DBool:
			case *tree.Delete:
				walk(node.Table)
				if node.With != nil {
					walk(node.With)
				}
				if node.Where != nil {
					walk(node.Where)
				}
				walk(node.OrderBy)
				if node.Limit != nil {
					walk(node.Limit)
				}
				if node.Returning != nil {
					walk(node.Returning)
				}
			case tree.Exprs:
				for _, expr := range node {
					walk(expr)
				}
			case *tree.FamilyTableDef:
			case *tree.FuncExpr:
				if node.WindowDef != nil {
					walk(node.WindowDef)
				}
				walk(node.Exprs, node.Filter)
			case *tree.IndexTableDef:
				walk(node.Predicate)
			case *tree.Insert:
				walk(node.Table)
				if node.Rows != nil {
					walk(node.Rows)
				}
				if node.With != nil {
					walk(node.With)
				}
				if node.Returning != nil {
					walk(node.Returning)
				}
			case *tree.JoinTableExpr:
				walk(node.Left, node.Right, node.Cond)
			case *tree.Limit:
				walk(node.Count)
			case *tree.NotExpr:
				walk(node.Expr)
			case *tree.NumVal:
			case *tree.OnJoinCond:
				walk(node.Expr)
			case *tree.Order:
				walk(node.Expr, node.Table)
			case *tree.OrExpr:
				walk(node.Left, node.Right)
			case *tree.ParenExpr:
				walk(node.Expr)
			case *tree.ParenSelect:
				walk(node.Select)
			case *tree.RangeCond:
				walk(node.Left, node.From, node.To)
			case *tree.RowsFromExpr:
				for _, expr := range node.Items {
					walk(expr)
				}
			case *tree.Select:
				if node.With != nil {
					walk(node.With)
				}
				walk(node.Select)
				if node.OrderBy != nil {
					for _, order := range node.OrderBy {
						walk(order)
					}
				}
				if node.Limit != nil {
					walk(node.Limit)
				}
			case *tree.SelectClause:
				walk(node.Exprs)
				if node.Where != nil {
					walk(node.Where)
				}
				if node.Having != nil {
					walk(node.Having)
				}
				for _, table := range node.From.Tables {
					walk(table)
				}
				if node.DistinctOn != nil {
					for _, distinct := range node.DistinctOn {
						walk(distinct)
					}
				}
				if node.GroupBy != nil {
					for _, group := range node.GroupBy {
						walk(group)
					}
				}
			case tree.SelectExpr:
				walk(node.Expr)
			case tree.SelectExprs:
				for _, expr := range node {
					walk(expr)
				}
			case *tree.SetVar:
				for _, expr := range node.Values {
					walk(expr)
				}
			case *tree.StrVal:
			case *tree.Subquery:
				walk(node.Select)
			case *tree.TableName, tree.TableName:
			case *tree.Tuple:
				for _, expr := range node.Exprs {
					walk(expr)
				}
			case *tree.UnaryExpr:
				walk(node.Expr)
			case *tree.UniqueConstraintTableDef:
			case *tree.UnionClause:
				walk(node.Left, node.Right)
			case tree.UnqualifiedStar:
			case *tree.UnresolvedName:
			case *tree.Update:
				walk(node.Table)
				if node.Exprs != nil {
					walk(node.Exprs)
				}
				if node.With != nil {
					walk(node.With)
				}
				if node.Where != nil {
					walk(node.Where)
				}
				walk(node.OrderBy)
				if node.Limit != nil {
					walk(node.Limit)
				}
				if node.Returning != nil {
					walk(node.Returning)
				}
			case *tree.ValuesClause:
				for _, row := range node.Rows {
					walk(row)
				}
			case *tree.Where:
				walk(node.Expr)
			case *tree.WindowDef:
				walk(node.Partitions)
				if node.Frame != nil {
					walk(node.Frame)
				}
			case *tree.WindowFrame:
				if node.Bounds.StartBound != nil {
					walk(node.Bounds.StartBound)
				}
				if node.Bounds.EndBound != nil {
					walk(node.Bounds.EndBound)
				}
			case *tree.WindowFrameBound:
				walk(node.OffsetExpr)
			case *tree.Window:
			case *tree.With:
				for _, expr := range node.CTEList {
					walk(expr)
				}
			default:
				if LogUnknown {
					n := fmt.Sprintf("%T", node)
					if !unknownTypes[n] {
						unknownTypes[n] = true
						fmt.Println("UNKNOWN", n)
					}
				}
			}
		}
	}

	for i, ast := range asts {
		replacement = nil
		nodeCount = 0
		walk(ast)
		if replacement != nil {
			asts[i] = replacement
		}
	}
	if i >= 0 {
		// Didn't find enough matches, so we're done.
		return s, false, nil
	}
	var sb strings.Builder
	for i, ast := range asts {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		sb.WriteString(tree.Pretty(ast))
		sb.WriteString(";")
	}
	return sb.String(), true, nil
}

// Pretty formats input SQL into a standard format. Input SQL should be run
// through this before reducing so file size comparisons are useful.
func Pretty(s []byte) ([]byte, error) {
	stmts, err := parser.Parse(string(s))
	if err != nil {
		return nil, err
	}

	var sb bytes.Buffer
	for i, stmt := range stmts {
		if i > 0 {
			sb.WriteString("\n\n")
		}
		sb.WriteString(tree.Pretty(stmt.AST))
		sb.WriteString(";")
	}
	return sb.Bytes(), nil
}

var (
	// Mutations.

	removeLimit = walkSQL("remove LIMIT", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.Delete:
			if node.Limit != nil {
				if xf {
					node.Limit = nil
				}
				return 1
			}
		case *tree.Select:
			if node.Limit != nil {
				if xf {
					node.Limit = nil
				}
				return 1
			}
		case *tree.Update:
			if node.Limit != nil {
				if xf {
					node.Limit = nil
				}
				return 1
			}
		}
		return 0
	})
	removeOrderBy = walkSQL("remove ORDER BY", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.Delete:
			if node.OrderBy != nil {
				if xf {
					node.OrderBy = nil
				}
				return 1
			}
		case *tree.FuncExpr:
			if node.OrderBy != nil {
				if xf {
					node.OrderBy = nil
				}
				return 1
			}
		case *tree.Select:
			if node.OrderBy != nil {
				if xf {
					node.OrderBy = nil
				}
				return 1
			}
		case *tree.Update:
			if node.OrderBy != nil {
				if xf {
					node.OrderBy = nil
				}
				return 1
			}
		case *tree.WindowDef:
			if node.OrderBy != nil {
				if xf {
					node.OrderBy = nil
				}
				return 1
			}
		}
		return 0
	})
	removeOrderByExprs = walkSQL("remove ORDER BY exprs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.Delete:
			n := len(node.OrderBy)
			if xfi < len(node.OrderBy) {
				node.OrderBy = append(node.OrderBy[:xfi], node.OrderBy[xfi+1:]...)
			}
			return n
		case *tree.FuncExpr:
			n := len(node.OrderBy)
			if xfi < len(node.OrderBy) {
				node.OrderBy = append(node.OrderBy[:xfi], node.OrderBy[xfi+1:]...)
			}
			return n
		case *tree.Select:
			n := len(node.OrderBy)
			if xfi < len(node.OrderBy) {
				node.OrderBy = append(node.OrderBy[:xfi], node.OrderBy[xfi+1:]...)
			}
			return n
		case *tree.Update:
			n := len(node.OrderBy)
			if xfi < len(node.OrderBy) {
				node.OrderBy = append(node.OrderBy[:xfi], node.OrderBy[xfi+1:]...)
			}
			return n
		case *tree.WindowDef:
			n := len(node.OrderBy)
			if xfi < len(node.OrderBy) {
				node.OrderBy = append(node.OrderBy[:xfi], node.OrderBy[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeGroupBy = walkSQL("remove GROUP BY", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.SelectClause:
			if node.GroupBy != nil {
				if xf {
					node.GroupBy = nil
				}
				return 1
			}
		}
		return 0
	})
	removeGroupByExprs = walkSQL("remove GROUP BY exprs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.SelectClause:
			n := len(node.GroupBy)
			if xfi < len(node.GroupBy) {
				node.GroupBy = append(node.GroupBy[:xfi], node.GroupBy[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	nullExprs = walkSQL("nullify SELECT exprs", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.SelectClause:
			if len(node.Exprs) != 1 || node.Exprs[0].Expr != tree.DNull {
				if xf {
					node.Exprs = tree.SelectExprs{tree.SelectExpr{Expr: tree.DNull}}
				}
				return 1
			}
		}
		return 0
	})
	removeSelectExprs = walkSQL("remove SELECT exprs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.SelectClause:
			n := len(node.Exprs)
			if xfi < len(node.Exprs) {
				node.Exprs = append(node.Exprs[:xfi], node.Exprs[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeWithSelectExprs = walkSQL("remove WITH SELECT exprs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.CTE:
			if len(node.Name.Cols) < 1 {
				break
			}
			slct, ok := node.Stmt.(*tree.Select)
			if !ok {
				break
			}
			clause, ok := slct.Select.(*tree.SelectClause)
			if !ok {
				break
			}
			n := len(clause.Exprs)
			if xfi < len(clause.Exprs) {
				node.Name.Cols = append(node.Name.Cols[:xfi], node.Name.Cols[xfi+1:]...)
				clause.Exprs = append(clause.Exprs[:xfi], clause.Exprs[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeValuesCols = walkSQL("remove VALUES cols", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.AliasedTableExpr:
			subq, ok := node.Expr.(*tree.Subquery)
			if !ok {
				break
			}
			values, ok := skipParenSelect(subq.Select).(*tree.ValuesClause)
			if !ok {
				break
			}
			if len(values.Rows) < 1 {
				break
			}
			n := len(values.Rows[0])
			if xfi < n {
				removeValuesCol(values, xfi)
				// Remove the VALUES alias.
				if len(node.As.Cols) > xfi {
					node.As.Cols = append(node.As.Cols[:xfi], node.As.Cols[xfi+1:]...)
				}
			}
			return n
		case *tree.CTE:
			slct, ok := node.Stmt.(*tree.Select)
			if !ok {
				break
			}
			clause, ok := slct.Select.(*tree.SelectClause)
			if !ok {
				break
			}
			if len(clause.From.Tables) != 1 {
				break
			}
			ate, ok := clause.From.Tables[0].(*tree.AliasedTableExpr)
			if !ok {
				break
			}
			subq, ok := ate.Expr.(*tree.Subquery)
			if !ok {
				break
			}
			values, ok := skipParenSelect(subq.Select).(*tree.ValuesClause)
			if !ok {
				break
			}
			if len(values.Rows) < 1 {
				break
			}
			n := len(values.Rows[0])
			if xfi < n {
				removeValuesCol(values, xfi)
				// Remove the WITH alias.
				if len(node.Name.Cols) > xfi {
					node.Name.Cols = append(node.Name.Cols[:xfi], node.Name.Cols[xfi+1:]...)
				}
				// Remove the VALUES alias.
				if len(ate.As.Cols) > xfi {
					ate.As.Cols = append(ate.As.Cols[:xfi], ate.As.Cols[xfi+1:]...)
				}
			}
			return n
		case *tree.ValuesClause:
			if len(node.Rows) < 1 {
				break
			}
			n := len(node.Rows[0])
			if xfi < n {
				removeValuesCol(node, xfi)
			}
			return n
		}
		return 0
	})
	removeSelectAsExprs = walkSQL("remove SELECT AS exprs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.AliasedTableExpr:
			if len(node.As.Cols) < 1 {
				break
			}
			subq, ok := node.Expr.(*tree.Subquery)
			if !ok {
				break
			}
			clause, ok := skipParenSelect(subq.Select).(*tree.SelectClause)
			if !ok {
				break
			}
			n := len(clause.Exprs)
			if xfi < len(clause.Exprs) {
				node.As.Cols = append(node.As.Cols[:xfi], node.As.Cols[xfi+1:]...)
				clause.Exprs = append(clause.Exprs[:xfi], clause.Exprs[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeIndexFlags = walkSQL("remove index flags", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.AliasedTableExpr:
			if node.IndexFlags != nil {
				if xf {
					node.IndexFlags = nil
				}
				return 1
			}
		}
		return 0
	})
	removeWith = walkSQL("remove WITH", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.Delete:
			if node.With != nil {
				if xf {
					node.With = nil
				}
				return 1
			}
		case *tree.Insert:
			if node.With != nil {
				if xf {
					node.With = nil
				}
				return 1
			}
		case *tree.Select:
			if node.With != nil {
				if xf {
					node.With = nil
				}
				return 1
			}
		case *tree.Update:
			if node.With != nil {
				if xf {
					node.With = nil
				}
				return 1
			}
		}
		return 0
	})
	removeCreateDefs = walkSQL("remove CREATE defs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.CreateTable:
			n := len(node.Defs)
			if xfi < len(node.Defs) {
				node.Defs = append(node.Defs[:xfi], node.Defs[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeComputedColumn = walkSQL("remove computed column", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.ColumnTableDef:
			if node.Computed.Computed {
				if xf {
					node.Computed.Computed = false
					node.Computed.Expr = nil
					node.Computed.Virtual = false
				}
				return 1
			}
		}
		return 0
	})
	removeCreateNullDefs = walkSQL("remove CREATE NULL defs", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.ColumnTableDef:
			if node.Nullable.Nullability != tree.SilentNull {
				if xf {
					node.Nullable.Nullability = tree.SilentNull
				}
				return 1
			}
		}
		return 0
	})
	removeIndexCols = walkSQL("remove INDEX cols", func(xfi int, node interface{}) int {
		removeCol := func(idx *tree.IndexTableDef) int {
			n := len(idx.Columns)
			if xfi < len(idx.Columns) {
				idx.Columns = append(idx.Columns[:xfi], idx.Columns[xfi+1:]...)
			}
			return n
		}
		switch node := node.(type) {
		case *tree.IndexTableDef:
			return removeCol(node)
		case *tree.UniqueConstraintTableDef:
			return removeCol(&node.IndexTableDef)
		}
		return 0
	})
	removeIndexPredicate = walkSQL("remove index predicate", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.IndexTableDef:
			if node.Predicate != nil {
				if xf {
					node.Predicate = nil
				}
				return 1
			}
		}
		return 0
	})
	removeWindowPartitions = walkSQL("remove WINDOW partitions", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.WindowDef:
			n := len(node.Partitions)
			if xfi < len(node.Partitions) {
				node.Partitions = append(node.Partitions[:xfi], node.Partitions[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeValuesRows = walkSQL("remove VALUES rows", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.ValuesClause:
			n := len(node.Rows)
			if xfi < len(node.Rows) {
				node.Rows = append(node.Rows[:xfi], node.Rows[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeWithCTEs = walkSQL("remove WITH CTEs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.With:
			n := len(node.CTEList)
			if xfi < len(node.CTEList) {
				node.CTEList = append(node.CTEList[:xfi], node.CTEList[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeCTENames = walkSQL("remove CTE names", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.CTE:
			if len(node.Name.Cols) > 0 {
				if xf {
					node.Name.Cols = nil
				}
				return 1
			}
		}
		return 0
	})
	removeFroms = walkSQL("remove FROMs", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.SelectClause:
			n := len(node.From.Tables)
			if xfi < len(node.From.Tables) {
				node.From.Tables = append(node.From.Tables[:xfi], node.From.Tables[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeJoins = walkSQL("remove JOINs", func(xfi int, node interface{}) int {
		// Remove JOINs. Replace them with either the left or right
		// side based on if xfi is even or odd.
		switch node := node.(type) {
		case *tree.SelectClause:
			idx := xfi / 2
			n := 0
			for i, t := range node.From.Tables {
				switch t := t.(type) {
				case *tree.JoinTableExpr:
					if n == idx {
						if xfi%2 == 0 {
							node.From.Tables[i] = t.Left
						} else {
							node.From.Tables[i] = t.Right
						}
					}
					n += 2
				}
			}
			return n
		}
		return 0
	})
	simplifyVal = walkSQL("simplify vals", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.StrVal:
			if node.RawString() != "" {
				if xf {
					*node = *tree.NewStrVal("")
				}
				return 1
			}
		case *tree.NumVal:
			if node.OrigString() != "0" {
				if xf {
					*node = *tree.NewNumVal(constant.MakeInt64(0), "0", false /* negative */)
				}
				return 1
			}
		}
		return 0
	})
	removeWhere = walkSQL("remove WHERE", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.SelectClause:
			if node.Where != nil {
				if xf {
					node.Where = nil
				}
				return 1
			}
		}
		return 0
	})
	removeHaving = walkSQL("remove HAVING", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.SelectClause:
			if node.Having != nil {
				if xf {
					node.Having = nil
				}
				return 1
			}
		}
		return 0
	})
	removeDistinct = walkSQL("remove DISTINCT", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.SelectClause:
			if node.Distinct {
				if xf {
					node.Distinct = false
				}
				return 1
			}
		}
		return 0
	})
	unparenthesize = walkSQL("unparenthesize", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case tree.Exprs:
			n := 0
			for i, x := range node {
				if x, ok := x.(*tree.ParenExpr); ok {
					if n == xfi {
						node[i] = x.Expr
					}
					n++
				}
			}
			return n
		}
		return 0
	})
	nullifyFuncArgs = walkSQL("nullify function args", func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.FuncExpr:
			n := 0
			for i, x := range node.Exprs {
				if x != tree.DNull {
					if n == xfi {
						node.Exprs[i] = tree.DNull
					}
					n++
				}
			}
			return n
		}
		return 0
	})
	simplifyOnCond = walkSQL("simplify ON conditions", func(xfi int, node interface{}) int {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.OnJoinCond:
			if node.Expr != tree.DBoolTrue {
				if xf {
					node.Expr = tree.DBoolTrue
				}
				return 1
			}
		}
		return 0
	})

	// Replacements.

	removeStatement = replaceTopStatement("remove statements", func(xfi int, node interface{}) (int, tree.NodeFormatter) {
		xf := xfi == 0
		if _, ok := node.(tree.Statement); ok {
			if xf {
				return 1, emptyStatement{}
			}
			return 1, nil
		}
		return 0, nil
	})
	replaceStmt = replaceStatement("replace statements", func(xfi int, node interface{}) (int, tree.NodeFormatter) {
		xf := xfi == 0
		switch node := node.(type) {
		case *tree.ParenSelect:
			if xf {
				return 1, node.Select
			}
			return 1, nil
		case *tree.Subquery:
			if xf {
				return 1, node.Select
			}
			return 1, nil
		case *tree.With:
			n := len(node.CTEList)
			if xfi < len(node.CTEList) {
				return n, node.CTEList[xfi].Stmt
			}
			return n, nil
		}
		return 0, nil
	})

	// Regexes.

	removeCastsRE = regexp.MustCompile(`:::?[a-zA-Z0-9]+`)
	removeCasts   = reduce.MakeIntPass("remove casts", func(s string, i int) (string, bool, error) {
		out := removeCastsRE.ReplaceAllStringFunc(s, func(found string) string {
			i--
			if i == -1 {
				return ""
			}
			return found
		})
		return out, i < 0, nil
	})
	removeAliasesRE = regexp.MustCompile(`\sAS\s+\w+`)
	removeAliases   = reduce.MakeIntPass("remove aliases", func(s string, i int) (string, bool, error) {
		out := removeAliasesRE.ReplaceAllStringFunc(s, func(found string) string {
			i--
			if i == -1 {
				return ""
			}
			return found
		})
		return out, i < 0, nil
	})
	removeDBSchemaRE = regexp.MustCompile(`\w+\.\w+\.`)
	removeDBSchema   = reduce.MakeIntPass("remove DB schema", func(s string, i int) (string, bool, error) {
		// Remove the database and schema from "default.public.xxx".
		out := removeDBSchemaRE.ReplaceAllStringFunc(s, func(found string) string {
			i--
			if i == -1 {
				return ""
			}
			return found
		})
		return out, i < 0, nil
	})
)

func skipParenSelect(stmt tree.SelectStatement) tree.SelectStatement {
	for {
		ps, ok := stmt.(*tree.ParenSelect)
		if !ok {
			return stmt
		}
		stmt = ps.Select.Select
	}

}

func removeValuesCol(values *tree.ValuesClause, col int) {
	for i, row := range values.Rows {
		values.Rows[i] = append(row[:col], row[col+1:]...)
	}
}

type emptyStatement struct{}

func (e emptyStatement) Format(*tree.FmtCtx) {}
