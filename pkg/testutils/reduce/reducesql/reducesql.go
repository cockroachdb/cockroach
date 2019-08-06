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
	removeCTENames,
	removeWithSelectExprs,
	removeValuesCols,
	removeSelectAsExprs,
	removeValuesRows,
	removeLimit,
	removeOrderBy,
	removeOrderByExprs,
	removeGroupBy,
	removeGroupByExprs,
	nullExprs,
	nullifyFuncArgs,
	removeSelectExprs,
	removeCreateDefs,
	removeCreateNullDefs,
	removeIndexCols,
	removeWindowPartitions,
	removeCasts,
	removeAliases,
	removeDBSchema,
	removeFroms,
	removeWhere,
	removeHaving,
	removeDistinct,
	simplifyOnCond,
	simplifyVal,
	unparenthesize,
}

type sqlWalker struct {
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
func walkSQL(match func(transform int, node interface{}) (matched int)) reduce.Pass {
	w := sqlWalker{
		match: match,
	}
	return reduce.MakeIntPass(w.Transform)
}

// replaceStatement is like walkSQL, except if it returns a non-nil
// replacement, the top-level SQL statement is completely replaced with the
// return value.
func replaceStatement(
	replace func(transform int, node interface{}) (matched int, replacement tree.NodeFormatter),
) reduce.Pass {
	w := sqlWalker{
		replace: replace,
	}
	return reduce.MakeIntPass(w.Transform)
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
	var walk func(...interface{})
	walk = func(nodes ...interface{}) {
		for _, node := range nodes {
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

			switch node := node.(type) {
			case *tree.AliasedTableExpr:
				walk(node.Expr)
			case *tree.AnnotateTypeExpr:
				walk(node.Expr)
			case *tree.Array:
				walk(node.Exprs)
			case *tree.BinaryExpr:
				walk(node.Left, node.Right)
			case *tree.CastExpr:
				walk(node.Expr)
			case *tree.ColumnTableDef:
			case *tree.CreateTable:
				for _, def := range node.Defs {
					walk(def)
				}
			case *tree.CTE:
				walk(node.Stmt)
			case *tree.DBool:
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
			case *tree.JoinTableExpr:
				walk(node.Left, node.Right, node.Cond)
			case *tree.NumVal:
			case *tree.OnJoinCond:
				walk(node.Expr)
			case *tree.ParenExpr:
				walk(node.Expr)
			case *tree.ParenSelect:
				walk(node.Select)
			case *tree.Select:
				if node.With != nil {
					walk(node.With)
				}
				walk(node.Select)
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
			case tree.SelectExpr:
				walk(node.Expr)
			case tree.SelectExprs:
				for _, expr := range node {
					walk(expr)
				}
			case *tree.StrVal:
			case *tree.Subquery:
				walk(node.Select)
			case *tree.TableName:
			case *tree.UnaryExpr:
				walk(node.Expr)
			case *tree.UniqueConstraintTableDef:
			case *tree.UnionClause:
				walk(node.Left, node.Right)
			case *tree.UnresolvedName:
			case *tree.ValuesClause:
				for _, row := range node.Rows {
					walk(row)
				}
			case *tree.Where:
				walk(node.Expr)
			case *tree.WindowDef:
				walk(node.Partitions, node.Frame)
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

var (
	// Mutations.

	removeLimit = walkSQL(func(xfi int, node interface{}) int {
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
	removeOrderBy = walkSQL(func(xfi int, node interface{}) int {
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
	removeOrderByExprs = walkSQL(func(xfi int, node interface{}) int {
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
	removeGroupBy = walkSQL(func(xfi int, node interface{}) int {
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
	removeGroupByExprs = walkSQL(func(xfi int, node interface{}) int {
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
	nullExprs = walkSQL(func(xfi int, node interface{}) int {
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
	removeSelectExprs = walkSQL(func(xfi int, node interface{}) int {
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
	removeWithSelectExprs = walkSQL(func(xfi int, node interface{}) int {
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
	removeValuesCols = walkSQL(func(xfi int, node interface{}) int {
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
	removeSelectAsExprs = walkSQL(func(xfi int, node interface{}) int {
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
	removeWith = walkSQL(func(xfi int, node interface{}) int {
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
	removeCreateDefs = walkSQL(func(xfi int, node interface{}) int {
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
	removeCreateNullDefs = walkSQL(func(xfi int, node interface{}) int {
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
	removeIndexCols = walkSQL(func(xfi int, node interface{}) int {
		switch node := node.(type) {
		case *tree.IndexTableDef:
			n := len(node.Columns)
			if xfi < len(node.Columns) {
				node.Columns = append(node.Columns[:xfi], node.Columns[xfi+1:]...)
			}
			return n
		}
		return 0
	})
	removeWindowPartitions = walkSQL(func(xfi int, node interface{}) int {
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
	removeValuesRows = walkSQL(func(xfi int, node interface{}) int {
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
	removeWithCTEs = walkSQL(func(xfi int, node interface{}) int {
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
	removeCTENames = walkSQL(func(xfi int, node interface{}) int {
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
	removeFroms = walkSQL(func(xfi int, node interface{}) int {
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
	simplifyVal = walkSQL(func(xfi int, node interface{}) int {
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
	removeWhere = walkSQL(func(xfi int, node interface{}) int {
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
	removeHaving = walkSQL(func(xfi int, node interface{}) int {
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
	removeDistinct = walkSQL(func(xfi int, node interface{}) int {
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
	unparenthesize = walkSQL(func(xfi int, node interface{}) int {
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
	nullifyFuncArgs = walkSQL(func(xfi int, node interface{}) int {
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
	simplifyOnCond = walkSQL(func(xfi int, node interface{}) int {
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

	removeStatement = replaceStatement(func(xfi int, node interface{}) (int, tree.NodeFormatter) {
		xf := xfi == 0
		if _, ok := node.(tree.Statement); ok {
			if xf {
				return 1, emptyStatement{}
			}
			return 1, nil
		}
		return 0, nil
	})
	replaceStmt = replaceStatement(func(xfi int, node interface{}) (int, tree.NodeFormatter) {
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
	removeCasts   = reduce.MakeIntPass(func(s string, i int) (string, bool, error) {
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
	removeAliases   = reduce.MakeIntPass(func(s string, i int) (string, bool, error) {
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
	removeDBSchema   = reduce.MakeIntPass(func(s string, i int) (string, bool, error) {
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
