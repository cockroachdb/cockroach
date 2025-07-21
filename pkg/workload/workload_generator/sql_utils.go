// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package workload_generator

import (
	"bufio"
	"fmt"
	"go/constant"
	"math/rand"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treecmp"
	"github.com/cockroachdb/errors"
)

const (
	// These are the columns required in the statement statistics tsv.
	applicationName  = "application_name"
	txnFingerprintID = "txn_fingerprint_id"
	keyColumnName    = "key"

	barePlaceholderPattern = `\b_\b|\b__more__\b`
	forceErrorPattern      = `\bcrdb_internal\.force_error\s*\(\s*_\s*,\s*_\s*\)`

	minSelectLimit = 1   // minimum value for LIMIT _ in SELECT statements
	maxSelectLimit = 100 // maximum value for LIMIT _ in SELECT statements

	// Minimum and maximum number of repetitions for IN operator placeholders.
	minINRepetitions = 1
	maxINRepetitions = 4

	// These constants are used to clean up crdb_internal.force_error(_, _) occurrences.
	forceErrorCode    = "'XX000'"
	forceErrorMessage = "'simulated error'"
)

var (
	// barePlaceholderRe is a regex that matches a single underscore or the
	// string "__more__" as a whole word, i.e. not part of another identifier.
	barePlaceholderRe = regexp.MustCompile(barePlaceholderPattern)
	// forceErrorRe is a regex that matches the crdb_internal.force_error(_, _)
	forceErrorRe = regexp.MustCompile(forceErrorPattern)
)

// extractTableName returns the unaliased table name for a
// simple TableExpr (either a bare *tree.TableName or an *tree.AliasedTableExpr).
// If the expr is something else (JOIN, subquery, etc.) it returns "".
func extractTableName(tbl tree.TableExpr) string {
	switch t := tbl.(type) {
	case *tree.TableName:
		return t.String() // exactly how it prints in SQL, with quotes if needed
	case *tree.AliasedTableExpr:
		if tn, ok := t.Expr.(*tree.TableName); ok {
			return tn.String()
		}
	}
	return "" // unknown or a subquery/join
}

// VisitPre handles any expr type of nodes with _ or __more__ inside them
func (v *placeholderRewriter) VisitPre(expr tree.Expr) (bool, tree.Expr) {

	if ce, ok := expr.(*tree.CaseExpr); ok {
		switch key := ce.Expr.(type) {
		// ── scalar CASE: CASE col WHEN _ THEN _ …
		case *tree.UnresolvedName:
			rewriteCaseExpr(
				ce,
				reconstructName(key), // e.g. "c_credit"
				v.schemas,
				"", //return type is unknown
			)
		// ── tuple CASE: CASE (col1,col2) WHEN … THEN …
		case *tree.Tuple:
			// rewriteCaseExpr’s tuple‐branch ignores the target string.
			// It uses the Tuple directly to regenerate each WHEN/THEN.
			rewriteCaseExpr(
				ce,
				"", // unused for tuple‐mode
				v.schemas,
				"", //return type is unknown
			)
		}

		// All arms of the When Then Else clause are handled by rewriteCaseExpr.
		// Do not need to recurse into the individual arms of the clause further.
		return false, ce
	}

	if co, ok := expr.(*tree.CoalesceExpr); ok && strings.EqualFold(co.Name, "IFNULL") {
		// Not recursing into the old IFNULL; swapping it out now.
		return false, handleIfNull(co, v.schemas, v.tableName)
	}

	//Handling BETWEEN … AND …
	if rc, ok := expr.(*tree.RangeCond); ok {
		if newExpr := handleRangeCondition(rc, v.schemas, v.tableName); newExpr != nil {
			return false, newExpr
		}
	}
	// If it is also not a comparison, then we just go further down the ast to look for a node that we handle.
	cmp, ok := expr.(*tree.ComparisonExpr)
	if !ok {
		return true, expr
	}
	// Handling IN operator.
	if cmp.Operator.Symbol == treecmp.In {
		if newTuple := handleInOperator(cmp, v.schemas, v.tableName); newTuple != nil {
			cmp.Right = newTuple
			// Not recursing further into the old RHS
			return false, cmp
		}
	}
	// Handling tuple comparisons.
	if lt, lok := cmp.Left.(*tree.Tuple); lok {
		if rt, rok := cmp.Right.(*tree.Tuple); rok {
			if newRt := handleTupleComparison(lt, rt, v.schemas, v.tableName); newRt != nil {
				cmp.Right = newRt
				return false, cmp
			}
		}
	}

	// Handling simple one-column comparisons.
	b, t, done := handleComparisonOperator(cmp, v.schemas, v.tableName)
	if done {
		return b, t
	}

	return true, expr
}

// handleIfNull is for rewriting IFNULL(a, b) or IFNULL(func(a),b) where b is a placeholder.
func handleIfNull(
	co *tree.CoalesceExpr, allSchemas map[string]*TableSchema, tableName string,
) tree.Expr {
	// It should be only IFNULL(a, b).
	if len(co.Exprs) != 2 {
		return co
	}

	// Second arg must be a placeholder.
	rhs, ok := co.Exprs[1].(*tree.UnresolvedName)
	if !ok || reconstructName(rhs) != "_" {
		return co
	}

	// Trying to extract the “key” column from the first arg.
	// It should be either a bare column, or any single‐arg FuncExpr(col).
	var keyCol string
	switch first := co.Exprs[0].(type) {
	case *tree.UnresolvedName:
		keyCol = reconstructName(first)
	case *tree.FuncExpr:
		if len(first.Exprs) == 1 {
			if col, ok := first.Exprs[0].(*tree.UnresolvedName); ok {
				keyCol = reconstructName(col)
			}
		}
	}

	if keyCol == "" {
		return co
	}

	// The _ is now tagged with the placeholder made using the keyCol.
	parts := getFieldCol(keyCol, "WHERE", allSchemas, tableName)
	rhs.NumParts = len(parts)
	copy(rhs.Parts[:], parts)
	return co
}

// handleRangeCondition rewrites any RangeCond (a BETWEEN b AND c or NOT BETWEEN)
// where b and/or c embed "_" or "__more__" placeholders. Supports both single-col
// and tuple-col BETWEEN.
func handleRangeCondition(
	rc *tree.RangeCond, allSchemas map[string]*TableSchema, tableName string,
) tree.Expr {
	// Single-column: col1 BETWEEN a AND b.
	if colUn, ok := rc.Left.(*tree.UnresolvedName); ok {
		return rewriteSingleRange(rc, colUn, allSchemas, tableName)
	}

	// Tuple-column: (col1,col2) BETWEEN (a,b) AND (c,d).
	if tpl, ok := rc.Left.(*tree.Tuple); ok {
		return rewriteTupleRange(rc, tpl, allSchemas, tableName)
	}

	return nil
}

// rewriteSingleRange handles the single-column BETWEEN case.
func rewriteSingleRange(
	rc *tree.RangeCond,
	colUn *tree.UnresolvedName,
	allSchemas map[string]*TableSchema,
	tableName string,
) tree.Expr {
	rc.From = rewriteRangeBound(rc.From, colUn, allSchemas, tableName)
	rc.To = rewriteRangeBound(rc.To, colUn, allSchemas, tableName)
	return rc
}

// rewriteRangeBound rewrites a single bound expression, replacing "_" or "__more__"
// with a tagged placeholder for the given column.
func rewriteRangeBound(
	expr tree.Expr, colUn *tree.UnresolvedName, allSchemas map[string]*TableSchema, tableName string,
) tree.Expr {
	switch t := expr.(type) {
	case *tree.UnresolvedName:
		// It should be a direct "_" or "__more__" to get replaced.
		if name := reconstructName(t); isBarePlaceholder(name) {
			colName := reconstructName(colUn)
			parts := getFieldCol(colName, "WHERE", allSchemas, tableName)
			t.NumParts = len(parts)
			copy(t.Parts[:], parts)
		}
		return t

	case *tree.ParenExpr:
		// We call the same function on the inner expression.
		t.Expr = rewriteRangeBound(t.Expr, colUn, allSchemas, tableName)
		return t

	case *tree.BinaryExpr:
		// Recursing into both sides of any arithmetic.
		left := rewriteRangeBound(t.Left, colUn, allSchemas, tableName)
		right := rewriteRangeBound(t.Right, colUn, allSchemas, tableName)

		// If both sides turned into placeholders, collapse into one.
		// Eg: col1 between (_ - _) and (_ + _). This eventually gets converted to col1 between ($1 - $2) and ($3 + $4).
		// This throws an error because sql compiler cannot figure out type of the expression inside parenthesis.
		if _, lok := left.(*tree.UnresolvedName); lok {
			if _, rok := right.(*tree.UnresolvedName); rok {
				colName := reconstructName(colUn)
				parts := getFieldCol(colName, "WHERE", allSchemas, tableName)
				var np tree.NameParts
				copy(np[:], parts)
				return &tree.UnresolvedName{
					NumParts: len(parts),
					Parts:    np,
				}
			}
		}
		return t

	default:
		return t
	}
}

// rewriteTupleRange handles the tuple-column BETWEEN case.
func rewriteTupleRange(
	rc *tree.RangeCond, keyTpl *tree.Tuple, allSchemas map[string]*TableSchema, tableName string,
) tree.Expr {
	// It must have tuple bounds.
	fromTpl, fromOk := rc.From.(*tree.Tuple)
	toTpl, toOk := rc.To.(*tree.Tuple)
	if !fromOk || !toOk {
		return nil
	}

	// Verifying both bounds are purely "_" or "__more__".
	for _, bound := range []*tree.Tuple{fromTpl, toTpl} {
		for _, e := range bound.Exprs {
			u, ok := e.(*tree.UnresolvedName)
			colName := reconstructName(u)
			if !ok || (colName != "_" && colName != "__more__") {
				return nil
			}
		}
	}

	// Extracting key columns.
	cols := make([]*tree.UnresolvedName, len(keyTpl.Exprs))
	for i, e := range keyTpl.Exprs {
		cols[i] = e.(*tree.UnresolvedName)
	}

	// A new placeholder tuple is built for each bound.
	build := func() *tree.Tuple {
		out := &tree.Tuple{Exprs: make(tree.Exprs, len(cols))}
		for j, colNode := range cols {
			colName := reconstructName(colNode)
			parts := getFieldCol(colName, "WHERE", allSchemas, tableName)
			var np tree.NameParts
			copy(np[:], parts)
			out.Exprs[j] = &tree.UnresolvedName{
				NumParts: len(parts),
				Parts:    np,
			}
		}
		return out
	}

	rc.From = build()
	rc.To = build()
	return rc
}

// handleComparisonOperator is for rewriting simple comparison expressions: col = _ or col > __more__ etc.
func handleComparisonOperator(
	cmp *tree.ComparisonExpr, allSchemas map[string]*TableSchema, tableName string,
) (bool, tree.Expr, bool) {
	lhs, lok := cmp.Left.(*tree.UnresolvedName)
	rhs, rok := cmp.Right.(*tree.UnresolvedName)
	if lok && rok {
		rhsName := reconstructName(rhs)
		if isBarePlaceholder(rhsName) {
			parts := getFieldCol(lhs.Parts[0], "WHERE", allSchemas, tableName)

			rhs.NumParts = len(parts)
			var np tree.NameParts
			copy(np[:], parts)
			rhs.Parts = np
			// Not recursing inside this placeholder node.
			return false, cmp, true
		}
	}
	return false, nil, false
}

func (v *placeholderRewriter) VisitPost(expr tree.Expr) tree.Expr {
	return expr
}

// handleTupleComparison rewrites ANY tuple-vs-tuple ComparisonExpr
// whose RHS is all "_" or "__more__", remapping it to a new tuple
// with exactly one placeholder per LHS column.
func handleTupleComparison(
	lt, rt *tree.Tuple, allSchemas map[string]*TableSchema, tableName string,
) *tree.Tuple {
	// 1) Verifying RHS is purely placeholders (any length).
	for _, e := range rt.Exprs {
		u, ok := e.(*tree.UnresolvedName)
		if !ok {
			return nil
		}
		if n := reconstructName(u); n != "_" && n != "__more__" {
			return nil
		}
	}

	// 2) Building a new RHS with one placeholder per LHS column.
	newExprs := make(tree.Exprs, len(lt.Exprs))
	for i, colExpr := range lt.Exprs {
		col, ok := colExpr.(*tree.UnresolvedName)
		if !ok {
			return nil // safety, though LHS should always be a tuple of names
		}
		// A tagged placeholder is built for this column.
		parts := getFieldCol(col.Parts[0], "WHERE", allSchemas, tableName) // preserves qualifiers, wraps part0
		var np tree.NameParts
		copy(np[:], parts)
		newExprs[i] = &tree.UnresolvedName{
			NumParts: len(parts),
			Parts:    np,
		}
	}

	// 3) Splicing it back into the existing rt node.
	rt.Exprs = newExprs
	return rt
}

// handleInOperator dispatches single-col vs multi-col IN (…) cases.
func handleInOperator(
	cmp *tree.ComparisonExpr, allSchemas map[string]*TableSchema, tableName string,
) *tree.Tuple {
	orig, ok := cmp.Right.(*tree.Tuple)
	if !ok {
		return nil
	}

	switch lhs := cmp.Left.(type) {
	case *tree.UnresolvedName:
		// Single-col IN: requires every orig.Exprs[i] be an UnresolvedName.
		return handleSingleColIn(lhs, orig, allSchemas, tableName)

	case *tree.Tuple:
		// Multi-col IN: requires every orig.Exprs[i] be a *tree.Tuple of UnresolvedNames.
		return handleMultiColIn(lhs, orig, allSchemas, tableName)

	default:
		return nil
	}
}

// handleSingleColIn rebuilds “col IN (_,__more__)” → col IN (p, p, …).
func handleSingleColIn(
	col *tree.UnresolvedName, orig *tree.Tuple, allSchemas map[string]*TableSchema, tableName string,
) *tree.Tuple {

	// 1) Verifying that each element is an UnresolvedName placeholder.
	for _, e := range orig.Exprs {
		u, ok := e.(*tree.UnresolvedName)
		if !ok || (reconstructName(u) != "_" && reconstructName(u) != "__more__") {
			return nil
		}
	}

	parts := getFieldCol(col.Parts[0], "WHERE", allSchemas, tableName) // wraps part0, preserves qualifiers
	// New flat list of placeholders is built.
	var newExprs tree.Exprs
	for _, e := range orig.Exprs {
		uOld := e.(*tree.UnresolvedName)
		name := reconstructName(uOld)

		// 1) One copy for every original placeholder.
		var np tree.NameParts
		copy(np[:], parts)
		newExprs = append(newExprs, &tree.UnresolvedName{
			NumParts: len(parts),
			Parts:    np,
		})

		// 2) If it was "__more__", 1–3 extra copies are added.
		if name == "__more__" {
			extra := rand.Intn(maxINRepetitions) + minINRepetitions
			for i := 0; i < extra; i++ {
				var np2 tree.NameParts
				copy(np2[:], parts)
				newExprs = append(newExprs, &tree.UnresolvedName{
					NumParts: len(parts),
					Parts:    np2,
				})
			}
		}
	}
	return &tree.Tuple{Exprs: newExprs}
}

// handleMultiColIn rebuilds “(a,b,…) IN ((_,__more__), __more__)” →
// (a,b,…) IN ((p_a,p_b,…), (p_a,p_b,…), …).
func handleMultiColIn(
	lhs *tree.Tuple, orig *tree.Tuple, allSchemas map[string]*TableSchema, tableName string,
) *tree.Tuple {
	// 1) Must have at least one tuple‐row.
	if len(orig.Exprs) == 0 {
		return nil
	}
	// 2) First element must be a tuple of placeholders.
	firstRow, ok := orig.Exprs[0].(*tree.Tuple)
	if !ok {
		return nil
	}
	for _, e := range firstRow.Exprs {
		u, ok := e.(*tree.UnresolvedName)
		if !ok {
			return nil
		}
		name := reconstructName(u)
		if name != "_" && name != "__more__" {
			return nil
		}
	}
	// 3) Any further elements must be single "__more__" markers.
	for i := 1; i < len(orig.Exprs); i++ {
		u, ok := orig.Exprs[i].(*tree.UnresolvedName)
		if !ok || reconstructName(u) != "__more__" {
			return nil
		}
	}

	// 4) The LHS columns are extracted.
	cols := make([]*tree.UnresolvedName, len(lhs.Exprs))
	for i, e := range lhs.Exprs {
		cols[i] = e.(*tree.UnresolvedName)
	}

	// 5) Deciding how many rows to generate.
	rowCount := rand.Intn(maxINRepetitions) + minINRepetitions

	// 6) The new outer tuple is built.
	rows := make(tree.Exprs, rowCount)
	for i := 0; i < rowCount; i++ {
		tpl := &tree.Tuple{Exprs: make(tree.Exprs, len(cols))}
		for j, col := range cols {
			parts := getFieldCol(col.Parts[0], "WHERE", allSchemas, tableName)
			var np tree.NameParts
			copy(np[:], parts)
			tpl.Exprs[j] = &tree.UnresolvedName{
				NumParts: len(parts),
				Parts:    np,
			}
		}
		rows[i] = tpl
	}
	return &tree.Tuple{Exprs: rows}
}

// reconstructName joins the first NumParts entries of an UnresolvedName.
func reconstructName(u *tree.UnresolvedName) string {
	parts := u.Parts[:u.NumParts]
	clean := parts[:0]
	for _, p := range parts {
		if p != "" {
			clean = append(clean, p)
		}
	}
	return strings.Join(clean, ".")
}

// getFieldCol returns a string that can be used to make UnresolvedName type of node.
// The string basically consists of col related schema information, the sql clause(WHERE , INSERT, or UPDATE) and the table name.
func getFieldCol(
	col string, clause string, allSchemas map[string]*TableSchema, tableName string,
) []string {
	numParts := 1
	parts := make([]string, numParts)
	var placeholder string
	for _, schema := range allSchemas {
		matched := false
		for _, column := range schema.Columns {
			if column.Name == col {
				placeholder = column.String()
				placeholder = placeholder[1 : len(placeholder)-1]
				matched = true
				break
			}
		}
		if matched {
			break
		}
	}
	parts[0] = fmt.Sprintf(":-:|'%s','%s','%s'|:-:", placeholder, clause, tableName)
	return parts
}

// rewriteCaseExpr tags every WHEN … THEN … and the final ELSE if it’s
// an UnresolvedName placeholder. You may already have a
// placeholderRewriter for WHERE/IN/BETWEEN—this is just the SET‐specific
// pass so you know the targetCol.
func rewriteCaseExpr(
	c *tree.CaseExpr, targetCol string, allSchemas map[string]*TableSchema, tableName string,
) {
	// Detecting as tuple‐CASE if c.Expr is a Tuple of key columns.
	if keyTpl, ok := c.Expr.(*tree.Tuple); ok {
		// Gather the key column names.
		var keyCols []string
		for _, ke := range keyTpl.Exprs {
			if un, ok := ke.(*tree.UnresolvedName); ok {
				keyCols = append(keyCols, reconstructName(un))
			}
		}
		// For each WHEN arm: both Cond (tuple) and Val are rewritten.
		for _, arm := range c.Whens {
			if _, ok := arm.Cond.(*tree.Tuple); ok {
				// Rebuild cond tuple with placeholders per keyCol.
				newCond := &tree.Tuple{Exprs: make(tree.Exprs, len(keyCols))}
				for i, col := range keyCols {
					parts := getFieldCol(col, "WHERE", allSchemas, tableName)
					var np tree.NameParts
					copy(np[:], parts)
					newCond.Exprs[i] = &tree.UnresolvedName{
						NumParts: len(parts),
						Parts:    np,
					}
				}
				arm.Cond = newCond
			}
			// THEN arm.Val → placeholder for targetCol
			if u, ok := arm.Val.(*tree.UnresolvedName); ok {
				populatePlaceholder(u, targetCol, allSchemas, tableName)
			}
		}
		// ELSE branch can be a placeholder or force_error; handle simple placeholder.
		if u, ok := c.Else.(*tree.UnresolvedName); ok {
			populatePlaceholder(u, targetCol, allSchemas, tableName)
		}
		return
	}

	// ── scalar‐CASE fallback ─────────────────────────────────────
	// Finding the column on which the CASE depends on.
	condCol := targetCol
	if un, ok := c.Expr.(*tree.UnresolvedName); ok {
		condCol = reconstructName(un)
	}
	for _, arm := range c.Whens {
		// Handling the WHEN arm.
		if u, ok := arm.Cond.(*tree.UnresolvedName); ok {
			populatePlaceholder(u, condCol, allSchemas, tableName)
		}
		// Handling the THEN arm.
		if u, ok := arm.Val.(*tree.UnresolvedName); ok {
			populatePlaceholder(u, targetCol, allSchemas, tableName)
		}
	}
	// Handling the ELSE arm.
	if u, ok := c.Else.(*tree.UnresolvedName); ok {
		populatePlaceholder(u, targetCol, allSchemas, tableName)
	}
}

// populatePlaceholder checks if u is a bare "_" or "__more__" placeholder.
// If so, it uses getFieldCol to build the parts for targetCol in the given clause
// and copies them into u.
func populatePlaceholder(
	u *tree.UnresolvedName, targetCol string, allSchemas map[string]*TableSchema, tableName string,
) {
	if name := reconstructName(u); isBarePlaceholder(name) {
		parts := getFieldCol(targetCol, "INSERT", allSchemas, tableName)
		u.NumParts = len(parts)
		copy(u.Parts[:], parts)
	}
}

// isBarePlaceholder checks if the name is a bare placeholder: "_" or "__more__".
func isBarePlaceholder(name string) bool {
	return name == "_" || name == "__more__"
}

// handleSelectLimit replaces the placeholder in LIMIT _ with a random integer between 1 and 100.
func handleSelectLimit(sel *tree.Select) {
	if sel.Limit == nil || sel.Limit.Count == nil {
		return
	}
	if u, ok := sel.Limit.Count.(*tree.UnresolvedName); ok && reconstructName(u) == "_" {
		// A random number between minSelectLimit and maxSelectLimit is picked. The range is 1-100 currently.
		n := rand.Intn(maxSelectLimit-minSelectLimit) + minSelectLimit
		lit := fmt.Sprintf("%d", n)
		// A constant.Value is built and a NumVal node is swapped in.
		sel.Limit.Count = tree.NewNumVal(
			constant.MakeInt64(int64(n)), // the constant.Value
			lit,                          // original string
			false,                        // not negative
		)
	}
}

// handleInsert focuses on INSERT … VALUES type of sqls.
func handleInsert(ins *tree.Insert, allSchemas map[string]*TableSchema) {
	// 1) The table name is extracted.
	fmtCtx := tree.NewFmtCtx(tree.FmtSimple)
	ins.Table.Format(fmtCtx)
	tableName := fmtCtx.CloseAndGetString()

	// 2) Only raw VALUES clauses are rewritten.
	vals, ok := ins.Rows.Select.(*tree.ValuesClause)
	if !ok {
		return
	}

	// 3) Gathering columns and rebuilding the placeholder rows.
	cols := collectInsertCols(ins, allSchemas, tableName)
	ins.Rows.Select.(*tree.ValuesClause).Rows = buildInsertPlaceholderRows(
		cols, len(vals.Rows), allSchemas, tableName,
	)
}

// collectInsertCols returns the ordered list of column names for this INSERT.
// If ins.Columns are non-empty, use that; otherwise look up the TableSchema.
func collectInsertCols(
	ins *tree.Insert, allSchemas map[string]*TableSchema, tableName string,
) []string {
	// If column names are explicitly mentioned then those are extracted.
	if len(ins.Columns) > 0 {
		cols := make([]string, len(ins.Columns))
		for i, n := range ins.Columns {
			cols[i] = string(n)
		}
		return cols
	}

	// If no explicit columns, looking up the table schema.
	var tblKey string
	switch t := ins.Table.(type) {
	case *tree.TableName:
		tblKey = strings.Trim(t.String(), `"`)
	case *tree.AliasedTableExpr:
		if tn, ok := t.Expr.(*tree.TableName); ok {
			tblKey = strings.Trim(tn.String(), `"`)
		}
	}
	if ts, ok := allSchemas[tblKey]; ok {
		cols := make([]string, 0, len(ts.Columns))
		for col := range ts.Columns {
			cols = append(cols, col)
		}
		return cols
	}

	// fallback: no schema found → empty.
	return nil
}

// buildPlaceholderRows creates exactly rowCount placeholder tuples,
// one per original VALUES row.
// Ex: INSERT INTO tbl(col1,col2) VALUES ( _ ,__more__ ) , (__, __more__).. k times becomes INSERT INTO tbl(col1,col2) VALUES (:-:col1:-:,:-:col2:-:) , (:-:col1:-:,:-:col2:-:)).. k times
func buildInsertPlaceholderRows(
	cols []string, rowCount int, allSchemas map[string]*TableSchema, tableName string,
) []tree.Exprs {
	newRows := make([]tree.Exprs, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		base := make(tree.Exprs, len(cols))
		for j, col := range cols {
			parts := getFieldCol(col, "INSERT", allSchemas, tableName)
			var np tree.NameParts
			copy(np[:], parts)
			base[j] = &tree.UnresolvedName{
				NumParts: len(parts),
				Parts:    np,
			}
		}
		newRows = append(newRows, base)
	}
	return newRows
}

// handleUpdateSet orchestrates rewriting the placeholders in the SET clause of an UPDATE statement.
func handleUpdateSet(upd *tree.Update, allSchemas map[string]*TableSchema) {
	tableName := extractTableName(upd.Table)
	for _, setExpr := range upd.Exprs {
		if !setExpr.Tuple {
			// Single‐column SET: "col = …"
			targetCol := string(setExpr.Names[0])
			setExpr.Expr = rewriteSingleColSetRHS(setExpr.Expr, targetCol, allSchemas, tableName)
		} else {
			// Tuple‐form SET: "(a,b,…) = (…)"
			// A new RHS tuple, one placeholder per LHS column are built.
			setExpr.Expr = rewriteMultiColSet(setExpr, allSchemas, tableName)
		}
	}
}

// rewriteMultiColSet rewrites the multi-column SET clause of an UPDATE statement.
func rewriteMultiColSet(
	setExpr *tree.UpdateExpr, allSchemas map[string]*TableSchema, tableName string,
) *tree.Tuple {
	cols := setExpr.Names
	newTuple := &tree.Tuple{Exprs: make(tree.Exprs, len(cols))}
	for i, name := range cols {
		col := string(name)
		parts := getFieldCol(col, "UPDATE", allSchemas, tableName)
		var np tree.NameParts
		copy(np[:], parts)
		newTuple.Exprs[i] = &tree.UnresolvedName{
			NumParts: len(parts),
			Parts:    np,
		}
	}
	return newTuple
}

// rewriteSingleColSetRHS rewrites the RHS of a single-column SET clause.
func rewriteSingleColSetRHS(
	expr tree.Expr, targetCol string, allSchemas map[string]*TableSchema, tableName string,
) tree.Expr {
	switch t := expr.(type) {
	case *tree.UnresolvedName:
		// It has to be a bare placeholder "_" or "__more__".
		name := reconstructName(t)
		if isBarePlaceholder(name) {
			parts := getFieldCol(targetCol, "UPDATE", allSchemas, tableName)
			t.NumParts = len(parts)
			copy(t.Parts[:], parts)
		}

	case *tree.BinaryExpr:
		// e.g. col + _  or deeper nested expressions.
		t.Left = rewriteSingleColSetRHS(t.Left, targetCol, allSchemas, tableName)
		t.Right = rewriteSingleColSetRHS(t.Right, targetCol, allSchemas, tableName)

	case *tree.Tuple:
		// e.g. CASE tuple‐arm might embed a Tuple
		for i, elt := range t.Exprs {
			t.Exprs[i] = rewriteSingleColSetRHS(elt, targetCol, allSchemas, tableName)
		}

	case *tree.CaseExpr:
		// Both tuple‐CASE and scalar‐CASE are handled here.
		rewriteCaseExpr(t, targetCol, allSchemas, tableName)
	}
	return expr
}

// writeTransaction writes out each transaction block, bracketed by BEGIN/COMMIT.
func writeTransaction(
	txnOrder []string, txnMap map[string][]string, dbName string, sqlLocation string,
) error {
	// Opening the output files.
	if fi, err := os.Stat(sqlLocation); err != nil || !fi.IsDir() {
		return fmt.Errorf("%s is not a directory", sqlLocation)
	}

	outPathRead := filepath.Join(sqlLocation, dbName+"_read.sql")
	outPathWrite := filepath.Join(sqlLocation, dbName+"_write.sql")
	// A file for unhandled statements is created other than the read and write.
	// Statements which are still left with bare _ and __more__ placeholders after sql handling are written there.
	// This helps to identify which statements were not handled by the generator. It also lets the workload generator run smoothly without failing on unhandled statements.
	// If wanted, the transactions that end up in that file can be handled manually.
	outPathUnhandled := filepath.Join(sqlLocation, dbName+"_unhandled.sql")

	outReadFile, err := os.Create(outPathRead)
	if err != nil {
		return errors.Wrapf(err, "creating %s", outPathRead)
	}
	defer outReadFile.Close()

	outWriteFile, err := os.Create(outPathWrite)
	if err != nil {
		return errors.Wrapf(err, "creating %s", outPathWrite)
	}
	defer outWriteFile.Close()

	outUnhandledFile, err := os.Create(outPathUnhandled)
	if err != nil {
		return errors.Wrapf(err, "creating %s", outPathUnhandled)
	}
	defer outUnhandledFile.Close()

	for _, txnID := range txnOrder {
		stmts := txnMap[txnID]
		// 1) Scanning for any bare "_" or "__more__".
		hasPlaceholder := false
		for _, stmt := range stmts {
			if barePlaceholderRe.MatchString(stmt) {
				hasPlaceholder = true
				break
			}
		}
		// 2) Picking the target file.
		var outFile *os.File
		if hasPlaceholder {
			outFile = outUnhandledFile
		} else if isWriteTransaction(stmts) {
			outFile = outWriteFile
		} else {
			outFile = outReadFile
		}
		// 3) Writing the transaction block.
		if _, err := fmt.Fprintln(outFile, "-------Begin Transaction------"); err != nil {
			return errors.Wrapf(err, "writing transaction %s to %s", txnID, outFile.Name())
		}
		if _, err := fmt.Fprintln(outFile, "BEGIN;"); err != nil {
			return errors.Wrapf(err, "writing transaction %s to %s", txnID, outFile.Name())
		}
		for _, stmt := range stmts {
			if _, err := fmt.Fprintf(outFile, "%s;\n", stmt); err != nil {
				return errors.Wrapf(err, "writing transaction %s to %s", txnID, outFile.Name())
			}
		}
		if _, err := fmt.Fprintln(outFile, "COMMIT;"); err != nil {
			return errors.Wrapf(err, "writing transaction %s to %s", txnID, outFile.Name())
		}
		if _, err := fmt.Fprintln(outFile, "-------End Transaction-------"); err != nil {
			return errors.Wrapf(err, "writing transaction %s to %s", txnID, outFile.Name())
		}

	}
	return nil
}

// getColumnIndexes maps column names to their respective indexes.
func getColumnIndexes(
	scanner *bufio.Scanner, f *os.File, statsPath string,
) (map[string]int, error) {
	header := strings.Split(scanner.Text(), "\t")
	idx := make(map[string]int, len(header))
	for i, col := range header {
		idx[col] = i
	}
	// These are the required columns.
	for _, want := range []string{
		databaseName, applicationName,
		txnFingerprintID, keyColumnName,
	} {
		if _, ok := idx[want]; !ok {
			if err := f.Close(); err != nil {
				return nil, err
			}
			return nil, errors.Errorf("missing column %q in %s", want, statsPath)
		}
	}
	return idx, nil
}

// Returns true if any statement is INSERT/UPDATE/DELETE.
func isWriteTransaction(stmts []string) bool {
	for _, s := range stmts {
		up := strings.ToUpper(strings.TrimSpace(s))
		if strings.HasPrefix(up, "INSERT") ||
			strings.HasPrefix(up, "UPDATE") ||
			strings.HasPrefix(up, "DELETE") {
			return true
		}
	}
	return false
}
