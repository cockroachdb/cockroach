// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in licenses/BSD-vitess.txt.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// This code was derived from https://github.com/youtube/vitess.

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree/treewindow"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// SelectStatement represents any SELECT statement.
type SelectStatement interface {
	Statement
	selectStatement()
}

func (*ParenSelect) selectStatement()         {}
func (*SelectClause) selectStatement()        {}
func (*UnionClause) selectStatement()         {}
func (*ValuesClause) selectStatement()        {}
func (*LiteralValuesClause) selectStatement() {}

// Select represents a SelectStatement with an ORDER and/or LIMIT.
type Select struct {
	With    *With
	Select  SelectStatement
	OrderBy OrderBy
	Limit   *Limit
	Locking LockingClause
}

// Format implements the NodeFormatter interface.
func (node *Select) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.With)
	ctx.FormatNode(node.Select)
	if len(node.OrderBy) > 0 {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.OrderBy)
	}
	if node.Limit != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(node.Limit)
	}
	ctx.FormatNode(&node.Locking)
}

// ParenSelect represents a parenthesized SELECT/UNION/VALUES statement.
type ParenSelect struct {
	Select *Select
}

// Format implements the NodeFormatter interface.
func (node *ParenSelect) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Select)
	ctx.WriteByte(')')
}

// SelectClause represents a SELECT statement.
type SelectClause struct {
	From        From
	DistinctOn  DistinctOn
	Exprs       SelectExprs
	GroupBy     GroupBy
	Window      Window
	Having      *Where
	Where       *Where
	Distinct    bool
	TableSelect bool
}

// Format implements the NodeFormatter interface.
func (node *SelectClause) Format(ctx *FmtCtx) {
	f := ctx.flags
	if f.HasFlags(FmtSummary) {
		ctx.WriteString("SELECT")
		if len(node.From.Tables) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.From)
		}
		return
	}
	if node.TableSelect {
		ctx.WriteString("TABLE ")
		ctx.FormatNode(node.From.Tables[0])
	} else {
		ctx.WriteString("SELECT ")
		if node.Distinct {
			if node.DistinctOn != nil {
				ctx.FormatNode(&node.DistinctOn)
				ctx.WriteByte(' ')
			} else {
				ctx.WriteString("DISTINCT ")
			}
		}
		ctx.FormatNode(&node.Exprs)
		if len(node.From.Tables) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.From)
		}
		if node.Where != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Where)
		}
		if len(node.GroupBy) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.GroupBy)
		}
		if node.Having != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Having)
		}
		if len(node.Window) > 0 {
			ctx.WriteByte(' ')
			ctx.FormatNode(&node.Window)
		}
	}
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

// Format implements the NodeFormatter interface.
func (node *SelectExprs) Format(ctx *FmtCtx) {
	for i := range *node {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*node)[i])
	}
}

// SelectExpr represents a SELECT expression.
type SelectExpr struct {
	Expr Expr
	As   UnrestrictedName
}

// NormalizeTopLevelVarName preemptively expands any UnresolvedName at
// the top level of the expression into a VarName. This is meant
// to catch stars so that sql.checkRenderStar() can see it prior to
// other expression transformations.
func (node *SelectExpr) NormalizeTopLevelVarName() error {
	if vBase, ok := node.Expr.(VarName); ok {
		v, err := vBase.NormalizeVarName()
		if err != nil {
			return err
		}
		node.Expr = v
	}
	return nil
}

// StarSelectExpr is a convenience function that represents an unqualified "*"
// in a select expression.
func StarSelectExpr() SelectExpr {
	return SelectExpr{Expr: StarExpr()}
}

// Format implements the NodeFormatter interface.
func (node *SelectExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Expr)
	if node.As != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&node.As)
	}
}

// AliasClause represents an alias, optionally with a column def list:
// "AS name", "AS name(col1, col2)", or "AS name(col1 INT, col2 STRING)".
// Note that the last form is only valid in the context of record-returning
// functions, which also require the last form to define their output types.
type AliasClause struct {
	Alias Name
	Cols  ColumnDefList
}

// Format implements the NodeFormatter interface.
func (f *AliasClause) Format(ctx *FmtCtx) {
	ctx.FormatNode(&f.Alias)
	if len(f.Cols) != 0 {
		// Format as "alias (col1, col2, ...)".
		ctx.WriteString(" (")
		ctx.FormatNode(&f.Cols)
		ctx.WriteByte(')')
	}
}

// ColumnDef represents a column definition in the context of a record type
// alias, like in select * from json_to_record(...) AS foo(a INT, b INT).
type ColumnDef struct {
	Name Name
	Type ResolvableTypeReference
}

// Format implements the NodeFormatter interface.
func (c *ColumnDef) Format(ctx *FmtCtx) {
	ctx.FormatNode(&c.Name)
	if c.Type != nil {
		ctx.WriteByte(' ')
		ctx.FormatTypeReference(c.Type)
	}
}

// ColumnDefList represents a list of ColumnDefs.
type ColumnDefList []ColumnDef

// Format implements the NodeFormatter interface.
func (c *ColumnDefList) Format(ctx *FmtCtx) {
	for i := range *c {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*c)[i])
	}
}

// AsOfClause represents an as of time.
type AsOfClause struct {
	Expr Expr
}

// Format implements the NodeFormatter interface.
func (a *AsOfClause) Format(ctx *FmtCtx) {
	ctx.WriteString("AS OF SYSTEM TIME ")
	ctx.FormatNode(a.Expr)
}

// From represents a FROM clause.
type From struct {
	Tables TableExprs
	AsOf   AsOfClause
}

// Format implements the NodeFormatter interface.
func (node *From) Format(ctx *FmtCtx) {
	ctx.WriteString("FROM ")
	ctx.FormatNode(&node.Tables)
	if node.AsOf.Expr != nil {
		ctx.WriteByte(' ')
		ctx.FormatNode(&node.AsOf)
	}
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

// Format implements the NodeFormatter interface.
func (node *TableExprs) Format(ctx *FmtCtx) {
	prefix := ""
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// TableExpr represents a table expression.
type TableExpr interface {
	NodeFormatter
	tableExpr()
	WalkTableExpr(Visitor) TableExpr
}

func (*AliasedTableExpr) tableExpr() {}
func (*ParenTableExpr) tableExpr()   {}
func (*JoinTableExpr) tableExpr()    {}
func (*RowsFromExpr) tableExpr()     {}
func (*Subquery) tableExpr()         {}
func (*StatementSource) tableExpr()  {}

// StatementSource encapsulates one of the other statements as a data source.
type StatementSource struct {
	Statement Statement
}

// Format implements the NodeFormatter interface.
func (node *StatementSource) Format(ctx *FmtCtx) {
	ctx.WriteByte('[')
	ctx.FormatNode(node.Statement)
	ctx.WriteByte(']')
}

// IndexID is a custom type for IndexDescriptor IDs.
type IndexID = catid.IndexID

// FamilyID is a custom type for column family ID.
type FamilyID = catid.FamilyID

// IndexFlags represents "@<index_name|index_id>" or "@{param[,param]}" where
// param is one of:
//   - FORCE_INDEX=<index_name|index_id>
//   - ASC / DESC
//   - NO_INDEX_JOIN
//   - NO_ZIGZAG_JOIN
//   - NO_FULL_SCAN
//   - AVOID_FULL_SCAN
//   - IGNORE_FOREIGN_KEYS
//   - FORCE_INVERTED_INDEX
//   - FORCE_ZIGZAG
//   - FORCE_ZIGZAG=<index_name|index_id>*
//   - FAMILY=[family_id]
//
// It is used optionally after a table name in SELECT statements.
type IndexFlags struct {
	Index   UnrestrictedName
	IndexID IndexID
	// Direction of the scan, if provided. Can only be set if
	// one of Index or IndexID is set.
	Direction Direction
	// NoIndexJoin cannot be specified together with an index.
	NoIndexJoin bool
	// NoZigzagJoin indicates we should not plan a zigzag join for this scan.
	NoZigzagJoin bool
	// NoFullScan indicates we should constrain this scan.
	NoFullScan bool
	// AvoidFullScan indicates we should constrain this scan if possible.
	AvoidFullScan bool
	// IgnoreForeignKeys disables optimizations based on outbound foreign key
	// references from this table. This is useful in particular for scrub queries
	// used to verify the consistency of foreign key relations.
	IgnoreForeignKeys bool
	// IgnoreUniqueWithoutIndexKeys disables optimizations based on unique without
	// index constraints.
	IgnoreUniqueWithoutIndexKeys bool
	// ForceInvertedIndex indicates that we should only plan scans over inverted
	// indexes.
	ForceInvertedIndex bool
	// Zigzag hinting fields are distinct:
	// ForceZigzag means we saw a TABLE@{FORCE_ZIGZAG}
	// ZigzagIndexes means we saw TABLE@{FORCE_ZIGZAG=name}
	// ZigzagIndexIDs means we saw TABLE@{FORCE_ZIGZAG=[ID]}
	// The only allowable combinations are when a valid id and name are combined.
	ForceZigzag    bool
	ZigzagIndexes  []UnrestrictedName
	ZigzagIndexIDs []IndexID

	// Restrict select to the specified column family.
	// Used by changefeed.
	FamilyID *FamilyID
}

// ForceIndex returns true if a forced index was specified, either using a name
// or an IndexID.
func (ih *IndexFlags) ForceIndex() bool {
	return ih.Index != "" || ih.IndexID != 0
}

// CombineWith combines two IndexFlags structures, returning an error if they
// conflict with one another.
func (ih *IndexFlags) CombineWith(other *IndexFlags) error {
	if ih.NoIndexJoin && other.NoIndexJoin {
		return errors.New("NO_INDEX_JOIN specified multiple times")
	}
	if ih.NoZigzagJoin && other.NoZigzagJoin {
		return errors.New("NO_ZIGZAG_JOIN specified multiple times")
	}
	if ih.NoFullScan && other.NoFullScan {
		return errors.New("NO_FULL_SCAN specified multiple times")
	}
	if ih.AvoidFullScan && other.AvoidFullScan {
		return errors.New("AVOID_FULL_SCAN specified multiple times")
	}
	if ih.IgnoreForeignKeys && other.IgnoreForeignKeys {
		return errors.New("IGNORE_FOREIGN_KEYS specified multiple times")
	}
	if ih.IgnoreUniqueWithoutIndexKeys && other.IgnoreUniqueWithoutIndexKeys {
		return errors.New("IGNORE_UNIQUE_WITHOUT_INDEX_KEYS specified multiple times")
	}
	if ih.ForceInvertedIndex && other.ForceInvertedIndex {
		return errors.New("FORCE_INVERTED_INDEX specified multiple times")
	}
	result := *ih
	result.NoIndexJoin = ih.NoIndexJoin || other.NoIndexJoin
	result.NoZigzagJoin = ih.NoZigzagJoin || other.NoZigzagJoin
	result.NoFullScan = ih.NoFullScan || other.NoFullScan
	result.AvoidFullScan = ih.AvoidFullScan || other.AvoidFullScan
	result.IgnoreForeignKeys = ih.IgnoreForeignKeys || other.IgnoreForeignKeys
	result.IgnoreUniqueWithoutIndexKeys = ih.IgnoreUniqueWithoutIndexKeys ||
		other.IgnoreUniqueWithoutIndexKeys
	result.ForceInvertedIndex = ih.ForceInvertedIndex || other.ForceInvertedIndex

	if other.Direction != 0 {
		if ih.Direction != 0 {
			return errors.New("ASC/DESC specified multiple times")
		}
		result.Direction = other.Direction
	}

	if other.ForceIndex() {
		if ih.ForceIndex() {
			return errors.New("FORCE_INDEX specified multiple times")
		}
		result.Index = other.Index
		result.IndexID = other.IndexID
	}

	if other.ForceZigzag {
		if ih.ForceZigzag {
			return errors.New("FORCE_ZIGZAG specified multiple times")
		}
		result.ForceZigzag = true
	}

	// We can have N zigzag indexes (in theory, we only support 2 now).
	if len(other.ZigzagIndexes) > 0 {
		if result.ForceZigzag {
			return errors.New("FORCE_ZIGZAG hints not distinct")
		}
		result.ZigzagIndexes = append(result.ZigzagIndexes, other.ZigzagIndexes...)
	}

	// We can have N zigzag indexes (in theory, we only support 2 now).
	if len(other.ZigzagIndexIDs) > 0 {
		if result.ForceZigzag {
			return errors.New("FORCE_ZIGZAG hints not distinct")
		}
		result.ZigzagIndexIDs = append(result.ZigzagIndexIDs, other.ZigzagIndexIDs...)
	}

	// We only set at the end to avoid a partially changed structure in one of the
	// error cases above.
	*ih = result
	return nil
}

// Check verifies if the flags are valid:
//   - ascending/descending is not specified without an index;
//   - no_index_join isn't specified with an index.
func (ih *IndexFlags) Check() error {
	if ih.NoIndexJoin && ih.ForceIndex() {
		return errors.New("FORCE_INDEX cannot be specified in conjunction with NO_INDEX_JOIN")
	}
	if ih.Direction != 0 && !ih.ForceIndex() {
		return errors.New("ASC/DESC must be specified in conjunction with an index")
	}
	if ih.ForceInvertedIndex && ih.ForceIndex() {
		return errors.New("FORCE_INVERTED_INDEX cannot be specified in conjunction with FORCE_INDEX")
	}
	if ih.zigzagForced() && ih.NoIndexJoin {
		return errors.New("FORCE_ZIGZAG cannot be specified in conjunction with NO_INDEX_JOIN")
	}
	if ih.zigzagForced() && ih.ForceIndex() {
		return errors.New("FORCE_ZIGZAG cannot be specified in conjunction with FORCE_INDEX")
	}
	if ih.zigzagForced() && ih.NoZigzagJoin {
		return errors.New("FORCE_ZIGZAG cannot be specified in conjunction with NO_ZIGZAG_JOIN")
	}
	for _, name := range ih.ZigzagIndexes {
		if len(string(name)) == 0 {
			return errors.New("FORCE_ZIGZAG index name cannot be empty string")
		}
	}

	// FamilyID is currently set internally by changefeed, and is never parsed/serialized.
	// TODO(#94900): Remove this restriction.
	if ih.FamilyID != nil && !enableFamilyIDIndexHintForTests {
		return pgerror.New(pgcode.InvalidParameterValue, "FAMILY is an internal hint used by CDC")
	}

	return nil
}

var enableFamilyIDIndexHintForTests = false

// TestingEnableFamilyIndexHint enables the use of Family index hint
// for tests.
func TestingEnableFamilyIndexHint() func() {
	enableFamilyIDIndexHintForTests = true
	return func() {
		enableFamilyIDIndexHintForTests = false
	}
}

// Format implements the NodeFormatter interface.
func (ih *IndexFlags) Format(ctx *FmtCtx) {
	ctx.WriteByte('@')
	if ih.indexOnlyHint() {
		if ih.Index != "" {
			ctx.FormatNode(&ih.Index)
		} else {
			ctx.Printf("[%d]", ih.IndexID)
		}
	} else {
		ctx.WriteByte('{')
		var sep func()
		sep = func() {
			sep = func() { ctx.WriteByte(',') }
		}
		if ih.Index != "" || ih.IndexID != 0 {
			sep()
			ctx.WriteString("FORCE_INDEX=")
			if ih.Index != "" {
				ctx.FormatNode(&ih.Index)
			} else {
				ctx.Printf("[%d]", ih.IndexID)
			}

			if ih.Direction != 0 {
				ctx.Printf(",%s", ih.Direction)
			}
		}
		if ih.NoIndexJoin {
			sep()
			ctx.WriteString("NO_INDEX_JOIN")
		}

		if ih.NoZigzagJoin {
			sep()
			ctx.WriteString("NO_ZIGZAG_JOIN")
		}

		if ih.NoFullScan {
			sep()
			ctx.WriteString("NO_FULL_SCAN")
		}

		if ih.AvoidFullScan {
			sep()
			ctx.WriteString("AVOID_FULL_SCAN")
		}

		if ih.IgnoreForeignKeys {
			sep()
			ctx.WriteString("IGNORE_FOREIGN_KEYS")
		}

		if ih.IgnoreUniqueWithoutIndexKeys {
			sep()
			ctx.WriteString("IGNORE_UNIQUE_WITHOUT_INDEX_KEYS")
		}

		if ih.ForceInvertedIndex {
			sep()
			ctx.WriteString("FORCE_INVERTED_INDEX")
		}

		if ih.ForceZigzag || len(ih.ZigzagIndexes) > 0 || len(ih.ZigzagIndexIDs) > 0 {
			sep()
			if ih.ForceZigzag {
				ctx.WriteString("FORCE_ZIGZAG")
			} else {
				needSep := false
				for _, name := range ih.ZigzagIndexes {
					if needSep {
						sep()
					}
					ctx.WriteString("FORCE_ZIGZAG=")
					ctx.FormatNode(&name)
					needSep = true
				}
				for _, id := range ih.ZigzagIndexIDs {
					if needSep {
						sep()
					}
					ctx.WriteString("FORCE_ZIGZAG=")
					ctx.Printf("[%d]", id)
					needSep = true
				}
			}
		}
		if ih.FamilyID != nil {
			ctx.Printf("FAMILY=[%d]", *ih.FamilyID)
		}
		ctx.WriteString("}")
	}
}

func (ih *IndexFlags) indexOnlyHint() bool {
	return !ih.NoIndexJoin && !ih.NoZigzagJoin && !ih.NoFullScan && !ih.AvoidFullScan &&
		!ih.IgnoreForeignKeys && !ih.IgnoreUniqueWithoutIndexKeys && ih.Direction == 0 &&
		!ih.ForceInvertedIndex && !ih.zigzagForced() && ih.FamilyID == nil
}

func (ih *IndexFlags) zigzagForced() bool {
	return ih.ForceZigzag || len(ih.ZigzagIndexes) > 0 || len(ih.ZigzagIndexIDs) > 0
}

// AliasedTableExpr represents a table expression coupled with an optional
// alias.
type AliasedTableExpr struct {
	Expr       TableExpr
	IndexFlags *IndexFlags
	Ordinality bool
	Lateral    bool
	As         AliasClause
}

// Format implements the NodeFormatter interface.
func (node *AliasedTableExpr) Format(ctx *FmtCtx) {
	if node.Lateral {
		ctx.WriteString("LATERAL ")
	}
	ctx.FormatNode(node.Expr)
	if node.IndexFlags != nil {
		ctx.FormatNode(node.IndexFlags)
	}
	if node.Ordinality {
		ctx.WriteString(" WITH ORDINALITY")
	}
	if node.As.Alias != "" {
		ctx.WriteString(" AS ")
		ctx.FormatNode(&node.As)
	}
}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	Expr TableExpr
}

// Format implements the NodeFormatter interface.
func (node *ParenTableExpr) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	ctx.FormatNode(node.Expr)
	ctx.WriteByte(')')
}

// StripTableParens strips any parentheses surrounding a selection clause.
func StripTableParens(expr TableExpr) TableExpr {
	if p, ok := expr.(*ParenTableExpr); ok {
		return StripTableParens(p.Expr)
	}
	return expr
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	JoinType string
	Left     TableExpr
	Right    TableExpr
	Cond     JoinCond
	Hint     string
}

// JoinTableExpr.Join
const (
	AstFull  = "FULL"
	AstLeft  = "LEFT"
	AstRight = "RIGHT"
	AstCross = "CROSS"
	AstInner = "INNER"
)

// JoinTableExpr.Hint
const (
	AstHash     = "HASH"
	AstLookup   = "LOOKUP"
	AstMerge    = "MERGE"
	AstInverted = "INVERTED"
	AstStraight = "STRAIGHT"
)

// Format implements the NodeFormatter interface.
func (node *JoinTableExpr) Format(ctx *FmtCtx) {
	ctx.FormatNode(node.Left)
	ctx.WriteByte(' ')
	if _, isNatural := node.Cond.(NaturalJoinCond); isNatural {
		// Natural joins have a different syntax: "<a> NATURAL <join_type> <b>"
		ctx.FormatNode(node.Cond)
		ctx.WriteByte(' ')
		if node.JoinType != "" {
			ctx.WriteString(node.JoinType)
			ctx.WriteByte(' ')
			if node.Hint != "" {
				ctx.WriteString(node.Hint)
				ctx.WriteByte(' ')
			}
		}
		ctx.WriteString("JOIN ")
		ctx.FormatNode(node.Right)
	} else {
		// General syntax: "<a> <join_type> [<join_hint>] JOIN <b> <condition>"
		if node.JoinType != "" {
			ctx.WriteString(node.JoinType)
			ctx.WriteByte(' ')
			if node.Hint != "" {
				ctx.WriteString(node.Hint)
				ctx.WriteByte(' ')
			}
		}
		ctx.WriteString("JOIN ")
		ctx.FormatNode(node.Right)
		if node.Cond != nil {
			ctx.WriteByte(' ')
			ctx.FormatNode(node.Cond)
		}
	}
}

// JoinCond represents a join condition.
type JoinCond interface {
	NodeFormatter
	joinCond()
}

func (NaturalJoinCond) joinCond() {}
func (*OnJoinCond) joinCond()     {}
func (*UsingJoinCond) joinCond()  {}

// NaturalJoinCond represents a NATURAL join condition
type NaturalJoinCond struct{}

// Format implements the NodeFormatter interface.
func (NaturalJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("NATURAL")
}

// OnJoinCond represents an ON join condition.
type OnJoinCond struct {
	Expr Expr
}

// Format implements the NodeFormatter interface.
func (node *OnJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("ON ")
	ctx.FormatNode(node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols NameList
}

// Format implements the NodeFormatter interface.
func (node *UsingJoinCond) Format(ctx *FmtCtx) {
	ctx.WriteString("USING (")
	ctx.FormatNode(&node.Cols)
	ctx.WriteByte(')')
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr Expr
}

// Where.Type
const (
	AstWhere  = "WHERE"
	AstHaving = "HAVING"
)

// NewWhere creates a WHERE or HAVING clause out of an Expr. If the expression
// is nil, it returns nil.
func NewWhere(typ string, expr Expr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

// Format implements the NodeFormatter interface.
func (node *Where) Format(ctx *FmtCtx) {
	ctx.WriteString(node.Type)
	ctx.WriteByte(' ')
	ctx.FormatNode(node.Expr)
}

// GroupBy represents a GROUP BY clause.
type GroupBy []Expr

// Format implements the NodeFormatter interface.
func (node *GroupBy) Format(ctx *FmtCtx) {
	prefix := "GROUP BY "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// DistinctOn represents a DISTINCT ON clause.
type DistinctOn []Expr

// Format implements the NodeFormatter interface.
func (node *DistinctOn) Format(ctx *FmtCtx) {
	ctx.WriteString("DISTINCT ON (")
	ctx.FormatNode((*Exprs)(node))
	ctx.WriteByte(')')
}

// OrderBy represents an ORDER BY clause.
type OrderBy []*Order

// Format implements the NodeFormatter interface.
func (node *OrderBy) Format(ctx *FmtCtx) {
	prefix := "ORDER BY "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// Direction for ordering results.
type Direction int8

// Direction values.
const (
	DefaultDirection Direction = iota
	Ascending
	Descending
)

var directionName = [...]string{
	DefaultDirection: "",
	Ascending:        "ASC",
	Descending:       "DESC",
}

func (d Direction) String() string {
	if d < 0 || d > Direction(len(directionName)-1) {
		return fmt.Sprintf("Direction(%d)", d)
	}
	return directionName[d]
}

// NullsOrder for specifying ordering of NULLs.
type NullsOrder int8

// Null order values.
const (
	DefaultNullsOrder NullsOrder = iota
	NullsFirst
	NullsLast
)

var nullsOrderName = [...]string{
	DefaultNullsOrder: "",
	NullsFirst:        "NULLS FIRST",
	NullsLast:         "NULLS LAST",
}

func (n NullsOrder) String() string {
	if n < 0 || n > NullsOrder(len(nullsOrderName)-1) {
		return fmt.Sprintf("NullsOrder(%d)", n)
	}
	return nullsOrderName[n]
}

// OrderType indicates which type of expression is used in ORDER BY.
type OrderType int

const (
	// OrderByColumn is the regular "by expression/column" ORDER BY specification.
	OrderByColumn OrderType = iota
	// OrderByIndex enables the user to specify a given index' columns implicitly.
	OrderByIndex
)

// Order represents an ordering expression.
type Order struct {
	OrderType  OrderType
	Expr       Expr
	Direction  Direction
	NullsOrder NullsOrder
	// Table/Index replaces Expr when OrderType = OrderByIndex.
	Table TableName
	// If Index is empty, then the order should use the primary key.
	Index UnrestrictedName
}

// Format implements the NodeFormatter interface.
func (node *Order) Format(ctx *FmtCtx) {
	if node.OrderType == OrderByColumn {
		ctx.FormatNode(node.Expr)
	} else {
		if node.Index == "" {
			ctx.WriteString("PRIMARY KEY ")
			ctx.FormatNode(&node.Table)
		} else {
			ctx.WriteString("INDEX ")
			ctx.FormatNode(&node.Table)
			ctx.WriteByte('@')
			ctx.FormatNode(&node.Index)
		}
	}
	if node.Direction != DefaultDirection {
		ctx.WriteByte(' ')
		ctx.WriteString(node.Direction.String())
	}
	if node.NullsOrder != DefaultNullsOrder {
		ctx.WriteByte(' ')
		ctx.WriteString(node.NullsOrder.String())
	}
}

// Equal checks if the node ordering is equivalent to other.
func (node *Order) Equal(other *Order) bool {
	return node.Expr.String() == other.Expr.String() && node.Direction == other.Direction &&
		node.Table == other.Table && node.OrderType == other.OrderType &&
		node.NullsOrder == other.NullsOrder
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Count Expr
	LimitAll      bool
}

// Format implements the NodeFormatter interface.
func (node *Limit) Format(ctx *FmtCtx) {
	needSpace := false
	if node.Count != nil {
		ctx.WriteString("LIMIT ")
		ctx.FormatNode(node.Count)
		needSpace = true
	} else if node.LimitAll {
		ctx.WriteString("LIMIT ALL")
		needSpace = true
	}
	if node.Offset != nil {
		if needSpace {
			ctx.WriteByte(' ')
		}
		ctx.WriteString("OFFSET ")
		ctx.FormatNode(node.Offset)
	}
}

// RowsFromExpr represents a ROWS FROM(...) expression.
type RowsFromExpr struct {
	Items Exprs
}

// Format implements the NodeFormatter interface.
func (node *RowsFromExpr) Format(ctx *FmtCtx) {
	ctx.WriteString("ROWS FROM (")
	ctx.FormatNode(&node.Items)
	ctx.WriteByte(')')
}

// Window represents a WINDOW clause.
type Window []*WindowDef

// Format implements the NodeFormatter interface.
func (node *Window) Format(ctx *FmtCtx) {
	prefix := "WINDOW "
	for _, n := range *node {
		ctx.WriteString(prefix)
		ctx.FormatNode(&n.Name)
		ctx.WriteString(" AS ")
		ctx.FormatNode(n)
		prefix = ", "
	}
}

// WindowDef represents a single window definition expression.
type WindowDef struct {
	Name       Name
	RefName    Name
	Partitions Exprs
	OrderBy    OrderBy
	Frame      *WindowFrame
}

// Format implements the NodeFormatter interface.
func (node *WindowDef) Format(ctx *FmtCtx) {
	ctx.WriteByte('(')
	needSpaceSeparator := false
	if node.RefName != "" {
		ctx.FormatNode(&node.RefName)
		needSpaceSeparator = true
	}
	if len(node.Partitions) > 0 {
		if needSpaceSeparator {
			ctx.WriteByte(' ')
		}
		ctx.WriteString("PARTITION BY ")
		ctx.FormatNode(&node.Partitions)
		needSpaceSeparator = true
	}
	if len(node.OrderBy) > 0 {
		if needSpaceSeparator {
			ctx.WriteByte(' ')
		}
		ctx.FormatNode(&node.OrderBy)
		needSpaceSeparator = true
	}
	if node.Frame != nil {
		if needSpaceSeparator {
			ctx.WriteByte(' ')
		}
		ctx.FormatNode(node.Frame)
	}
	ctx.WriteRune(')')
}

// OverrideWindowDef implements the logic to have a base window definition which
// then gets augmented by a different window definition.
func OverrideWindowDef(base *WindowDef, override WindowDef) (WindowDef, error) {
	// base.Partitions is always used.
	if len(override.Partitions) > 0 {
		return WindowDef{}, pgerror.Newf(pgcode.Windowing, "cannot override PARTITION BY clause of window %q", base.Name)
	}
	override.Partitions = base.Partitions

	// base.OrderBy is used if set.
	if len(base.OrderBy) > 0 {
		if len(override.OrderBy) > 0 {
			return WindowDef{}, pgerror.Newf(pgcode.Windowing, "cannot override ORDER BY clause of window %q", base.Name)
		}
		override.OrderBy = base.OrderBy
	}

	if base.Frame != nil {
		return WindowDef{}, pgerror.Newf(pgcode.Windowing, "cannot copy window %q because it has a frame clause", base.Name)
	}

	return override, nil
}

// WindowFrameBound specifies the offset and the type of boundary.
type WindowFrameBound struct {
	BoundType  treewindow.WindowFrameBoundType
	OffsetExpr Expr
}

// HasOffset returns whether node contains an offset.
func (node *WindowFrameBound) HasOffset() bool {
	return node.BoundType.IsOffset()
}

// WindowFrameBounds specifies boundaries of the window frame.
// The row at StartBound is included whereas the row at EndBound is not.
type WindowFrameBounds struct {
	StartBound *WindowFrameBound
	EndBound   *WindowFrameBound
}

// HasOffset returns whether node contains an offset in either of the bounds.
func (node *WindowFrameBounds) HasOffset() bool {
	return node.StartBound.HasOffset() || (node.EndBound != nil && node.EndBound.HasOffset())
}

// WindowFrame represents static state of window frame over which calculations are made.
type WindowFrame struct {
	Mode      treewindow.WindowFrameMode      // the mode of framing being used
	Bounds    WindowFrameBounds               // the bounds of the frame
	Exclusion treewindow.WindowFrameExclusion // optional frame exclusion
}

// IsDefaultFrame returns whether a frame equivalent to the default frame
// is being used (default is RANGE UNBOUNDED PRECEDING).
func (f *WindowFrame) IsDefaultFrame() bool {
	if f == nil {
		return true
	}
	if f.Bounds.StartBound.BoundType == treewindow.UnboundedPreceding {
		return f.DefaultFrameExclusion() && f.Mode == treewindow.RANGE &&
			(f.Bounds.EndBound == nil || f.Bounds.EndBound.BoundType == treewindow.CurrentRow)
	}
	return false
}

// DefaultFrameExclusion returns true if optional frame exclusion is omitted.
func (f *WindowFrame) DefaultFrameExclusion() bool {
	return f == nil || f.Exclusion == treewindow.NoExclusion
}

// Format implements the NodeFormatter interface.
func (node *WindowFrameBound) Format(ctx *FmtCtx) {
	switch node.BoundType {
	case treewindow.UnboundedPreceding:
		ctx.WriteString("UNBOUNDED PRECEDING")
	case treewindow.OffsetPreceding:
		ctx.FormatNode(node.OffsetExpr)
		ctx.WriteString(" PRECEDING")
	case treewindow.CurrentRow:
		ctx.WriteString("CURRENT ROW")
	case treewindow.OffsetFollowing:
		ctx.FormatNode(node.OffsetExpr)
		ctx.WriteString(" FOLLOWING")
	case treewindow.UnboundedFollowing:
		ctx.WriteString("UNBOUNDED FOLLOWING")
	default:
		panic(errors.AssertionFailedf("unhandled case: %d", redact.Safe(node.BoundType)))
	}
}

// Format implements the NodeFormatter interface.
func (f *WindowFrame) Format(ctx *FmtCtx) {
	ctx.WriteString(treewindow.WindowModeName(f.Mode))
	ctx.WriteByte(' ')
	if f.Bounds.EndBound != nil {
		ctx.WriteString("BETWEEN ")
		ctx.FormatNode(f.Bounds.StartBound)
		ctx.WriteString(" AND ")
		ctx.FormatNode(f.Bounds.EndBound)
	} else {
		ctx.FormatNode(f.Bounds.StartBound)
	}
	if f.Exclusion != treewindow.NoExclusion {
		ctx.WriteByte(' ')
		ctx.WriteString(f.Exclusion.String())
	}
}

// LockingClause represents a locking clause, like FOR UPDATE.
type LockingClause []*LockingItem

// Format implements the NodeFormatter interface.
func (node *LockingClause) Format(ctx *FmtCtx) {
	for _, n := range *node {
		ctx.FormatNode(n)
	}
}

// LockingItem represents a single locking item in a locking clause.
type LockingItem struct {
	Strength   LockingStrength
	Targets    TableNames
	WaitPolicy LockingWaitPolicy
	Form       LockingForm
}

// Format implements the NodeFormatter interface.
func (f *LockingItem) Format(ctx *FmtCtx) {
	ctx.FormatNode(f.Strength)
	if len(f.Targets) > 0 {
		ctx.WriteString(" OF ")
		ctx.FormatNode(&f.Targets)
	}
	ctx.FormatNode(f.WaitPolicy)
	// Form and durability are not currently exposed through SQL.
}

// LockingStrength represents the possible row-level lock modes for a SELECT
// statement.
type LockingStrength byte

// The ordering of the variants is important, because the highest numerical
// value takes precedence when row-level locking is specified multiple ways.
const (
	// ForNone represents the default - no for statement at all.
	// LockingItem AST nodes are never created with this strength.
	ForNone LockingStrength = iota
	// ForKeyShare represents FOR KEY SHARE.
	ForKeyShare
	// ForShare represents FOR SHARE.
	ForShare
	// ForNoKeyUpdate represents FOR NO KEY UPDATE.
	ForNoKeyUpdate
	// ForUpdate represents FOR UPDATE.
	ForUpdate
)

var lockingStrengthName = [...]string{
	ForNone:        "",
	ForKeyShare:    "FOR KEY SHARE",
	ForShare:       "FOR SHARE",
	ForNoKeyUpdate: "FOR NO KEY UPDATE",
	ForUpdate:      "FOR UPDATE",
}

func (s LockingStrength) String() string {
	return lockingStrengthName[s]
}

// Format implements the NodeFormatter interface.
func (s LockingStrength) Format(ctx *FmtCtx) {
	if s != ForNone {
		ctx.WriteString(" ")
		ctx.WriteString(s.String())
	}
}

// Max returns the maximum of the two locking strengths.
func (s LockingStrength) Max(s2 LockingStrength) LockingStrength {
	return LockingStrength(max(byte(s), byte(s2)))
}

// LockingWaitPolicy represents the possible policies for handling conflicting
// locks held by other active transactions when attempting to lock rows due to
// FOR UPDATE/SHARE clauses (i.e. it represents the NOWAIT and SKIP LOCKED
// options).
type LockingWaitPolicy byte

// The ordering of the variants is important, because the highest numerical
// value takes precedence when row-level locking is specified multiple ways.
const (
	// LockWaitBlock represents the default - wait for the lock to become
	// available.
	LockWaitBlock LockingWaitPolicy = iota
	// LockWaitSkipLocked represents SKIP LOCKED - skip rows that can't be locked.
	LockWaitSkipLocked
	// LockWaitError represents NOWAIT - raise an error if a row cannot be
	// locked.
	LockWaitError
)

var lockingWaitPolicyName = [...]string{
	LockWaitBlock:      "",
	LockWaitSkipLocked: "SKIP LOCKED",
	LockWaitError:      "NOWAIT",
}

func (p LockingWaitPolicy) String() string {
	return lockingWaitPolicyName[p]
}

// Format implements the NodeFormatter interface.
func (p LockingWaitPolicy) Format(ctx *FmtCtx) {
	if p != LockWaitBlock {
		ctx.WriteString(" ")
		ctx.WriteString(p.String())
	}
}

// Max returns the maximum of the two locking wait policies.
func (p LockingWaitPolicy) Max(p2 LockingWaitPolicy) LockingWaitPolicy {
	return LockingWaitPolicy(max(byte(p), byte(p2)))
}

// LockingForm represents the form of locking to use, record locking or
// predicate locking. It is not currently exposed through SQL, but could be once
// more fully supported.
type LockingForm byte

// The ordering of the variants is important, because the highest numerical
// value takes precedence when row-level locking is specified multiple ways.
const (
	// LockRecord represents the default: lock existing rows within the specified
	// span(s), which prevents modification of those rows but does not prevent
	// insertion of new rows (phantoms) into the span(s).
	LockRecord LockingForm = iota
	// LockPredicate represents locking the logical predicate defined by the
	// span(s), preventing modification of existing rows as well as insertion of
	// new rows (phantoms). This is similar to the behavior of "next-key locks" in
	// InnoDB, "key-range locks" in SQL Server, "phantom locks" in Sybase, etc.
	// (Postgres also has predicate locks, which it uses under serializable
	// isolation, but these are used to detect serializable violations rather than
	// for mutual exclusion.)
	//
	// We currently only use predicate locks for uniqueness checks under snapshot
	// and read committed isolation, and only support predicate locks on
	// single-key spans.
	LockPredicate
)

var lockingClassName = [...]string{
	LockRecord:    "record",
	LockPredicate: "predicate",
}

func (p LockingForm) String() string {
	return lockingClassName[p]
}

// Max returns the maximum of the two locking forms.
func (p LockingForm) Max(p2 LockingForm) LockingForm {
	return LockingForm(max(byte(p), byte(p2)))
}

// LockingDurability represents the durability of a lock. It is not exposed
// through SQL, but is instead set by the system according to statement type and
// isolation level. It is included here for completeness.
type LockingDurability byte

// The ordering of the variants is important, because the highest numerical
// value takes precedence when row-level locking is specified multiple ways.
const (
	// LockDurabilityBestEffort represents the default: make a best-effort attempt
	// to hold the lock until commit while keeping it unreplicated and in-memory
	// on the leaseholder of the locked row. Best-effort locks do not propagate
	// via Raft to other nodes, and are therefore much faster to acquire than
	// guaranteed-durable locks. For this reason we prefer to use best-effort
	// locks when possible (i.e. whenever locking is used as an optimization
	// rather than as a guarantor of exclusion).
	LockDurabilityBestEffort LockingDurability = iota

	// LockDurabilityGuaranteed guarantees that if the transaction commits, the
	// lock was held until commit, even in the face of lease transfers, range
	// splits, range merges, node failures, memory limits, etc. Guaranteed-durable
	// locks *must* be used whenever correctness depends on locking.
	LockDurabilityGuaranteed
)

var lockingDurabilityName = [...]string{
	LockDurabilityBestEffort: "best-effort",
	LockDurabilityGuaranteed: "guaranteed",
}

func (d LockingDurability) String() string {
	return lockingDurabilityName[d]
}

// Max returns the most durable of the two locking durabilities.
func (d LockingDurability) Max(d2 LockingDurability) LockingDurability {
	return LockingDurability(max(byte(d), byte(d2)))
}
