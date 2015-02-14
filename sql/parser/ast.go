// Copyright 2012, Google Inc. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package parser

import (
	"bytes"
	"errors"
	"fmt"
)

//go:generate go tool yacc -o sql.go sql.y

// Instructions for creating new types: If a type needs to satisfy an
// interface, declare that function along with that interface. This
// will help users identify the list of types to which they can assert
// those interfaces. If the member of a type has a string with a
// predefined list of values, declare those values as const following
// the type. For interfaces that define dummy functions to
// consolidate a set of types, define the function as typeName().
// This will help avoid name collisions.

// Parse parses the sql and returns a Statement, which is the AST
// representation of the query.
func Parse(sql string) (Statement, error) {
	tokenizer := NewStringTokenizer(sql)
	if yyParse(tokenizer) != 0 {
		return nil, errors.New(tokenizer.LastError)
	}
	return tokenizer.ParseTree, nil
}

// Statement represents a statement.
type Statement interface {
	fmt.Stringer
	statement()
}

func (*Union) statement()  {}
func (*Select) statement() {}
func (*Insert) statement() {}
func (*Update) statement() {}
func (*Delete) statement() {}
func (*Set) statement()    {}
func (*Use) statement()    {}
func (*DDL) statement()    {}

// SelectStatement any SELECT statement.
type SelectStatement interface {
	fmt.Stringer
	selectStatement()
	statement()
	insertRows()
}

func (*Select) selectStatement() {}
func (*Union) selectStatement()  {}

// Select represents a SELECT statement.
type Select struct {
	Comments Comments
	Distinct string
	Exprs    SelectExprs
	From     TableExprs
	Where    *Where
	GroupBy  GroupBy
	Having   *Where
	OrderBy  OrderBy
	Limit    *Limit
	Lock     string
}

// Select.Distinct
const (
	astDistinct = "DISTINCT "
)

// Select.Lock
const (
	astForUpdate = " FOR UPDATE"
	astShareMode = " LOCK IN SHARE MODE"
)

func (node *Select) String() string {
	return fmt.Sprintf("SELECT %v%s%v FROM %v%v%v%v%v%v%s",
		node.Comments, node.Distinct, node.Exprs,
		node.From, node.Where,
		node.GroupBy, node.Having, node.OrderBy,
		node.Limit, node.Lock)
}

// Union represents a UNION statement.
type Union struct {
	Type        string
	Left, Right SelectStatement
}

// Union.Type
const (
	astUnion     = "UNION"
	astUnionAll  = "UNION ALL"
	astSetMinus  = "MINUS"
	astExcept    = "EXCEPT"
	astIntersect = "INTERSECT"
)

func (node *Union) String() string {
	return fmt.Sprintf("%v %s %v", node.Left, node.Type, node.Right)
}

// Insert represents an INSERT statement.
type Insert struct {
	Comments Comments
	Table    *TableName
	Columns  Columns
	Rows     InsertRows
	OnDup    OnDup
}

func (node *Insert) String() string {
	return fmt.Sprintf("INSERT %vINTO %v%v %v%v",
		node.Comments,
		node.Table, node.Columns, node.Rows, node.OnDup)
}

// InsertRows represents the rows for an INSERT statement.
type InsertRows interface {
	insertRows()
}

func (*Select) insertRows() {}
func (*Union) insertRows()  {}
func (Values) insertRows()  {}

// Update represents an UPDATE statement.
type Update struct {
	Comments Comments
	Table    *TableName
	Exprs    UpdateExprs
	Where    *Where
	OrderBy  OrderBy
	Limit    *Limit
}

func (node *Update) String() string {
	return fmt.Sprintf("UPDATE %v%v SET %v%v%v%v",
		node.Comments, node.Table,
		node.Exprs, node.Where, node.OrderBy, node.Limit)
}

// Delete represents a DELETE statement.
type Delete struct {
	Comments Comments
	Table    *TableName
	Where    *Where
	OrderBy  OrderBy
	Limit    *Limit
}

func (node *Delete) String() string {
	return fmt.Sprintf("DELETE %vFROM %v%v%v%v",
		node.Comments,
		node.Table, node.Where, node.OrderBy, node.Limit)
}

// Set represents a SET statement.
type Set struct {
	Comments Comments
	Exprs    UpdateExprs
}

func (node *Set) String() string {
	return fmt.Sprintf("SET %v%v", node.Comments, node.Exprs)
}

// Use represents a USE statement.
type Use struct {
	Comments Comments
	Name     string
}

func (node *Use) String() string {
	return fmt.Sprintf("USE %v%s", node.Comments, node.Name)
}

// DDL represents a CREATE, ALTER, DROP or RENAME statement.
// Table is set for astAlter, astDrop, astRename.
// NewName is set for astAlter, astCreate, astRename.
type DDL struct {
	Action  string
	Name    string
	NewName string
}

const (
	astCreateDatabase  = "CREATE DATABASE"
	astCreateIndex     = "CREATE INDEX"
	astCreateTable     = "CREATE TABLE"
	astCreateView      = "CREATE VIEW"
	astAlterTable      = "ALTER TABLE"
	astAlterView       = "ALTER VIEW"
	astDropDatabase    = "DROP DATABASE"
	astDropIndex       = "DROP INDEX"
	astDropTable       = "DROP TABLE"
	astDropView        = "DROP VIEW"
	astRenameTable     = "RENAME TABLE"
	astShowTables      = "SHOW TABLES"
	astShowIndex       = "SHOW INDEX FROM"
	astShowColumns     = "SHOW COLUMNS FROM"
	astShowFullColumns = "SHOW FULL COLUMNS FROM"
	astTruncateTable   = "TRUNCATE TABLE"
)

func (node *DDL) String() string {
	switch node.Action {
	case astCreateIndex, astDropIndex:
		return fmt.Sprintf("%s %s ON %s", node.Action, node.Name, node.NewName)
	case astRenameTable:
		return fmt.Sprintf("%s %s %s", node.Action, node.Name, node.NewName)
	case astCreateDatabase, astCreateTable, astCreateView:
		return fmt.Sprintf("%s %s", node.Action, node.NewName)
	case astShowTables:
		return node.Action
	default:
		return fmt.Sprintf("%s %s", node.Action, node.Name)
	}
}

// Comments represents a list of comments.
type Comments []string

func (node Comments) String() string {
	var buf bytes.Buffer
	for _, c := range node {
		fmt.Fprintf(&buf, "%s ", c)
	}
	return buf.String()
}

// SelectExprs represents SELECT expressions.
type SelectExprs []SelectExpr

func (node SelectExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// SelectExpr represents a SELECT expression.
type SelectExpr interface {
	selectExpr()
}

func (*StarExpr) selectExpr()    {}
func (*NonStarExpr) selectExpr() {}

// StarExpr defines a '*' or 'table.*' expression.
type StarExpr struct {
	TableName string
}

func (node *StarExpr) String() string {
	var buf bytes.Buffer
	if node.TableName != "" {
		fmt.Fprintf(&buf, "%s.", node.TableName)
	}
	fmt.Fprintf(&buf, "*")
	return buf.String()
}

// NonStarExpr defines a non-'*' select expr.
type NonStarExpr struct {
	Expr Expr
	As   string
}

func (node *NonStarExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", node.Expr)
	if node.As != "" {
		fmt.Fprintf(&buf, " AS %s", node.As)
	}
	return buf.String()
}

// Columns represents an insert column list.
// The syntax for Columns is a subset of SelectExprs.
// So, it's castable to a SelectExprs and can be analyzed
// as such.
type Columns []SelectExpr

func (node Columns) String() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf("(%v)", SelectExprs(node))
}

// TableExprs represents a list of table expressions.
type TableExprs []TableExpr

func (node TableExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// TableExpr represents a table expression.
type TableExpr interface {
	tableExpr()
}

func (*AliasedTableExpr) tableExpr() {}
func (*ParenTableExpr) tableExpr()   {}
func (*JoinTableExpr) tableExpr()    {}

// AliasedTableExpr represents a table expression
// coupled with an optional alias or index hint.
type AliasedTableExpr struct {
	Expr  SimpleTableExpr
	As    string
	Hints *IndexHints
}

func (node *AliasedTableExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v", node.Expr)
	if node.As != "" {
		fmt.Fprintf(&buf, " AS %s", node.As)
	}
	if node.Hints != nil {
		// Hint node provides the space padding.
		fmt.Fprintf(&buf, "%v", node.Hints)
	}
	return buf.String()
}

// SimpleTableExpr represents a simple table expression.
type SimpleTableExpr interface {
	simpleTableExpr()
}

func (*TableName) simpleTableExpr() {}
func (*Subquery) simpleTableExpr()  {}

// TableName represents a table  name.
type TableName struct {
	Name, Qualifier string
}

func (node *TableName) String() string {
	var buf bytes.Buffer
	if node.Qualifier != "" {
		escape(&buf, node.Qualifier)
		fmt.Fprintf(&buf, ".")
	}
	escape(&buf, node.Name)
	return buf.String()
}

// ParenTableExpr represents a parenthesized TableExpr.
type ParenTableExpr struct {
	Expr TableExpr
}

func (node *ParenTableExpr) String() string {
	return fmt.Sprintf("(%v)", node.Expr)
}

// JoinTableExpr represents a TableExpr that's a JOIN operation.
type JoinTableExpr struct {
	LeftExpr  TableExpr
	Join      string
	RightExpr TableExpr
	Cond      JoinCond
}

// JoinTableExpr.Join
const (
	astJoin         = "JOIN"
	astStraightJoin = "STRAIGHT_JOIN"
	astLeftJoin     = "LEFT JOIN"
	astRightJoin    = "RIGHT JOIN"
	astCrossJoin    = "CROSS JOIN"
	astNaturalJoin  = "NATURAL JOIN"
)

func (node *JoinTableExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%v %s %v", node.LeftExpr, node.Join, node.RightExpr)
	if node.Cond != nil {
		fmt.Fprintf(&buf, "%v", node.Cond)
	}
	return buf.String()
}

// JoinCond represents a join condition.
type JoinCond interface {
	joinCond()
}

func (*OnJoinCond) joinCond()    {}
func (*UsingJoinCond) joinCond() {}

// OnJoinCond represents an ON join condition.
type OnJoinCond struct {
	Expr BoolExpr
}

func (node *OnJoinCond) String() string {
	return fmt.Sprintf(" ON %v", node.Expr)
}

// UsingJoinCond represents a USING join condition.
type UsingJoinCond struct {
	Cols Columns
}

func (node *UsingJoinCond) String() string {
	return fmt.Sprintf(" USING %v", node.Cols)
}

// IndexHints represents a list of index hints.
type IndexHints struct {
	Type    string
	Indexes []string
}

const (
	astUse    = "USE"
	astIgnore = "IGNORE"
	astForce  = "FORCE"
)

func (node *IndexHints) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, " %s INDEX ", node.Type)
	prefix := "("
	for _, n := range node.Indexes {
		fmt.Fprintf(&buf, "%s%s", prefix, n)
		prefix = ", "
	}
	fmt.Fprintf(&buf, ")")
	return buf.String()
}

// Where represents a WHERE or HAVING clause.
type Where struct {
	Type string
	Expr BoolExpr
}

// Where.Type
const (
	astWhere  = "WHERE"
	astHaving = "HAVING"
)

// NewWhere creates a WHERE or HAVING clause out
// of a BoolExpr. If the expression is nil, it returns nil.
func NewWhere(typ string, expr BoolExpr) *Where {
	if expr == nil {
		return nil
	}
	return &Where{Type: typ, Expr: expr}
}

func (node *Where) String() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf(" %s %v", node.Type, node.Expr)
}

// Expr represents an expression.
type Expr interface {
	expr()
}

func (*AndExpr) expr()        {}
func (*OrExpr) expr()         {}
func (*NotExpr) expr()        {}
func (*ParenBoolExpr) expr()  {}
func (*ComparisonExpr) expr() {}
func (*RangeCond) expr()      {}
func (*NullCheck) expr()      {}
func (*ExistsExpr) expr()     {}
func (StrVal) expr()          {}
func (NumVal) expr()          {}
func (ValArg) expr()          {}
func (*NullVal) expr()        {}
func (*ColName) expr()        {}
func (ValTuple) expr()        {}
func (*Subquery) expr()       {}
func (*BinaryExpr) expr()     {}
func (*UnaryExpr) expr()      {}
func (*FuncExpr) expr()       {}
func (*CaseExpr) expr()       {}

// BoolExpr represents a boolean expression.
type BoolExpr interface {
	boolExpr()
	Expr
}

func (*AndExpr) boolExpr()        {}
func (*OrExpr) boolExpr()         {}
func (*NotExpr) boolExpr()        {}
func (*ParenBoolExpr) boolExpr()  {}
func (*ComparisonExpr) boolExpr() {}
func (*RangeCond) boolExpr()      {}
func (*NullCheck) boolExpr()      {}
func (*ExistsExpr) boolExpr()     {}

// AndExpr represents an AND expression.
type AndExpr struct {
	Op          string
	Left, Right BoolExpr
}

func (node *AndExpr) String() string {
	return fmt.Sprintf("%v %s %v", node.Left, node.Op, node.Right)
}

// OrExpr represents an OR expression.
type OrExpr struct {
	Op          string
	Left, Right BoolExpr
}

func (node *OrExpr) String() string {
	return fmt.Sprintf("%v %s %v", node.Left, node.Op, node.Right)
}

// NotExpr represents a NOT expression.
type NotExpr struct {
	Op   string
	Expr BoolExpr
}

func (node *NotExpr) String() string {
	return fmt.Sprintf("%s %v", node.Op, node.Expr)
}

// ParenBoolExpr represents a parenthesized boolean expression.
type ParenBoolExpr struct {
	Expr BoolExpr
}

func (node *ParenBoolExpr) String() string {
	return fmt.Sprintf("(%v)", node.Expr)
}

// ComparisonExpr represents a two-value comparison expression.
type ComparisonExpr struct {
	Operator    string
	Left, Right ValExpr
}

// ComparisonExpr.Operator
const (
	astEQ      = "="
	astLT      = "<"
	astGT      = ">"
	astLE      = "<="
	astGE      = ">="
	astNE      = "!="
	astNSE     = "<=>"
	astIn      = "IN"
	astNot     = "NOT"
	astNotIn   = "NOT IN"
	astLike    = "LIKE"
	astNotLike = "NOT LIKE"
)

func (node *ComparisonExpr) String() string {
	return fmt.Sprintf("%v %s %v", node.Left, node.Operator, node.Right)
}

// RangeCond represents a BETWEEN or a NOT BETWEEN expression.
type RangeCond struct {
	Operator string
	Left     ValExpr
	From, To ValExpr
}

// RangeCond.Operator
const (
	astBetween    = "BETWEEN"
	astNotBetween = "NOT BETWEEN"
)

func (node *RangeCond) String() string {
	return fmt.Sprintf("%v %s %v AND %v", node.Left, node.Operator, node.From, node.To)
}

// NullCheck represents an IS NULL or an IS NOT NULL expression.
type NullCheck struct {
	Operator string
	Expr     ValExpr
}

// NullCheck.Operator
const (
	astIsNull    = "IS NULL"
	astIsNotNull = "IS NOT NULL"
)

func (node *NullCheck) String() string {
	return fmt.Sprintf("%v %s", node.Expr, node.Operator)
}

// ExistsExpr represents an EXISTS expression.
type ExistsExpr struct {
	Subquery *Subquery
}

func (node *ExistsExpr) String() string {
	return fmt.Sprintf("EXISTS %v", node.Subquery)
}

// ValExpr represents a value expression.
type ValExpr interface {
	valExpr()
	Expr
}

func (StrVal) valExpr()      {}
func (NumVal) valExpr()      {}
func (ValArg) valExpr()      {}
func (*NullVal) valExpr()    {}
func (*ColName) valExpr()    {}
func (ValTuple) valExpr()    {}
func (*Subquery) valExpr()   {}
func (*BinaryExpr) valExpr() {}
func (*UnaryExpr) valExpr()  {}
func (*FuncExpr) valExpr()   {}
func (*CaseExpr) valExpr()   {}

// StrVal represents a string value.
type StrVal string

func (node StrVal) String() string {
	var scratch [64]byte
	return string(encodeSQLString(scratch[0:0], []byte(node)))
}

// BytesVal represents a string of unprintable value.
type BytesVal string

func (BytesVal) expr()    {}
func (BytesVal) valExpr() {}

func (node BytesVal) String() string {
	var scratch [64]byte
	return string(encodeSQLBytes(scratch[0:0], []byte(node)))
}

// ErrVal represents an error condition that occurred while
// constructing a tree.
type ErrVal struct {
	Err error
}

func (ErrVal) expr()    {}
func (ErrVal) valExpr() {}

func (v ErrVal) String() string {
	return fmt.Sprintf("<ERROR: %s>", v.Err)
}

// NumVal represents a number.
type NumVal string

func (node NumVal) String() string {
	return fmt.Sprintf("%s", string(node))
}

// ValArg represents a named bind var argument.
type ValArg string

func (node ValArg) String() string {
	// return "?"
	return fmt.Sprintf(":%s", string(node)[1:])
}

// NullVal represents a NULL value.
type NullVal struct{}

func (node *NullVal) String() string {
	return fmt.Sprintf("NULL")
}

// ColName represents a column name.
type ColName struct {
	Name, Qualifier string
}

func (node *ColName) String() string {
	var buf bytes.Buffer
	if node.Qualifier != "" {
		escape(&buf, node.Qualifier)
		fmt.Fprintf(&buf, ".")
	}
	escape(&buf, node.Name)
	return buf.String()
}

func escape(buf *bytes.Buffer, name string) {
	if _, ok := keywords[name]; ok {
		fmt.Fprintf(buf, "`%s`", name)
	} else {
		fmt.Fprintf(buf, "%s", name)
	}
}

// Tuple represents a tuple. It can be ValTuple, Subquery.
type Tuple interface {
	tuple()
	ValExpr
}

func (ValTuple) tuple()  {}
func (*Subquery) tuple() {}

// ValTuple represents a tuple of actual values.
type ValTuple ValExprs

func (node ValTuple) String() string {
	return fmt.Sprintf("(%v)", ValExprs(node))
}

// ValExprs represents a list of value expressions.
// It's not a valid expression because it's not parenthesized.
type ValExprs []ValExpr

func (node ValExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// Subquery represents a subquery.
type Subquery struct {
	Select SelectStatement
}

func (node *Subquery) String() string {
	return fmt.Sprintf("(%v)", node.Select)
}

// BinaryExpr represents a binary value expression.
type BinaryExpr struct {
	Operator    byte
	Left, Right Expr
}

// BinaryExpr.Operator
const (
	astBitand = '&'
	astBitor  = '|'
	astBitxor = '^'
	astPlus   = '+'
	astMinus  = '-'
	astMult   = '*'
	astDiv    = '/'
	astMod    = '%'
)

func (node *BinaryExpr) String() string {
	return fmt.Sprintf("%v%c%v", node.Left, node.Operator, node.Right)
}

// UnaryExpr represents a unary value expression.
type UnaryExpr struct {
	Operator byte
	Expr     Expr
}

// UnaryExpr.Operator
const (
	astUnaryPlus  = '+'
	astUnaryMinus = '-'
	astTilda      = '~'
)

func (node *UnaryExpr) String() string {
	return fmt.Sprintf("%c%v", node.Operator, node.Expr)
}

// FuncExpr represents a function call.
type FuncExpr struct {
	Name     string
	Distinct bool
	Exprs    SelectExprs
}

func (node *FuncExpr) String() string {
	var distinct string
	if node.Distinct {
		distinct = "DISTINCT "
	}
	return fmt.Sprintf("%s(%s%v)", node.Name, distinct, node.Exprs)
}

// CaseExpr represents a CASE expression.
type CaseExpr struct {
	Expr  ValExpr
	Whens []*When
	Else  ValExpr
}

func (node *CaseExpr) String() string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "CASE ")
	if node.Expr != nil {
		fmt.Fprintf(&buf, "%v ", node.Expr)
	}
	for _, when := range node.Whens {
		fmt.Fprintf(&buf, "%v ", when)
	}
	if node.Else != nil {
		fmt.Fprintf(&buf, "ELSE %v ", node.Else)
	}
	fmt.Fprintf(&buf, "END")
	return buf.String()
}

// When represents a WHEN sub-expression.
type When struct {
	Cond BoolExpr
	Val  ValExpr
}

func (node *When) String() string {
	return fmt.Sprintf("WHEN %v THEN %v", node.Cond, node.Val)
}

// Values represents a VALUES clause.
type Values []Tuple

func (node Values) String() string {
	prefix := "VALUES "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// GroupBy represents a GROUP BY clause.
type GroupBy []ValExpr

func (node GroupBy) String() string {
	prefix := " GROUP BY "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// OrderBy represents an ORDER By clause.
type OrderBy []*Order

func (node OrderBy) String() string {
	prefix := " ORDER BY "
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// Order represents an ordering expression.
type Order struct {
	Expr      ValExpr
	Direction string
}

// Order.Direction
const (
	astAsc  = " ASC"
	astDesc = " DESC"
)

func (node *Order) String() string {
	return fmt.Sprintf("%v%s", node.Expr, node.Direction)
}

// Limit represents a LIMIT clause.
type Limit struct {
	Offset, Rowcount ValExpr
}

func (node *Limit) String() string {
	if node == nil {
		return ""
	}
	var buf bytes.Buffer
	fmt.Fprintf(&buf, " LIMIT ")
	if node.Offset != nil {
		fmt.Fprintf(&buf, "%v, ", node.Offset)
	}
	fmt.Fprintf(&buf, "%v", node.Rowcount)
	return buf.String()
}

// UpdateExprs represents a list of update expressions.
type UpdateExprs []*UpdateExpr

func (node UpdateExprs) String() string {
	var prefix string
	var buf bytes.Buffer
	for _, n := range node {
		fmt.Fprintf(&buf, "%s%v", prefix, n)
		prefix = ", "
	}
	return buf.String()
}

// UpdateExpr represents an update expression.
type UpdateExpr struct {
	Name *ColName
	Expr ValExpr
}

func (node *UpdateExpr) String() string {
	return fmt.Sprintf("%v = %v", node.Name, node.Expr)
}

// OnDup represents an ON DUPLICATE KEY clause.
type OnDup UpdateExprs

func (node OnDup) String() string {
	if node == nil {
		return ""
	}
	return fmt.Sprintf(" ON DUPLICATE KEY UPDATE %v", UpdateExprs(node))
}
