// Copyright 2016 The Cockroach Authors.
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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
)

// There are many things that can be named in SQL: databases, tables,
// indices, columns in tables, subfields in things that have composite
// type, array elements, functions.
//
// What is being named decides how to interpret the lexical structure
// of the name. This is decided entirely based on context.
//
// - if the context is a function call then the name is for a function.
//
//   Syntax: [ <namespace> . ] <function-name>
//
//   Represented by: FunctionName
//   Found by: NormalizeFunctionName()
//
// - if the context is the argument list for the COUNT function, then
//   the name may be a single '*'.
//
//   Syntax: '*'
//
//   Represented by: UnqualifiedStar
//   Found by: NormalizeNameInExpr()
//
// - if the context is a CREATE/DROP/RENAME DATABASE, then the name is
//   for a database.
//
//   Syntax: <database-name>
//
//   Represented by: DatabaseName
//
// - if the context is a direct argument to a FROM clause, or in
//   appropriate positions to UPDATE/INSERT/ALTER/etc then the name is
//   for a table.
//
//   Syntax: [ <database-name> . ] <table-name>
//   (the qualified name always *ends* with a table name)
//
//   Represented by: TableName
//   Found by: NormalizeTableName()
//
// - if the context is a GRANT statement, then the name is for a table
//   or table glob.
//
//   Syntax: [ <database-name> . ] [ <table-name> | '*' ]
//
//   Represented by: TableName, AllTablesSelector
//   Found by: NormalizeTablePattern()
//
// - if the context is the LHS of an UPDATE, then the name is for an
//   unqualified column or part of a column.
//
//   Syntax: <column-name> [ . <subfield-name> | '[' <index> ']' ]*
//   (the qualified name always *starts* with a column name)
//
//   Represented by: ColumnItem
//   Found by: NormalizeUnqualifiedColumnItem()
//
// - if the context is a direct select target, then the name may end
//   with '*' for a column group, with optional database and table prefix.
//
//   Syntax: [ [ <database-name> . ] <table-name> ] . '*'
//
//   Represented by: AllColumnsSelector
//   Found by: NormalizeNameInExpr()
//
// - elsewhere, the name is for a optionally-qualified column name
//   with optional array subscript followed by additional optional
//   subfield or array subscripts.
//
//   Syntax: [ [ <database-name> '.' ] <table-name> '.' ]
//              <column-name>
//           [ '[' <index> ']' [ '[' <index> ']' | '.' <subfield> ] * ]
//   (either there is no array subscript and the qualified name *ends*
//   with a column name; or there is an array subscript and the
//   supporting column's name is the last unqualified name before the first
//   array subscript).
//
//   Represented by: ColumnItem
//   Found by: NormalizeNameInExpr()
//
// To access a direct subfield of a column with composite type in
// other contexts than an UPDATE LHS, one must really use a separate
// grammar rule for expressions: '(' <expr> ')' '.' <subfield-name>
// This is really a subscript *operator*, so it is not handled here.
//
// Clarifying examples, assuming the following schema:
// CREATE DATABASE foo
// CREATE TYPE mypoint (x INT, y INT)
// CREATE TABLE foo.foo (x INT, foo mypoint)
// CREATE TABLE foo.x (x INT)
//
// Consider:
//
// SELECT * FROM foo.x WHERE ... -- selects from table "x" in db "foo"
// SELECT * FROM foo.foo WHERE foo.x = 3  -- selects column "x" of table "foo" in db "foo"
// SELECT * FROM foo.x, foo.foo WHERE foo.x = foo.foo.x -- cross join of tables "x" and "foo" in db "foo",
// -- but the condition is (column "x" of table "foo.foo") equals (column "x" of table "foo.foo").
// -- The equality RHS does not contain an array subscript so the last unqualified name must be a column name.
//
// SELECT * FROM foo.foo WHERE (foo).x = 3  -- selects subfield "x" of column "foo" in table "foo" of db "foo"
// SELECT * FROM foo.x, foo.foo WHERE foo.x = (foo.foo).x -- same cross join as above,
// -- but the condition is (column "x" of table "foo.foo") equals (field "x" of column "foo" of table "foo.foo").
//
// UPDATE foo.foo SET foo.x = 3 -- updates subfield "x" of column "foo" of table "foo.foo"
// UPDATE foo.foo SET foo.foo.x = 3 -- invalid, no subfield "foo" in column "foo" of table "foo.foo"
// UPDATE foo.x SET foo.x = 3 -- invalid, no column "foo" in table "x" of db "foo"
//

// QualifiedName is the AST node that holds a SQL name. Initially
// its Target member is an instance of UnresolvedName. Upon the first
// Normalize call, this is replaced by an instance of one of the
// resolved name structs.
type QualifiedName struct {
	Target NameTarget

	// We preserve the "original" string representation (before normalization)
	// for debugging and to accelerate calls to String().
	origString string
}

var _ VariableExpr = &QualifiedName{}

// NameTarget is the interface for all the concrete structs referred
// to by QualifiedName.
type NameTarget interface {
	fmt.Stringer
	NodeFormatter
	nameTarget()
}

var _ NameTarget = UnresolvedName{}
var _ NameTarget = UnqualifiedStar{}
var _ NameTarget = DatabaseName{}
var _ NameTarget = &AllTablesSelector{}
var _ NameTarget = &FunctionName{}
var _ NameTarget = &TableName{}
var _ NameTarget = &AllColumnsSelector{}
var _ NameTarget = &ColumnItem{}

func (u UnresolvedName) String() string      { return AsString(u) }
func (u UnqualifiedStar) String() string     { return AsString(u) }
func (d DatabaseName) String() string        { return AsString(d) }
func (fn *FunctionName) String() string      { return AsString(fn) }
func (t *TableName) String() string          { return AsString(t) }
func (at *AllTablesSelector) String() string { return AsString(at) }
func (a *AllColumnsSelector) String() string { return AsString(a) }
func (c *ColumnItem) String() string         { return AsString(c) }

// UnresolvedName holds the initial syntax of a QualifiedName as
// determined during parsing.
type UnresolvedName NameParts

// Format implements the NodeFormatter interface.
func (u UnresolvedName) Format(buf *bytes.Buffer, f FmtFlags) { NameParts(u).Format(buf, f) }
func (UnresolvedName) nameTarget()                            {}

// NewUnresolvedName creates a new QualifiedName
// holding an unresolved (non-normalized) name.
func NewUnresolvedName(n string) *QualifiedName {
	return &QualifiedName{Target: UnresolvedName{Name(n)}}
}

// NewUnresolvedNameWithSuffix creates a new QualifiedName
// holding an unresolved (non-normalized) name, followed
// by an existing unresolved suffix.
func NewUnresolvedNameWithSuffix(n string, u UnresolvedName) *QualifiedName {
	return &QualifiedName{Target: append(UnresolvedName{Name(n)}, u...)}
}

// NewUnresolvedCompoundName creates a new QualifiedName
// holding an unresolved (non-normalized) name, followed
// by a single existing unresolved part.
func NewUnresolvedCompoundName(n string, p NamePart) *QualifiedName {
	return &QualifiedName{Target: UnresolvedName{Name(n), p}}
}

// UnqualifiedStar corresponds to a standalone '*' in an expression.
type UnqualifiedStar struct{}

// Format implements the NodeFormatter interface.
func (UnqualifiedStar) Format(buf *bytes.Buffer, _ FmtFlags) { buf.WriteByte('*') }
func (UnqualifiedStar) nameTarget()                          {}

// DatabaseName corresponds to a database identifier in a
// CREATE/DROP/RENAME DATABASE statement.
type DatabaseName struct{ Name }

// Format implements the NodeFormatter interface.
func (d DatabaseName) Format(buf *bytes.Buffer, f FmtFlags) { d.Name.Format(buf, f) }
func (DatabaseName) nameTarget()                            {}

/* UNUSED FOR NOW
// NormalizeDatabaseName identifies a simple database name.
func (node *QualifiedName) NormalizeDatabaseName() (DatabaseName, error) {
	if d, ok := node.Target.(DatabaseName); ok {
		return d, nil
	}

	n, ok := node.Target.(UnresolvedName)
	if !ok {
		return DatabaseName{}, fmt.Errorf("internal error: database name already resolved: %+v (%T)", node.Target, node.Target)
	}

	if len(n) != 1 {
		return DatabaseName{}, fmt.Errorf("invalid database name: %q", n)
	}

	name, ok := n[0].(Name)
	if !ok {
		return DatabaseName{}, fmt.Errorf("invalid database name: %q", n)
	}

	if len(name) == 0 {
		return DatabaseName{}, fmt.Errorf("empty database name: %q", n)
	}

	res := DatabaseName{Name: name}
	node.Target = res

	return res, nil
}
*/

// FunctionName corresponds to the name of a function in a
// FunctionExpr node.
type FunctionName struct {
	FunctionName Name
	Context      NameParts
}

// Format implements the NodeFormatter interface.
func (fn *FunctionName) Format(buf *bytes.Buffer, f FmtFlags) {
	if len(fn.Context) > 0 {
		FormatNode(buf, f, fn.Context)
		buf.WriteByte('.')
	}
	FormatNode(buf, f, fn.FunctionName)
}
func (fn *FunctionName) nameTarget() {}

// NewQualifiedFunctionName creates a new QualifiedName
// holding a pre-normalized function name.
func NewQualifiedFunctionName(n string) *QualifiedName {
	return &QualifiedName{Target: &FunctionName{FunctionName: Name(n)}}
}

// NormalizeFunctionName normalizes the QualifiedName to a function
// name if not done already, then returns the resulting FunctionName.
func (node *QualifiedName) NormalizeFunctionName() (*FunctionName, error) {
	if f, ok := node.Target.(*FunctionName); ok {
		return f, nil
	}

	n, ok := node.Target.(UnresolvedName)
	if !ok {
		panic(fmt.Sprintf("function name already resolved: %+v (%T)", node.Target, node.Target))
	}

	if len(n) == 0 {
		return nil, fmt.Errorf("invalid function name: %q", n)
	}

	name, ok := n[len(n)-1].(Name)
	if !ok {
		return nil, fmt.Errorf("invalid function name: %q", n)
	}

	if len(name) == 0 {
		return nil, fmt.Errorf("empty function name: %q", n)
	}

	res := &FunctionName{FunctionName: name, Context: NameParts(n[:len(n)-1])}
	node.Target = res
	return res, nil
}

// TableName corresponds to the name of a table in a FROM clause,
// INSERT or UPDATE statement (and possibly other places).
type TableName struct {
	DatabaseName Name
	TableName    Name
}

// EmptyTableName defines TableName for an anonymous table.
var EmptyTableName = &TableName{}

// Format implements the NodeFormatter interface.
func (t *TableName) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, t.DatabaseName)
	buf.WriteByte('.')
	FormatNode(buf, f, t.TableName)
}
func (*TableName) nameTarget() {}

// NormalizeTableNameWithDatabaseName normalizes the QualifiedName to a table
// name if not done already, then returns the resulting TableName with
// the given database name set if not already.
func (node *QualifiedName) NormalizeTableNameWithDatabaseName(database string) (*TableName, error) {
	t, err := node.NormalizeTableName()
	if err != nil {
		return nil, err
	}
	err = t.QualifyWithDatabase(database)
	return t, err
}

// NormalizeTableName normalizes the QualifiedName to a table
// name if not done already, then returns the resulting TableName.
func (node *QualifiedName) NormalizeTableName() (*TableName, error) {
	if t, ok := node.Target.(*TableName); ok {
		return t, nil
	}

	n, ok := node.Target.(UnresolvedName)
	if !ok {
		panic(fmt.Sprintf("table name already resolved: %+v (%T)", node.Target, node.Target))
	}

	if len(n) == 0 || len(n) > 2 {
		return nil, fmt.Errorf("invalid table name: %q", n)
	}

	name, ok := n[len(n)-1].(Name)
	if !ok {
		return nil, fmt.Errorf("invalid table name: %q", n)
	}

	if len(name) == 0 {
		return nil, fmt.Errorf("empty table name: %q", n)
	}

	var db Name
	if len(n) > 1 {
		db, ok = n[0].(Name)
		if !ok {
			return nil, fmt.Errorf("invalid database name: %q", n[0])
		}

		if len(db) == 0 {
			return nil, fmt.Errorf("empty database name: %q", n)
		}
	}

	node.setString()
	res := &TableName{DatabaseName: db, TableName: name}
	node.Target = res

	return res, nil
}

// QualifyWithDatabase adds an indirection for the database, if it's missing.
// It transforms:
// table       -> database.table
// table@index -> database.table@index
func (t *TableName) QualifyWithDatabase(database string) error {
	if t.DatabaseName != "" {
		return nil
	}
	if database == "" {
		return fmt.Errorf("no database specified: %q", t)
	}
	t.DatabaseName = Name(database)
	return nil
}

// TableNames represents a comma separated list (see the Format method)
// of table names.
type TableNames []*TableName

// Format implements the NodeFormatter interface.
func (t TableNames) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, t := range t {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, t)
	}
}
func (t TableNames) String() string { return AsString(t) }

// AllTablesSelector corresponds to a selection of all
// tables in a database, e.g. when used with GRANT.
type AllTablesSelector struct{ Database Name }

func (*AllTablesSelector) nameTarget() {}

// Format implements the NodeFormatter interface.
func (at *AllTablesSelector) Format(buf *bytes.Buffer, f FmtFlags) {
	if at.Database != "" {
		FormatNode(buf, f, at.Database)
		buf.WriteByte('.')
	}
	buf.WriteByte('*')
}

// NormalizeTablePattern normalizes the QualifiedName to a table
// pattern (TableName or AllTablesSelector) if not done already.
func (node *QualifiedName) NormalizeTablePattern() error {
	switch node.Target.(type) {
	case *TableName, *AllTablesSelector:
		return nil
	}

	n, ok := node.Target.(UnresolvedName)
	if !ok {
		panic(fmt.Sprintf("table pattern already resolved: %+v (%T)", node.Target, node.Target))
	}

	if len(n) == 0 || len(n) > 2 {
		return fmt.Errorf("invalid table name: %q", n)
	}

	var db Name
	if len(n) > 1 {
		db, ok = n[0].(Name)
		if !ok {
			return fmt.Errorf("invalid database name: %q", n[0])
		}
	}

	var res NameTarget
	switch t := n[len(n)-1].(type) {
	case UnqualifiedStar:
		res = &AllTablesSelector{Database: db}
	case Name:
		if len(t) == 0 {
			return fmt.Errorf("empty table name: %q", n)
		}
		res = &TableName{DatabaseName: db, TableName: t}
	}

	node.Target = res
	return nil
}

// QualifyWithDatabase adds an indirection for the database, if it's missing.
// It transforms:  * -> database.*
func (at *AllTablesSelector) QualifyWithDatabase(database string) error {
	if at.Database != "" {
		return nil
	}
	if database == "" {
		return fmt.Errorf("no database specified: %q", at)
	}
	at.Database = Name(database)
	return nil
}

// AllColumnsSelector corresponds to a selection of all
// columns in a table when used in a SELECT clause.
// (e.g. `table.*`)
type AllColumnsSelector struct {
	TableName
}

// Format implements the NodeFormatter interface.
func (a *AllColumnsSelector) Format(buf *bytes.Buffer, f FmtFlags) {
	if a.TableName.DatabaseName != "" {
		FormatNode(buf, f, a.TableName.DatabaseName)
		buf.WriteByte('.')
	}
	FormatNode(buf, f, a.TableName.TableName)
	buf.WriteString(".*")
}
func (*AllColumnsSelector) nameTarget() {}

// ColumnItem corresponds to the name of a column or sub-item
// of a column in an expression.
type ColumnItem struct {
	TableName
	ColumnName Name
	Selector   NameParts
}

// Format implements the NodeFormatter interface.
func (c *ColumnItem) Format(buf *bytes.Buffer, f FmtFlags) {
	if c.TableName.TableName != "" {
		if c.TableName.DatabaseName != "" {
			FormatNode(buf, f, c.TableName.DatabaseName)
			buf.WriteByte('.')
		}
		FormatNode(buf, f, c.TableName.TableName)
		buf.WriteByte('.')
	}
	FormatNode(buf, f, c.ColumnName)
	if len(c.Selector) > 0 {
		if _, ok := c.Selector[0].(*ArraySubscript); !ok {
			buf.WriteByte('.')
		}
		FormatNode(buf, f, c.Selector)
	}
}
func (c *ColumnItem) nameTarget() {}

// NamePart is the interface for the sub-parts of an UnresolvedName or
// the Selector/Context members of ColumnItem and FunctionName.
type NamePart interface {
	NodeFormatter
	namePart()
}

var _ NamePart = Name("")
var _ NamePart = &ArraySubscript{}
var _ NamePart = UnqualifiedStar{}

// We reuse Name as an unresolved name part.
func (Name) namePart() {}

// We also reuse the UnqualifiedStar as an unresolved name part.
func (UnqualifiedStar) namePart() {}

// ArraySubscript corresponds to the syntax `<name>[ ... ]`.
type ArraySubscript struct {
	Begin Expr
	End   Expr
}

// Format implements the NodeFormatter interface.
func (a *ArraySubscript) Format(buf *bytes.Buffer, f FmtFlags) {
	buf.WriteByte('[')
	FormatNode(buf, f, a.Begin)
	if a.End != nil {
		buf.WriteByte(':')
		FormatNode(buf, f, a.End)
	}
	buf.WriteByte(']')
}
func (a *ArraySubscript) namePart() {}

// NameParts represents a combination of names with array and
// sub-field subscripts.
type NameParts []NamePart

// Format implements the NodeFormatter interface.
func (l NameParts) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, p := range l {
		_, isArraySubscript := p.(*ArraySubscript)
		if !isArraySubscript && i > 0 {
			buf.WriteByte('.')
		}
		FormatNode(buf, f, p)
	}
}

// NormalizeNameInExpr normalizes a QualifiedName for all the forms it can have
// inside an expression context.
func (node *QualifiedName) NormalizeNameInExpr() error {
	var n UnresolvedName
	switch t := node.Target.(type) {
	case *ColumnItem, *AllColumnsSelector, UnqualifiedStar:
		// Already normalized
		return nil
	case UnresolvedName:
		n = t
		break
	default:
		panic(fmt.Sprintf("name already resolved: %+v (%T)", node.Target, node.Target))
	}

	if len(n) == 0 {
		return fmt.Errorf("invalid name: %q", n)
	}

	node.setString()

	if s, isStar := n[len(n)-1].(UnqualifiedStar); isStar {
		// Either a single '*' or a name of the form [db.]table.*

		if len(n) == 1 {
			node.Target = s
			return nil
		}

		// The prefix before the star must be a valid table name.  Use the
		// existing normalize code to enforce that, since we can reuse the
		// resulting TableName.
		node.Target = n[:len(n)-1]
		t, err := node.NormalizeTableName()
		if err != nil {
			node.Target = n
			return err
		}

		node.Target = &AllColumnsSelector{*t}
		return nil
	}

	// In the remaining case, we have an optional table name prefix,
	// followed by a column name, followed by some additional indirections.
	// See the initial comment in this file for the particular syntax.

	// Find the first array subscript, if any.
	i := len(n)
	for j, p := range n {
		if _, ok := p.(*ArraySubscript); ok {
			i = j
			break
		}
	}
	// The element at position i - 1 must be the column name.
	if i == 0 {
		return fmt.Errorf("invalid column name: %q", n)
	}
	colName, ok := n[i-1].(Name)
	if !ok {
		return fmt.Errorf("invalid column name: %q", n[:i])
	}
	if len(colName) == 0 {
		return fmt.Errorf("empty column name: %q", n)
	}

	// Everything afterwards is the selector.
	res := &ColumnItem{ColumnName: colName, Selector: NameParts(n[i:])}

	if i-1 > 0 {
		// What's before must be a valid table name.  Use the existing
		// normalize code to enforce that, since we can reuse the
		// resulting TableName.
		node.Target = n[:i-1]
		t, err := node.NormalizeTableName()
		if err != nil {
			node.Target = n
			return err
		}
		res.TableName = *t
	}

	node.Target = res
	return nil
}

// NormalizeUnqualifiedColumnItem normalizes a QualifiedName for all
// the forms it can have inside a context that requires an unqualified
// column item (e.g. UPDATE LHS).
func (node *QualifiedName) NormalizeUnqualifiedColumnItem() (*ColumnItem, error) {
	if c, ok := node.Target.(*ColumnItem); ok {
		if c.TableName.TableName != "" {
			return nil, fmt.Errorf("cannot use a table name prefix here: %q", c)
		}
		return c, nil
	}

	n, ok := node.Target.(UnresolvedName)
	if !ok {
		panic(fmt.Sprintf("name already resolved: %+v (%T)", node.Target, node.Target))
	}

	if len(n) == 0 {
		return nil, fmt.Errorf("invalid column name: %q", n)
	}

	colName, ok := n[0].(Name)
	if !ok {
		return nil, fmt.Errorf("invalid column name: %q", n)
	}

	if len(colName) == 0 {
		return nil, fmt.Errorf("empty column name: %q", n)
	}

	// Remainder is a selector.
	res := &ColumnItem{ColumnName: colName, Selector: NameParts(n[1:])}

	node.Target = res
	return res, nil
}

// ReturnType implements the TypedExpr interface.
func (node *QualifiedName) ReturnType() Datum {
	if qualifiedNameTypes == nil {
		return nil
	}
	return qualifiedNameTypes[node.String()]
}

// Variable implements the VariableExpr interface.
func (*QualifiedName) Variable() {}

var singletonStarName = QualifiedName{Target: UnqualifiedStar{}}

// StarExpr is a convenience function that represents an unqualified "*".
func StarExpr() *QualifiedName { return &singletonStarName }

// ClearString causes String to return the current (possibly normalized) name instead of the
// original name (used for testing).
func (node *QualifiedName) ClearString() {
	node.origString = ""
}

func (node *QualifiedName) setString() {
	// We preserve the representation pre-normalization.
	if node.origString != "" {
		return
	}
	node.origString = AsString(node.Target)
}

// Format implements the NodeFormatter interface.
func (node *QualifiedName) Format(buf *bytes.Buffer, _ FmtFlags) {
	node.setString()
	buf.WriteString(node.origString)
}

// QualifiedNames represents a comma separated list (see the Format method)
// of qualified names.
type QualifiedNames []*QualifiedName

// Format implements the NodeFormatter interface.
func (n QualifiedNames) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, e := range n {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, e)
	}
}

// TableNameWithIndex represents a "table@index", used in statements that
// specifically refer to an index.
type TableNameWithIndex struct {
	Table *QualifiedName
	Index Name
}

// Format implements the NodeFormatter interface.
func (n *TableNameWithIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, n.Table)
	buf.WriteByte('@')
	FormatNode(buf, f, n.Index)
}

// TableNameWithIndexList is a list of indexes.
type TableNameWithIndexList []*TableNameWithIndex

// Format implements the NodeFormatter interface.
func (n TableNameWithIndexList) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, e := range n {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, e)
	}
}

// TableName asserts that the qualified name has been previously resolved as a table name.
func (node *QualifiedName) TableName() *TableName {
	return node.Target.(*TableName)
}

// Table retrieves the unqualified table name.
func (t *TableName) Table() string {
	return string(t.TableName)
}

// Database retrieves the unqualified database name.
func (t *TableName) Database() string {
	return string(t.DatabaseName)
}

// FunctionName asserts that the qualified name has been previously resolved as a function name.
func (node *QualifiedName) FunctionName() *FunctionName {
	return node.Target.(*FunctionName)
}

// Function retrieves the unqualified function name.
func (fn *FunctionName) Function() string {
	return string(fn.FunctionName)
}

// ColumnItem asserts that the qualified name has been previously resolved as a column item reference.
func (node *QualifiedName) ColumnItem() *ColumnItem {
	return node.Target.(*ColumnItem)
}

// Column retrieves the unqualified column name.
func (c *ColumnItem) Column() string {
	return string(c.ColumnName)
}
