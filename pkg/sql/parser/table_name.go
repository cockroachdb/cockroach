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

// Table names are used in statements like CREATE TABLE,
// INSERT INTO, etc.
// General syntax:
//    [ <database-name> '.' ] <table-name>
//
// The other syntax nodes hold a mutable NormalizableTableName
// attribute.  This is populated during parsing with an
// UnresolvedName, and gets assigned an actual TableName upon the first
// call to its Normalize() method.

// NormalizableTableName implements an editable table name.
type NormalizableTableName struct {
	TableNameReference
}

// Format implements the NodeFormatter interface.
func (nt NormalizableTableName) Format(buf *bytes.Buffer, f FmtFlags) {
	tnr := nt.TableNameReference
	if f.tableNameNormalizer != nil {
		if tn := f.tableNameNormalizer(&nt); tn != nil {
			tnr = tn
		}
	}
	tnr.Format(buf, f)
}
func (nt NormalizableTableName) String() string { return AsString(nt) }

// Normalize checks if the table name is already normalized and
// normalizes it as necessary.
func (nt *NormalizableTableName) Normalize() (*TableName, error) {
	switch t := nt.TableNameReference.(type) {
	case *TableName:
		return t, nil
	case UnresolvedName:
		tn, err := t.NormalizeTableName()
		if err != nil {
			return nil, err
		}
		nt.TableNameReference = tn
		return tn, nil
	default:
		panic(fmt.Sprintf("unsupported function name: %+v (%T)",
			nt.TableNameReference, nt.TableNameReference))
	}
}

// NormalizeWithDatabaseName combines Normalize and QualifyWithDatabase.
func (nt *NormalizableTableName) NormalizeWithDatabaseName(database string) (*TableName, error) {
	tn, err := nt.Normalize()
	if err != nil {
		return nil, err
	}
	if err := tn.QualifyWithDatabase(database); err != nil {
		return nil, err
	}
	return tn, nil
}

// TableName asserts that the table name has been previously normalized.
func (nt *NormalizableTableName) TableName() *TableName {
	return nt.TableNameReference.(*TableName)
}

// tableExpr implements the TableExpr interface.
func (*NormalizableTableName) tableExpr() {}

// TableNameReference implements the editable cell of a TableExpr that
// refers to a single table.
type TableNameReference interface {
	fmt.Stringer
	NodeFormatter
	NormalizeTableName() (*TableName, error)
}

// TableName corresponds to the name of a table in a FROM clause,
// INSERT or UPDATE statement (and possibly other places).
type TableName struct {
	DatabaseName Name
	TableName    Name

	// dbNameOriginallyOmitted, when set to true, causes the
	// String()/Format() methods to omit the database name even if one
	// is set. This is used to ensure that pretty-printing
	// a TableName normalized from a parser input yields back
	// the original syntax.
	DBNameOriginallyOmitted bool
}

// Format implements the NodeFormatter interface.
func (t *TableName) Format(buf *bytes.Buffer, f FmtFlags) {
	if !t.DBNameOriginallyOmitted || f.tableNameNormalizer != nil {
		FormatNode(buf, f, t.DatabaseName)
		buf.WriteByte('.')
	}
	FormatNode(buf, f, t.TableName)
}
func (t *TableName) String() string { return AsString(t) }

// NormalizeTableName implements the TableNameReference interface.
func (t *TableName) NormalizeTableName() (*TableName, error) { return t, nil }

// NormalizedTableName normalize DatabaseName and TableName to lowercase
// and performs Unicode Normalization.
func (t *TableName) NormalizedTableName() TableName {
	return TableName{
		DatabaseName: Name(t.DatabaseName.Normalize()),
		TableName:    Name(t.TableName.Normalize()),
	}
}

// Table retrieves the unqualified table name.
func (t *TableName) Table() string {
	return string(t.TableName)
}

// Database retrieves the unqualified database name.
func (t *TableName) Database() string {
	return string(t.DatabaseName)
}

// normalizeTableNameAsValue transforms an UnresolvedName to a TableName.
// The resulting TableName may lack a db qualification. This is
// valid if e.g. the name refers to a in-query table alias
// (AS) or is qualified later using the QualifyWithDatabase method.
func (n UnresolvedName) normalizeTableNameAsValue() (TableName, error) {
	if len(n) == 0 || len(n) > 2 {
		return TableName{}, fmt.Errorf("invalid table name: %q", n)
	}

	name, ok := n[len(n)-1].(Name)
	if !ok {
		return TableName{}, fmt.Errorf("invalid table name: %q", n)
	}

	if len(name) == 0 {
		return TableName{}, fmt.Errorf("empty table name: %q", n)
	}

	res := TableName{TableName: name, DBNameOriginallyOmitted: true}

	if len(n) > 1 {
		res.DatabaseName, ok = n[0].(Name)
		if !ok {
			return TableName{}, fmt.Errorf("invalid database name: %q", n[0])
		}

		if len(res.DatabaseName) == 0 {
			return TableName{}, fmt.Errorf("empty database name: %q", n)
		}

		res.DBNameOriginallyOmitted = false
	}

	return res, nil
}

// NormalizeTableName implements the TableNameReference interface.
func (n UnresolvedName) NormalizeTableName() (*TableName, error) {
	tn, err := n.normalizeTableNameAsValue()
	if err != nil {
		return nil, err
	}
	return &tn, nil
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
type TableNames []TableName

// Format implements the NodeFormatter interface.
func (ts TableNames) Format(buf *bytes.Buffer, f FmtFlags) {
	for i := range ts {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, &ts[i])
	}
}
func (ts TableNames) String() string { return AsString(ts) }

// TableNameReferences corresponds to a comma-delimited
// list of table name references.
type TableNameReferences []TableNameReference

// Format implements the NodeFormatter interface.
func (t TableNameReferences) Format(buf *bytes.Buffer, f FmtFlags) {
	for i, t := range t {
		if i > 0 {
			buf.WriteString(", ")
		}
		FormatNode(buf, f, t)
	}
}

// TableNameWithIndex represents a "table@index", used in statements that
// specifically refer to an index.
type TableNameWithIndex struct {
	Table       NormalizableTableName
	Index       Name
	SearchTable bool
}

// Format implements the NodeFormatter interface.
func (n *TableNameWithIndex) Format(buf *bytes.Buffer, f FmtFlags) {
	FormatNode(buf, f, n.Table)
	if !n.SearchTable {
		buf.WriteByte('@')
		FormatNode(buf, f, n.Index)
	}
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
