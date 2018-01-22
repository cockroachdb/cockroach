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

package tree

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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
func (nt *NormalizableTableName) Format(ctx *FmtCtx) {
	if ctx.tableNameFormatter != nil {
		ctx.tableNameFormatter(ctx, nt)
	} else {
		ctx.FormatNode(nt.TableNameReference)
	}
}
func (nt *NormalizableTableName) String() string { return AsString(nt) }

// Normalize checks if the table name is already normalized and
// normalizes it as necessary.
func (nt *NormalizableTableName) Normalize() (*TableName, error) {
	switch t := nt.TableNameReference.(type) {
	case *TableName:
		return t, nil
	case *UnresolvedName:
		tn, err := t.NormalizeTableName()
		if err != nil {
			return nil, err
		}
		nt.TableNameReference = tn
		return tn, nil
	default:
		panic(fmt.Sprintf("unsupported table name reference: %+v (%T)",
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
	PrefixName   Name
	DatabaseName Name
	TableName    Name

	// OmitDBNameDuringFormatting, when set to true, causes the
	// String()/Format() methods to omit the database name even if one
	// is set. This is used to ensure that pretty-printing
	// a TableName normalized from a parser input yields back
	// the original syntax.
	OmitDBNameDuringFormatting bool

	// PrefixOriginallySpecified indicates whether a prefix was
	// explicitly indicated in the input syntax.
	PrefixOriginallySpecified bool
}

// Format implements the NodeFormatter interface.
func (t *TableName) Format(ctx *FmtCtx) {
	f := ctx.flags
	if !t.OmitDBNameDuringFormatting ||
		f.HasFlags(FmtAlwaysQualifyTableNames) || ctx.tableNameFormatter != nil {
		if t.PrefixOriginallySpecified {
			ctx.FormatNode(&t.PrefixName)
			ctx.WriteByte('.')
		}
		ctx.FormatNode(&t.DatabaseName)
		ctx.WriteByte('.')
	}
	ctx.FormatNode(&t.TableName)
}
func (t *TableName) String() string { return AsString(t) }

// NormalizeTableName implements the TableNameReference interface.
func (t *TableName) NormalizeTableName() (*TableName, error) { return t, nil }

// Table retrieves the unqualified table name.
func (t *TableName) Table() string {
	return string(t.TableName)
}

// Database retrieves the unqualified database name.
func (t *TableName) Database() string {
	return string(t.DatabaseName)
}

// NewInvalidNameErrorf initializes an error carrying the pg code CodeInvalidNameError.
func NewInvalidNameErrorf(fmt string, args ...interface{}) error {
	return pgerror.NewErrorf(pgerror.CodeInvalidNameError, fmt, args...)
}

// normalizeTableNameAsValue transforms an UnresolvedName to a TableName.
// The resulting TableName may lack a db qualification. This is
// valid if e.g. the name refers to a in-query table alias
// (AS) or is qualified later using the QualifyWithDatabase method.
func (n *UnresolvedName) normalizeTableNameAsValue() (res TableName, err error) {
	if len(*n) == 0 || len(*n) > 3 {
		return res, NewInvalidNameErrorf("invalid table name: %q", ErrString(n))
	}

	name, ok := (*n)[len(*n)-1].(*Name)
	if !ok {
		return res, NewInvalidNameErrorf("invalid table name: %q", ErrString(n))
	}

	if len(*name) == 0 {
		return res, NewInvalidNameErrorf("empty table name: %q", ErrString(n))
	}

	res = TableName{TableName: *name, OmitDBNameDuringFormatting: true}

	if len(*n) > 1 {
		ndb, ok := (*n)[len(*n)-2].(*Name)
		if !ok {
			return res, NewInvalidNameErrorf("invalid database name: %q", ErrString((*n)[len(*n)-2]))
		}
		res.DatabaseName = *ndb

		if len(res.DatabaseName) == 0 {
			return res, NewInvalidNameErrorf("empty database name: %q", ErrString(n))
		}

		res.OmitDBNameDuringFormatting = false

		if len(*n) > 2 {
			pn, ok := (*n)[len(*n)-3].(*Name)
			if !ok {
				return res, NewInvalidNameErrorf("invalid prefix: %q", ErrString((*n)[len(*n)-3]))
			}
			res.PrefixName = *pn

			res.PrefixOriginallySpecified = true
		}
	}

	return res, nil
}

// NormalizeTableName implements the TableNameReference interface.
func (n *UnresolvedName) NormalizeTableName() (*TableName, error) {
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
	if !t.OmitDBNameDuringFormatting {
		return nil
	}
	if database == "" {
		return pgerror.NewErrorf(pgerror.CodeUndefinedTableError, "no database specified: %q", t)
	}
	t.DatabaseName = Name(database)
	return nil
}

// TableNames represents a comma separated list (see the Format method)
// of table names.
type TableNames []TableName

// Format implements the NodeFormatter interface.
func (ts *TableNames) Format(ctx *FmtCtx) {
	for i := range *ts {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(&(*ts)[i])
	}
}
func (ts *TableNames) String() string { return AsString(ts) }

// TableNameReferences corresponds to a comma-delimited
// list of table name references.
type TableNameReferences []TableNameReference

// Format implements the NodeFormatter interface.
func (t *TableNameReferences) Format(ctx *FmtCtx) {
	for i, tr := range *t {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(tr)
	}
}

// TableNameWithIndex represents a "table@index", used in statements that
// specifically refer to an index.
type TableNameWithIndex struct {
	Table NormalizableTableName
	Index UnrestrictedName

	// SearchTable indicates that we have just an index (no table name); we will
	// need to search for a table that has an index with the given name.
	//
	// To allow schema-qualified index names in this case, the index is actually
	// specified in Table as the table name, and Index is empty.
	SearchTable bool
}

// Format implements the NodeFormatter interface.
func (n *TableNameWithIndex) Format(ctx *FmtCtx) {
	ctx.FormatNode(&n.Table)
	if n.Index != "" {
		ctx.WriteByte('@')
		ctx.FormatNode(&n.Index)
	}
}

func (n *TableNameWithIndex) String() string { return AsString(n) }

// TableNameWithIndexList is a list of indexes.
type TableNameWithIndexList []*TableNameWithIndex

// Format implements the NodeFormatter interface.
func (n *TableNameWithIndexList) Format(ctx *FmtCtx) {
	for i, e := range *n {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(e)
	}
}
