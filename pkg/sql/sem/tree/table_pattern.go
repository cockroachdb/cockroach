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

// Table patterns are used by e.g. GRANT statements, to designate
// zero, one or more table names.  For example:
//   GRANT ... ON foo ...
//   GRANT ... ON * ...
//   GRANT ... ON db.*  ...
//
// The other syntax nodes hold a TablePattern reference.  This is
// initially populated during parsing with an UnresolvedName, which
// can be transformed to either a TableName (single name) or
// AllTablesSelector instance (all tables of a given database) using
// NormalizeTablePattern().

// TablePattern is the common interface to UnresolvedName, TableName
// and AllTablesSelector.
type TablePattern interface {
	fmt.Stringer
	NodeFormatter

	NormalizeTablePattern() (TablePattern, error)
}

// DatabaseQualifiable identifiers can be qualifed with a database name.
type DatabaseQualifiable interface {
	QualifyWithDatabase(database string) error
}

var _ TablePattern = &UnresolvedName{}
var _ TablePattern = &TableName{}
var _ TablePattern = &AllTablesSelector{}
var _ DatabaseQualifiable = &AllTablesSelector{}
var _ DatabaseQualifiable = &TableName{}

// NormalizeTablePattern resolves an UnresolvedName to either a
// TableName or AllTablesSelector.
func (n *UnresolvedName) NormalizeTablePattern() (TablePattern, error) {
	if n.NumParts > 2 {
		// db/cat.schema.table: We don't support those just yet.
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
			"invalid table name: %q", ErrString(n))
	}
	firstCheck := 0
	if n.Star {
		firstCheck = 1
	}
	for i := firstCheck; i < n.NumParts; i++ {
		if len(n.Parts[i]) == 0 {
			return nil, pgerror.NewErrorf(pgerror.CodeInvalidNameError,
				"invalid table name: %q", ErrString(n))
		}
	}

	if n.Star {
		return &AllTablesSelector{
			Schema:         Name(n.Parts[1]),
			ExplicitSchema: n.NumParts >= 2,
		}, nil
	}
	return &TableName{
		SchemaName:     Name(n.Parts[1]),
		TableName:      Name(n.Parts[0]),
		ExplicitSchema: n.NumParts >= 2,
	}, nil
}

// NormalizeTablePattern implements the TablePattern interface.
func (t *TableName) NormalizeTablePattern() (TablePattern, error) { return t, nil }

// AllTablesSelector corresponds to a selection of all
// tables in a database, e.g. when used with GRANT.
type AllTablesSelector struct {
	Schema         Name
	ExplicitSchema bool
}

// Format implements the NodeFormatter interface.
func (at *AllTablesSelector) Format(ctx *FmtCtx) {
	if at.ExplicitSchema {
		ctx.FormatNode(&at.Schema)
		ctx.WriteByte('.')
	}
	ctx.WriteByte('*')
}
func (at *AllTablesSelector) String() string { return AsString(at) }

// NormalizeTablePattern implements the TablePattern interface.
func (at *AllTablesSelector) NormalizeTablePattern() (TablePattern, error) { return at, nil }

// QualifyWithDatabase adds an indirection for the database, if it's missing.
// It transforms:  * -> database.*
func (at *AllTablesSelector) QualifyWithDatabase(database string) error {
	if at.ExplicitSchema {
		// Database already qualified, nothing to do.
		return nil
	}
	if database == "" {
		return pgerror.NewErrorf(pgerror.CodeInvalidDatabaseDefinitionError, "no database specified: %q", at)
	}
	at.Schema = Name(database)
	return nil
}

// TablePatterns implement a comma-separated list of table patterns.
// Used by e.g. the GRANT statement.
type TablePatterns []TablePattern

// Format implements the NodeFormatter interface.
func (tt *TablePatterns) Format(ctx *FmtCtx) {
	for i, t := range *tt {
		if i > 0 {
			ctx.WriteString(", ")
		}
		ctx.FormatNode(t)
	}
}
