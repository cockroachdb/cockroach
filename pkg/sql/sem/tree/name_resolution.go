// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
)

// This file contains the two major components to name resolution:
//
// - classification algorithms. These are used when two different
//   semantic constructs can appear in the same position in the SQL grammar.
//   For example, table patterns and table names in GRANT.
//
// - resolution algorithms. These are used to map an unresolved name
//   or pattern to something looked up from the database.
//

// classifyTablePattern distinguishes between a TableName (last name
// part is a table name) and an AllTablesSelector.
// Used e.g. for GRANT.
func classifyTablePattern(n *UnresolvedName) (TablePattern, error) {
	if n.NumParts < 1 || n.NumParts > 3 {
		return nil, newInvTableNameError(n)
	}
	// Check that all the parts specified are not empty.
	firstCheck := 0
	if n.Star {
		firstCheck = 1
	}
	// It's OK if the catalog name is empty.
	// We allow this in e.g. `select * from "".crdb_internal.tables`.
	lastCheck := n.NumParts
	if lastCheck > 2 {
		lastCheck = 2
	}
	for i := firstCheck; i < lastCheck; i++ {
		if len(n.Parts[i]) == 0 {
			return nil, newInvTableNameError(n)
		}
	}

	// Construct the result.
	if n.Star {
		return &AllTablesSelector{makeObjectNamePrefixFromUnresolvedName(n)}, nil
	}
	tb := makeTableNameFromUnresolvedName(n)
	return &tb, nil
}

// classifyColumnItem distinguishes between a ColumnItem (last name
// part is a column name) and an AllColumnsSelector.
//
// Used e.g. in SELECT clauses.
func classifyColumnItem(n *UnresolvedName) (VarName, error) {
	if n.NumParts < 1 || n.NumParts > 4 {
		return nil, newInvColRef(n)
	}

	// Check that all the parts specified are not empty.
	firstCheck := 0
	if n.Star {
		firstCheck = 1
	}
	// It's OK if the catalog name is empty.
	// We allow this in e.g.
	// `select "".crdb_internal.tables.table_id from "".crdb_internal.tables`.
	lastCheck := n.NumParts
	if lastCheck > 3 {
		lastCheck = 3
	}
	for i := firstCheck; i < lastCheck; i++ {
		if len(n.Parts[i]) == 0 {
			return nil, newInvColRef(n)
		}
	}

	// Construct the result.
	var tn *UnresolvedObjectName
	if n.NumParts > 1 {
		var err error
		tn, err = NewUnresolvedObjectName(
			n.NumParts-1,
			[3]string{n.Parts[1], n.Parts[2], n.Parts[3]},
			NoAnnotation,
		)
		if err != nil {
			return nil, err
		}
	}
	if n.Star {
		return &AllColumnsSelector{TableName: tn}, nil
	}
	return &ColumnItem{TableName: tn, ColumnName: Name(n.Parts[0])}, nil
}

// Resolution algorithms follow.

const (
	// PublicSchema is the name of the physical schema in every
	// database/catalog.
	PublicSchema string = catconstants.PublicSchemaName
	// PublicSchemaName is the same, typed as Name.
	PublicSchemaName Name = Name(PublicSchema)
)

// QualifiedNameResolver is the helper interface to resolve qualified
// table names given an ID and the required table kind, as well as the
// current database to determine whether or not to include the
// database in the qualification.
type QualifiedNameResolver interface {
	GetQualifiedTableNameByID(ctx context.Context, id int64, requiredType RequiredTableKind) (*TableName, error)
	CurrentDatabase() string
}

// SearchPath encapsulates the ordered list of schemas in the current database
// to search during name resolution.
type SearchPath interface {
	// NumElements returns the number of elements in the SearchPath.
	NumElements() int

	// GetSchema returns the schema at the ord offset in the SearchPath.
	// Note that it will return the empty string if the ordinal is out of range.
	GetSchema(ord int) string
}

// EmptySearchPath is a SearchPath with no members.
var EmptySearchPath SearchPath = emptySearchPath{}

type emptySearchPath struct{}

func (emptySearchPath) NumElements() int       { return 0 }
func (emptySearchPath) GetSchema(i int) string { return "" }

func newInvColRef(n *UnresolvedName) error {
	return pgerror.NewWithDepthf(1, pgcode.InvalidColumnReference,
		"invalid column name: %s", n)
}

func newInvTableNameError(n fmt.Stringer) error {
	return pgerror.NewWithDepthf(1, pgcode.InvalidName,
		"invalid table name: %s", n)
}

// RequiredTableKind controls what kind of TableDescriptor backed object is
// requested to be resolved.
type RequiredTableKind byte

// RequiredTableKind options have descriptive names.
const (
	ResolveAnyTableKind RequiredTableKind = iota
	ResolveRequireTableDesc
	ResolveRequireViewDesc
	ResolveRequireTableOrViewDesc
	ResolveRequireSequenceDesc
)

var requiredTypeNames = [...]string{
	ResolveAnyTableKind:           "any",
	ResolveRequireTableDesc:       "table",
	ResolveRequireViewDesc:        "view",
	ResolveRequireTableOrViewDesc: "table or view",
	ResolveRequireSequenceDesc:    "sequence",
}

func (r RequiredTableKind) String() string {
	return requiredTypeNames[r]
}
