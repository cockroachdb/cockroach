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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
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

// CommonLookupFlags is the common set of flags for the various accessor interfaces.
type CommonLookupFlags struct {
	// Required specifies that the lookup will return an error if the item is
	// not found.
	Required bool
	// RequireMutable specifies whether to return a mutable descriptor.
	RequireMutable bool
	// AvoidLeased, if set, avoid the leased (possibly stale) version of the
	// descriptor. It must be set when callers want consistent reads.
	AvoidLeased bool
	// IncludeOffline specifies if offline descriptors should be visible.
	IncludeOffline bool
	// IncludeOffline specifies if dropped descriptors should be visible.
	IncludeDropped bool
	// AvoidSynthetic specifies if the synthetic descriptors will be ignored.
	AvoidSynthetic bool
	// ParentID enforces that the resolved descriptor exist with this parent
	// ID if non-zero.
	ParentID catid.DescID
	// SkipHydration enforce descriptor lookups to skip hydration. This can be set
	// to true only when looking up descriptors when hydrating another group of
	// descriptors. The purpose is to avoid potential infinite recursion loop when
	// trying to hydrate a descriptor which would lead to hydration of another
	// descriptor depends on it.
	SkipHydration bool
}

// SchemaLookupFlags is the flag struct suitable for GetSchemaByName().
type SchemaLookupFlags = CommonLookupFlags

// DatabaseLookupFlags is the flag struct suitable for GetImmutableDatabaseByName().
type DatabaseLookupFlags = CommonLookupFlags

// DatabaseListFlags is the flag struct suitable for GetObjectNamesAndIDs().
type DatabaseListFlags = CommonLookupFlags

// DesiredObjectKind represents what kind of object is desired in a name
// resolution attempt.
type DesiredObjectKind byte

const (
	// TableObject is used when a table-like object is desired from resolution.
	TableObject DesiredObjectKind = iota
	// TypeObject is used when a type-like object is desired from resolution.
	TypeObject
)

// NewQualifiedObjectName returns an ObjectName of the corresponding kind.
// It is used mainly for constructing appropriate error messages depending
// on what kind of object was requested.
func NewQualifiedObjectName(catalog, schema, object string, kind DesiredObjectKind) ObjectName {
	switch kind {
	case TableObject:
		name := MakeTableNameWithSchema(Name(catalog), Name(schema), Name(object))
		return &name
	case TypeObject:
		name := MakeQualifiedTypeName(catalog, schema, object)
		return &name
	}
	return nil
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

// ObjectLookupFlags is the flag struct suitable for GetObjectByName().
type ObjectLookupFlags struct {
	CommonLookupFlags
	AllowWithoutPrimaryKey bool
	// Control what type of object is being requested.
	DesiredObjectKind DesiredObjectKind
	// Control what kind of table object is being requested. This field is
	// only respected when DesiredObjectKind is TableObject.
	DesiredTableDescKind RequiredTableKind
}

// ObjectLookupFlagsWithRequired returns a default ObjectLookupFlags object
// with just the Required flag true. This is a common configuration of the
// flags.
func ObjectLookupFlagsWithRequired() ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags: CommonLookupFlags{Required: true},
	}
}

// ObjectLookupFlagsWithRequiredTableKind returns an ObjectLookupFlags with
// Required set to true, and the DesiredTableDescKind set to the input kind.
func ObjectLookupFlagsWithRequiredTableKind(kind RequiredTableKind) ObjectLookupFlags {
	return ObjectLookupFlags{
		CommonLookupFlags:    CommonLookupFlags{Required: true},
		DesiredObjectKind:    TableObject,
		DesiredTableDescKind: kind,
	}
}

// IndexLookupFlags is the flag struct used for resolver.ResolveIndex() only.
type IndexLookupFlags struct {
	// Required, if true, indicates lookup can return nil index without
	// returning an error if the index does not exist.
	Required bool
	// IncludeNonActiveIndex expands the lookup to also consider
	// non-active indexes. By default, only active indexes are
	// considered.
	IncludeNonActiveIndex bool
	// IncludeOfflineTable expands the lookup to also consider offline
	// tables. By default, only online tables are considered.
	IncludeOfflineTable bool
}
