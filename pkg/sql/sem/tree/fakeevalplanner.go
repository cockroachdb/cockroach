// Copyright 2021 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/security"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

type FakeEvalPlanner struct{}

// ResolveOIDFromString is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) ResolveOIDFromString(
	ctx context.Context, resultType *types.T, toResolve *DString,
) (*DOid, error) {
	return NewDOid(0), nil
}

// ResolveOIDFromOID is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) ResolveOIDFromOID(
	ctx context.Context, resultType *types.T, toResolve *DOid,
) (*DOid, error) {
	return NewDOid(toResolve.DInt), nil
}

// UnsafeUpsertDescriptor is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) UnsafeUpsertDescriptor(
	ctx context.Context, descID int64, encodedDescriptor []byte, force bool,
) error {
	return nil
}

// GetImmutableTableInterfaceByID is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) GetImmutableTableInterfaceByID(
	ctx context.Context, id int,
) (interface{}, error) {
	return nil, nil
}

// UnsafeDeleteDescriptor is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) UnsafeDeleteDescriptor(
	ctx context.Context, descID int64, force bool,
) error {
	return nil
}

// ForceDeleteTableData is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) ForceDeleteTableData(ctx context.Context, descID int64) error {
	return nil
}

// UnsafeUpsertNamespaceEntry is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) UnsafeUpsertNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID int64, name string, descID int64, force bool,
) error {
	return nil
}

// UnsafeDeleteNamespaceEntry is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) UnsafeDeleteNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID int64, name string, descID int64, force bool,
) error {
	return nil
}

// UserHasAdminRole is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) UserHasAdminRole(
	ctx context.Context, user security.SQLUsername,
) (bool, error) {
	return false, nil
}

// MemberOfWithAdminOption is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) MemberOfWithAdminOption(
	ctx context.Context, member security.SQLUsername,
) (map[security.SQLUsername]bool, error) {
	return nil, nil
}

// ExternalReadFile is part of the EvalPlanner interface.
func (*FakeEvalPlanner) ExternalReadFile(ctx context.Context, uri string) ([]byte, error) {
	return nil, nil
}

// ExternalWriteFile is part of the EvalPlanner interface.
func (*FakeEvalPlanner) ExternalWriteFile(ctx context.Context, uri string, content []byte) error {
	return nil
}

var _ EvalPlanner = &FakeEvalPlanner{}

var errEvalPlanner = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate scalar expressions using table lookups in this context")

// CurrentDatabaseRegionConfig is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) CurrentDatabaseRegionConfig(
	_ context.Context,
) (DatabaseRegionConfig, error) {
	return nil, nil
}

// ResetMultiRegionZoneConfigsForTable is part of the EvalDatabase
// interface.
func (ep *FakeEvalPlanner) ResetMultiRegionZoneConfigsForTable(_ context.Context, _ int64) error {
	return nil
}

// ResetMultiRegionZoneConfigsForDatabase is part of the EvalDatabase
// interface.
func (ep *FakeEvalPlanner) ResetMultiRegionZoneConfigsForDatabase(
	_ context.Context, _ int64,
) error {
	return nil
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return nil
}

// ParseQualifiedTableName is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) ParseQualifiedTableName(sql string) (*TableName, error) {
	return nil, nil
}

// SchemaExists is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) SchemaExists(ctx context.Context, dbName, scName string) (bool, error) {
	return false, nil
}

// IsTableVisible is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) IsTableVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, tableID oid.Oid,
) (bool, bool, error) {
	return false, false, nil
}

// IsTypeVisible is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) IsTypeVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, typeID oid.Oid,
) (bool, bool, error) {
	return false, false, nil
}

// HasPrivilege is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) HasPrivilege(
	ctx context.Context,
	specifier HasPrivilegeSpecifier,
	user security.SQLUsername,
	kind privilege.Kind,
) (bool, error) {
	return false, nil
}

// ResolveTableName is part of the EvalDatabase interface.
func (ep *FakeEvalPlanner) ResolveTableName(
	ctx context.Context, tn *TableName,
) (ID, error) {
	return 0, nil
}

// GetTypeFromValidSQLSyntax is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) GetTypeFromValidSQLSyntax(sql string) (*types.T, error) {
	return nil, nil
}

// EvalSubquery is part of the EvalPlanner interface.
func (ep *FakeEvalPlanner) EvalSubquery(expr *Subquery) (Datum, error) {
	return nil, nil
}

// ResolveTypeByOID implements the TypeReferenceResolver interface.
func (ep *FakeEvalPlanner) ResolveTypeByOID(_ context.Context, _ oid.Oid) (*types.T, error) {
	return nil, nil
}

// ResolveType implements the TypeReferenceResolver interface.
func (ep *FakeEvalPlanner) ResolveType(
	_ context.Context, _ *UnresolvedObjectName,
) (*types.T, error) {
	return nil, nil
}
