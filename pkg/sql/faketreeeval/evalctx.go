// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package faketreeeval provides fake implementations of tree eval interfaces.
package faketreeeval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// DummySequenceOperators implements the tree.SequenceOperators interface by
// returning errors.
type DummySequenceOperators struct{}

var _ tree.SequenceOperators = &DummySequenceOperators{}

var errSequenceOperators = unimplemented.NewWithIssue(42508,
	"cannot evaluate scalar expressions containing sequence operations in this context")

// GetSerialSequenceNameFromColumn is part of the tree.SequenceOperators interface.
func (so *DummySequenceOperators) GetSerialSequenceNameFromColumn(
	ctx context.Context, tn *tree.TableName, columnName tree.Name,
) (*tree.TableName, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// ParseQualifiedTableName is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// ResolveTableName is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// SchemaExists is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) SchemaExists(
	ctx context.Context, dbName, scName string,
) (bool, error) {
	return false, errors.WithStack(errSequenceOperators)
}

// IsTableVisible is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) IsTableVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, tableID oid.Oid,
) (bool, bool, error) {
	return false, false, errors.WithStack(errSequenceOperators)
}

// IsTypeVisible is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) IsTypeVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, typeID oid.Oid,
) (bool, bool, error) {
	return false, false, errors.WithStack(errEvalPlanner)
}

// HasAnyPrivilege is part of the tree.EvalDatabase interface.
func (so *DummySequenceOperators) HasAnyPrivilege(
	ctx context.Context,
	specifier tree.HasPrivilegeSpecifier,
	user security.SQLUsername,
	privs []privilege.Privilege,
) (tree.HasAnyPrivilegeResult, error) {
	return tree.HasNoPrivilege, errors.WithStack(errEvalPlanner)
}

// IncrementSequenceByID is part of the tree.SequenceOperators interface.
func (so *DummySequenceOperators) IncrementSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// GetLatestValueInSessionForSequenceByID implements the tree.SequenceOperators
// interface.
func (so *DummySequenceOperators) GetLatestValueInSessionForSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// SetSequenceValueByID implements the tree.SequenceOperators interface.
func (so *DummySequenceOperators) SetSequenceValueByID(
	ctx context.Context, seqID uint32, newVal int64, isCalled bool,
) error {
	return errors.WithStack(errSequenceOperators)
}

// DummyRegionOperator implements the tree.RegionOperator interface by
// returning errors.
type DummyRegionOperator struct{}

var _ tree.RegionOperator = &DummyRegionOperator{}

var errRegionOperator = unimplemented.NewWithIssue(42508,
	"cannot evaluate scalar expressions containing region operations in this context")

// CurrentDatabaseRegionConfig is part of the tree.EvalDatabase interface.
func (so *DummyRegionOperator) CurrentDatabaseRegionConfig(
	_ context.Context,
) (tree.DatabaseRegionConfig, error) {
	return nil, errors.WithStack(errRegionOperator)
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the tree.EvalDatabase interface.
func (so *DummyRegionOperator) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForTable is part of the tree.EvalDatabase
// interface.
func (so *DummyRegionOperator) ResetMultiRegionZoneConfigsForTable(
	_ context.Context, id int64,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForDatabase is part of the tree.EvalDatabase
// interface.
func (so *DummyRegionOperator) ResetMultiRegionZoneConfigsForDatabase(
	_ context.Context, id int64,
) error {
	return errors.WithStack(errRegionOperator)
}

// DummyEvalPlanner implements the tree.EvalPlanner interface by returning
// errors.
type DummyEvalPlanner struct{}

// ResolveOIDFromString is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) ResolveOIDFromString(
	ctx context.Context, resultType *types.T, toResolve *tree.DString,
) (*tree.DOid, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ResolveOIDFromOID is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) ResolveOIDFromOID(
	ctx context.Context, resultType *types.T, toResolve *tree.DOid,
) (*tree.DOid, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// UnsafeUpsertDescriptor is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) UnsafeUpsertDescriptor(
	ctx context.Context, descID int64, encodedDescriptor []byte, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// GetImmutableTableInterfaceByID is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) GetImmutableTableInterfaceByID(
	ctx context.Context, id int,
) (interface{}, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// UnsafeDeleteDescriptor is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) UnsafeDeleteDescriptor(
	ctx context.Context, descID int64, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// ForceDeleteTableData is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) ForceDeleteTableData(ctx context.Context, descID int64) error {
	return errors.WithStack(errEvalPlanner)
}

// UnsafeUpsertNamespaceEntry is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) UnsafeUpsertNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID int64, name string, descID int64, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// UnsafeDeleteNamespaceEntry is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) UnsafeDeleteNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID int64, name string, descID int64, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// UserHasAdminRole is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) UserHasAdminRole(
	ctx context.Context, user security.SQLUsername,
) (bool, error) {
	return false, errors.WithStack(errEvalPlanner)
}

// MemberOfWithAdminOption is part of the EvalPlanner interface.
func (ep *DummyEvalPlanner) MemberOfWithAdminOption(
	ctx context.Context, member security.SQLUsername,
) (map[security.SQLUsername]bool, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ExternalReadFile is part of the EvalPlanner interface.
func (*DummyEvalPlanner) ExternalReadFile(ctx context.Context, uri string) ([]byte, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ExternalWriteFile is part of the EvalPlanner interface.
func (*DummyEvalPlanner) ExternalWriteFile(ctx context.Context, uri string, content []byte) error {
	return errors.WithStack(errEvalPlanner)
}

// DecodeGist is part of the EvalPlanner interface.
func (*DummyEvalPlanner) DecodeGist(gist string) ([]string, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// SerializeSessionState is part of the EvalPlanner interface.
func (*DummyEvalPlanner) SerializeSessionState() (*tree.DBytes, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// DeserializeSessionState is part of the EvalPlanner interface.
func (*DummyEvalPlanner) DeserializeSessionState(token *tree.DBytes) (*tree.DBool, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// CreateSessionRevivalToken is part of the EvalPlanner interface.
func (*DummyEvalPlanner) CreateSessionRevivalToken() (*tree.DBytes, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ValidateSessionRevivalToken is part of the EvalPlanner interface.
func (*DummyEvalPlanner) ValidateSessionRevivalToken(token *tree.DBytes) (*tree.DBool, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// RevalidateUniqueConstraintsInCurrentDB is part of the EvalPlanner interface.
func (*DummyEvalPlanner) RevalidateUniqueConstraintsInCurrentDB(ctx context.Context) error {
	return errors.WithStack(errEvalPlanner)
}

// RevalidateUniqueConstraintsInTable is part of the EvalPlanner interface.
func (*DummyEvalPlanner) RevalidateUniqueConstraintsInTable(
	ctx context.Context, tableID int,
) error {
	return errors.WithStack(errEvalPlanner)
}

// RevalidateUniqueConstraint is part of the EvalPlanner interface.
func (*DummyEvalPlanner) RevalidateUniqueConstraint(
	ctx context.Context, tableID int, constraintName string,
) error {
	return errors.WithStack(errEvalPlanner)
}

// ExecutorConfig is part of the EvalPlanner interface.
func (*DummyEvalPlanner) ExecutorConfig() interface{} {
	return nil
}

var _ tree.EvalPlanner = &DummyEvalPlanner{}

var errEvalPlanner = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate scalar expressions using table lookups in this context")

// CurrentDatabaseRegionConfig is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) CurrentDatabaseRegionConfig(
	_ context.Context,
) (tree.DatabaseRegionConfig, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ResetMultiRegionZoneConfigsForTable is part of the tree.EvalDatabase
// interface.
func (ep *DummyEvalPlanner) ResetMultiRegionZoneConfigsForTable(_ context.Context, _ int64) error {
	return errors.WithStack(errEvalPlanner)
}

// ResetMultiRegionZoneConfigsForDatabase is part of the tree.EvalDatabase
// interface.
func (ep *DummyEvalPlanner) ResetMultiRegionZoneConfigsForDatabase(
	_ context.Context, _ int64,
) error {
	return errors.WithStack(errEvalPlanner)
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return errors.WithStack(errEvalPlanner)
}

// ParseQualifiedTableName is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) ParseQualifiedTableName(sql string) (*tree.TableName, error) {
	return parser.ParseQualifiedTableName(sql)
}

// SchemaExists is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) SchemaExists(ctx context.Context, dbName, scName string) (bool, error) {
	return false, errors.WithStack(errEvalPlanner)
}

// IsTableVisible is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) IsTableVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, tableID oid.Oid,
) (bool, bool, error) {
	return false, false, errors.WithStack(errEvalPlanner)
}

// IsTypeVisible is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) IsTypeVisible(
	ctx context.Context, curDB string, searchPath sessiondata.SearchPath, typeID oid.Oid,
) (bool, bool, error) {
	return false, false, errors.WithStack(errEvalPlanner)
}

// HasAnyPrivilege is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) HasAnyPrivilege(
	ctx context.Context,
	specifier tree.HasPrivilegeSpecifier,
	user security.SQLUsername,
	privs []privilege.Privilege,
) (tree.HasAnyPrivilegeResult, error) {
	return tree.HasNoPrivilege, errors.WithStack(errEvalPlanner)
}

// ResolveTableName is part of the tree.EvalDatabase interface.
func (ep *DummyEvalPlanner) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errors.WithStack(errEvalPlanner)
}

// GetTypeFromValidSQLSyntax is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) GetTypeFromValidSQLSyntax(sql string) (*types.T, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// EvalSubquery is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) EvalSubquery(expr *tree.Subquery) (tree.Datum, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (ep *DummyEvalPlanner) ResolveTypeByOID(_ context.Context, _ oid.Oid) (*types.T, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (ep *DummyEvalPlanner) ResolveType(
	_ context.Context, _ *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// QueryRowEx is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) QueryRowEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// QueryIteratorEx is part of the tree.EvalPlanner interface.
func (ep *DummyEvalPlanner) QueryIteratorEx(
	ctx context.Context,
	opName string,
	txn *kv.Txn,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.InternalRows, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// DummyPrivilegedAccessor implements the tree.PrivilegedAccessor interface by returning errors.
type DummyPrivilegedAccessor struct{}

var _ tree.PrivilegedAccessor = &DummyPrivilegedAccessor{}

var errEvalPrivileged = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate privileged expressions in this context")

// LookupNamespaceID is part of the tree.PrivilegedAccessor interface.
func (ep *DummyPrivilegedAccessor) LookupNamespaceID(
	ctx context.Context, parentID int64, parentSchemaID int64, name string,
) (tree.DInt, bool, error) {
	return 0, false, errors.WithStack(errEvalPrivileged)
}

// LookupZoneConfigByNamespaceID is part of the tree.PrivilegedAccessor interface.
func (ep *DummyPrivilegedAccessor) LookupZoneConfigByNamespaceID(
	ctx context.Context, id int64,
) (tree.DBytes, bool, error) {
	return "", false, errors.WithStack(errEvalPrivileged)
}

// DummySessionAccessor implements the tree.EvalSessionAccessor interface by returning errors.
type DummySessionAccessor struct{}

var _ tree.EvalSessionAccessor = &DummySessionAccessor{}

var errEvalSessionVar = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate scalar expressions that access session variables in this context")

// GetSessionVar is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) GetSessionVar(
	_ context.Context, _ string, _ bool,
) (bool, string, error) {
	return false, "", errors.WithStack(errEvalSessionVar)
}

// SetSessionVar is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) SetSessionVar(
	ctx context.Context, settingName, newValue string, isLocal bool,
) error {
	return errors.WithStack(errEvalSessionVar)
}

// HasAdminRole is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) HasAdminRole(_ context.Context) (bool, error) {
	return false, errors.WithStack(errEvalSessionVar)
}

// HasRoleOption is part of the tree.EvalSessionAccessor interface.
func (ep *DummySessionAccessor) HasRoleOption(
	ctx context.Context, roleOption roleoption.Option,
) (bool, error) {
	return false, errors.WithStack(errEvalSessionVar)
}

// DummyClientNoticeSender implements the tree.ClientNoticeSender interface.
type DummyClientNoticeSender struct{}

var _ tree.ClientNoticeSender = &DummyClientNoticeSender{}

// BufferClientNotice is part of the tree.ClientNoticeSender interface.
func (c *DummyClientNoticeSender) BufferClientNotice(context.Context, pgnotice.Notice) {}

// DummyTenantOperator implements the tree.TenantOperator interface.
type DummyTenantOperator struct{}

var _ tree.TenantOperator = &DummyTenantOperator{}

var errEvalTenant = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate tenant operation in this context")

// CreateTenant is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) CreateTenant(_ context.Context, _ uint64) error {
	return errors.WithStack(errEvalTenant)
}

// DestroyTenant is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) DestroyTenant(
	ctx context.Context, tenantID uint64, synchronous bool,
) error {
	return errors.WithStack(errEvalTenant)
}

// GCTenant is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) GCTenant(_ context.Context, _ uint64) error {
	return errors.WithStack(errEvalTenant)
}

// UpdateTenantResourceLimits is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) UpdateTenantResourceLimits(
	_ context.Context,
	tenantID uint64,
	availableRU float64,
	refillRate float64,
	maxBurstRU float64,
	asOf time.Time,
	asOfConsumedRequestUnits float64,
) error {
	return errors.WithStack(errEvalTenant)
}

// DummyPreparedStatementState implements the tree.PreparedStatementState
// interface.
type DummyPreparedStatementState struct{}

var _ tree.PreparedStatementState = (*DummyPreparedStatementState)(nil)

// HasActivePortals is part of the tree.PreparedStatementState interface.
func (ps *DummyPreparedStatementState) HasActivePortals() bool {
	return false
}

// MigratablePreparedStatements is part of the tree.PreparedStatementState interface.
func (ps *DummyPreparedStatementState) MigratablePreparedStatements() []sessiondatapb.MigratableSession_PreparedStatement {
	return nil
}
