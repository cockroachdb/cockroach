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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// DummySequenceOperators implements the eval.SequenceOperators interface by
// returning errors.
type DummySequenceOperators struct{}

var _ eval.SequenceOperators = &DummySequenceOperators{}

var errSequenceOperators = unimplemented.NewWithIssue(42508,
	"cannot evaluate scalar expressions containing sequence operations in this context")

// GetSerialSequenceNameFromColumn is part of the eval.SequenceOperators interface.
func (so *DummySequenceOperators) GetSerialSequenceNameFromColumn(
	ctx context.Context, tn *tree.TableName, columnName tree.Name,
) (*tree.TableName, error) {
	return nil, errors.WithStack(errSequenceOperators)
}

// ResolveTableName is part of the eval.DatabaseCatalog interface.
func (so *DummySequenceOperators) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// SchemaExists is part of the eval.DatabaseCatalog interface.
func (so *DummySequenceOperators) SchemaExists(
	ctx context.Context, dbName, scName string,
) (bool, error) {
	return false, errors.WithStack(errSequenceOperators)
}

// HasAnyPrivilegeForSpecifier is part of the eval.DatabaseCatalog interface.
func (so *DummySequenceOperators) HasAnyPrivilegeForSpecifier(
	ctx context.Context,
	specifier eval.HasPrivilegeSpecifier,
	user username.SQLUsername,
	privs []privilege.Privilege,
) (eval.HasAnyPrivilegeResult, error) {
	return eval.HasNoPrivilege, errors.WithStack(errEvalPlanner)
}

// IncrementSequenceByID is part of the eval.SequenceOperators interface.
func (so *DummySequenceOperators) IncrementSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// GetLatestValueInSessionForSequenceByID implements the eval.SequenceOperators
// interface.
func (so *DummySequenceOperators) GetLatestValueInSessionForSequenceByID(
	ctx context.Context, seqID int64,
) (int64, error) {
	return 0, errors.WithStack(errSequenceOperators)
}

// SetSequenceValueByID implements the eval.SequenceOperators interface.
func (so *DummySequenceOperators) SetSequenceValueByID(
	ctx context.Context, seqID uint32, newVal int64, isCalled bool,
) error {
	return errors.WithStack(errSequenceOperators)
}

// DummyRegionOperator implements the tree.RegionOperator interface by
// returning errors.
type DummyRegionOperator struct{}

var _ eval.RegionOperator = &DummyRegionOperator{}

var errRegionOperator = unimplemented.NewWithIssue(42508,
	"cannot evaluate scalar expressions containing region operations in this context")

// CurrentDatabaseRegionConfig is part of the eval.RegionOperator interface.
func (so *DummyRegionOperator) CurrentDatabaseRegionConfig(
	_ context.Context,
) (eval.DatabaseRegionConfig, error) {
	return nil, errors.WithStack(errRegionOperator)
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the eval.RegionOperator interface.
func (so *DummyRegionOperator) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForTable is part of the eval.RegionOperator
// interface.
func (so *DummyRegionOperator) ResetMultiRegionZoneConfigsForTable(
	_ context.Context, id int64,
) error {
	return errors.WithStack(errRegionOperator)
}

// ResetMultiRegionZoneConfigsForDatabase is part of the eval.RegionOperator
// interface.
func (so *DummyRegionOperator) ResetMultiRegionZoneConfigsForDatabase(
	_ context.Context, id int64,
) error {
	return errors.WithStack(errRegionOperator)
}

// OptimizeSystemDatabase is part of the eval.RegionOperator
// interface.
func (so *DummyRegionOperator) OptimizeSystemDatabase(_ context.Context) error {
	return errors.WithStack(errRegionOperator)
}

// DummyStreamManagerFactory implements the eval.StreamManagerFactory interface by
// returning errors.
type DummyStreamManagerFactory struct{}

// GetReplicationStreamManager implements the eval.StreamManagerFactory interface.
func (smf *DummyStreamManagerFactory) GetReplicationStreamManager(
	ctx context.Context,
) (eval.ReplicationStreamManager, error) {
	return nil, errors.WithStack(errors.New("Stream manager factory not implemented"))
}

// GetStreamIngestManager implements the eval.StreamManagerFactory interface.
func (smf *DummyStreamManagerFactory) GetStreamIngestManager(
	ctx context.Context,
) (eval.StreamIngestManager, error) {
	return nil, errors.WithStack(errors.New("Stream manager factory not implemented"))
}

// DummyEvalPlanner implements the eval.Planner interface by returning
// errors.
type DummyEvalPlanner struct {
	Monitor *mon.BytesMonitor
}

// ResolveOIDFromString is part of the Planner interface.
func (ep *DummyEvalPlanner) ResolveOIDFromString(
	ctx context.Context, resultType *types.T, toResolve *tree.DString,
) (*tree.DOid, bool, error) {
	return nil, false, errors.WithStack(errEvalPlanner)
}

// ResolveOIDFromOID is part of the Planner interface.
func (ep *DummyEvalPlanner) ResolveOIDFromOID(
	ctx context.Context, resultType *types.T, toResolve *tree.DOid,
) (*tree.DOid, bool, error) {
	return nil, false, errors.WithStack(errEvalPlanner)
}

// GenerateTestObjects is part of the Planner interface.
func (ep *DummyEvalPlanner) GenerateTestObjects(
	ctx context.Context, params string,
) (string, error) {
	return "", errors.WithStack(errEvalPlanner)
}

// UnsafeUpsertDescriptor is part of the Planner interface.
func (ep *DummyEvalPlanner) UnsafeUpsertDescriptor(
	ctx context.Context, descID int64, encodedDescriptor []byte, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// UnsafeDeleteDescriptor is part of the Planner interface.
func (ep *DummyEvalPlanner) UnsafeDeleteDescriptor(
	ctx context.Context, descID int64, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// ForceDeleteTableData is part of the Planner interface.
func (ep *DummyEvalPlanner) ForceDeleteTableData(ctx context.Context, descID int64) error {
	return errors.WithStack(errEvalPlanner)
}

// UnsafeUpsertNamespaceEntry is part of the Planner interface.
func (ep *DummyEvalPlanner) UnsafeUpsertNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID int64, name string, descID int64, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// UnsafeDeleteNamespaceEntry is part of the Planner interface.
func (ep *DummyEvalPlanner) UnsafeDeleteNamespaceEntry(
	ctx context.Context, parentID, parentSchemaID int64, name string, descID int64, force bool,
) error {
	return errors.WithStack(errEvalPlanner)
}

// UpsertDroppedRelationGCTTL is part of the Planner interface.
func (ep *DummyEvalPlanner) UpsertDroppedRelationGCTTL(
	ctx context.Context, id int64, ttl duration.Duration,
) error {
	return errors.WithStack(errEvalPlanner)
}

// UserHasAdminRole is part of the Planner interface.
func (ep *DummyEvalPlanner) UserHasAdminRole(
	ctx context.Context, user username.SQLUsername,
) (bool, error) {
	return false, errors.WithStack(errEvalPlanner)
}

// MemberOfWithAdminOption is part of the Planner interface.
func (ep *DummyEvalPlanner) MemberOfWithAdminOption(
	ctx context.Context, member username.SQLUsername,
) (map[username.SQLUsername]bool, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ExternalReadFile is part of the Planner interface.
func (*DummyEvalPlanner) ExternalReadFile(ctx context.Context, uri string) ([]byte, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ExternalWriteFile is part of the Planner interface.
func (*DummyEvalPlanner) ExternalWriteFile(ctx context.Context, uri string, content []byte) error {
	return errors.WithStack(errEvalPlanner)
}

// DecodeGist is part of the Planner interface.
func (*DummyEvalPlanner) DecodeGist(gist string, external bool) ([]string, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// SerializeSessionState is part of the Planner interface.
func (*DummyEvalPlanner) SerializeSessionState() (*tree.DBytes, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// DeserializeSessionState is part of the Planner interface.
func (*DummyEvalPlanner) DeserializeSessionState(
	ctx context.Context, token *tree.DBytes,
) (*tree.DBool, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// CreateSessionRevivalToken is part of the Planner interface.
func (*DummyEvalPlanner) CreateSessionRevivalToken() (*tree.DBytes, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ValidateSessionRevivalToken is part of the Planner interface.
func (*DummyEvalPlanner) ValidateSessionRevivalToken(token *tree.DBytes) (*tree.DBool, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// RevalidateUniqueConstraintsInCurrentDB is part of the Planner interface.
func (*DummyEvalPlanner) RevalidateUniqueConstraintsInCurrentDB(ctx context.Context) error {
	return errors.WithStack(errEvalPlanner)
}

// RevalidateUniqueConstraintsInTable is part of the Planner interface.
func (*DummyEvalPlanner) RevalidateUniqueConstraintsInTable(
	ctx context.Context, tableID int,
) error {
	return errors.WithStack(errEvalPlanner)
}

// RevalidateUniqueConstraint is part of the Planner interface.
func (*DummyEvalPlanner) RevalidateUniqueConstraint(
	ctx context.Context, tableID int, constraintName string,
) error {
	return errors.WithStack(errEvalPlanner)
}

// IsConstraintActive is part of the EvalPlanner interface.
func (*DummyEvalPlanner) IsConstraintActive(
	ctx context.Context, tableID int, constraintName string,
) (bool, error) {
	return false, errors.WithStack(errEvalPlanner)
}

// ValidateTTLScheduledJobsInCurrentDB is part of the Planner interface.
func (*DummyEvalPlanner) ValidateTTLScheduledJobsInCurrentDB(ctx context.Context) error {
	return errors.WithStack(errEvalPlanner)
}

// RepairTTLScheduledJobForTable is part of the Planner interface.
func (*DummyEvalPlanner) RepairTTLScheduledJobForTable(ctx context.Context, tableID int64) error {
	return errors.WithStack(errEvalPlanner)
}

// Mon is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) Mon() *mon.BytesMonitor {
	return ep.Monitor
}

// ExecutorConfig is part of the Planner interface.
func (*DummyEvalPlanner) ExecutorConfig() interface{} {
	return nil
}

var _ eval.Planner = &DummyEvalPlanner{}

var errEvalPlanner = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate scalar expressions using table lookups in this context")

// CurrentDatabaseRegionConfig is part of the eval.RegionOperator interface.
func (ep *DummyEvalPlanner) CurrentDatabaseRegionConfig(
	_ context.Context,
) (eval.DatabaseRegionConfig, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// ResetMultiRegionZoneConfigsForTable is part of the eval.RegionOperator
// interface.
func (ep *DummyEvalPlanner) ResetMultiRegionZoneConfigsForTable(_ context.Context, _ int64) error {
	return errors.WithStack(errEvalPlanner)
}

// ResetMultiRegionZoneConfigsForDatabase is part of the eval.RegionOperator
// interface.
func (ep *DummyEvalPlanner) ResetMultiRegionZoneConfigsForDatabase(
	_ context.Context, _ int64,
) error {
	return errors.WithStack(errEvalPlanner)
}

// ValidateAllMultiRegionZoneConfigsInCurrentDatabase is part of the eval.RegionOperator interface.
func (ep *DummyEvalPlanner) ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
	_ context.Context,
) error {
	return errors.WithStack(errEvalPlanner)
}

// SchemaExists is part of the eval.DatabaseCatalog interface.
func (ep *DummyEvalPlanner) SchemaExists(ctx context.Context, dbName, scName string) (bool, error) {
	return false, errors.WithStack(errEvalPlanner)
}

// HasAnyPrivilegeForSpecifier is part of the eval.DatabaseCatalog interface.
func (ep *DummyEvalPlanner) HasAnyPrivilegeForSpecifier(
	ctx context.Context,
	specifier eval.HasPrivilegeSpecifier,
	user username.SQLUsername,
	privs []privilege.Privilege,
) (eval.HasAnyPrivilegeResult, error) {
	return eval.HasNoPrivilege, errors.WithStack(errEvalPlanner)
}

// ResolveTableName is part of the eval.DatabaseCatalog interface.
func (ep *DummyEvalPlanner) ResolveTableName(
	ctx context.Context, tn *tree.TableName,
) (tree.ID, error) {
	return 0, errors.WithStack(errEvalPlanner)
}

// GetTypeFromValidSQLSyntax is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) GetTypeFromValidSQLSyntax(sql string) (*types.T, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// EvalSubquery is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) EvalSubquery(expr *tree.Subquery) (tree.Datum, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// EvalRoutineExpr is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) EvalRoutineExpr(
	ctx context.Context, expr *tree.RoutineExpr, input tree.Datums,
) (tree.Datum, error) {
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

// QueryRowEx is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) QueryRowEx(
	ctx context.Context,
	opName string,
	session sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// QueryIteratorEx is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) QueryIteratorEx(
	ctx context.Context,
	opName string,
	override sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (eval.InternalRows, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// IsActive is part of the Planner interface.
func (ep *DummyEvalPlanner) IsActive(_ context.Context, _ clusterversion.Key) bool {
	return true
}

// ResolveFunction implements FunctionReferenceResolver interface.
func (ep *DummyEvalPlanner) ResolveFunction(
	ctx context.Context, name *tree.UnresolvedName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	return nil, errors.AssertionFailedf("ResolveFunction unimplemented")
}

// ResolveFunctionByOID implements FunctionReferenceResolver interface.
func (ep *DummyEvalPlanner) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (string, *tree.Overload, error) {
	return "", nil, errors.AssertionFailedf("ResolveFunctionByOID unimplemented")
}

// GetMultiregionConfig is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) GetMultiregionConfig(
	ctx context.Context, databaseID descpb.ID,
) (interface{}, bool) {
	return nil /* regionConfig */, false
}

// IsANSIDML is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) IsANSIDML() bool {
	return false
}

// GetRangeDescByID is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) GetRangeDescByID(
	context.Context, roachpb.RangeID,
) (rangeDesc roachpb.RangeDescriptor, err error) {
	return
}

// DummyPrivilegedAccessor implements the tree.PrivilegedAccessor interface by returning errors.
type DummyPrivilegedAccessor struct{}

var _ eval.PrivilegedAccessor = &DummyPrivilegedAccessor{}

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

// DummySessionAccessor implements the eval.SessionAccessor interface by returning errors.
type DummySessionAccessor struct{}

var _ eval.SessionAccessor = &DummySessionAccessor{}

var errEvalSessionVar = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate scalar expressions that access session variables in this context")

// GetSessionVar is part of the eval.SessionAccessor interface.
func (ep *DummySessionAccessor) GetSessionVar(
	_ context.Context, _ string, _ bool,
) (bool, string, error) {
	return false, "", errors.WithStack(errEvalSessionVar)
}

// SetSessionVar is part of the eval.SessionAccessor interface.
func (ep *DummySessionAccessor) SetSessionVar(
	ctx context.Context, settingName, newValue string, isLocal bool,
) error {
	return errors.WithStack(errEvalSessionVar)
}

// HasAdminRole is part of the eval.SessionAccessor interface.
func (ep *DummySessionAccessor) HasAdminRole(_ context.Context) (bool, error) {
	return false, errors.WithStack(errEvalSessionVar)
}

// HasRoleOption is part of the eval.SessionAccessor interface.
func (ep *DummySessionAccessor) HasRoleOption(
	ctx context.Context, roleOption roleoption.Option,
) (bool, error) {
	return false, errors.WithStack(errEvalSessionVar)
}

// DummyClientNoticeSender implements the eval.ClientNoticeSender interface.
type DummyClientNoticeSender struct{}

var _ eval.ClientNoticeSender = &DummyClientNoticeSender{}

// BufferClientNotice is part of the eval.ClientNoticeSender interface.
func (c *DummyClientNoticeSender) BufferClientNotice(context.Context, pgnotice.Notice) {}

// DummyTenantOperator implements the tree.TenantOperator interface.
type DummyTenantOperator struct{}

var _ eval.TenantOperator = &DummyTenantOperator{}

var errEvalTenant = pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
	"cannot evaluate tenant operation in this context")

// CreateTenant is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) CreateTenant(_ context.Context, _ string) (roachpb.TenantID, error) {
	return roachpb.TenantID{}, errors.WithStack(errEvalTenant)
}

// LookupTenantID is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) LookupTenantID(
	ctx context.Context, tenantName roachpb.TenantName,
) (roachpb.TenantID, error) {
	return roachpb.TenantID{}, errors.WithStack(errEvalTenant)
}

// DropTenantByID is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) DropTenantByID(
	ctx context.Context, tenantID uint64, synchronous, ignoreServiceMode bool,
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

var _ eval.PreparedStatementState = (*DummyPreparedStatementState)(nil)

// HasActivePortals is part of the tree.PreparedStatementState interface.
func (ps *DummyPreparedStatementState) HasActivePortals() bool {
	return false
}

// MigratablePreparedStatements is part of the tree.PreparedStatementState interface.
func (ps *DummyPreparedStatementState) MigratablePreparedStatements() []sessiondatapb.MigratableSession_PreparedStatement {
	return nil
}

// HasPortal is part of the tree.PreparedStatementState interface.
func (ps *DummyPreparedStatementState) HasPortal(_ string) bool {
	return false
}
