// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package faketreeeval provides fake implementations of tree eval interfaces.
package faketreeeval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
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

// GetLastSequenceValueByID implements the eval.SequenceOperators interface.
func (so *DummySequenceOperators) GetLastSequenceValueByID(
	ctx context.Context, seqID uint32,
) (int64, bool, error) {
	return 0, false, errors.WithStack(errSequenceOperators)
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
	_ context.Context, id int64, forceSurviveZone bool,
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
func (*DummyEvalPlanner) DecodeGist(
	ctx context.Context, gist string, external bool,
) ([]string, error) {
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

// Optimizer is part of the cat.Catalog interface.
func (*DummyEvalPlanner) Optimizer() interface{} {
	return nil
}

// GenUniqueCursorName is part of the eval.Planner interface.
func (*DummyEvalPlanner) GenUniqueCursorName() tree.Name {
	return ""
}

// PLpgSQLCloseCursor is part of the eval.Planner interface.
func (*DummyEvalPlanner) PLpgSQLCloseCursor(_ tree.Name) error {
	return errors.WithStack(errEvalPlanner)
}

// PLpgSQLFetchCursor is part of the Planner interface.
func (*DummyEvalPlanner) PLpgSQLFetchCursor(
	context.Context, *tree.CursorStmt,
) (tree.Datums, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

func (p *DummyEvalPlanner) StartHistoryRetentionJob(
	ctx context.Context, desc string, protectTS hlc.Timestamp, expiration time.Duration,
) (jobspb.JobID, error) {
	return 0, errors.WithStack(errEvalPlanner)
}

func (p *DummyEvalPlanner) ExtendHistoryRetention(ctx context.Context, id jobspb.JobID) error {
	return errors.WithStack(errEvalPlanner)
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

func (ep *DummyEvalPlanner) FingerprintSpan(
	_ context.Context, _ roachpb.Span, _ hlc.Timestamp, _ bool, _ bool,
) (uint64, error) {
	return 0, errors.AssertionFailedf("FingerprintSpan unimplemented")
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
func (ep *DummyEvalPlanner) GetTypeFromValidSQLSyntax(
	ctx context.Context, sql string,
) (*types.T, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// EvalSubquery is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) EvalSubquery(expr *tree.Subquery) (tree.Datum, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// EvalRoutineExpr is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) EvalRoutineExpr(
	ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
) (tree.Datum, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// RoutineExprGenerator is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) RoutineExprGenerator(
	ctx context.Context, expr *tree.RoutineExpr, args tree.Datums,
) eval.ValueGenerator {
	return nil
}

// EvalTxnControlExpr is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) EvalTxnControlExpr(
	ctx context.Context, expr *tree.TxnControlExpr, args tree.Datums,
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
	opName redact.RedactableString,
	override sessiondata.InternalExecutorOverride,
	stmt string,
	qargs ...interface{},
) (tree.Datums, error) {
	return nil, errors.WithStack(errEvalPlanner)
}

// QueryIteratorEx is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) QueryIteratorEx(
	ctx context.Context,
	opName redact.RedactableString,
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
	ctx context.Context, name tree.UnresolvedRoutineName, path tree.SearchPath,
) (*tree.ResolvedFunctionDefinition, error) {
	return nil, errors.AssertionFailedf("ResolveFunction unimplemented")
}

// ResolveFunctionByOID implements FunctionReferenceResolver interface.
func (ep *DummyEvalPlanner) ResolveFunctionByOID(
	ctx context.Context, oid oid.Oid,
) (*tree.RoutineName, *tree.Overload, error) {
	return nil, nil, errors.AssertionFailedf("ResolveFunctionByOID unimplemented")
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

// EnforceHomeRegion is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) EnforceHomeRegion() bool {
	return false
}

// GetRangeDescIterator is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) GetRangeDescIterator(
	context.Context, roachpb.Span,
) (_ rangedesc.Iterator, _ error) {
	return
}

// GetRangeDescByID is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) GetRangeDescByID(
	context.Context, roachpb.RangeID,
) (rangeDesc roachpb.RangeDescriptor, err error) {
	return
}

// SpanStats is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) SpanStats(
	context.Context, roachpb.Spans,
) (stats *roachpb.SpanStatsResponse, err error) {
	return
}

// GetDetailsForSpanStats is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) GetDetailsForSpanStats(
	context.Context, int, int,
) (it eval.InternalRows, err error) {
	return
}

// MaybeReallocateAnnotations is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) MaybeReallocateAnnotations(numAnnotations tree.AnnotationIdx) {
}

// AutoCommit is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) AutoCommit() bool {
	return false
}

// InsertTemporarySchema is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) InsertTemporarySchema(
	tempSchemaName string, databaseID descpb.ID, schemaID descpb.ID,
) {

}

// ClearQueryPlanCache is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) ClearQueryPlanCache() {}

// ClearTableStatsCache is part of the eval.Planner interface.
func (ep *DummyEvalPlanner) ClearTableStatsCache() {}

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

// IsSystemTable is part of the tree.PrivilegedAccessor interface.
func (ep *DummyPrivilegedAccessor) IsSystemTable(ctx context.Context, id int64) (bool, error) {
	return false, errors.WithStack(errEvalPrivileged)
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

// HasGlobalPrivilegeOrRoleOption is part of the eval.SessionAccessor interface.
func (ep *DummySessionAccessor) HasGlobalPrivilegeOrRoleOption(
	ctx context.Context, privilege privilege.Kind,
) (bool, error) {
	return false, nil
}

// CheckPrivilege is part of the eval.SessionAccessor interface.
func (ep *DummySessionAccessor) CheckPrivilege(
	_ context.Context, _ privilege.Object, _ privilege.Kind,
) error {
	return errors.WithStack(errEvalSessionVar)
}

// HasViewActivityOrViewActivityRedactedRole is part of the eval.SessionAccessor interface.
func (ep *DummySessionAccessor) HasViewActivityOrViewActivityRedactedRole(
	context.Context,
) (bool, bool, error) {
	return false, false, errors.WithStack(errEvalSessionVar)
}

// DummyClientNoticeSender implements the eval.ClientNoticeSender interface.
type DummyClientNoticeSender struct{}

var _ eval.ClientNoticeSender = &DummyClientNoticeSender{}

// BufferClientNotice is part of the eval.ClientNoticeSender interface.
func (c *DummyClientNoticeSender) BufferClientNotice(context.Context, pgnotice.Notice) {}

// SendClientNotice is part of the eval.ClientNoticeSender interface.
func (c *DummyClientNoticeSender) SendClientNotice(context.Context, pgnotice.Notice, bool) error {
	return nil
}

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

// UpdateTenantResourceLimits is part of the tree.TenantOperator interface.
func (c *DummyTenantOperator) UpdateTenantResourceLimits(
	_ context.Context,
	tenantID uint64,
	availableTokens float64,
	refillRate float64,
	maxBurstTokens float64,
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
