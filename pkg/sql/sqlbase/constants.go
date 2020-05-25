// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlbase

import (
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
)

// DefaultSearchPath is the search path used by virgin sessions.
var DefaultSearchPath = sessiondata.MakeSearchPath([]string{"public"})

// AdminRole is the default (and non-droppable) role with superuser privileges.
var AdminRole = "admin"

// PublicRole is the special "public" pseudo-role.
// All users are implicit members of "public". The role cannot be created,
// dropped, assigned to another role, and is generally not listed.
// It can be granted privileges, implicitly granting them to all users (current and future).
var PublicRole = "public"

// ReportableAppNamePrefix indicates that the application name can be
// reported in telemetry without scrubbing. (Note this only applies to
// the application name itself. Query data is still scrubbed as
// usual.)
const ReportableAppNamePrefix = "$ "

// InternalAppNamePrefix indicates that the application name identifies
// an internal task / query / job to CockroachDB. Different application
// names are used to classify queries in different categories.
const InternalAppNamePrefix = ReportableAppNamePrefix + "internal"

// DelegatedAppNamePrefix is added to a regular client application
// name for SQL queries that are ran internally on behalf of other SQL
// queries inside that application. This is not the same as
// RepotableAppNamePrefix; in particular the application name with
// DelegatedAppNamePrefix should be scrubbed in reporting.
const DelegatedAppNamePrefix = "$$ "

// Oid for virtual database and table.
const (
	CrdbInternalID = math.MaxUint32 - iota
	CrdbInternalBackwardDependenciesTableID
	CrdbInternalBuildInfoTableID
	CrdbInternalBuiltinFunctionsTableID
	CrdbInternalClusterQueriesTableID
	CrdbInternalClusterTransactionsTableID
	CrdbInternalClusterSessionsTableID
	CrdbInternalClusterSettingsTableID
	CrdbInternalCreateStmtsTableID
	CrdbInternalCreateTypeStmtsTableID
	CrdbInternalDatabasesTableID
	CrdbInternalFeatureUsageID
	CrdbInternalForwardDependenciesTableID
	CrdbInternalGossipNodesTableID
	CrdbInternalGossipAlertsTableID
	CrdbInternalGossipLivenessTableID
	CrdbInternalGossipNetworkTableID
	CrdbInternalIndexColumnsTableID
	CrdbInternalJobsTableID
	CrdbInternalKVNodeStatusTableID
	CrdbInternalKVStoreStatusTableID
	CrdbInternalLeasesTableID
	CrdbInternalLocalQueriesTableID
	CrdbInternalLocalTransactionsTableID
	CrdbInternalLocalSessionsTableID
	CrdbInternalLocalMetricsTableID
	CrdbInternalPartitionsTableID
	CrdbInternalPredefinedCommentsTableID
	CrdbInternalRangesNoLeasesTableID
	CrdbInternalRangesViewID
	CrdbInternalRuntimeInfoTableID
	CrdbInternalSchemaChangesTableID
	CrdbInternalSessionTraceTableID
	CrdbInternalSessionVariablesTableID
	CrdbInternalStmtStatsTableID
	CrdbInternalTableColumnsTableID
	CrdbInternalTableIndexesTableID
	CrdbInternalTablesTableID
	CrdbInternalTxnStatsTableID
	CrdbInternalZonesTableID
	InformationSchemaID
	InformationSchemaAdministrableRoleAuthorizationsID
	InformationSchemaApplicableRolesID
	InformationSchemaCheckConstraints
	InformationSchemaColumnPrivilegesID
	InformationSchemaColumnsTableID
	InformationSchemaConstraintColumnUsageTableID
	InformationSchemaEnabledRolesID
	InformationSchemaKeyColumnUsageTableID
	InformationSchemaParametersTableID
	InformationSchemaReferentialConstraintsTableID
	InformationSchemaRoleTableGrantsID
	InformationSchemaRoutineTableID
	InformationSchemaSchemataTableID
	InformationSchemaSchemataTablePrivilegesID
	InformationSchemaSequencesID
	InformationSchemaStatisticsTableID
	InformationSchemaTableConstraintTableID
	InformationSchemaTablePrivilegesID
	InformationSchemaTablesTableID
	InformationSchemaViewsTableID
	InformationSchemaUserPrivilegesID
	PgCatalogID
	PgCatalogAggregateTableID
	PgCatalogAmTableID
	PgCatalogAttrDefTableID
	PgCatalogAttributeTableID
	PgCatalogAuthIDTableID
	PgCatalogAuthMembersTableID
	PgCatalogAvailableExtensionsTableID
	PgCatalogCastTableID
	PgCatalogClassTableID
	PgCatalogCollationTableID
	PgCatalogConstraintTableID
	PgCatalogConversionTableID
	PgCatalogDatabaseTableID
	PgCatalogDefaultACLTableID
	PgCatalogDependTableID
	PgCatalogDescriptionTableID
	PgCatalogSharedDescriptionTableID
	PgCatalogEnumTableID
	PgCatalogEventTriggerTableID
	PgCatalogExtensionTableID
	PgCatalogForeignDataWrapperTableID
	PgCatalogForeignServerTableID
	PgCatalogForeignTableTableID
	PgCatalogIndexTableID
	PgCatalogIndexesTableID
	PgCatalogInheritsTableID
	PgCatalogLanguageTableID
	PgCatalogLocksTableID
	PgCatalogMatViewsTableID
	PgCatalogNamespaceTableID
	PgCatalogOperatorTableID
	PgCatalogPreparedStatementsTableID
	PgCatalogPreparedXactsTableID
	PgCatalogProcTableID
	PgCatalogRangeTableID
	PgCatalogRewriteTableID
	PgCatalogRolesTableID
	PgCatalogSecLabelsTableID
	PgCatalogSequencesTableID
	PgCatalogSettingsTableID
	PgCatalogShdependTableID
	PgCatalogUserTableID
	PgCatalogUserMappingTableID
	PgCatalogTablesTableID
	PgCatalogTablespaceTableID
	PgCatalogTriggerTableID
	PgCatalogTypeTableID
	PgCatalogViewsTableID
	PgCatalogStatActivityTableID
	PgCatalogSecurityLabelTableID
	PgCatalogSharedSecurityLabelTableID
	PgExtensionSchemaID
	PgExtensionGeographyColumnsTableID
	PgExtensionGeometryColumnsTableID
	PgExtensionSpatialRefSysTableID
	MinVirtualID = PgExtensionSpatialRefSysTableID
)
