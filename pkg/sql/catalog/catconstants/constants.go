// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catconstants

import "math"

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
	CrdbInternalClusterContentionEventsTableID
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
	CrdbInternalInflightTraceSpanTableID
	CrdbInternalJobsTableID
	CrdbInternalKVNodeStatusTableID
	CrdbInternalKVStoreStatusTableID
	CrdbInternalLeasesTableID
	CrdbInternalLocalContentionEventsTableID
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
	CrdbInternalTablesTableLastStatsID
	CrdbInternalTransactionStatsTableID
	CrdbInternalTxnStatsTableID
	CrdbInternalZonesTableID
	CrdbInternalInvalidDescriptorsTableID
	CrdbInternalClusterDatabasePrivilegesTableID
	CrdbInternalInterleaved
	CrdbInternalCrossDbRefrences
	InformationSchemaID
	InformationSchemaAdministrableRoleAuthorizationsID
	InformationSchemaApplicableRolesID
	InformationSchemaCharacterSets
	InformationSchemaCheckConstraints
	InformationSchemaCollationCharacterSetApplicability
	InformationSchemaCollations
	InformationSchemaColumnPrivilegesID
	InformationSchemaColumnsTableID
	InformationSchemaColumnUDTUsageID
	InformationSchemaConstraintColumnUsageTableID
	InformationSchemaEnabledRolesID
	InformationSchemaKeyColumnUsageTableID
	InformationSchemaParametersTableID
	InformationSchemaReferentialConstraintsTableID
	InformationSchemaRoleTableGrantsID
	InformationSchemaRoutineTableID
	InformationSchemaSchemataTableID
	InformationSchemaSchemataTablePrivilegesID
	InformationSchemaSessionVariables
	InformationSchemaSequencesID
	InformationSchemaStatisticsTableID
	InformationSchemaTableConstraintTableID
	InformationSchemaTablePrivilegesID
	InformationSchemaTablesTableID
	InformationSchemaTypePrivilegesID
	InformationSchemaViewsTableID
	InformationSchemaUserPrivilegesID
	PgCatalogID
	PgCatalogAggregateTableID
	PgCatalogAmTableID
	PgCatalogAmopTableID
	PgCatalogAmprocTableID
	PgCatalogAttrDefTableID
	PgCatalogAttributeTableID
	PgCatalogAuthIDTableID
	PgCatalogAuthMembersTableID
	PgCatalogAvailableExtensionVersionsTableID
	PgCatalogAvailableExtensionsTableID
	PgCatalogCastTableID
	PgCatalogClassTableID
	PgCatalogCollationTableID
	PgCatalogConfigTableID
	PgCatalogConstraintTableID
	PgCatalogConversionTableID
	PgCatalogCursorsTableID
	PgCatalogDatabaseTableID
	PgCatalogDbRoleSettingTableID
	PgCatalogDefaultACLTableID
	PgCatalogDependTableID
	PgCatalogDescriptionTableID
	PgCatalogEnumTableID
	PgCatalogEventTriggerTableID
	PgCatalogExtensionTableID
	PgCatalogFileSettingsTableID
	PgCatalogForeignDataWrapperTableID
	PgCatalogForeignServerTableID
	PgCatalogForeignTableTableID
	PgCatalogGroupTableID
	PgCatalogHbaFileRulesTableID
	PgCatalogIndexTableID
	PgCatalogIndexesTableID
	PgCatalogInheritsTableID
	PgCatalogLanguageTableID
	PgCatalogLargeobjectTableID
	PgCatalogLocksTableID
	PgCatalogMatViewsTableID
	PgCatalogNamespaceTableID
	PgCatalogOpclassTableID
	PgCatalogOperatorTableID
	PgCatalogOpfamilyTableID
	PgCatalogPoliciesTableID
	PgCatalogPreparedStatementsTableID
	PgCatalogPreparedXactsTableID
	PgCatalogProcTableID
	PgCatalogPublicationRelTableID
	PgCatalogPublicationTableID
	PgCatalogPublicationTablesTableID
	PgCatalogRangeTableID
	PgCatalogReplicationOriginTableID
	PgCatalogRewriteTableID
	PgCatalogRolesTableID
	PgCatalogRulesTableID
	PgCatalogSecLabelsTableID
	PgCatalogSecurityLabelTableID
	PgCatalogSequencesTableID
	PgCatalogSettingsTableID
	PgCatalogShadowTableID
	PgCatalogSharedDescriptionTableID
	PgCatalogSharedSecurityLabelTableID
	PgCatalogShdependTableID
	PgCatalogShmemAllocationsTableID
	PgCatalogStatActivityTableID
	PgCatalogStatisticExtTableID
	PgCatalogSubscriptionTableID
	PgCatalogTablesTableID
	PgCatalogTablespaceTableID
	PgCatalogTimezoneAbbrevsTableID
	PgCatalogTimezoneNamesTableID
	PgCatalogTransformTableID
	PgCatalogTriggerTableID
	PgCatalogTsConfigMapTableID
	PgCatalogTsConfigTableID
	PgCatalogTsDictTableID
	PgCatalogTsParserTableID
	PgCatalogTsTemplateTableID
	PgCatalogTypeTableID
	PgCatalogUserMappingTableID
	PgCatalogUserMappingsTableID
	PgCatalogUserTableID
	PgCatalogViewsTableID
	PgExtensionSchemaID
	PgExtensionGeographyColumnsTableID
	PgExtensionGeometryColumnsTableID
	PgExtensionSpatialRefSysTableID
	MinVirtualID = PgExtensionSpatialRefSysTableID
)

// ValidationTelemetryKeyPrefix is the prefix of telemetry keys pertaining to
// descriptor validation failures.
const ValidationTelemetryKeyPrefix = "sql.schema.validation_errors."
