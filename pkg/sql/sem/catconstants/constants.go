// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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

// AttributedToUserInternalAppNamePrefix indicates that the application name
// identifies an internally-executed query that should be attributed to the
// user.
const AttributedToUserInternalAppNamePrefix = ReportableAppNamePrefix + "public-internal"

// DelegatedAppNamePrefix is added to a regular client application
// name for SQL queries that are ran internally on behalf of other SQL
// queries inside that application. This is not the same as
// ReportableAppNamePrefix; in particular the application name with
// DelegatedAppNamePrefix should be scrubbed in reporting.
const DelegatedAppNamePrefix = "$$ "

// InternalSQLAppName is the application_name used by
// the cockroach CLI by default
const InternalSQLAppName = "cockroach sql"

// SystemDatabaseName is the name of the system database.
const SystemDatabaseName = "system"

// SystemTableName is a type for system table names.
type SystemTableName string

// SystemTenantName is the tenant name of the system tenant.
const SystemTenantName = "system"

// Names of tables in the system database.
const (
	NamespaceTableName                     SystemTableName = "namespace"
	DescriptorTableName                    SystemTableName = "descriptor"
	UsersTableName                         SystemTableName = "users"
	ZonesTableName                         SystemTableName = "zones"
	SettingsTableName                      SystemTableName = "settings"
	DescIDSequenceTableName                SystemTableName = "descriptor_id_seq"
	TenantIDSequenceTableName              SystemTableName = "tenant_id_seq"
	TenantsTableName                       SystemTableName = "tenants"
	LeaseTableName                         SystemTableName = "lease"
	EventLogTableName                      SystemTableName = "eventlog"
	RangeEventTableName                    SystemTableName = "rangelog"
	UITableName                            SystemTableName = "ui"
	JobsTableName                          SystemTableName = "jobs"
	JobsProgressTableName                  SystemTableName = "job_progress"
	JobsProgressHistoryTableName           SystemTableName = "job_progress_history"
	JobsStatusTableName                    SystemTableName = "job_status"
	JobsMessageTableName                   SystemTableName = "job_message"
	WebSessionsTableName                   SystemTableName = "web_sessions"
	TableStatisticsTableName               SystemTableName = "table_statistics"
	LocationsTableName                     SystemTableName = "locations"
	RoleMembersTableName                   SystemTableName = "role_members"
	CommentsTableName                      SystemTableName = "comments"
	ReportsMetaTableName                   SystemTableName = "reports_meta"
	ReplicationConstraintStatsTableName    SystemTableName = "replication_constraint_stats"
	ReplicationCriticalLocalitiesTableName SystemTableName = "replication_critical_localities"
	ReplicationStatsTableName              SystemTableName = "replication_stats"
	ProtectedTimestampsMetaTableName       SystemTableName = "protected_ts_meta"
	ProtectedTimestampsRecordsTableName    SystemTableName = "protected_ts_records"
	RoleOptionsTableName                   SystemTableName = "role_options"
	StatementBundleChunksTableName         SystemTableName = "statement_bundle_chunks"
	StatementDiagnosticsRequestsTableName  SystemTableName = "statement_diagnostics_requests"
	StatementDiagnosticsTableName          SystemTableName = "statement_diagnostics"
	ScheduledJobsTableName                 SystemTableName = "scheduled_jobs"
	SqllivenessTableName                   SystemTableName = "sqlliveness"
	MigrationsTableName                    SystemTableName = "migrations"
	JoinTokensTableName                    SystemTableName = "join_tokens"
	StatementStatisticsTableName           SystemTableName = "statement_statistics"
	TransactionStatisticsTableName         SystemTableName = "transaction_statistics"
	StatementActivityTableName             SystemTableName = "statement_activity"
	TransactionActivityTableName           SystemTableName = "transaction_activity"
	DatabaseRoleSettingsTableName          SystemTableName = "database_role_settings"
	TenantUsageTableName                   SystemTableName = "tenant_usage"
	SQLInstancesTableName                  SystemTableName = "sql_instances"
	SpanConfigurationsTableName            SystemTableName = "span_configurations"
	TaskPayloadsTableName                  SystemTableName = "task_payloads"
	TenantSettingsTableName                SystemTableName = "tenant_settings"
	TenantTasksTableName                   SystemTableName = "tenant_tasks"
	SpanCountTableName                     SystemTableName = "span_count"
	SystemPrivilegeTableName               SystemTableName = "privileges"
	SystemExternalConnectionsTableName     SystemTableName = "external_connections"
	RoleIDSequenceName                     SystemTableName = "role_id_seq"
	SystemJobInfoTableName                 SystemTableName = "job_info"
	SpanStatsUniqueKeys                    SystemTableName = "span_stats_unique_keys"
	SpanStatsBuckets                       SystemTableName = "span_stats_buckets"
	SpanStatsSamples                       SystemTableName = "span_stats_samples"
	SpanStatsTenantBoundaries              SystemTableName = "span_stats_tenant_boundaries"
	RegionalLiveness                       SystemTableName = "region_liveness"
	MVCCStatistics                         SystemTableName = "mvcc_statistics"
	StmtExecInsightsTableName              SystemTableName = "statement_execution_insights"
	TxnExecInsightsTableName               SystemTableName = "transaction_execution_insights"
	TableMetadata                          SystemTableName = "table_metadata"
	PreparedTransactionsTableName          SystemTableName = "prepared_transactions"
)

// Oid for virtual database and table.
const (
	CrdbInternalID = math.MaxUint32 - iota
	CrdbInternalBackwardDependenciesTableID
	CrdbInternalBuildInfoTableID
	CrdbInternalBuiltinFunctionsTableID
	CrdbInternalBuiltinFunctionCommentsTableID
	CrdbInternalCatalogCommentsTableID
	CrdbInternalCatalogDescriptorTableID
	CrdbInternalCatalogNamespaceTableID
	CrdbInternalCatalogZonesTableID
	CrdbInternalClusterContendedIndexesViewID
	CrdbInternalClusterContendedKeysViewID
	CrdbInternalClusterContendedTablesViewID
	CrdbInternalClusterContentionEventsTableID
	CrdbInternalClusterDistSQLFlowsTableID
	CrdbInternalClusterExecutionInsightsTableID
	CrdbInternalClusterTxnExecutionInsightsTableID
	CrdbInternalNodeTxnExecutionInsightsTableID
	CrdbInternalClusterLocksTableID
	CrdbInternalClusterQueriesTableID
	CrdbInternalClusterTransactionsTableID
	CrdbInternalClusterSessionsTableID
	CrdbInternalClusterSettingsTableID
	CrdbInternalClusterStmtStatsTableID
	CrdbInternalClusterTxnStatsTableID
	CrdbInternalCreateFunctionStmtsTableID
	CrdbInternalCreateProcedureStmtsTableID
	CrdbInternalCreateSchemaStmtsTableID
	CrdbInternalCreateStmtsTableID
	CrdbInternalCreateTypeStmtsTableID
	CrdbInternalDatabasesTableID
	CrdbInternalFeatureUsageID
	CrdbInternalForwardDependenciesTableID
	CrdbInternalKVNodeLivenessTableID
	CrdbInternalGossipNodesTableID
	CrdbInternalGossipAlertsTableID
	CrdbInternalGossipLivenessTableID
	CrdbInternalGossipNetworkTableID
	CrdbInternalTransactionContentionEvents
	CrdbInternalIndexColumnsTableID
	CrdbInternalIndexSpansTableID
	CrdbInternalIndexUsageStatisticsTableID
	CrdbInternalInflightTraceSpanTableID
	CrdbInternalJobsTableID
	CrdbInternalSystemJobsTableID
	CrdbInternalKVNodeStatusTableID
	CrdbInternalKVStoreStatusTableID
	CrdbInternalLeasesTableID
	CrdbInternalLocalContentionEventsTableID
	CrdbInternalLocalDistSQLFlowsTableID
	CrdbInternalNodeExecutionInsightsTableID
	CrdbInternalLocalQueriesTableID
	CrdbInternalLocalTransactionsTableID
	CrdbInternalLocalSessionsTableID
	CrdbInternalLocalMetricsTableID
	CrdbInternalNodeMemoryMonitorsTableID
	CrdbInternalNodeStmtStatsTableID
	CrdbInternalNodeTxnStatsTableID
	CrdbInternalPartitionsTableID
	CrdbInternalRangesNoLeasesTableID
	CrdbInternalRangesViewID
	CrdbInternalRuntimeInfoTableID
	CrdbInternalSchemaChangesTableID
	CrdbInternalSessionTraceTableID
	CrdbInternalSessionVariablesTableID
	CrdbInternalStmtActivityTableID
	CrdbInternalStmtStatsTableID
	CrdbInternalStmtStatsPersistedTableID
	CrdbInternalStmtStatsPersistedV22_2TableID
	CrdbInternalTableColumnsTableID
	CrdbInternalTableIndexesTableID
	CrdbInternalTableSpansTableID
	CrdbInternalTablesTableID
	CrdbInternalTablesTableLastStatsID
	CrdbInternalTransactionStatsTableID
	CrdbInternalTxnActivityTableID
	CrdbInternalTxnStatsTableID
	CrdbInternalTxnStatsPersistedTableID
	CrdbInternalTxnStatsPersistedV22_2TableID
	CrdbInternalZonesTableID
	CrdbInternalInvalidDescriptorsTableID
	CrdbInternalClusterDatabasePrivilegesTableID
	CrdbInternalCrossDbRefrences
	CrdbInternalLostTableDescriptors
	CrdbInternalClusterInflightTracesTable
	CrdbInternalRegionsTable
	CrdbInternalDefaultPrivilegesTable
	CrdbInternalActiveRangeFeedsTable
	CrdbInternalTenantUsageDetailsViewID
	CrdbInternalPgCatalogTableIsImplementedTableID
	CrdbInternalSuperRegions
	CrdbInternalDroppedRelationsViewID
	CrdbInternalShowTenantCapabilitiesCacheTableID
	CrdbInternalInheritedRoleMembersTableID
	CrdbInternalKVSystemPrivilegesViewID
	CrdbInternalKVFlowControllerID
	CrdbInternalKVFlowControllerIDV2
	CrdbInternalKVFlowHandlesID
	CrdbInternalKVFlowHandlesIDV2
	CrdbInternalKVFlowTokenDeductions
	CrdbInternalKVFlowTokenDeductionsV2
	CrdbInternalRepairableCatalogCorruptionsViewID
	CrdbInternalKVProtectedTS
	CrdbInternalKVSessionBasedLeases
	CrdbInternalClusterReplicationResolvedViewID
	CrdbInternalLogicalReplicationResolvedViewID
	CrdbInternalPCRStreamsTableID
	CrdbInternalPCRStreamSpansTableID
	CrdbInternalPCRStreamCheckpointsTableID
	CrdbInternalLDRProcessorTableID
	CrdbInternalFullyQualifiedNamesViewID
	CrdbInternalStoreLivenessSupportFrom
	CrdbInternalStoreLivenessSupportFor
	// CrdbInternalTestID is reserved for tests that need to inject virtual tables
	// into crdb_internal.
	CrdbInternalTestID
	InformationSchemaID
	InformationSchemaAdministrableRoleAuthorizationsID
	InformationSchemaApplicableRolesID
	InformationSchemaAttributesTableID
	InformationSchemaCharacterSets
	InformationSchemaCheckConstraintRoutineUsageTableID
	InformationSchemaCheckConstraints
	InformationSchemaCollationCharacterSetApplicability
	InformationSchemaCollations
	InformationSchemaColumnColumnUsageTableID
	InformationSchemaColumnDomainUsageTableID
	InformationSchemaColumnOptionsTableID
	InformationSchemaColumnPrivilegesID
	InformationSchemaColumnStatisticsTableID
	InformationSchemaColumnUDTUsageID
	InformationSchemaColumnsExtensionsTableID
	InformationSchemaColumnsTableID
	InformationSchemaConstraintColumnUsageTableID
	InformationSchemaConstraintTableUsageTableID
	InformationSchemaDataTypePrivilegesTableID
	InformationSchemaDomainConstraintsTableID
	InformationSchemaDomainUdtUsageTableID
	InformationSchemaDomainsTableID
	InformationSchemaElementTypesTableID
	InformationSchemaEnabledRolesID
	InformationSchemaEnginesTableID
	InformationSchemaEventsTableID
	InformationSchemaFilesTableID
	InformationSchemaForeignDataWrapperOptionsTableID
	InformationSchemaForeignDataWrappersTableID
	InformationSchemaForeignServerOptionsTableID
	InformationSchemaForeignServersTableID
	InformationSchemaForeignTableOptionsTableID
	InformationSchemaForeignTablesTableID
	InformationSchemaInformationSchemaCatalogNameTableID
	InformationSchemaKeyColumnUsageTableID
	InformationSchemaKeywordsTableID
	InformationSchemaOptimizerTraceTableID
	InformationSchemaParametersTableID
	InformationSchemaPartitionsTableID
	InformationSchemaPluginsTableID
	InformationSchemaProcesslistTableID
	InformationSchemaProfilingTableID
	InformationSchemaReferentialConstraintsTableID
	InformationSchemaResourceGroupsTableID
	InformationSchemaRoleColumnGrantsTableID
	InformationSchemaRoleRoutineGrantsTableID
	InformationSchemaRoleTableGrantsID
	InformationSchemaRoleUdtGrantsTableID
	InformationSchemaRoleUsageGrantsTableID
	InformationSchemaRoutinePrivilegesTableID
	InformationSchemaRoutineTableID
	InformationSchemaSQLFeaturesTableID
	InformationSchemaSQLImplementationInfoTableID
	InformationSchemaSQLPartsTableID
	InformationSchemaSQLSizingTableID
	InformationSchemaSchemataExtensionsTableID
	InformationSchemaSchemataTableID
	InformationSchemaSchemataTablePrivilegesID
	InformationSchemaSequencesID
	InformationSchemaSessionVariables
	InformationSchemaStGeometryColumnsTableID
	InformationSchemaStSpatialReferenceSystemsTableID
	InformationSchemaStUnitsOfMeasureTableID
	InformationSchemaStatisticsTableID
	InformationSchemaTableConstraintTableID
	InformationSchemaTableConstraintsExtensionsTableID
	InformationSchemaTablePrivilegesID
	InformationSchemaTablesExtensionsTableID
	InformationSchemaTablesTableID
	InformationSchemaTablespacesExtensionsTableID
	InformationSchemaTablespacesTableID
	InformationSchemaTransformsTableID
	InformationSchemaTriggeredUpdateColumnsTableID
	InformationSchemaTriggersTableID
	InformationSchemaTypePrivilegesID
	InformationSchemaUdtPrivilegesTableID
	InformationSchemaUsagePrivilegesTableID
	InformationSchemaUserAttributesTableID
	InformationSchemaUserDefinedTypesTableID
	InformationSchemaUserMappingOptionsTableID
	InformationSchemaUserMappingsTableID
	InformationSchemaUserPrivilegesID
	InformationSchemaViewColumnUsageTableID
	InformationSchemaViewRoutineUsageTableID
	InformationSchemaViewTableUsageTableID
	InformationSchemaViewsTableID
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
	PgCatalogInitPrivsTableID
	PgCatalogLanguageTableID
	PgCatalogLargeobjectMetadataTableID
	PgCatalogLargeobjectTableID
	PgCatalogLocksTableID
	PgCatalogMatViewsTableID
	PgCatalogNamespaceTableID
	PgCatalogOpclassTableID
	PgCatalogOperatorTableID
	PgCatalogOpfamilyTableID
	PgCatalogPartitionedTableTableID
	PgCatalogPoliciesTableID
	PgCatalogPolicyTableID
	PgCatalogPreparedStatementsTableID
	PgCatalogPreparedXactsTableID
	PgCatalogProcTableID
	PgCatalogPublicationRelTableID
	PgCatalogPublicationTableID
	PgCatalogPublicationTablesTableID
	PgCatalogRangeTableID
	PgCatalogReplicationOriginStatusTableID
	PgCatalogReplicationOriginTableID
	PgCatalogReplicationSlotsTableID
	PgCatalogRewriteTableID
	PgCatalogRolesTableID
	PgCatalogRulesTableID
	PgCatalogSecLabelsTableID
	PgCatalogSecurityLabelTableID
	PgCatalogSequenceTableID
	PgCatalogSequencesTableID
	PgCatalogSettingsTableID
	PgCatalogShadowTableID
	PgCatalogSharedDescriptionTableID
	PgCatalogSharedSecurityLabelTableID
	PgCatalogShdependTableID
	PgCatalogShmemAllocationsTableID
	PgCatalogStatActivityTableID
	PgCatalogStatAllIndexesTableID
	PgCatalogStatAllTablesTableID
	PgCatalogStatArchiverTableID
	PgCatalogStatBgwriterTableID
	PgCatalogStatDatabaseConflictsTableID
	PgCatalogStatDatabaseTableID
	PgCatalogStatGssapiTableID
	PgCatalogStatProgressAnalyzeTableID
	PgCatalogStatProgressBasebackupTableID
	PgCatalogStatProgressClusterTableID
	PgCatalogStatProgressCreateIndexTableID
	PgCatalogStatProgressVacuumTableID
	PgCatalogStatReplicationTableID
	PgCatalogStatSlruTableID
	PgCatalogStatSslTableID
	PgCatalogStatSubscriptionTableID
	PgCatalogStatSysIndexesTableID
	PgCatalogStatSysTablesTableID
	PgCatalogStatUserFunctionsTableID
	PgCatalogStatUserIndexesTableID
	PgCatalogStatUserTablesTableID
	PgCatalogStatWalReceiverTableID
	PgCatalogStatXactAllTablesTableID
	PgCatalogStatXactSysTablesTableID
	PgCatalogStatXactUserFunctionsTableID
	PgCatalogStatXactUserTablesTableID
	PgCatalogStatioAllIndexesTableID
	PgCatalogStatioAllSequencesTableID
	PgCatalogStatioAllTablesTableID
	PgCatalogStatioSysIndexesTableID
	PgCatalogStatioSysSequencesTableID
	PgCatalogStatioSysTablesTableID
	PgCatalogStatioUserIndexesTableID
	PgCatalogStatioUserSequencesTableID
	PgCatalogStatioUserTablesTableID
	PgCatalogStatisticExtDataTableID
	PgCatalogStatisticExtTableID
	PgCatalogStatisticTableID
	PgCatalogStatsExtTableID
	PgCatalogStatsTableID
	PgCatalogSubscriptionRelTableID
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

// ConstraintType is used to identify the type of a constraint.
type ConstraintType string

const (
	// ConstraintTypePK identifies a PRIMARY KEY constraint.
	ConstraintTypePK ConstraintType = "PRIMARY KEY"
	// ConstraintTypeFK identifies a FOREIGN KEY constraint.
	ConstraintTypeFK ConstraintType = "FOREIGN KEY"
	// ConstraintTypeUnique identifies a UNIQUE constraint.
	ConstraintTypeUnique ConstraintType = "UNIQUE"
	// ConstraintTypeCheck identifies a CHECK constraint.
	ConstraintTypeCheck ConstraintType = "CHECK"
	// ConstraintTypeUniqueWithoutIndex identifies a UNIQUE_WITHOUT_INDEX constraint.
	ConstraintTypeUniqueWithoutIndex ConstraintType = "UNIQUE WITHOUT INDEX"
)

// SafeValue implements the redact.SafeValue interface.
func (ConstraintType) SafeValue() {}
