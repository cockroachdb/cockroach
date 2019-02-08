// Copyright 2017 The Cockroach Authors.
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

// Oid for virtual database and table.
const (
	CrdbInternalID = math.MaxUint32 - iota
	CrdbInternalBackwardDependenciesTableID
	CrdbInternalBuildInfoTableID
	CrdbInternalBuiltinFunctionsTableID
	CrdbInternalClusterQueriesTableID
	CrdbInternalClusterSessionsTableID
	CrdbInternalClusterSettingsTableID
	CrdbInternalCreateStmtsTableID
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
	CrdbInternalLocalSessionsTableID
	CrdbInternalLocalMetricsTableID
	CrdbInternalPartitionsTableID
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
	CrdbInternalZonesTableID
	InformationSchemaID
	InformationSchemaAdministrableRoleAuthorizationsID
	InformationSchemaApplicableRolesID
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
	PgCatalogAmTableID
	PgCatalogAttrDefTableID
	PgCatalogAttributeTableID
	PgCatalogAuthMembersTableID
	PgCatalogClassTableID
	PgCatalogCollationTableID
	PgCatalogConstraintTableID
	PgCatalogDatabaseTableID
	PgCatalogDependTableID
	PgCatalogDescriptionTableID
	PgCatalogSharedDescriptionTableID
	PgCatalogEnumTableID
	PgCatalogExtensionTableID
	PgCatalogForeignDataWrapperTableID
	PgCatalogForeignServerTableID
	PgCatalogForeignTableTableID
	PgCatalogIndexTableID
	PgCatalogIndexesTableID
	PgCatalogInheritsTableID
	PgCatalogLanguageTableID
	PgCatalogNamespaceTableID
	PgCatalogOperatorTableID
	PgCatalogProcTableID
	PgCatalogRangeTableID
	PgCatalogRewriteTableID
	PgCatalogRolesTableID
	PgCatalogSequencesTableID
	PgCatalogSettingsTableID
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
	MinVirtualID = PgCatalogSharedSecurityLabelTableID
)
