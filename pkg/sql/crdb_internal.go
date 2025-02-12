// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsauth"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprotectedts"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	slpb "github.com/cockroachdb/cockroach/pkg/kv/kvserver/storeliveness/storelivenesspb"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiespb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/scheduledjobs"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catformat"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/multiregion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/clusterunique"
	"github.com/cockroachdb/cockroach/pkg/sql/contentionpb"
	"github.com/cockroachdb/cockroach/pkg/sql/idxusage"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/idxtype"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/sslocal"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/sql/vtable"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lib/pq/oid"
)

// CrdbInternalName is the name of the crdb_internal schema.
const CrdbInternalName = catconstants.CRDBInternalSchemaName

// Naming convention:
//   - if the response is served from memory, prefix with node_
//   - if the response is served via a kv request, prefix with kv_
//   - if the response is not from kv requests but is cluster-wide (i.e. the
//     answer isn't specific to the sql connection being used, prefix with cluster_.
//
// Adding something new here will require an update to `pkg/cli` for inclusion in
// a `debug zip`; the unit tests will guide you.
//
// Many existing tables don't follow the conventions above, but please apply
// them to future additions.
var crdbInternal = virtualSchema{
	name: CrdbInternalName,
	tableDefs: map[descpb.ID]virtualSchemaDef{
		catconstants.CrdbInternalBackwardDependenciesTableID:        crdbInternalBackwardDependenciesTable,
		catconstants.CrdbInternalBuildInfoTableID:                   crdbInternalBuildInfoTable,
		catconstants.CrdbInternalBuiltinFunctionsTableID:            crdbInternalBuiltinFunctionsTable,
		catconstants.CrdbInternalBuiltinFunctionCommentsTableID:     crdbInternalBuiltinFunctionCommentsTable,
		catconstants.CrdbInternalCatalogCommentsTableID:             crdbInternalCatalogCommentsTable,
		catconstants.CrdbInternalCatalogDescriptorTableID:           crdbInternalCatalogDescriptorTable,
		catconstants.CrdbInternalCatalogNamespaceTableID:            crdbInternalCatalogNamespaceTable,
		catconstants.CrdbInternalCatalogZonesTableID:                crdbInternalCatalogZonesTable,
		catconstants.CrdbInternalClusterContendedIndexesViewID:      crdbInternalClusterContendedIndexesView,
		catconstants.CrdbInternalClusterContendedKeysViewID:         crdbInternalClusterContendedKeysView,
		catconstants.CrdbInternalClusterContendedTablesViewID:       crdbInternalClusterContendedTablesView,
		catconstants.CrdbInternalClusterContentionEventsTableID:     crdbInternalClusterContentionEventsTable,
		catconstants.CrdbInternalClusterDistSQLFlowsTableID:         crdbInternalClusterDistSQLFlowsTable,
		catconstants.CrdbInternalClusterExecutionInsightsTableID:    crdbInternalClusterExecutionInsightsTable,
		catconstants.CrdbInternalClusterTxnExecutionInsightsTableID: crdbInternalClusterTxnExecutionInsightsTable,
		catconstants.CrdbInternalClusterLocksTableID:                crdbInternalClusterLocksTable,
		catconstants.CrdbInternalClusterQueriesTableID:              crdbInternalClusterQueriesTable,
		catconstants.CrdbInternalClusterTransactionsTableID:         crdbInternalClusterTxnsTable,
		catconstants.CrdbInternalClusterSessionsTableID:             crdbInternalClusterSessionsTable,
		catconstants.CrdbInternalClusterSettingsTableID:             crdbInternalClusterSettingsTable,
		catconstants.CrdbInternalClusterStmtStatsTableID:            crdbInternalClusterStmtStatsTable,
		catconstants.CrdbInternalCreateFunctionStmtsTableID:         crdbInternalCreateFunctionStmtsTable,
		catconstants.CrdbInternalCreateProcedureStmtsTableID:        crdbInternalCreateProcedureStmtsTable,
		catconstants.CrdbInternalCreateSchemaStmtsTableID:           crdbInternalCreateSchemaStmtsTable,
		catconstants.CrdbInternalCreateStmtsTableID:                 crdbInternalCreateStmtsTable,
		catconstants.CrdbInternalCreateTypeStmtsTableID:             crdbInternalCreateTypeStmtsTable,
		catconstants.CrdbInternalDatabasesTableID:                   crdbInternalDatabasesTable,
		catconstants.CrdbInternalDroppedRelationsViewID:             crdbInternalDroppedRelationsView,
		catconstants.CrdbInternalSuperRegions:                       crdbInternalSuperRegions,
		catconstants.CrdbInternalFeatureUsageID:                     crdbInternalFeatureUsage,
		catconstants.CrdbInternalForwardDependenciesTableID:         crdbInternalForwardDependenciesTable,
		catconstants.CrdbInternalGossipNodesTableID:                 crdbInternalGossipNodesTable,
		catconstants.CrdbInternalKVNodeLivenessTableID:              crdbInternalKVNodeLivenessTable,
		catconstants.CrdbInternalGossipAlertsTableID:                crdbInternalGossipAlertsTable,
		catconstants.CrdbInternalGossipLivenessTableID:              crdbInternalGossipLivenessTable,
		catconstants.CrdbInternalGossipNetworkTableID:               crdbInternalGossipNetworkTable,
		catconstants.CrdbInternalTransactionContentionEvents:        crdbInternalTransactionContentionEventsTable,
		catconstants.CrdbInternalIndexColumnsTableID:                crdbInternalIndexColumnsTable,
		catconstants.CrdbInternalIndexSpansTableID:                  crdbInternalIndexSpansTable,
		catconstants.CrdbInternalIndexUsageStatisticsTableID:        crdbInternalIndexUsageStatistics,
		catconstants.CrdbInternalInflightTraceSpanTableID:           crdbInternalInflightTraceSpanTable,
		catconstants.CrdbInternalJobsTableID:                        crdbInternalJobsTable,
		catconstants.CrdbInternalSystemJobsTableID:                  crdbInternalSystemJobsTable,
		catconstants.CrdbInternalKVNodeStatusTableID:                crdbInternalKVNodeStatusTable,
		catconstants.CrdbInternalKVStoreStatusTableID:               crdbInternalKVStoreStatusTable,
		catconstants.CrdbInternalLeasesTableID:                      crdbInternalLeasesTable,
		catconstants.CrdbInternalLocalContentionEventsTableID:       crdbInternalLocalContentionEventsTable,
		catconstants.CrdbInternalLocalDistSQLFlowsTableID:           crdbInternalLocalDistSQLFlowsTable,
		catconstants.CrdbInternalLocalQueriesTableID:                crdbInternalLocalQueriesTable,
		catconstants.CrdbInternalLocalTransactionsTableID:           crdbInternalLocalTxnsTable,
		catconstants.CrdbInternalLocalSessionsTableID:               crdbInternalLocalSessionsTable,
		catconstants.CrdbInternalLocalMetricsTableID:                crdbInternalLocalMetricsTable,
		catconstants.CrdbInternalNodeExecutionInsightsTableID:       crdbInternalNodeExecutionInsightsTable,
		catconstants.CrdbInternalNodeMemoryMonitorsTableID:          crdbInternalNodeMemoryMonitors,
		catconstants.CrdbInternalNodeStmtStatsTableID:               crdbInternalNodeStmtStatsTable,
		catconstants.CrdbInternalNodeTxnExecutionInsightsTableID:    crdbInternalNodeTxnExecutionInsightsTable,
		catconstants.CrdbInternalNodeTxnStatsTableID:                crdbInternalNodeTxnStatsTable,
		catconstants.CrdbInternalPartitionsTableID:                  crdbInternalPartitionsTable,
		catconstants.CrdbInternalRangesNoLeasesTableID:              crdbInternalRangesNoLeasesTable,
		catconstants.CrdbInternalRangesViewID:                       crdbInternalRangesView,
		catconstants.CrdbInternalRuntimeInfoTableID:                 crdbInternalRuntimeInfoTable,
		catconstants.CrdbInternalSchemaChangesTableID:               crdbInternalSchemaChangesTable,
		catconstants.CrdbInternalSessionTraceTableID:                crdbInternalSessionTraceTable,
		catconstants.CrdbInternalSessionVariablesTableID:            crdbInternalSessionVariablesTable,
		catconstants.CrdbInternalStmtActivityTableID:                crdbInternalStmtActivityView,
		catconstants.CrdbInternalStmtStatsTableID:                   crdbInternalStmtStatsView,
		catconstants.CrdbInternalStmtStatsPersistedTableID:          crdbInternalStmtStatsPersistedView,
		catconstants.CrdbInternalStmtStatsPersistedV22_2TableID:     crdbInternalStmtStatsPersistedViewV22_2,
		catconstants.CrdbInternalTableColumnsTableID:                crdbInternalTableColumnsTable,
		catconstants.CrdbInternalTableIndexesTableID:                crdbInternalTableIndexesTable,
		catconstants.CrdbInternalTableSpansTableID:                  crdbInternalTableSpansTable,
		catconstants.CrdbInternalTablesTableLastStatsID:             crdbInternalTablesTableLastStats,
		catconstants.CrdbInternalTablesTableID:                      crdbInternalTablesTable,
		catconstants.CrdbInternalClusterTxnStatsTableID:             crdbInternalClusterTxnStatsTable,
		catconstants.CrdbInternalTxnActivityTableID:                 crdbInternalTxnActivityView,
		catconstants.CrdbInternalTxnStatsTableID:                    crdbInternalTxnStatsView,
		catconstants.CrdbInternalTxnStatsPersistedTableID:           crdbInternalTxnStatsPersistedView,
		catconstants.CrdbInternalTxnStatsPersistedV22_2TableID:      crdbInternalTxnStatsPersistedViewV22_2,
		catconstants.CrdbInternalTransactionStatsTableID:            crdbInternalTransactionStatisticsTable,
		catconstants.CrdbInternalZonesTableID:                       crdbInternalZonesTable,
		catconstants.CrdbInternalInvalidDescriptorsTableID:          crdbInternalInvalidDescriptorsTable,
		catconstants.CrdbInternalClusterDatabasePrivilegesTableID:   crdbInternalClusterDatabasePrivilegesTable,
		catconstants.CrdbInternalCrossDbRefrences:                   crdbInternalCrossDbReferences,
		catconstants.CrdbInternalLostTableDescriptors:               crdbLostTableDescriptors,
		catconstants.CrdbInternalClusterInflightTracesTable:         crdbInternalClusterInflightTracesTable,
		catconstants.CrdbInternalRegionsTable:                       crdbInternalRegionsTable,
		catconstants.CrdbInternalDefaultPrivilegesTable:             crdbInternalDefaultPrivilegesTable,
		catconstants.CrdbInternalActiveRangeFeedsTable:              crdbInternalActiveRangeFeedsTable,
		catconstants.CrdbInternalTenantUsageDetailsViewID:           crdbInternalTenantUsageDetailsView,
		catconstants.CrdbInternalPgCatalogTableIsImplementedTableID: crdbInternalPgCatalogTableIsImplementedTable,
		catconstants.CrdbInternalShowTenantCapabilitiesCacheTableID: crdbInternalShowTenantCapabilitiesCache,
		catconstants.CrdbInternalInheritedRoleMembersTableID:        crdbInternalInheritedRoleMembers,
		catconstants.CrdbInternalKVSystemPrivilegesViewID:           crdbInternalKVSystemPrivileges,
		catconstants.CrdbInternalKVFlowHandlesID:                    crdbInternalKVFlowHandles,
		catconstants.CrdbInternalKVFlowHandlesIDV2:                  crdbInternalKVFlowHandlesV2,
		catconstants.CrdbInternalKVFlowControllerID:                 crdbInternalKVFlowController,
		catconstants.CrdbInternalKVFlowControllerIDV2:               crdbInternalKVFlowControllerV2,
		catconstants.CrdbInternalKVFlowTokenDeductions:              crdbInternalKVFlowTokenDeductions,
		catconstants.CrdbInternalKVFlowTokenDeductionsV2:            crdbInternalKVFlowTokenDeductionsV2,
		catconstants.CrdbInternalRepairableCatalogCorruptionsViewID: crdbInternalRepairableCatalogCorruptions,
		catconstants.CrdbInternalKVProtectedTS:                      crdbInternalKVProtectedTSTable,
		catconstants.CrdbInternalKVSessionBasedLeases:               crdbInternalSessionBasedLeases,
		catconstants.CrdbInternalClusterReplicationResolvedViewID:   crdbInternalClusterReplicationResolvedView,
		catconstants.CrdbInternalLogicalReplicationResolvedViewID:   crdbInternalLogicalReplicationResolvedView,
		catconstants.CrdbInternalPCRStreamsTableID:                  crdbInternalPCRStreamsTable,
		catconstants.CrdbInternalPCRStreamSpansTableID:              crdbInternalPCRStreamSpansTable,
		catconstants.CrdbInternalPCRStreamCheckpointsTableID:        crdbInternalPCRStreamCheckpointsTable,
		catconstants.CrdbInternalLDRProcessorTableID:                crdbInternalLDRProcessorTable,
		catconstants.CrdbInternalFullyQualifiedNamesViewID:          crdbInternalFullyQualifiedNamesView,
		catconstants.CrdbInternalStoreLivenessSupportFrom:           crdbInternalStoreLivenessSupportFromTable,
		catconstants.CrdbInternalStoreLivenessSupportFor:            crdbInternalStoreLivenessSupportForTable,
	},
	validWithNoDatabaseContext: true,
}

// SupportedVTables are the crdb_internal tables that are "supported" for real
// customer use in production for legacy reasons. Avoid addding to this list if
// possible and prefer to add new cusotmer-facing tables that should be public
// under the non-"internal" namespace of information_schema.
var SupportedVTables = map[string]struct{}{
	`"".crdb_internal.cluster_contended_indexes`:     {},
	`"".crdb_internal.cluster_contended_keys`:        {},
	`"".crdb_internal.cluster_contended_tables`:      {},
	`"".crdb_internal.cluster_contention_events`:     {},
	`"".crdb_internal.cluster_locks`:                 {},
	`"".crdb_internal.cluster_queries`:               {},
	`"".crdb_internal.cluster_sessions`:              {},
	`"".crdb_internal.cluster_transactions`:          {},
	`"".crdb_internal.index_usage_statistics`:        {},
	`"".crdb_internal.statement_statistics`:          {},
	`"".crdb_internal.transaction_contention_events`: {},
	`"".crdb_internal.transaction_statistics`:        {},
	`"".crdb_internal.zones`:                         {},
}

var crdbInternalBuildInfoTable = virtualSchemaTable{
	comment: `detailed identification strings (RAM, local node only)`,
	schema: `
CREATE TABLE crdb_internal.node_build_info (
  node_id INT NOT NULL,
  field   STRING NOT NULL,
  value   STRING NOT NULL
)`,
	populate: func(_ context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		execCfg := p.ExecCfg()
		nodeID, _ := execCfg.NodeInfo.NodeID.OptionalNodeID() // zero if not available

		info := build.GetInfo()
		for k, v := range map[string]string{
			"Name":         "CockroachDB",
			"ClusterID":    execCfg.NodeInfo.LogicalClusterID().String(),
			"Organization": execCfg.Organization(),
			"Build":        info.Short().StripMarkers(),
			"Version":      info.Tag,
			"Channel":      info.Channel,

			"VirtualClusterName": string(execCfg.VirtualClusterName),
		} {
			if err := addRow(
				tree.NewDInt(tree.DInt(nodeID)),
				tree.NewDString(k),
				tree.NewDString(v),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var crdbInternalRuntimeInfoTable = virtualSchemaTable{
	comment: `server parameters, useful to construct connection URLs (RAM, local node only)`,
	schema: `
CREATE TABLE crdb_internal.node_runtime_info (
  node_id   INT NOT NULL,
  component STRING NOT NULL,
  field     STRING NOT NULL,
  value     STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}

		node := p.ExecCfg().NodeInfo

		nodeID, _ := node.NodeID.OptionalNodeID() // zero if not available
		dbURL, err := node.PGURL(url.User(username.RootUser))
		if err != nil {
			return err
		}

		for _, item := range []struct {
			component string
			url       *url.URL
		}{
			{"DB", dbURL.ToPQ()}, {"UI", node.AdminURL()},
		} {
			var user string
			if item.url.User != nil {
				user = item.url.User.String()
			}
			host, port, err := net.SplitHostPort(item.url.Host)
			if err != nil {
				return err
			}
			for _, kv := range [][2]string{
				{"URL", item.url.String()},
				{"Scheme", item.url.Scheme},
				{"User", user},
				{"Host", host},
				{"Port", port},
				{"URI", item.url.RequestURI()},
			} {
				k, v := kv[0], kv[1]
				if err := addRow(
					tree.NewDInt(tree.DInt(nodeID)),
					tree.NewDString(item.component),
					tree.NewDString(k),
					tree.NewDString(v),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

var crdbInternalDatabasesTable = virtualSchemaTable{
	comment: `databases accessible by the current user (KV scan)`,
	schema: `
CREATE TABLE crdb_internal.databases (
	id INT NOT NULL,
	name STRING NOT NULL,
	owner NAME NOT NULL,
	primary_region STRING,
	secondary_region STRING,
	regions STRING[],
	survival_goal STRING,
	placement_policy STRING,
	create_statement STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /* all databases */, true, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				var survivalGoal = tree.DNull
				var primaryRegion = tree.DNull
				var secondaryRegion = tree.DNull
				var placement = tree.DNull
				regions := tree.NewDArray(types.String)

				createNode := tree.CreateDatabase{}
				createNode.ConnectionLimit = -1
				createNode.Name = tree.Name(db.GetName())
				if db.IsMultiRegion() {
					primaryRegion = tree.NewDString(string(db.GetRegionConfig().PrimaryRegion))
					createNode.PrimaryRegion = tree.Name(db.GetRegionConfig().PrimaryRegion)
					secondaryRegion = tree.NewDString(string(db.GetRegionConfig().SecondaryRegion))
					createNode.SecondaryRegion = tree.Name(db.GetRegionConfig().SecondaryRegion)

					regionConfig, err := SynthesizeRegionConfig(ctx, p.txn, db.GetID(), p.Descriptors())
					if err != nil {
						return err
					}

					createNode.Regions = make(tree.NameList, len(regionConfig.Regions()))
					for i, region := range regionConfig.Regions() {
						if err := regions.Append(tree.NewDString(string(region))); err != nil {
							return err
						}
						createNode.Regions[i] = tree.Name(region)
					}

					if db.GetRegionConfig().Placement == descpb.DataPlacement_RESTRICTED {
						placement = tree.NewDString("restricted")
						createNode.Placement = tree.DataPlacementRestricted
					} else {
						placement = tree.NewDString("default")
						// We can't differentiate between a database that was created with
						// unspecified and default, and we don't want to expose PLACEMENT
						// unless we know the user wants to use PLACEMENT. Therefore, we
						// only add a PLACEMENT clause if the database was configured with
						// restricted placement.
						createNode.Placement = tree.DataPlacementUnspecified
					}

					switch db.GetRegionConfig().SurvivalGoal {
					case descpb.SurvivalGoal_ZONE_FAILURE:
						survivalGoal = tree.NewDString("zone")
						createNode.SurvivalGoal = tree.SurvivalGoalZoneFailure
					case descpb.SurvivalGoal_REGION_FAILURE:
						survivalGoal = tree.NewDString("region")
						createNode.SurvivalGoal = tree.SurvivalGoalRegionFailure
					default:
						return errors.Newf("unknown survival goal: %d", db.GetRegionConfig().SurvivalGoal)
					}
				}
				owner, err := p.getOwnerOfPrivilegeObject(ctx, db)
				if err != nil {
					return err
				}
				return addRow(
					tree.NewDInt(tree.DInt(db.GetID())),  // id
					tree.NewDString(db.GetName()),        // name
					tree.NewDName(owner.Normalized()),    // owner
					primaryRegion,                        // primary_region
					secondaryRegion,                      // secondary_region
					regions,                              // regions
					survivalGoal,                         // survival_goal
					placement,                            // data_placement
					tree.NewDString(createNode.String()), // create_statement
				)
			})
	},
}

var crdbInternalSuperRegions = virtualSchemaTable{
	comment: `list super regions of databases visible to the current user`,
	schema: `
CREATE TABLE crdb_internal.super_regions (
	id INT NOT NULL,
	database_name STRING NOT NULL,
  super_region_name STRING NOT NULL,
	regions STRING[]
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /* all databases */, true, /* requiresPrivileges */
			func(ctx context.Context, db catalog.DatabaseDescriptor) error {
				if !db.IsMultiRegion() {
					return nil
				}

				typeID, err := db.MultiRegionEnumID()
				if err != nil {
					return err
				}
				typeDesc, err := p.Descriptors().ByIDWithLeased(p.txn).WithoutNonPublic().Get().Type(ctx, typeID)
				if err != nil {
					return err
				}
				regionDesc := typeDesc.AsRegionEnumTypeDescriptor()
				if regionDesc == nil {
					return errors.AssertionFailedf("expected region enum type, not %s for type %q (%d)",
						typeDesc.GetKind(), typeDesc.GetName(), typeDesc.GetID())
				}

				return regionDesc.ForEachSuperRegion(func(superRegion string) error {
					regionList := tree.NewDArray(types.String)
					if err := regionDesc.ForEachRegionInSuperRegion(superRegion, func(region catpb.RegionName) error {
						return regionList.Append(tree.NewDString(region.String()))
					}); err != nil {
						return err
					}
					return addRow(
						tree.NewDInt(tree.DInt(db.GetID())), // id
						tree.NewDString(db.GetName()),       // database_name
						tree.NewDString(superRegion),        // super_region_name
						regionList,                          // regions
					)
				})
			})
	},
}

func makeCrdbInternalTablesAddRowFn(
	p *planner, pusher func(...tree.Datum) error,
) func(table catalog.TableDescriptor, dbName tree.Datum, scName string) error {
	const numDatums = 16
	row := make(tree.Datums, numDatums)
	return func(table catalog.TableDescriptor, dbName tree.Datum, scName string) (err error) {
		dropTimeDatum := tree.DNull
		if dropTime := table.GetDropTime(); dropTime != 0 {
			dropTimeDatum, err = tree.MakeDTimestamp(
				timeutil.Unix(0, dropTime), time.Nanosecond,
			)
			if err != nil {
				return err
			}
		}
		locality := tree.DNull
		if c := table.GetLocalityConfig(); c != nil {
			f := p.EvalContext().FmtCtx(tree.FmtSimple)
			if err := multiregion.FormatTableLocalityConfig(c, f); err != nil {
				return err
			}
			locality = tree.NewDString(f.String())
		}
		row = append(row[:0],
			tree.NewDInt(tree.DInt(int64(table.GetID()))),
			tree.NewDInt(tree.DInt(int64(table.GetParentID()))),
			tree.NewDString(table.GetName()),
			dbName,
			tree.NewDInt(tree.DInt(int64(table.GetVersion()))),
			eval.TimestampToInexactDTimestamp(table.GetModificationTime()),
			eval.TimestampToDecimalDatum(table.GetModificationTime()),
			tree.NewDString(table.GetFormatVersion().String()),
			tree.NewDString(table.GetState().String()),
			tree.DNull, // sc_lease_node_id is deprecated
			tree.DNull, // sc_lease_expiration_time is deprecated
			dropTimeDatum,
			tree.NewDString(table.GetAuditMode().String()),
			tree.NewDString(scName),
			tree.NewDInt(tree.DInt(int64(table.GetParentSchemaID()))),
			locality,
		)
		if buildutil.CrdbTestBuild {
			if len(row) != numDatums {
				return errors.AssertionFailedf("expected %d datums, got %d", numDatums, len(row))
			}
		}
		return pusher(row...)
	}
}

// TODO(tbg): prefix with kv_.
var crdbInternalTablesTable = virtualSchemaTable{
	comment: `table descriptors accessible by current user, including non-public and virtual (KV scan; expensive!)`,
	schema: `
CREATE TABLE crdb_internal.tables (
    table_id                 INT8 NOT NULL,
    parent_id                INT8 NOT NULL,
    name                     STRING NOT NULL,
    database_name            STRING,
    version                  INT8 NOT NULL,
    mod_time                 TIMESTAMP NOT NULL,
    mod_time_logical         DECIMAL NOT NULL,
    format_version           STRING NOT NULL,
    state                    STRING NOT NULL,
    sc_lease_node_id         INT8,
    sc_lease_expiration_time TIMESTAMP,
    drop_time                TIMESTAMP,
    audit_mode               STRING NOT NULL,
    schema_name              STRING NOT NULL,
    parent_schema_id         INT8 NOT NULL,
    locality                 STRING,
    INDEX (parent_id) WHERE drop_time IS NULL,
    INDEX (database_name) WHERE drop_time IS NULL
);`,
	indexes: []virtualIndex{
		{
			// INDEX(parent_id) WHERE drop_time IS NULL
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				dbID := descpb.ID(tree.MustBeDInt(unwrappedConstraint))
				db, err := p.byIDGetterBuilder().WithoutDropped().Get().Database(ctx, dbID)
				if err != nil {
					return false, err
				}
				return crdbInternalTablesDatabaseLookupFunc(ctx, p, db, addRow)
			},
		},
		{
			// INDEX(database_name) WHERE drop_time IS NULL
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				dbName := string(tree.MustBeDString(unwrappedConstraint))
				db, err := p.byNameGetterBuilder().WithOffline().MaybeGet().Database(ctx, dbName)
				if db == nil || err != nil {
					return false, err
				}
				return crdbInternalTablesDatabaseLookupFunc(ctx, p, db, addRow)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		all, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		descs := all.OrderedDescriptors()
		dbNames := make(map[descpb.ID]string)
		scNames := make(map[descpb.ID]string)
		// TODO(richardjcai): Remove this case for keys.PublicSchemaID in 22.2.
		scNames[keys.PublicSchemaID] = catconstants.PublicSchemaName
		// Record database descriptors for name lookups.
		for _, desc := range descs {
			if dbDesc, ok := desc.(catalog.DatabaseDescriptor); ok {
				dbNames[dbDesc.GetID()] = dbDesc.GetName()
			}
			if scDesc, ok := desc.(catalog.SchemaDescriptor); ok {
				scNames[scDesc.GetID()] = scDesc.GetName()
			}
		}
		addDesc := makeCrdbInternalTablesAddRowFn(p, addRow)

		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, isTable := desc.(catalog.TableDescriptor)
			if !isTable {
				continue
			}
			if ok, err := p.HasAnyPrivilege(ctx, table); err != nil {
				return err
			} else if !ok {
				continue
			}
			dbName := dbNames[table.GetParentID()]
			if dbName == "" {
				// The parent database was deleted. This is possible e.g. when
				// a database is dropped with CASCADE, and someone queries
				// this virtual table before the dropped table descriptors are
				// effectively deleted.
				dbName = fmt.Sprintf("[%d]", table.GetParentID())
			}
			schemaName := scNames[table.GetParentSchemaID()]
			if schemaName == "" {
				// The parent schema was deleted, possibly due to reasons mentioned above.
				schemaName = fmt.Sprintf("[%d]", table.GetParentSchemaID())
			}
			if err := addDesc(table, tree.NewDString(dbName), schemaName); err != nil {
				return err
			}
		}

		// Also add all the virtual descriptors.
		vt := p.getVirtualTabler()
		vSchemas := vt.getSchemas()
		for _, virtSchemaName := range vt.getSchemaNames() {
			e := vSchemas[virtSchemaName]
			for _, tName := range e.orderedDefNames {
				vTableEntry := e.defs[tName]
				if err := addDesc(vTableEntry.desc, tree.DNull, virtSchemaName); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

func crdbInternalTablesDatabaseLookupFunc(
	ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
) (bool, error) {
	var descs nstree.Catalog
	var err error
	if useIndexLookupForDescriptorsInDatabase.Get(&p.EvalContext().Settings.SV) {
		descs, err = p.Descriptors().GetAllDescriptorsForDatabase(ctx, p.Txn(), db)
	} else {
		descs, err = p.Descriptors().GetAllDescriptors(ctx, p.Txn())
	}
	if err != nil {
		return false, err
	}

	scNames := make(map[descpb.ID]string)
	// TODO(richardjcai): Remove this case for keys.PublicSchemaID in 22.2.
	scNames[keys.PublicSchemaID] = catconstants.PublicSchemaName
	// Record database descriptors for name lookups.
	dbID := db.GetID()
	_ = descs.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetParentID() != dbID {
			return nil
		}
		if scDesc, ok := desc.(catalog.SchemaDescriptor); ok {
			scNames[scDesc.GetID()] = scDesc.GetName()
		}
		return nil
	})
	rf := makeCrdbInternalTablesAddRowFn(p, addRow)
	var seenAny bool
	if err := descs.ForEachDescriptor(func(desc catalog.Descriptor) error {
		if desc.GetParentID() != dbID {
			return nil
		}
		table, isTable := desc.(catalog.TableDescriptor)
		if !isTable {
			return nil
		}
		if ok, err := p.HasAnyPrivilege(ctx, table); err != nil {
			return err
		} else if !ok {
			return nil
		}
		seenAny = true
		schemaName := scNames[table.GetParentSchemaID()]
		if schemaName == "" {
			// The parent schema was deleted, possibly due to reasons mentioned above.
			schemaName = fmt.Sprintf("[%d]", table.GetParentSchemaID())
		}
		return rf(table, tree.NewDString(db.GetName()), schemaName)
	}); err != nil {
		return false, err
	}

	return seenAny, nil
}

var crdbInternalPgCatalogTableIsImplementedTable = virtualSchemaTable{
	comment: `which entries of pg_catalog are implemented in this version of CockroachDB`,
	schema: `
CREATE TABLE crdb_internal.pg_catalog_table_is_implemented (
  name                     STRING NOT NULL,
  implemented              BOOL
)`,
	generator: func(ctx context.Context, p *planner, dbDesc catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		row := make(tree.Datums, 2)
		worker := func(ctx context.Context, pusher rowPusher) error {
			addDesc := func(table *virtualDefEntry, dbName tree.Datum, scName string) error {
				tableDesc := table.desc
				row = append(row[:0],
					tree.NewDString(tableDesc.GetName()),
					tree.MakeDBool(tree.DBool(table.unimplemented)),
				)
				return pusher.pushRow(row...)
			}
			vt := p.getVirtualTabler()
			vSchemas := vt.getSchemas()
			e := vSchemas["pg_catalog"]
			for _, tName := range e.orderedDefNames {
				vTableEntry := e.defs[tName]
				if err := addDesc(vTableEntry, tree.DNull, "pg_catalog"); err != nil {
					return err
				}
			}
			return nil
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// statsAsOfTimeClusterMode controls the cluster setting for the duration which
// is used to define the AS OF time for querying the system.table_statistics
// table when building crdb_internal.table_row_statistics.
var statsAsOfTimeClusterMode = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	"sql.crdb_internal.table_row_statistics.as_of_time",
	"historical query time used to build the crdb_internal.table_row_statistics table",
	-10*time.Second,
)

var crdbInternalTablesTableLastStats = virtualSchemaTable{
	comment: "stats for all tables accessible by current user in current database as of 10s ago",
	schema: `
CREATE TABLE crdb_internal.table_row_statistics (
  table_id                   INT         NOT NULL,
  table_name                 STRING      NOT NULL,
  estimated_row_count        INT
)`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// Collect the statistics for all tables AS OF 10 seconds ago to avoid
		// contention on the stats table. We pass a nil transaction so that the AS
		// OF clause can be independent of any outer query.
		query := fmt.Sprintf(`
           SELECT s."tableID", max(s."rowCount")
             FROM system.table_statistics AS s
             JOIN (
                    SELECT "tableID", max("createdAt") AS last_dt
                      FROM system.table_statistics
                     GROUP BY "tableID"
                  ) AS l ON l."tableID" = s."tableID" AND l.last_dt = s."createdAt"
            AS OF SYSTEM TIME '%s'
            GROUP BY s."tableID"`, statsAsOfTimeClusterMode.String(&p.ExecCfg().Settings.SV))
		statRows, err := p.ExtendedEvalContext().ExecCfg.InternalDB.Executor().QueryBufferedEx(
			ctx, "crdb-internal-statistics-table", nil,
			sessiondata.NodeUserSessionDataOverride,
			query)
		if err != nil {
			// This query is likely to cause errors due to SHOW TABLES being run less
			// than 10 seconds after cluster startup (10s is the default AS OF time
			// for the query), causing the error "descriptor not found". We should
			// tolerate this error and return nil.
			if errors.Is(err, catalog.ErrDescriptorNotFound) {
				return nil
			}
			return err
		}

		// Convert statistics into map: tableID -> rowCount.
		statMap := make(map[tree.DInt]tree.Datum, len(statRows))
		for _, r := range statRows {
			statMap[tree.MustBeDInt(r[0])] = r[1]
		}

		// Walk over all available tables and show row count for each of them
		// using collected statistics.
		opts := forEachTableDescOptions{virtualOpts: virtualMany, allowAdding: true}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				table := descCtx.table
				tableID := tree.DInt(table.GetID())
				rowCount := tree.DNull
				// For Virtual Tables report NULL row count.
				if !table.IsVirtualTable() {
					rowCount = tree.NewDInt(0)
					if cnt, ok := statMap[tableID]; ok {
						rowCount = cnt
					}
				}
				return addRow(
					tree.NewDInt(tableID),
					tree.NewDString(table.GetName()),
					rowCount,
				)
			},
		)
	},
}

// TODO(tbg): prefix with kv_.
var crdbInternalSchemaChangesTable = virtualSchemaTable{
	comment: `ongoing schema changes, across all descriptors accessible by current user (KV scan; expensive!)`,
	schema: `
CREATE TABLE crdb_internal.schema_changes (
  table_id      INT NOT NULL,
  parent_id     INT NOT NULL,
  name          STRING NOT NULL,
  type          STRING NOT NULL,
  target_id     INT,
  target_name   STRING,
  state         STRING NOT NULL,
  direction     STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		all, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		descs := all.OrderedDescriptors()
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, isTable := desc.(catalog.TableDescriptor)
			if !isTable {
				continue
			}
			if ok, err := p.HasAnyPrivilege(ctx, table); err != nil {
				return err
			} else if !ok {
				continue
			}
			tableID := tree.NewDInt(tree.DInt(int64(table.GetID())))
			parentID := tree.NewDInt(tree.DInt(int64(table.GetParentID())))
			tableName := tree.NewDString(table.GetName())
			for _, mut := range table.TableDesc().Mutations {
				mutType := "UNKNOWN"
				targetID := tree.DNull
				targetName := tree.DNull
				switch d := mut.Descriptor_.(type) {
				case *descpb.DescriptorMutation_Column:
					mutType = "COLUMN"
					targetID = tree.NewDInt(tree.DInt(int64(d.Column.ID)))
					targetName = tree.NewDString(d.Column.Name)
				case *descpb.DescriptorMutation_Index:
					mutType = "INDEX"
					targetID = tree.NewDInt(tree.DInt(int64(d.Index.ID)))
					targetName = tree.NewDString(d.Index.Name)
				case *descpb.DescriptorMutation_Constraint:
					mutType = "CONSTRAINT VALIDATION"
					targetName = tree.NewDString(d.Constraint.Name)
				}
				if err := addRow(
					tableID,
					parentID,
					tableName,
					tree.NewDString(mutType),
					targetID,
					targetName,
					tree.NewDString(mut.State.String()),
					tree.NewDString(mut.Direction.String()),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

type crdbInternalLeasesTableEntry struct {
	desc         catalog.Descriptor
	takenOffline bool
	expiration   tree.DTimestamp
}

// TODO(tbg): prefix with node_.
var crdbInternalLeasesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.leases (
  node_id     INT NOT NULL,
  table_id    INT NOT NULL,
  name        STRING NOT NULL,
  parent_id   INT NOT NULL,
  expiration  TIMESTAMP NOT NULL,
  deleted     BOOL NOT NULL
)`,
	comment: `acquired table leases (RAM; local node only)`,
	populate: func(
		ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}
		nodeID, _ := p.execCfg.NodeInfo.NodeID.OptionalNodeID() // zero if not available
		var leaseEntries []crdbInternalLeasesTableEntry
		p.LeaseMgr().VisitLeases(func(desc catalog.Descriptor, takenOffline bool, _ int, expiration tree.DTimestamp) (wantMore bool) {
			// Because we have locked the lease manager while visiting every lease,
			// it not safe to *acquire* or *release* any leases in the process. As
			// a result, build an in memory cache of entries and process them after
			// all the locks have been released. Previously we would check privileges,
			// which would need to get the lease on the role_member table. Simply skipping
			// this check on that table will not be sufficient since any active lease
			// on it could expire or need an renewal and cause the sample problem.
			leaseEntries = append(leaseEntries, crdbInternalLeasesTableEntry{
				desc:         desc,
				takenOffline: takenOffline,
				expiration:   expiration,
			})
			return true
		})
		for _, entry := range leaseEntries {
			if ok, err := p.HasAnyPrivilege(ctx, entry.desc); err != nil {
				return err
			} else if !ok {
				continue
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(nodeID)),
				tree.NewDInt(tree.DInt(int64(entry.desc.GetID()))),
				tree.NewDString(entry.desc.GetName()),
				tree.NewDInt(tree.DInt(int64(entry.desc.GetParentID()))),
				&entry.expiration,
				tree.MakeDBool(tree.DBool(entry.takenOffline)),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

func tsOrNull(micros int64) (tree.Datum, error) {
	if micros == 0 {
		return tree.DNull, nil
	}
	ts := timeutil.Unix(0, micros*time.Microsecond.Nanoseconds())
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

const (
	// systemJobsAndJobInfoBaseQuery consults both the `system.jobs` and
	// `system.job_info` tables to return relevant information about a job.
	//
	// NB: Every job on creation writes a row each for its payload and progress to
	// the `system.job_info` table. For a given job there will always be at most
	// one row each for its payload and progress. This is because of the
	// `system.job_info` write semantics described `InfoStorage.Write`.
	// Theoretically, a job could have no rows corresponding to its progress and
	// so we perform a LEFT JOIN to get a NULL value when no progress row is
	// found.
	systemJobsAndJobInfoBaseQuery = `
SELECT
DISTINCT(id), status, created, payload.value AS payload, progress.value AS progress,
created_by_type, created_by_id, claim_session_id, claim_instance_id, num_runs, last_run, job_type
FROM
system.jobs AS j
LEFT JOIN system.job_info AS progress ON j.id = progress.job_id AND progress.info_key = 'legacy_progress'
INNER JOIN system.job_info AS payload ON j.id = payload.job_id AND payload.info_key = 'legacy_payload'
`
	systemJobsIDPredicate     = ` WHERE id = $1`
	systemJobsTypePredicate   = ` WHERE job_type = $1`
	systemJobsStatusPredicate = ` WHERE status = $1`
)

type systemJobsPredicate int

const (
	noPredicate systemJobsPredicate = iota
	jobID
	jobType
	jobStatus
)

func getInternalSystemJobsQuery(predicate systemJobsPredicate) string {
	switch predicate {
	case noPredicate:
		return systemJobsAndJobInfoBaseQuery
	case jobID:
		return systemJobsAndJobInfoBaseQuery + systemJobsIDPredicate
	case jobType:
		return systemJobsAndJobInfoBaseQuery + systemJobsTypePredicate
	case jobStatus:
		return systemJobsAndJobInfoBaseQuery + systemJobsStatusPredicate
	}

	return ""
}

// TODO(tbg): prefix with kv_.
var crdbInternalSystemJobsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.system_jobs (
  id                INT8      NOT NULL,
  status            STRING    NOT NULL,
  created           TIMESTAMP NOT NULL,
  payload           BYTES     NOT NULL,
  progress          BYTES,
  created_by_type   STRING,
  created_by_id     INT,
  claim_session_id  BYTES,
  claim_instance_id INT8,
  num_runs          INT8,
  last_run          TIMESTAMP,
  job_type          STRING,
  INDEX (id),
  INDEX (job_type),
  INDEX (status)
)`,
	comment: `wrapper over system.jobs with row access control (KV scan)`,
	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				q := getInternalSystemJobsQuery(jobID)
				targetType := tree.MustBeDInt(unwrappedConstraint)
				return populateSystemJobsTableRows(ctx, p, addRow, q, targetType)
			},
		},
		{
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				q := getInternalSystemJobsQuery(jobType)
				targetType := tree.MustBeDString(unwrappedConstraint)
				return populateSystemJobsTableRows(ctx, p, addRow, q, targetType)
			},
		},
		{
			populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				q := getInternalSystemJobsQuery(jobStatus)
				targetType := tree.MustBeDString(unwrappedConstraint)
				return populateSystemJobsTableRows(ctx, p, addRow, q, targetType)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		_, err := populateSystemJobsTableRows(ctx, p, addRow, getInternalSystemJobsQuery(noPredicate))
		return err
	},
}

// populateSystemJobsTableRows calls addRow for all rows of the system.jobs table
// except for rows that the user does not have access to. It returns true
// if at least one row was generated.
func populateSystemJobsTableRows(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	query string,
	params ...interface{},
) (result bool, retErr error) {
	const jobIdIdx = 0
	const jobPayloadIdx = 3

	matched := false

	// Note: we query system.jobs as root, so we must be careful about which rows we return.
	it, err := p.InternalSQLTxn().QueryIteratorEx(ctx,
		"system-jobs-scan",
		p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		query,
		params...,
	)
	if err != nil {
		return matched, err
	}

	cleanup := func(ctx context.Context) {
		if err := it.Close(); err != nil {
			retErr = errors.CombineErrors(retErr, err)
		}
	}
	defer cleanup(ctx)

	globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(ctx, p)
	if err != nil {
		return matched, err
	}

	for {
		hasNext, err := it.Next(ctx)
		if !hasNext || err != nil {
			return matched, err
		}

		currentRow := it.Cur()
		jobID, err := strconv.Atoi(currentRow[jobIdIdx].String())
		if err != nil {
			return matched, err
		}
		payloadBytes := currentRow[jobPayloadIdx]
		payload, err := jobs.UnmarshalPayload(payloadBytes)
		if err != nil {
			return matched, wrapPayloadUnMarshalError(err, currentRow[jobIdIdx])
		}
		getLegacyPayload := func(ctx context.Context) (*jobspb.Payload, error) {
			return payload, nil
		}
		err = jobsauth.Authorize(
			ctx, p, jobspb.JobID(jobID), getLegacyPayload, payload.UsernameProto.Decode(), payload.Type(), jobsauth.ViewAccess, globalPrivileges,
		)
		if err != nil {
			// Filter out jobs which the user is not allowed to see.
			if IsInsufficientPrivilegeError(err) {
				continue
			}
			return matched, err
		}

		if err := addRow(currentRow...); err != nil {
			return matched, err
		}
		matched = true
	}
}

func wrapPayloadUnMarshalError(err error, jobID tree.Datum) error {
	return errors.WithHintf(err, "could not decode the payload for job %s."+
		" consider deleting this job from system.jobs", jobID)
}

const (
	jobsQuery = `SELECT id, status, created::timestamptz, payload, progress, claim_session_id, claim_instance_id FROM crdb_internal.system_jobs j`
	// Note that we are querying crdb_internal.system_jobs instead of system.jobs directly.
	// The former has access control built in and will filter out jobs that the
	// user is not allowed to see.
	jobsQFrom        = ` `
	jobIDFilter      = ` WHERE j.id = $1`
	jobsStatusFilter = ` WHERE j.status = $1`
	jobsTypeFilter   = ` WHERE j.job_type = $1`
)

// TODO(tbg): prefix with kv_.
var crdbInternalJobsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.jobs (
  job_id                INT,
  job_type              STRING,
  description           STRING,
  statement             STRING,
  user_name             STRING,
  descriptor_ids        INT[],
  status                STRING,
  running_status        STRING,
  created               TIMESTAMPTZ,
  started               TIMESTAMPTZ,
  finished              TIMESTAMPTZ,
  modified              TIMESTAMPTZ,
  fraction_completed    FLOAT,
  high_water_timestamp  DECIMAL,
  error                 STRING,
  coordinator_id        INT,
  trace_id              INT,
  execution_errors      STRING[],
  execution_events      JSONB,
  INDEX(job_id),
  INDEX(status),
  INDEX(job_type)
)`,
	comment: `decoded job metadata from crdb_internal.system_jobs (KV scan)`,
	indexes: []virtualIndex{{
		populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
			targetID := tree.MustBeDInt(unwrappedConstraint)
			return makeJobsTableRows(ctx, p, addRow, jobIDFilter, targetID)
		},
	}, {
		populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
			targetStatus := tree.MustBeDString(unwrappedConstraint)
			return makeJobsTableRows(ctx, p, addRow, jobsStatusFilter, targetStatus)
		},
	}, {
		populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
			targetType := tree.MustBeDString(unwrappedConstraint)
			return makeJobsTableRows(ctx, p, addRow, jobsTypeFilter, targetType)
		},
	}},
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		_, err := makeJobsTableRows(ctx, p, addRow, "")
		return err
	},
}

var useOldJobsVTable = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.jobs.legacy_vtable.enabled",
	"cause the crdb_internal.jobs vtable to be produced from the legacy payload info records",
	false, // TODO(dt): flip this once we add permissive auth checks.
)

// makeJobsTableRows calls addRow for each job. It returns true if addRow was called
// successfully at least once.
func makeJobsTableRows(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	queryFilterSuffix string,
	params ...interface{},
) (matched bool, err error) {

	v, err := p.InternalSQLTxn().GetSystemSchemaVersion(ctx)
	if err != nil {
		return false, err
	}
	if !v.AtLeast(clusterversion.V25_1.Version()) || useOldJobsVTable.Get(&p.EvalContext().Settings.SV) {
		query := jobsQuery + queryFilterSuffix
		return makeLegacyJobsTableRows(ctx, p, addRow, query, params...)
	}
	return makeJobBasedJobsTableRows(ctx, p, addRow, queryFilterSuffix, params...)
}

func makeLegacyJobsTableRows(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	query string,
	params ...interface{},
) (matched bool, err error) {
	// We use QueryIteratorEx here and specify the current user
	// instead of using InternalExecutor.QueryIterator because
	// the latter is being deprecated for sometimes executing
	// the query as the root user.
	it, err := p.InternalSQLTxn().QueryIteratorEx(
		ctx, "crdb-internal-jobs-table", p.txn,
		sessiondata.InternalExecutorOverride{User: p.User()},
		query, params...)
	if err != nil {
		return matched, err
	}

	cleanup := func(ctx context.Context) {
		if err := it.Close(); err != nil {
			// TODO(yuzefovich): this error should be propagated further up
			// and not simply being logged. Fix it (#61123).
			//
			// Doing that as a return parameter would require changes to
			// `planNode.Close` signature which is a bit annoying. One other
			// possible solution is to panic here and catch the error
			// somewhere.
			log.Warningf(ctx, "error closing an iterator: %v", err)
		}
	}
	defer cleanup(ctx)

	sessionJobs := make([]*jobs.Record, 0, p.extendedEvalCtx.jobs.numToCreate())
	uniqueJobs := make(map[*jobs.Record]struct{})
	if err := p.extendedEvalCtx.jobs.forEachToCreate(func(job *jobs.Record) error {
		if _, ok := uniqueJobs[job]; ok {
			return nil
		}
		sessionJobs = append(sessionJobs, job)
		uniqueJobs[job] = struct{}{}
		return nil
	}); err != nil {
		return matched, err
	}

	// Loop while we need to skip a row.
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return matched, err
		}
		var id, status, created, payloadBytes, progressBytes, sessionIDBytes,
			instanceID tree.Datum
		if ok {
			r := it.Cur()
			id, status, created, payloadBytes, progressBytes, sessionIDBytes, instanceID =
				r[0], r[1], r[2], r[3], r[4], r[5], r[6]
		} else if !ok {
			if len(sessionJobs) == 0 {
				return matched, nil
			}
			job := sessionJobs[len(sessionJobs)-1]
			sessionJobs = sessionJobs[:len(sessionJobs)-1]
			// Convert the job into datums, where protobufs will be intentionally,
			// marshalled.
			id = tree.NewDInt(tree.DInt(job.JobID))
			status = tree.NewDString(string(jobs.StatePending))
			created = tree.MustMakeDTimestampTZ(timeutil.Unix(0, p.txn.ReadTimestamp().WallTime), time.Microsecond)
			progressBytes, payloadBytes, err = getPayloadAndProgressFromJobsRecord(p, job)
			if err != nil {
				return matched, err
			}
			sessionIDBytes = tree.NewDBytes(tree.DBytes(p.extendedEvalCtx.SessionID.GetBytes()))
			instanceID = tree.NewDInt(tree.DInt(p.extendedEvalCtx.ExecCfg.JobRegistry.ID()))
		}

		var jobType, description, statement, user, descriptorIDs, started, runningStatus,
			finished, modified, fractionCompleted, highWaterTimestamp, errorStr, coordinatorID,
			traceID, executionErrors, executionEvents = tree.DNull, tree.DNull, tree.DNull,
			tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull,
			tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull

		// Extract data from the payload.
		payload, err := jobs.UnmarshalPayload(payloadBytes)
		if err != nil {
			return matched, wrapPayloadUnMarshalError(err, id)
		}

		// We filter out masked rows before we allocate all the
		// datums. Needless allocate when not necessary.
		sqlUsername := payload.UsernameProto.Decode()
		if sessionID, ok := sessionIDBytes.(*tree.DBytes); ok {
			if isAlive, err := p.EvalContext().SQLLivenessReader.IsAlive(
				ctx, sqlliveness.SessionID(*sessionID),
			); err != nil {
				// Silently swallow the error for checking for liveness.
			} else if instanceID, ok := instanceID.(*tree.DInt); ok && isAlive {
				coordinatorID = instanceID
			}
		}

		// TODO(jayant): we can select the job_type as a column
		// rather than decoding the payload. This would allow us
		// to create a virtual index on it.
		jobType = tree.NewDString(payload.Type().String())
		description = tree.NewDString(payload.Description)
		statement = tree.NewDString(strings.Join(payload.Statement, "; "))
		user = tree.NewDString(sqlUsername.Normalized())
		descriptorIDsArr := tree.NewDArray(types.Int)
		for _, descID := range payload.DescriptorIDs {
			if err := descriptorIDsArr.Append(tree.NewDInt(tree.DInt(int(descID)))); err != nil {
				return matched, err
			}
		}
		descriptorIDs = descriptorIDsArr
		started, err = tsOrNull(payload.StartedMicros)
		if err != nil {
			return matched, err
		}
		finished, err = tsOrNull(payload.FinishedMicros)
		if err != nil {
			return matched, err
		}
		errorStr = tree.NewDString(payload.Error)

		// Extract data from the progress field.
		if progressBytes != tree.DNull {
			progress, err := jobs.UnmarshalProgress(progressBytes)
			if err != nil {
				baseErr := ""
				if s, ok := errorStr.(*tree.DString); ok {
					baseErr = string(*s)
					if baseErr != "" {
						baseErr += "\n"
					}
				}
				errorStr = tree.NewDString(fmt.Sprintf("%serror decoding progress: %v", baseErr, err))
			} else {
				// Progress contains either fractionCompleted for traditional jobs,
				// or the highWaterTimestamp for change feeds.
				if highwater := progress.GetHighWater(); highwater != nil {
					highWaterTimestamp = eval.TimestampToDecimalDatum(*highwater)
				} else {
					fractionCompleted = tree.NewDFloat(tree.DFloat(progress.GetFractionCompleted()))
				}
				modified, err = tsOrNull(progress.ModifiedMicros)
				if err != nil {
					return matched, err
				}

				if s, ok := status.(*tree.DString); ok {
					if jobs.State(*s) == jobs.StateRunning && len(progress.StatusMessage) > 0 {
						runningStatus = tree.NewDString(progress.StatusMessage)
					} else if jobs.State(*s) == jobs.StatePaused && payload != nil && payload.PauseReason != "" {
						errorStr = tree.NewDString(fmt.Sprintf("%s: %s", jobs.PauseRequestExplained, payload.PauseReason))
					}
				}
				traceID = tree.NewDInt(tree.DInt(progress.TraceID))
			}
		}

		if err = addRow(
			id,
			jobType,
			description,
			statement,
			user,
			descriptorIDs,
			status,
			runningStatus,
			created,
			started,
			finished,
			modified,
			fractionCompleted,
			highWaterTimestamp,
			errorStr,
			coordinatorID,
			traceID,
			executionErrors,
			executionEvents,
		); err != nil {
			return matched, err
		}
		matched = true
	}
}

var enablePerJobDetailedAuthLookups = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.jobs.legacy_per_job_access_via_details.enabled",
	"enables granting additional access to jobs beyond owners and roles based on the specific details (tables being watched/backed up/etc) of the individual jobs (may make SHOW JOBS less performant)",
	true,
)

var errLegacyPerJobAuthDisabledSentinel = pgerror.Newf(pgcode.InsufficientPrivilege, "legacy job access based on details is disabled")

func makeJobBasedJobsTableRows(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	whereClause string,
	params ...interface{},
) (emitted bool, retErr error) {
	globalPrivileges, err := jobsauth.GetGlobalJobPrivileges(ctx, p)
	if err != nil {
		return false, err
	}

	query := `SELECT 
j.id, j.job_type, coalesce(j.description, ''), coalesce(j.owner, ''), j.status as state,
s.status, j.created::timestamptz, j.finished, greatest(j.created, j.finished, p.written, s.written)::timestamptz AS last_modified,
p.fraction,
p.resolved,
j.error_msg,
j.claim_instance_id
FROM system.public.jobs AS j
LEFT OUTER JOIN system.public.job_progress AS p ON j.id = p.job_id
LEFT OUTER JOIN system.public.job_status AS s ON j.id = s.job_id  
	` + whereClause

	it, err := p.InternalSQLTxn().QueryIteratorEx(
		ctx, "system-jobs-join", p.txn, sessiondata.NodeUserSessionDataOverride, query, params...)
	if err != nil {
		return emitted, err
	}
	defer func() {
		if err := it.Close(); err != nil {
			retErr = errors.CombineErrors(retErr, err)
		}
	}()

	sessionJobs := make([]*jobs.Record, 0, p.extendedEvalCtx.jobs.numToCreate())
	uniqueJobs := make(map[*jobs.Record]struct{})
	if err := p.extendedEvalCtx.jobs.forEachToCreate(func(job *jobs.Record) error {
		if _, ok := uniqueJobs[job]; ok {
			return nil
		}
		sessionJobs = append(sessionJobs, job)
		uniqueJobs[job] = struct{}{}
		return nil
	}); err != nil {
		return emitted, err
	}

	// Loop while we need to skip a row.
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return emitted, err
		}
		// We will read the columns from the query on joined jobs tables into a wide
		// row, and then copy the values from read rows into named variables to then
		// use when emitting our output row. If we need to synthesize rows for jobs
		// pending creation in the session, we'll do so in those same named vars to
		// keep things organized.
		//   0,      1,    2,        3,     4,      5,       6,        7,        8,        9,       10,       11,         12
		var id, typStr, desc, ownerStr, state, status, created, finished, modified, fraction, resolved, errorMsg, instanceID tree.Datum

		if ok {
			r := it.Cur()
			id, typStr, desc, ownerStr, state, status, created, finished, modified, fraction, resolved, errorMsg, instanceID =
				r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9], r[10], r[11], r[12]

			owner := username.MakeSQLUsernameFromPreNormalizedString(string(tree.MustBeDString(ownerStr)))
			jobID := jobspb.JobID(tree.MustBeDInt(id))
			typ, err := jobspb.TypeFromString(string(tree.MustBeDString(typStr)))
			if err != nil {
				return emitted, err
			}

			getLegacyPayloadForAuth := func(ctx context.Context) (*jobspb.Payload, error) {
				if !enablePerJobDetailedAuthLookups.Get(&p.EvalContext().Settings.SV) {
					return nil, errLegacyPerJobAuthDisabledSentinel
				}
				if p.EvalContext().Settings.Version.IsActive(ctx, clusterversion.V25_1) {
					log.Warningf(ctx, "extended job access control based on job-specific details is deprecated and can make SHOW JOBS less performant; consider disabling %s",
						enablePerJobDetailedAuthLookups.Name())
					p.BufferClientNotice(ctx,
						pgnotice.Newf("extended job access control based on job-specific details has been deprecated and can make SHOW JOBS less performant; consider disabling %s",
							enablePerJobDetailedAuthLookups.Name()))
				}
				payload := &jobspb.Payload{}
				infoStorage := jobs.InfoStorageForJob(p.InternalSQLTxn(), jobID)
				payloadBytes, exists, err := infoStorage.GetLegacyPayload(ctx, "getLegacyPayload-for-custom-auth")
				if err != nil {
					return nil, err
				}
				if !exists {
					return nil, errors.New("job payload not found in system.job_info")
				}
				if err := protoutil.Unmarshal(payloadBytes, payload); err != nil {
					return nil, err
				}
				return payload, nil
			}
			if errorMsg == tree.DNull {
				errorMsg = emptyString
			}

			if err := jobsauth.Authorize(
				ctx, p, jobID, getLegacyPayloadForAuth, owner, typ, jobsauth.ViewAccess, globalPrivileges,
			); err != nil {
				// Filter out jobs which the user is not allowed to see.
				if IsInsufficientPrivilegeError(err) {
					continue
				}
				return emitted, err
			}
		} else if !ok {
			if len(sessionJobs) == 0 {
				return emitted, nil
			}
			job := sessionJobs[len(sessionJobs)-1]
			sessionJobs = sessionJobs[:len(sessionJobs)-1]
			payloadType, err := jobspb.DetailsType(jobspb.WrapPayloadDetails(job.Details))
			if err != nil {
				return emitted, err
			}
			// synthesize the fields we'd read from the jobs table if this job were in it.
			id, typStr, desc, ownerStr, state, status, created, finished, modified, fraction, resolved, errorMsg, instanceID =
				tree.NewDInt(tree.DInt(job.JobID)),
				tree.NewDString(payloadType.String()),
				tree.NewDString(job.Description),
				tree.NewDString(job.Username.Normalized()),
				tree.NewDString(string(jobs.StatePending)),
				tree.DNull,
				tree.MustMakeDTimestampTZ(p.txn.ReadTimestamp().GoTime(), time.Microsecond),
				tree.DNull,
				tree.MustMakeDTimestampTZ(p.txn.ReadTimestamp().GoTime(), time.Microsecond),
				tree.NewDFloat(tree.DFloat(0)),
				tree.DZeroDecimal,
				tree.DNull,
				tree.NewDInt(tree.DInt(p.extendedEvalCtx.ExecCfg.JobRegistry.ID()))
		}

		if err = addRow(
			id,
			typStr,
			desc,
			desc,
			ownerStr,
			tree.DNull, // deperecated "descriptor_ids"
			state,
			status,
			created,
			created, // deprecated "started" field.
			finished,
			modified,
			fraction,
			resolved,
			errorMsg,
			instanceID,
			tree.DNull, // deprecated "trace_id" field.
			tree.DNull, // deprecated "executionErrors" field.
			tree.DNull, // deprecated "executionEvents" field.
		); err != nil {
			return emitted, err
		}
		emitted = true
	}
}

const crdbInternalKVProtectedTSTableQuery = `
	SELECT id, ts, meta_type, meta, num_spans, spans, verified, target,
		crdb_internal.pb_to_json(
		  	'cockroach.protectedts.Target',
		  	target,
		    false /* emit defaults */,
		    false /* include redaction marker */
		          /* NB: redactions in the debug zip are handled elsewhere by marking columns as sensitive */
		) as decoded_targets,
	    crdb_internal_mvcc_timestamp
	FROM system.protected_ts_records
`

// TODO (#104161): num_ranges excludes ranges when targeting a database,
// cluster, or tenant.
var crdbInternalKVProtectedTSTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.kv_protected_ts_records (
   id        			UUID NOT NULL,
   ts        			DECIMAL NOT NULL,
   meta_type 			STRING NOT NULL,
   meta      			BYTES,
   num_spans 			INT8 NOT NULL,
   spans     			BYTES NOT NULL, -- We do not decode this column since it is deprecated in 22.2+.
   verified  			BOOL NOT NULL,
   target    			BYTES,
   decoded_meta 		JSON,   -- Decoded data from the meta column above.
                                -- This data can have different structures depending on the meta_type.
   decoded_target 		JSON,   -- Decoded data from the target column above.
   internal_meta        JSON,   -- Additional metadata added by this virtual table (ex. job owner for job meta_type)
   num_ranges  			INT,     -- Number of ranges protected by this PTS record.
   last_updated         DECIMAL -- crdb_internal_mvcc_timestamp of the row
)`,
	comment: `decoded protected timestamp metadata from system.protected_ts_records (KV scan). does not decode `,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (err error) {
		defer func() {
			err = pgerror.Wrap(err, pgcode.Internal, "internal pts table")
		}()

		it, err := p.InternalSQLTxn().QueryIteratorEx(
			ctx, "crdb-internal-protected-timestamps-table", p.txn,
			sessiondata.NodeUserSessionDataOverride,
			crdbInternalKVProtectedTSTableQuery)
		if err != nil {
			return err
		}

		cleanup := func(ctx context.Context) {
			if err := it.Close(); err != nil {
				// TODO(yuzefovich): this error should be propagated further up
				// and not simply being logged. Fix it (#61123).
				log.Warningf(ctx, "error closing an iterator: %v", err)
			}
		}
		defer cleanup(ctx)

		// Create JSON object builders for re-use.
		jobsDecodedMetaBuilder, err := json.NewFixedKeysObjectBuilder(jobsDecodedMetaKeys)
		if err != nil {
			return err
		}
		jobsInternalMetaBuilder, err := json.NewFixedKeysObjectBuilder(jobsInternalMetaKeys)
		if err != nil {
			return err
		}
		schedulesDecodedMetaBuilder, err := json.NewFixedKeysObjectBuilder(schedulesDecodedMetaKeys)
		if err != nil {
			return err
		}
		schedulesInternalMetaBuilder, err := json.NewFixedKeysObjectBuilder(schedulesInternalMetaKeys)
		if err != nil {
			return err
		}

		var id, ts, metaType, meta, numSpans, spans, verified, target, decodedTarget, lastUpdated tree.Datum

		for {
			hasNext, err := it.Next(ctx)
			if err != nil {
				return err
			}
			if !hasNext {
				return nil
			}

			r := it.Cur()
			id, ts, metaType, meta, numSpans, spans, verified, target, decodedTarget, lastUpdated =
				r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8], r[9]

			metaTypeDatum, ok := metaType.(*tree.DString)
			if !ok {
				return errors.AssertionFailedf("could not get datum string from meta type")
			}
			metaTypeBytes := tree.NewFmtCtx(tree.FmtBareStrings)
			metaTypeDatum.Format(metaTypeBytes)
			metaTypeString := metaTypeBytes.CloseAndGetString()

			var decodedMeta tree.Datum
			decodedMeta = tree.DNull
			var internalMeta tree.Datum
			internalMeta = tree.DNull

			// Since this column is nullable in the system table, we should do a check.
			if meta != tree.DNull {
				metaBytes, ok := meta.(*tree.DBytes)
				if !ok {
					return errors.AssertionFailedf("could not get datum bytes from meta")
				}

				switch metaTypeString {
				case jobsprotectedts.GetMetaType(jobsprotectedts.Jobs):
					decodedMeta, internalMeta, err = decodeMetaFieldsForJobs(ctx, p, metaBytes, jobsDecodedMetaBuilder,
						jobsInternalMetaBuilder)
					if err != nil {
						return err
					}
				case jobsprotectedts.GetMetaType(jobsprotectedts.Schedules):
					decodedMeta, internalMeta, err = decodeMetaFieldsForSchedules(ctx, p, metaBytes,
						schedulesDecodedMetaBuilder, schedulesInternalMetaBuilder)
					if err != nil {
						return err
					}
				default:
				}
			}

			// Calculate the number of ranges protected by the PTS record.
			var numRanges tree.Datum
			// -1 is a sentinel value indicating that targets could not be
			// decoded.
			numRanges = tree.NewDInt(-1)
			// Before 22.2, there was no target column in the system table. PTS records created before then which have
			// persisted until the present will have a null target field. Instead of the target field, these PTS records
			// use the spans column. Since the spans column is deprecated, we don't bother decoding it in this virtual
			// table.
			if target != tree.DNull {
				targetBytes, ok := target.(*tree.DBytes)
				if !ok {
					return errors.AssertionFailedf("could not decode target")
				}
				var targetProto ptpb.Target
				// It is safe to use UnsafeBytes if we do not mutate them.
				if err := protoutil.Unmarshal(targetBytes.UnsafeBytes(), &targetProto); err != nil {
					return err
				}
				switch t := targetProto.Union.(type) {
				// TODO (#104161): support range estimate for clusters, tenants, and database schema objects.
				case *ptpb.Target_Cluster:
				case *ptpb.Target_Tenants:
				case *ptpb.Target_SchemaObjects:
					// Looking up leased descriptors can be faster mean they are one version off, which is acceptable for computing
					// the number of ranges.
					descs, err := p.Descriptors().ByIDWithLeased(p.InternalSQLTxn().KV()).Get().Descs(ctx, t.SchemaObjects.IDs)
					if err != nil {
						return err
					}
					var allSpans []roachpb.Span
					for _, desc := range descs {
						switch desc.DescriptorType() {
						case catalog.Table:
							tableDesc := desc.(catalog.TableDescriptor)
							for _, idx := range tableDesc.AllIndexes() {
								idxSpan := tableDesc.IndexSpan(p.execCfg.Codec, idx.GetID())
								allSpans = append(allSpans, idxSpan)
							}
						default:
						}
					}
					ranges, _, err := p.DistSQLPlanner().distSender.AllRangeSpans(ctx, allSpans)
					if err != nil {
						return err
					}
					numRanges = tree.NewDInt(tree.DInt(len(ranges)))
				}
			}

			if err := addRow(id, ts, metaType, meta, numSpans, spans, verified, target, decodedMeta, decodedTarget, internalMeta, numRanges, lastUpdated); err != nil {
				return err
			}

		}
	},
}

var crdbInternalSessionBasedLeases = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.kv_session_based_leases (
  desc_id,
  version, 
  sql_instance_id,
  session_id, 
  crdb_region
) AS (
	SELECT desc_id, version, sql_instance_id, session_id, crdb_region 
	FROM system.lease
);
`,
	comment: `reads from the internal session based leases table`,
	resultColumns: colinfo.ResultColumns{
		{Name: "desc_id", Typ: types.Int},
		{Name: "version", Typ: types.Int},
		{Name: "sql_instance_id", Typ: types.Int},
		{Name: "session_id", Typ: types.Bytes},
		{Name: "crdb_region", Typ: types.Bytes},
	},
}

var schedulesDecodedMetaKeys = []string{"scheduleID"}
var schedulesInternalMetaKeys = []string{"scheduleLabel", "scheduleOwner"}

func decodeMetaFieldsForSchedules(
	ctx context.Context,
	p *planner,
	metaBytes *tree.DBytes,
	decodedMetaBuilder *json.FixedKeysObjectBuilder,
	internalMetaBuilder *json.FixedKeysObjectBuilder,
) (decodedMeta tree.Datum, internalMeta tree.Datum, err error) {
	schedID, err := jobsprotectedts.DecodeID(metaBytes.UnsafeBytes())
	if err != nil {
		return nil, nil, err
	}
	if err := decodedMetaBuilder.Set(schedulesDecodedMetaKeys[0], json.FromInt64(schedID)); err != nil {
		return nil, nil, err
	}
	decodedMetaJSON, err := decodedMetaBuilder.Build()
	if err != nil {
		return nil, nil, err
	}
	decodedMeta = tree.NewDJSON(decodedMetaJSON)

	schedEnv := scheduledjobs.ProdJobSchedulerEnv
	sched, err := jobs.ScheduledJobTxn(p.InternalSQLTxn()).Load(ctx, schedEnv, jobspb.ScheduleID(schedID))
	if err != nil {
		return nil, nil, err
	}
	if err := internalMetaBuilder.Set(schedulesInternalMetaKeys[0], json.FromString(sched.ScheduleLabel())); err != nil {
		return nil, nil, err
	}
	if err := internalMetaBuilder.Set(schedulesInternalMetaKeys[1], json.FromString(sched.Owner().Normalized())); err != nil {
		return nil, nil, err
	}
	internalMetaJSON, err := internalMetaBuilder.Build()
	if err != nil {
		return nil, nil, err
	}
	internalMeta = tree.NewDJSON(internalMetaJSON)

	return decodedMeta, internalMeta, nil
}

var jobsDecodedMetaKeys = []string{"jobID"}
var jobsInternalMetaKeys = []string{"jobUsername"}

func decodeMetaFieldsForJobs(
	ctx context.Context,
	p *planner,
	metaBytes *tree.DBytes,
	decodedMetaBuilder *json.FixedKeysObjectBuilder,
	internalMetaBuilder *json.FixedKeysObjectBuilder,
) (decodedMeta tree.Datum, internalMeta tree.Datum, err error) {
	jobID, err := jobsprotectedts.DecodeID(metaBytes.UnsafeBytes())
	if err != nil {
		return nil, nil, err
	}
	if err := decodedMetaBuilder.Set(jobsDecodedMetaKeys[0], json.FromInt64(jobID)); err != nil {
		return nil, nil, err
	}
	decodedMetaJSON, err := decodedMetaBuilder.Build()
	if err != nil {
		return nil, nil, err
	}
	decodedMeta = tree.NewDJSON(decodedMetaJSON)

	job, err := p.ExecCfg().JobRegistry.LoadJob(ctx, jobspb.JobID(jobID))
	if err != nil {
		return nil, nil, err
	}
	if err := internalMetaBuilder.Set(jobsInternalMetaKeys[0], json.FromString(job.Payload().UsernameProto.Decode().Normalized())); err != nil {
		return nil, nil, err
	}
	internalMetaJSON, err := internalMetaBuilder.Build()
	if err != nil {
		return nil, nil, err
	}
	internalMeta = tree.NewDJSON(internalMetaJSON)

	return decodedMeta, internalMeta, nil
}

// execStatAvg is a helper for execution stats shown in virtual tables. Returns
// NULL when the count is 0, or the mean of the given NumericStat.
func execStatAvg(alloc *tree.DatumAlloc, count int64, n appstatspb.NumericStat) tree.Datum {
	if count == 0 {
		return tree.DNull
	}
	return alloc.NewDFloat(tree.DFloat(n.Mean))
}

// execStatVar is a helper for execution stats shown in virtual tables. Returns
// NULL when the count is 0, or the variance of the given NumericStat.
func execStatVar(alloc *tree.DatumAlloc, count int64, n appstatspb.NumericStat) tree.Datum {
	if count == 0 {
		return tree.DNull
	}
	return alloc.NewDFloat(tree.DFloat(n.GetVariance(count)))
}

// legacyAnonymizedStmt is a placeholder value for the
// crdb_internal.node_statement_statistics(anonymized) column. The column used
// to contain the SQL statement scrubbed of all identifiers. At the time
// of writing this comment, the column was unused for several releases. Since
// it's expensive to compute, we no longer populate it. We keep the column in
// the table to avoid breaking tools that scan crdb_internal tables (even though
// we don't officially support that, this is an easy step to take).
var legacyAnonymizedStmt = tree.NewDString("")

var crdbInternalNodeStmtStatsTable = virtualSchemaTable{
	comment: `statement statistics. ` +
		`The contents of this table are flushed to the system.statement_statistics table at the interval set by the ` +
		`cluster setting sql.stats.flush.interval (by default, 10m).`,
	schema: `
CREATE TABLE crdb_internal.node_statement_statistics (
  node_id             INT NOT NULL,
  application_name    STRING NOT NULL,
  flags               STRING NOT NULL,
  statement_id        STRING NOT NULL,
  key                 STRING NOT NULL,
  anonymized          STRING,
  count               INT NOT NULL,
  first_attempt_count INT NOT NULL,
  max_retries         INT NOT NULL,
  last_error          STRING,
  last_error_code     STRING,
  rows_avg            FLOAT NOT NULL,
  rows_var            FLOAT NOT NULL,
  idle_lat_avg        FLOAT NOT NULL,
  idle_lat_var        FLOAT NOT NULL,
  parse_lat_avg       FLOAT NOT NULL,
  parse_lat_var       FLOAT NOT NULL,
  plan_lat_avg        FLOAT NOT NULL,
  plan_lat_var        FLOAT NOT NULL,
  run_lat_avg         FLOAT NOT NULL,
  run_lat_var         FLOAT NOT NULL,
  service_lat_avg     FLOAT NOT NULL,
  service_lat_var     FLOAT NOT NULL,
  overhead_lat_avg    FLOAT NOT NULL,
  overhead_lat_var    FLOAT NOT NULL,
  bytes_read_avg      FLOAT NOT NULL,
  bytes_read_var      FLOAT NOT NULL,
  rows_read_avg       FLOAT NOT NULL,
  rows_read_var       FLOAT NOT NULL,
  rows_written_avg    FLOAT NOT NULL,
  rows_written_var    FLOAT NOT NULL,
  network_bytes_avg   FLOAT,
  network_bytes_var   FLOAT,
  network_msgs_avg    FLOAT,
  network_msgs_var    FLOAT,
  max_mem_usage_avg   FLOAT,
  max_mem_usage_var   FLOAT,
  max_disk_usage_avg  FLOAT,
  max_disk_usage_var  FLOAT,
  contention_time_avg FLOAT,
  contention_time_var FLOAT,
  cpu_sql_nanos_avg       FLOAT,
  cpu_sql_nanos_var       FLOAT,
  mvcc_step_avg       FLOAT,
  mvcc_step_var       FLOAT,
  mvcc_step_internal_avg FLOAT,
  mvcc_step_internal_var FLOAT,
  mvcc_seek_avg       FLOAT,
  mvcc_seek_var       FLOAT,
  mvcc_seek_internal_avg FLOAT,
  mvcc_seek_internal_var FLOAT,
  mvcc_block_bytes_avg       FLOAT,
  mvcc_block_bytes_var       FLOAT,
  mvcc_block_bytes_in_cache_avg FLOAT,
  mvcc_block_bytes_in_cache_var FLOAT,
  mvcc_key_bytes_avg FLOAT,
  mvcc_key_bytes_var FLOAT,
  mvcc_value_bytes_avg       FLOAT,
  mvcc_value_bytes_var       FLOAT,
  mvcc_point_count_avg       FLOAT,
  mvcc_point_count_var       FLOAT,
  mvcc_points_covered_by_range_tombstones_avg FLOAT,
  mvcc_points_covered_by_range_tombstones_var FLOAT,
  mvcc_range_key_count_avg FLOAT,
  mvcc_range_key_count_var FLOAT,
  mvcc_range_key_contained_points_avg       FLOAT,
  mvcc_range_key_contained_points_var       FLOAT,
  mvcc_range_key_skipped_points_avg      FLOAT,
  mvcc_range_key_skipped_points_var      FLOAT,
  implicit_txn        BOOL NOT NULL,
  full_scan           BOOL NOT NULL,
  sample_plan         JSONB,
  database_name       STRING NOT NULL,
  exec_node_ids       INT[] NOT NULL,
  kv_node_ids         INT[] NOT NULL,
  used_follower_read  BOOL NOT NULL,
  txn_fingerprint_id  STRING,
  index_recommendations STRING[] NOT NULL,
  latency_seconds_min FLOAT,
  latency_seconds_max FLOAT,
  failure_count INT NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// If the user is not admin, check the individual VIEWACTIVITY and VIEWACTIVITYREDACTED
		// privileges.
		hasPriv, shouldRedactError, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		} else if !hasPriv {
			// If the user is not admin and does not have VIEWACTIVITY or VIEWACTIVITYREDACTED,
			// return insufficient privileges error.
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		var alloc tree.DatumAlloc
		localSqlStats := p.extendedEvalCtx.localStatsProvider
		nodeID, _ := p.execCfg.NodeInfo.NodeID.OptionalNodeID() // zero if not available

		statementVisitor := func(_ context.Context, stats *appstatspb.CollectedStatementStatistics) error {
			errString := tree.DNull
			if shouldRedactError {
				errString = alloc.NewDString(tree.DString("<redacted>"))
			} else {
				if stats.Stats.SensitiveInfo.LastErr != "" {
					errString = alloc.NewDString(tree.DString(stats.Stats.SensitiveInfo.LastErr))
				}
			}

			errCode := tree.DNull
			if stats.Stats.LastErrorCode != "" {
				errCode = alloc.NewDString(tree.DString(stats.Stats.LastErrorCode))
			}

			var flags string
			if stats.Key.DistSQL {
				flags = "+"
			}

			samplePlan := sqlstatsutil.ExplainTreePlanNodeToJSON(&stats.Stats.SensitiveInfo.MostRecentPlanDescription)

			execNodeIDs := tree.NewDArray(types.Int)
			for _, nodeID := range stats.Stats.Nodes {
				if err := execNodeIDs.Append(alloc.NewDInt(tree.DInt(nodeID))); err != nil {
					return err
				}
			}

			kvNodeIDs := tree.NewDArray(types.Int)
			for _, kvNodeID := range stats.Stats.KVNodeIDs {
				if err := kvNodeIDs.Append(alloc.NewDInt(tree.DInt(kvNodeID))); err != nil {
					return err
				}
			}

			txnFingerprintID := tree.DNull
			if stats.Key.TransactionFingerprintID != appstatspb.InvalidTransactionFingerprintID {
				txnFingerprintID = alloc.NewDString(tree.DString(strconv.FormatUint(uint64(stats.Key.TransactionFingerprintID), 10)))

			}

			indexRecommendations := tree.NewDArray(types.String)
			for _, recommendation := range stats.Stats.IndexRecommendations {
				if err := indexRecommendations.Append(alloc.NewDString(tree.DString(recommendation))); err != nil {
					return err
				}
			}

			err := addRow(
				alloc.NewDInt(tree.DInt(nodeID)),                                         // node_id
				alloc.NewDString(tree.DString(stats.Key.App)),                            // application_name
				alloc.NewDString(tree.DString(flags)),                                    // flags
				alloc.NewDString(tree.DString(strconv.FormatUint(uint64(stats.ID), 10))), // statement_id
				alloc.NewDString(tree.DString(stats.Key.Query)),                          // key
				legacyAnonymizedStmt,                                                     // anonymized
				alloc.NewDInt(tree.DInt(stats.Stats.Count)),                              // count
				alloc.NewDInt(tree.DInt(stats.Stats.FirstAttemptCount)),                  // first_attempt_count
				alloc.NewDInt(tree.DInt(stats.Stats.MaxRetries)),                         // max_retries
				errString, // last_error
				errCode,   // last_error_code
				alloc.NewDFloat(tree.DFloat(stats.Stats.NumRows.Mean)),                                                                   // rows_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.NumRows.GetVariance(stats.Stats.Count))),                                         // rows_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.IdleLat.Mean)),                                                                   // idle_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.IdleLat.GetVariance(stats.Stats.Count))),                                         // idle_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.ParseLat.Mean)),                                                                  // parse_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.ParseLat.GetVariance(stats.Stats.Count))),                                        // parse_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.PlanLat.Mean)),                                                                   // plan_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.PlanLat.GetVariance(stats.Stats.Count))),                                         // plan_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.RunLat.Mean)),                                                                    // run_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.RunLat.GetVariance(stats.Stats.Count))),                                          // run_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.Mean)),                                                                // service_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.GetVariance(stats.Stats.Count))),                                      // service_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.OverheadLat.Mean)),                                                               // overhead_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.OverheadLat.GetVariance(stats.Stats.Count))),                                     // overhead_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.BytesRead.Mean)),                                                                 // bytes_read_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.BytesRead.GetVariance(stats.Stats.Count))),                                       // bytes_read_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.RowsRead.Mean)),                                                                  // rows_read_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.RowsRead.GetVariance(stats.Stats.Count))),                                        // rows_read_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.RowsWritten.Mean)),                                                               // rows_written_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.RowsWritten.GetVariance(stats.Stats.Count))),                                     // rows_written_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),                                     // network_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),                                     // network_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),                                  // network_msgs_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),                                  // network_msgs_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),                                      // max_mem_usage_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),                                      // max_mem_usage_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),                                     // max_disk_usage_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),                                     // max_disk_usage_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),                                   // contention_time_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),                                   // contention_time_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.CPUSQLNanos),                                      // cpu_sql_nanos_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.CPUSQLNanos),                                      // cpu_sql_nanos_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCount),                      // mvcc_step_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCount),                      // mvcc_step_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCountInternal),              // mvcc_step_internal_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCountInternal),              // mvcc_step_internal_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCount),                      // mvcc_seek_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCount),                      // mvcc_seek_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal),              // mvcc_seek_internal_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal),              // mvcc_seek_internal_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytes),                     // mvcc_block_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytes),                     // mvcc_block_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache),              // mvcc_block_bytes_in_cache_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache),              // mvcc_block_bytes_in_cache_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.KeyBytes),                       // mvcc_key_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.KeyBytes),                       // mvcc_key_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.ValueBytes),                     // mvcc_value_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.ValueBytes),                     // mvcc_value_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointCount),                     // mvcc_point_count_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointCount),                     // mvcc_point_count_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones), // mvcc_points_covered_by_range_tombstones_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones), // mvcc_points_covered_by_range_tombstones_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount),                  // mvcc_range_key_count_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount),                  // mvcc_range_key_count_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints),        // mvcc_range_key_contained_points_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints),        // mvcc_range_key_contained_points_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints),          // mvcc_range_key_skipped_points_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints),          // mvcc_range_key_skipped_points_var
				tree.MakeDBool(tree.DBool(stats.Key.ImplicitTxn)),                                                                        // implicit_txn
				tree.MakeDBool(tree.DBool(stats.Key.FullScan)),                                                                           // full_scan
				alloc.NewDJSON(tree.DJSON{JSON: samplePlan}),                                                                             // sample_plan
				alloc.NewDString(tree.DString(stats.Key.Database)),                                                                       // database_name
				execNodeIDs, // exec_node_ids
				kvNodeIDs,   // kv_node_ids
				tree.MakeDBool(tree.DBool(stats.Stats.UsedFollowerRead)), // used_follower_read
				txnFingerprintID,     // txn_fingerprint_id
				indexRecommendations, // index_recommendations
				alloc.NewDFloat(tree.DFloat(stats.Stats.LatencyInfo.Min)), // latency_seconds_min
				alloc.NewDFloat(tree.DFloat(stats.Stats.LatencyInfo.Max)), // latency_seconds_max
				alloc.NewDInt(tree.DInt(stats.Stats.FailureCount)),        // failure_count
			)
			if err != nil {
				return err
			}

			return nil
		}

		return localSqlStats.IterateStatementStats(ctx, sqlstats.IteratorOptions{
			SortedAppNames: true,
			SortedKey:      true,
		}, statementVisitor)
	},
}

// TODO(arul): Explore updating the schema below to have key be an INT and
// statement_ids be INT[] now that we've moved to having uint64 as the type of
// StmtFingerprintID and TxnKey. Issue #55284
var crdbInternalTransactionStatisticsTable = virtualSchemaTable{
	comment: `finer-grained transaction statistics. ` +
		`The contents of this table are flushed to the system.transaction_statistics table at the interval set by the ` +
		`cluster setting sql.stats.flush.interval (by default, 10m).`,
	schema: `
CREATE TABLE crdb_internal.node_transaction_statistics (
  node_id             INT NOT NULL,
  application_name    STRING NOT NULL,
  key                 STRING,
  statement_ids       STRING[],
  count               INT,
  max_retries         INT,
  service_lat_avg     FLOAT NOT NULL,
  service_lat_var     FLOAT NOT NULL,
  retry_lat_avg       FLOAT NOT NULL,
  retry_lat_var       FLOAT NOT NULL,
  commit_lat_avg      FLOAT NOT NULL,
  commit_lat_var      FLOAT NOT NULL,
  idle_lat_avg        FLOAT NOT NULL,
  idle_lat_var        FLOAT NOT NULL,
  rows_read_avg       FLOAT NOT NULL,
  rows_read_var       FLOAT NOT NULL,
  network_bytes_avg   FLOAT,
  network_bytes_var   FLOAT,
  network_msgs_avg    FLOAT,
  network_msgs_var    FLOAT,
  max_mem_usage_avg   FLOAT,
  max_mem_usage_var   FLOAT,
  max_disk_usage_avg  FLOAT,
  max_disk_usage_var  FLOAT,
  contention_time_avg FLOAT,
  contention_time_var FLOAT,
  cpu_sql_nanos_avg       FLOAT,
  cpu_sql_nanos_var       FLOAT,
  mvcc_step_avg       FLOAT,
  mvcc_step_var       FLOAT,
  mvcc_step_internal_avg FLOAT,
  mvcc_step_internal_var FLOAT,
  mvcc_seek_avg       FLOAT,
  mvcc_seek_var       FLOAT,
  mvcc_seek_internal_avg FLOAT,
  mvcc_seek_internal_var FLOAT,
  mvcc_block_bytes_avg       FLOAT,
  mvcc_block_bytes_var       FLOAT,
  mvcc_block_bytes_in_cache_avg FLOAT,
  mvcc_block_bytes_in_cache_var FLOAT,
  mvcc_key_bytes_avg FLOAT,
  mvcc_key_bytes_var FLOAT,
  mvcc_value_bytes_avg       FLOAT,
  mvcc_value_bytes_var       FLOAT,
  mvcc_point_count_avg       FLOAT,
  mvcc_point_count_var       FLOAT,
  mvcc_points_covered_by_range_tombstones_avg FLOAT,
  mvcc_points_covered_by_range_tombstones_var FLOAT,
  mvcc_range_key_count_avg FLOAT,
  mvcc_range_key_count_var FLOAT,
  mvcc_range_key_contained_points_avg       FLOAT,
  mvcc_range_key_contained_points_var       FLOAT,
  mvcc_range_key_skipped_points_avg      FLOAT,
  mvcc_range_key_skipped_points_var      FLOAT
)
`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasViewActivityOrhasViewActivityRedacted, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasViewActivityOrhasViewActivityRedacted {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		localMemSqlStats := p.extendedEvalCtx.localStatsProvider
		nodeID, _ := p.execCfg.NodeInfo.NodeID.OptionalNodeID() // zero if not available

		var alloc tree.DatumAlloc

		transactionVisitor := func(_ context.Context, stats *appstatspb.CollectedTransactionStatistics) error {
			stmtFingerprintIDsDatum := tree.NewDArray(types.String)
			for _, stmtFingerprintID := range stats.StatementFingerprintIDs {
				if err := stmtFingerprintIDsDatum.Append(alloc.NewDString(tree.DString(strconv.FormatUint(uint64(stmtFingerprintID), 10)))); err != nil {
					return err
				}
			}

			err := addRow(
				alloc.NewDInt(tree.DInt(nodeID)),          // node_id
				alloc.NewDString(tree.DString(stats.App)), // application_name
				alloc.NewDString(tree.DString(strconv.FormatUint(uint64(stats.TransactionFingerprintID), 10))), // key
				stmtFingerprintIDsDatum,                                                                                                  // statement_ids
				alloc.NewDInt(tree.DInt(stats.Stats.Count)),                                                                              // count
				alloc.NewDInt(tree.DInt(stats.Stats.MaxRetries)),                                                                         // max_retries
				alloc.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.Mean)),                                                                // service_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.GetVariance(stats.Stats.Count))),                                      // service_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.RetryLat.Mean)),                                                                  // retry_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.RetryLat.GetVariance(stats.Stats.Count))),                                        // retry_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.CommitLat.Mean)),                                                                 // commit_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.CommitLat.GetVariance(stats.Stats.Count))),                                       // commit_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.IdleLat.Mean)),                                                                   // idle_lat_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.IdleLat.GetVariance(stats.Stats.Count))),                                         // idle_lat_var
				alloc.NewDFloat(tree.DFloat(stats.Stats.NumRows.Mean)),                                                                   // rows_read_avg
				alloc.NewDFloat(tree.DFloat(stats.Stats.NumRows.GetVariance(stats.Stats.Count))),                                         // rows_read_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),                                     // network_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),                                     // network_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),                                  // network_msgs_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),                                  // network_msgs_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),                                      // max_mem_usage_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),                                      // max_mem_usage_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),                                     // max_disk_usage_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),                                     // max_disk_usage_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),                                   // contention_time_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),                                   // contention_time_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.CPUSQLNanos),                                      // cpu_sql_nanos_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.CPUSQLNanos),                                      // cpu_sql_nanos_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCount),                      // mvcc_step_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCount),                      // mvcc_step_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCountInternal),              // mvcc_step_internal_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.StepCountInternal),              // mvcc_step_internal_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCount),                      // mvcc_seek_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCount),                      // mvcc_seek_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal),              // mvcc_seek_internal_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.SeekCountInternal),              // mvcc_seek_internal_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytes),                     // mvcc_block_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytes),                     // mvcc_block_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache),              // mvcc_block_bytes_in_cache_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.BlockBytesInCache),              // mvcc_block_bytes_in_cache_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.KeyBytes),                       // mvcc_key_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.KeyBytes),                       // mvcc_key_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.ValueBytes),                     // mvcc_value_bytes_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.ValueBytes),                     // mvcc_value_bytes_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointCount),                     // mvcc_point_count_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointCount),                     // mvcc_point_count_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones), // mvcc_points_covered_by_range_tombstones_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.PointsCoveredByRangeTombstones), // mvcc_points_covered_by_range_tombstones_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount),                  // mvcc_range_key_count_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyCount),                  // mvcc_range_key_count_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints),        // mvcc_range_key_contained_points_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeyContainedPoints),        // mvcc_range_key_contained_points_var
				execStatAvg(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints),          // mvcc_range_key_skipped_points_avg
				execStatVar(&alloc, stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MVCCIteratorStats.RangeKeySkippedPoints),          // mvcc_range_key_skipped_points_var
			)

			if err != nil {
				return err
			}

			return nil
		}

		return localMemSqlStats.IterateTransactionStats(ctx, sqlstats.IteratorOptions{
			SortedAppNames: true,
			SortedKey:      true,
		}, transactionVisitor)
	},
}

var crdbInternalNodeTxnStatsTable = virtualSchemaTable{
	comment: `per-application transaction statistics (in-memory, not durable; local node only). ` +
		`This table is wiped periodically (by default, at least every two hours)`,
	schema: `
CREATE TABLE crdb_internal.node_txn_stats (
  node_id            INT NOT NULL,
  application_name   STRING NOT NULL,
  txn_count          INT NOT NULL,
  txn_time_avg_sec   FLOAT NOT NULL,
  txn_time_var_sec   FLOAT NOT NULL,
  committed_count    INT NOT NULL,
  implicit_count     INT NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}

		localMemSqlStats := p.extendedEvalCtx.localStatsProvider
		nodeID, _ := p.execCfg.NodeInfo.NodeID.OptionalNodeID() // zero if not available

		appTxnStatsVisitor := func(appName string, stats *appstatspb.TxnStats) error {
			return addRow(
				tree.NewDInt(tree.DInt(nodeID)),
				tree.NewDString(appName),
				tree.NewDInt(tree.DInt(stats.TxnCount)),
				tree.NewDFloat(tree.DFloat(stats.TxnTimeSec.Mean)),
				tree.NewDFloat(tree.DFloat(stats.TxnTimeSec.GetVariance(stats.TxnCount))),
				tree.NewDInt(tree.DInt(stats.CommittedCount)),
				tree.NewDInt(tree.DInt(stats.ImplicitCount)),
			)
		}

		return localMemSqlStats.IterateAggregatedTransactionStats(ctx, sqlstats.IteratorOptions{
			SortedAppNames: true,
		}, appTxnStatsVisitor)
	},
}

// crdbInternalSessionTraceTable exposes the latest trace collected on this
// session (via SET TRACING={ON/OFF})
//
// TODO(tbg): prefix with node_.
var crdbInternalSessionTraceTable = virtualSchemaTable{
	comment: `session trace accumulated so far (RAM)`,
	schema: `
CREATE TABLE crdb_internal.session_trace (
  span_idx    INT NOT NULL,        -- The span's index.
  message_idx INT NOT NULL,        -- The message's index within its span.
  timestamp   TIMESTAMPTZ NOT NULL,-- The message's timestamp.
  duration    INTERVAL,            -- The span's duration. Set only on the first
                                   -- (dummy) message on a span.
                                   -- NULL if the span was not finished at the time
                                   -- the trace has been collected.
  operation   STRING NULL,         -- The span's operation.
  loc         STRING NOT NULL,     -- The file name / line number prefix, if any.
  tag         STRING NOT NULL,     -- The logging tag, if any.
  message     STRING NOT NULL,     -- The logged message.
  age         INTERVAL NOT NULL    -- The age of this message relative to the beginning of the trace.
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		rows, err := p.ExtendedEvalContext().Tracing.getSessionTrace()
		if err != nil {
			return err
		}
		for _, r := range rows {
			if err := addRow(r[:]...); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalClusterInflightTracesTable exposes cluster-wide inflight spans
// for a trace_id. The spans returned are restricted to the tenant querying the
// table; the system tenant will not get span belonging to other tenants
// (although perhaps it should).
//
// crdbInternalClusterInflightTracesTable is an indexed, virtual table that only
// returns rows when accessed with an index constraint specifying the trace_id
// for which inflight spans need to be aggregated from all nodes in the cluster.
//
// Each row in the virtual table corresponds to a single `tracingpb.Recording` on
// a particular node. A `tracingpb.Recording` is the trace of a single operation
// rooted at a root span on that node. Under the hood, the virtual table
// contacts all "live" nodes in the cluster via the trace collector which
// streams back a recording at a time.
//
// The underlying trace collector only buffers recordings one node at a time.
// The virtual table also produces rows lazily, i.e. as and when they are
// consumed by the consumer. Therefore, the memory overhead of querying this
// table will be the size of all the `tracing.Recordings` of a particular
// `trace_id` on a single node in the cluster. Each `tracingpb.Recording` has its
// own memory protections via ring buffers, and so we do not expect this
// overhead to grow in an unbounded manner.
var crdbInternalClusterInflightTracesTable = virtualSchemaTable{
	comment: `traces for in-flight spans across all nodes in the cluster (cluster RPC; expensive!)`,
	schema: `
CREATE TABLE crdb_internal.cluster_inflight_traces (
  trace_id     INT NOT NULL,    -- The trace's ID.
  node_id      INT NOT NULL,    -- The node's ID.
  root_op_name STRING NOT NULL, -- The operation name of the root span in the current trace.
  trace_str    STRING NULL,     -- human readable representation of the traced remote operation.
  jaeger_json  STRING NULL,     -- Jaeger JSON representation of the traced remote operation.
  INDEX(trace_id)
)`,
	indexes: []virtualIndex{{populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner,
		db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
		var traceID tracingpb.TraceID
		switch t := unwrappedConstraint.(type) {
		case *tree.DInt:
			traceID = tracingpb.TraceID(*t)
		default:
			return false, errors.AssertionFailedf(
				"unexpected type %T for trace_id column in virtual table crdb_internal.cluster_inflight_traces", unwrappedConstraint)
		}

		traceCollector := p.ExecCfg().TraceCollector
		iter, err := traceCollector.StartIter(ctx, traceID)
		if err != nil {
			return false, err
		}
		for ; iter.Valid(); iter.Next(ctx) {
			nodeID, recording := iter.Value()
			traceString := recording.String()
			traceJaegerJSON, err := recording.ToJaegerJSON("", "", fmt.Sprintf("node %d", nodeID))
			if err != nil {
				return false, err
			}
			if err := addRow(tree.NewDInt(tree.DInt(traceID)),
				tree.NewDInt(tree.DInt(nodeID)),
				tree.NewDString(recording[0].Operation),
				tree.NewDString(traceString),
				tree.NewDString(traceJaegerJSON)); err != nil {
				return false, err
			}
		}

		return true, nil
	}}},
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor,
		addRow func(...tree.Datum) error) error {
		// We refuse queries without an index constraint. This index constraint will
		// provide the trace_id for which we will collect inflight trace spans from
		// the cluster.
		return pgerror.New(pgcode.FeatureNotSupported, "a trace_id value needs to be specified")
	},
}

// crdbInternalInflightTraceSpanTable exposes the node-local registry of in-flight spans.
var crdbInternalInflightTraceSpanTable = virtualSchemaTable{
	comment: `in-flight spans (RAM; local node only)`,
	schema: `
CREATE TABLE crdb_internal.node_inflight_trace_spans (
  trace_id       INT NOT NULL,    -- The trace's ID.
  parent_span_id INT NOT NULL,    -- The span's parent ID.
  span_id        INT NOT NULL,    -- The span's ID.
  goroutine_id   INT NOT NULL,    -- The ID of the goroutine on which the span was created.
  finished       BOOL NOT NULL,   -- True if the span has been Finish()ed, false otherwise.
  start_time     TIMESTAMPTZ,     -- The span's start time.
  duration       INTERVAL,        -- The span's duration, measured from start to Finish().
                                  -- A span whose recording is collected before it's finished will
                                  -- have the duration set as the "time of collection - start time".
  operation      STRING NULL      -- The span's operation.
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(
			ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA,
		); err != nil {
			return err
		}

		return p.ExecCfg().AmbientCtx.Tracer.VisitSpans(func(span tracing.RegistrySpan) error {
			trace := span.GetFullRecording(tracingpb.RecordingVerbose)
			for _, rec := range trace.Flatten() {
				traceID := rec.TraceID
				parentSpanID := rec.ParentSpanID
				spanID := rec.SpanID
				goroutineID := rec.GoroutineID
				finished := rec.Finished

				startTime, err := tree.MakeDTimestampTZ(rec.StartTime, time.Microsecond)
				if err != nil {
					return err
				}

				spanDuration := rec.Duration
				operation := rec.Operation

				if err := addRow(
					// TODO(angelapwen): we're casting uint64s to int64 here,
					// is that ok?
					tree.NewDInt(tree.DInt(traceID)),
					tree.NewDInt(tree.DInt(parentSpanID)),
					tree.NewDInt(tree.DInt(spanID)),
					tree.NewDInt(tree.DInt(goroutineID)),
					tree.MakeDBool(tree.DBool(finished)),
					startTime,
					tree.NewDInterval(
						duration.MakeDuration(spanDuration.Nanoseconds(), 0, 0),
						types.DefaultIntervalTypeMetadata,
					),
					tree.NewDString(operation),
				); err != nil {
					return err
				}
			}
			return nil
		})
	},
}

// crdbInternalClusterSettingsTable exposes the list of current
// cluster settings.
//
// TODO(tbg): prefix with node_.
var crdbInternalClusterSettingsTable = virtualSchemaTable{
	comment: `cluster settings (RAM)`,
	schema: `
CREATE TABLE crdb_internal.cluster_settings (
  variable      STRING NOT NULL,
  value         STRING NOT NULL,
  type          STRING NOT NULL,
  public        BOOL NOT NULL, -- whether the setting is documented, which implies the user can expect support.
  description   STRING NOT NULL,
  default_value STRING NOT NULL,
  origin        STRING NOT NULL, -- the origin of the value: 'default' , 'override' or 'external-override'
  key           STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasModify, err := p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.MODIFYCLUSTERSETTING)
		if err != nil {
			return err
		}
		canViewAll := hasModify
		if !canViewAll {
			canViewAll, err = p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.VIEWCLUSTERSETTING)
			if err != nil {
				return err
			}
		}
		canViewSqlOnly := false
		if !canViewAll {
			canViewSqlOnly, err = p.HasGlobalPrivilegeOrRoleOption(ctx, privilege.MODIFYSQLCLUSTERSETTING)
			if err != nil {
				return err
			}
		}
		if !canViewAll && !canViewSqlOnly {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only users with %s, %s or %s system privileges are allowed to read "+
					"crdb_internal.cluster_settings", privilege.MODIFYCLUSTERSETTING, privilege.MODIFYSQLCLUSTERSETTING, privilege.VIEWCLUSTERSETTING)
		}
		for _, k := range settings.Keys(p.ExecCfg().Codec.ForSystemTenant()) {
			// If the user can only view sql.defaults settings, hide all other settings.
			if canViewSqlOnly && !strings.HasPrefix(string(k), "sql.defaults") {
				continue
			}
			setting, _ := settings.LookupForDisplayByKey(k, p.ExecCfg().Codec.ForSystemTenant(), hasModify)
			strVal := setting.String(&p.ExecCfg().Settings.SV)
			isPublic := setting.Visibility() == settings.Public
			desc := setting.Description()
			defaultVal := setting.DefaultString()
			origin := setting.ValueOrigin(ctx, &p.ExecCfg().Settings.SV).String()
			if err := addRow(
				tree.NewDString(string(setting.Name())),
				tree.NewDString(strVal),
				tree.NewDString(setting.Typ()),
				tree.MakeDBool(tree.DBool(isPublic)),
				tree.NewDString(desc),
				tree.NewDString(defaultVal),
				tree.NewDString(origin),
				tree.NewDString(string(setting.InternalKey())),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalSessionVariablesTable exposes the session variables.
var crdbInternalSessionVariablesTable = virtualSchemaTable{
	comment: `session variables (RAM)`,
	schema: `
CREATE TABLE crdb_internal.session_variables (
  variable STRING NOT NULL,
  value    STRING NOT NULL,
  hidden   BOOL   NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, vName := range varNames {
			gen := varGen[vName]
			value, err := gen.Get(&p.extendedEvalCtx, p.Txn())
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDString(vName),
				tree.NewDString(value),
				tree.MakeDBool(tree.DBool(gen.Hidden)),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

const txnsSchemaPattern = `
CREATE TABLE crdb_internal.%s (
  id UUID,                         -- the unique ID of the transaction
  node_id INT,                     -- the ID of the node running the transaction
  session_id STRING,               -- the ID of the session
  start TIMESTAMP,                 -- the start time of the transaction
  txn_string STRING,               -- the string representation of the transaction
  application_name STRING,         -- the name of the application as per SET application_name
  num_stmts INT,                   -- the number of statements executed so far
  num_retries INT,                 -- the number of times the transaction was restarted
  num_auto_retries INT,            -- the number of times the transaction was automatically restarted
  last_auto_retry_reason STRING,   -- the error causing the last automatic retry for this txn
  isolation_level STRING,          -- the isolation level of the transaction
  priority STRING,                 -- the priority of the transaction
  quality_of_service STRING        -- the quality of service of the transaction
)`

var crdbInternalLocalTxnsTable = virtualSchemaTable{
	comment: "running user transactions visible by the current user (RAM; local node only)",
	schema:  fmt.Sprintf(txnsSchemaPattern, "node_transactions"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasViewActivityOrhasViewActivityRedacted, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasViewActivityOrhasViewActivityRedacted {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}
		req, err := p.makeSessionsRequest(ctx, true /* excludeClosed */)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateTransactionsTable(ctx, addRow, response)
	},
}

var crdbInternalClusterTxnsTable = virtualSchemaTable{
	comment: "running user transactions visible by the current user (cluster RPC; expensive!)",
	schema:  fmt.Sprintf(txnsSchemaPattern, "cluster_transactions"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasViewActivityOrhasViewActivityRedacted, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasViewActivityOrhasViewActivityRedacted {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}
		req, err := p.makeSessionsRequest(ctx, true /* excludeClosed */)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateTransactionsTable(ctx, addRow, response)
	},
}

func populateTransactionsTable(
	ctx context.Context, addRow func(...tree.Datum) error, response *serverpb.ListSessionsResponse,
) error {
	for _, session := range response.Sessions {
		sessionID := getSessionID(session)
		if txn := session.ActiveTxn; txn != nil {
			ts, err := tree.MakeDTimestamp(txn.Start, time.Microsecond)
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDUuid(tree.DUuid{UUID: txn.ID}),
				tree.NewDInt(tree.DInt(session.NodeID)),
				sessionID,
				ts,
				tree.NewDString(txn.TxnDescription),
				tree.NewDString(session.ApplicationName),
				tree.NewDInt(tree.DInt(txn.NumStatementsExecuted)),
				tree.NewDInt(tree.DInt(txn.NumRetries)),
				tree.NewDInt(tree.DInt(txn.NumAutoRetries)),
				tree.NewDString(txn.LastAutoRetryReason),
				tree.NewDString(txn.IsolationLevel),
				tree.NewDString(txn.Priority),
				tree.NewDString(txn.QualityOfService),
			); err != nil {
				return err
			}
		}
	}
	for _, rpcErr := range response.Errors {
		log.Warningf(ctx, "%v", rpcErr.Message)
		if rpcErr.NodeID != 0 {
			// Add a row with this node ID, the error for the txn string,
			// and nulls for all other columns.
			if err := addRow(
				tree.DNull,                             // txn ID
				tree.NewDInt(tree.DInt(rpcErr.NodeID)), // node ID
				tree.DNull,                             // session ID
				tree.DNull,                             // start
				tree.NewDString("-- "+rpcErr.Message),  // txn string
				tree.DNull,                             // application name
				tree.DNull,                             // NumStatementsExecuted
				tree.DNull,                             // NumRetries
				tree.DNull,                             // NumAutoRetries
				tree.DNull,                             // LastAutoRetryReason
				tree.DNull,                             // IsolationLevel
				tree.DNull,                             // Priority
				tree.DNull,                             // QualityOfService
			); err != nil {
				return err
			}
		}
	}
	return nil
}

const queriesSchemaPattern = `
CREATE TABLE crdb_internal.%s (
  query_id         STRING,         -- the cluster-unique ID of the query
  txn_id           UUID,           -- the unique ID of the query's transaction
  node_id          INT NOT NULL,   -- the node on which the query is running
  session_id       STRING,         -- the ID of the session
  user_name        STRING,         -- the user running the query
  start            TIMESTAMPTZ,    -- the start time of the query
  query            STRING,         -- the SQL code of the query
  client_address   STRING,         -- the address of the client that issued the query
  application_name STRING,         -- the name of the application as per SET application_name
  distributed      BOOL,           -- whether the query is running distributed
  phase            STRING,         -- the current execution phase
  full_scan        BOOL,           -- whether the query contains a full table or index scan
  plan_gist        STRING,         -- Compressed logical plan.
  database         STRING          -- the database the statement was executed on
)`

func (p *planner) makeSessionsRequest(
	ctx context.Context, excludeClosed bool,
) (serverpb.ListSessionsRequest, error) {
	req := serverpb.ListSessionsRequest{
		Username:              p.SessionData().User().Normalized(),
		ExcludeClosedSessions: excludeClosed,
		IncludeInternal:       true,
	}
	hasViewActivityOrhasViewActivityRedacted, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
	if err != nil {
		return serverpb.ListSessionsRequest{}, err
	}
	if hasViewActivityOrhasViewActivityRedacted {
		req.Username = ""
	}
	return req, nil
}

func getSessionID(session serverpb.Session) tree.Datum {
	// TODO(knz): serverpb.Session is always constructed with an ID
	// set from a 16-byte session ID. Yet we get crash reports
	// that fail in IDFromBytes() with a byte slice that's
	// too short. See #32517.
	var sessionID tree.Datum
	if session.ID == nil {
		// TODO(knz): NewInternalTrackingError is misdesigned. Change to
		// not use this. See the other facilities in
		// pgerror/internal_errors.go.
		telemetry.RecordError(
			pgerror.NewInternalTrackingError(32517 /* issue */, "null"))
		sessionID = tree.DNull
	} else if len(session.ID) != 16 {
		// TODO(knz): ditto above.
		telemetry.RecordError(
			pgerror.NewInternalTrackingError(32517 /* issue */, fmt.Sprintf("len=%d", len(session.ID))))
		sessionID = tree.NewDString("<invalid>")
	} else {
		clusterSessionID := clusterunique.IDFromBytes(session.ID)
		sessionID = tree.NewDString(clusterSessionID.String())
	}
	return sessionID
}

// crdbInternalLocalQueriesTable exposes the list of running queries
// on the current node. The results are dependent on the current user.
var crdbInternalLocalQueriesTable = virtualSchemaTable{
	comment: "running queries visible by current user (RAM; local node only)",
	schema:  fmt.Sprintf(queriesSchemaPattern, "node_queries"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req, err := p.makeSessionsRequest(ctx, true /* excludeClosed */)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateQueriesTable(ctx, p, addRow, response)
	},
}

// crdbInternalClusterQueriesTable exposes the list of running queries
// on the entire cluster. The result is dependent on the current user.
var crdbInternalClusterQueriesTable = virtualSchemaTable{
	comment: "running queries visible by current user (cluster RPC; expensive!)",
	schema:  fmt.Sprintf(queriesSchemaPattern, "cluster_queries"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req, err := p.makeSessionsRequest(ctx, true /* excludeClosed */)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateQueriesTable(ctx, p, addRow, response)
	},
}

func populateQueriesTable(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	response *serverpb.ListSessionsResponse,
) error {
	shouldRedactOtherUserQuery := false
	canViewOtherUser := false
	// Check if the user is admin.
	if isAdmin, err := p.HasAdminRole(ctx); err != nil {
		return err
	} else if isAdmin {
		canViewOtherUser = true
	} else if !isAdmin {
		// If the user is not admin, check the individual VIEWACTIVITY and VIEWACTIVITYREDACTED
		// privileges.
		if hasPriv, shouldRedact, err := p.HasViewActivityOrViewActivityRedactedRole(ctx); err != nil {
			return err
		} else if hasPriv {
			canViewOtherUser = true
			if shouldRedact {
				// If the user has VIEWACTIVITYREDACTED, redact the query as it takes precedence
				// over VIEWACTIVITY.
				shouldRedactOtherUserQuery = true
			}
		}
	}
	for _, session := range response.Sessions {
		normalizedUser, err := username.MakeSQLUsernameFromUserInput(session.Username, username.PurposeValidation)
		if err != nil {
			return err
		}
		if !canViewOtherUser && normalizedUser != p.User() {
			continue
		}
		sessionID := getSessionID(session)
		for _, query := range session.ActiveQueries {
			isDistributedDatum := tree.DNull
			isFullScanDatum := tree.DNull
			phase := strings.ToLower(query.Phase.String())
			if phase == "executing" {
				isDistributedDatum = tree.DBoolFalse
				if query.IsDistributed {
					isDistributedDatum = tree.DBoolTrue
				}

				isFullScanDatum = tree.DBoolFalse
				if query.IsFullScan {
					isFullScanDatum = tree.DBoolTrue
				}
			}

			if query.Progress > 0 {
				phase = fmt.Sprintf("%s (%.2f%%)", phase, query.Progress*100)
			}

			var txnID tree.Datum
			// query.TxnID and query.TxnStart were only added in 20.1. In case this
			// is a mixed cluster setting, report NULL if these values were not filled
			// out by the remote session.
			if query.ID == "" {
				txnID = tree.DNull
			} else {
				txnID = tree.NewDUuid(tree.DUuid{UUID: query.TxnID})
			}

			ts, err := tree.MakeDTimestampTZ(query.Start, time.Microsecond)
			if err != nil {
				return err
			}

			planGistDatum := tree.DNull
			if len(query.PlanGist) > 0 {
				planGistDatum = tree.NewDString(query.PlanGist)
			}

			// Interpolate placeholders into the SQL statement.
			sql := formatActiveQuery(query)
			// If the user does not have the correct privileges, show the query
			// without literals or constants.
			if shouldRedactOtherUserQuery && session.Username != p.SessionData().User().Normalized() {
				sql = query.SqlNoConstants
			}
			if err := addRow(
				tree.NewDString(query.ID),
				txnID,
				tree.NewDInt(tree.DInt(session.NodeID)),
				sessionID,
				tree.NewDString(session.Username),
				ts,
				tree.NewDString(sql),
				tree.NewDString(session.ClientAddress),
				tree.NewDString(session.ApplicationName),
				isDistributedDatum,
				tree.NewDString(phase),
				isFullScanDatum,
				planGistDatum,
				tree.NewDString(query.Database),
			); err != nil {
				return err
			}
		}
	}

	for _, rpcErr := range response.Errors {
		log.Warningf(ctx, "%v", rpcErr.Message)
		if rpcErr.NodeID != 0 {
			// Add a row with this node ID, the error for query, and
			// nulls for all other columns.
			if err := addRow(
				tree.DNull,                             // query ID
				tree.DNull,                             // txn ID
				tree.NewDInt(tree.DInt(rpcErr.NodeID)), // node ID
				tree.DNull,                             // session ID
				tree.DNull,                             // username
				tree.DNull,                             // start
				tree.NewDString("-- "+rpcErr.Message),  // query
				tree.DNull,                             // client_address
				tree.DNull,                             // application_name
				tree.DNull,                             // distributed
				tree.DNull,                             // phase
				tree.DNull,                             // full_scan
				tree.DNull,                             // plan_gist
				tree.DNull,                             // database
			); err != nil {
				return err
			}
		}
	}
	return nil
}

// formatActiveQuery formats a serverpb.ActiveQuery by interpolating its
// placeholders within the string.
func formatActiveQuery(query serverpb.ActiveQuery) string {
	parsed, parseErr := parser.ParseOneRetainComments(query.Sql)
	if parseErr != nil {
		// If we failed to interpolate, rather than give up just send out the
		// SQL without interpolated placeholders. Hallelujah!
		return query.Sql
	}
	var sb strings.Builder
	sql := tree.AsStringWithFlags(parsed.AST, tree.FmtSimple,
		tree.FmtPlaceholderFormat(func(ctx *tree.FmtCtx, p *tree.Placeholder) {
			if int(p.Idx) > len(query.Placeholders) {
				ctx.Printf("$%d", p.Idx+1)
				return
			}
			ctx.Printf("%s", query.Placeholders[p.Idx])
		}),
	)
	sb.WriteString(sql)
	for i := range parsed.Comments {
		sb.WriteString(" ")
		sb.WriteString(parsed.Comments[i])
	}
	return sb.String()
}

const sessionsSchemaPattern = `
CREATE TABLE crdb_internal.%s (
  node_id            INT NOT NULL,   -- the node on which the query is running
  session_id         STRING,         -- the ID of the session
  user_name          STRING,         -- the user running the query
  client_address     STRING,         -- the address of the client that issued the query
  application_name   STRING,         -- the name of the application as per SET application_name
  active_queries     STRING,         -- the currently running queries as SQL
  last_active_query  STRING,         -- the query that finished last on this session as SQL
  num_txns_executed  INT,            -- the number of transactions that were executed so far on this session
  session_start      TIMESTAMPTZ,    -- the time when the session was opened
  active_query_start TIMESTAMPTZ,    -- the time when the current active query in the session was started
  kv_txn             STRING,         -- the ID of the current KV transaction
  alloc_bytes        INT,            -- the number of bytes allocated by the session
  max_alloc_bytes    INT,            -- the high water mark of bytes allocated by the session
  status             STRING,         -- the status of the session (open, closed)
  session_end        TIMESTAMPTZ,    -- the time when the session was closed
  pg_backend_pid     INT,            -- the numerical ID attached to the session which is used to mimic a Postgres backend PID
  trace_id           INT,            -- the ID of the trace of the session
  goroutine_id       INT,            -- the ID of the goroutine of the session
  authentication_method STRING       -- the method used to authenticate the session
)
`

// crdbInternalLocalSessionsTable exposes the list of running sessions
// on the current node. The results are dependent on the current user.
var crdbInternalLocalSessionsTable = virtualSchemaTable{
	comment: "running sessions visible by current user (RAM; local node only)",
	schema:  fmt.Sprintf(sessionsSchemaPattern, "node_sessions"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req, err := p.makeSessionsRequest(ctx, true /* excludeClosed */)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateSessionsTable(ctx, p, addRow, response)
	},
}

// crdbInternalClusterSessionsTable exposes the list of running sessions
// on the entire cluster. The result is dependent on the current user.
var crdbInternalClusterSessionsTable = virtualSchemaTable{
	comment: "running sessions visible to current user (cluster RPC; expensive!)",
	schema:  fmt.Sprintf(sessionsSchemaPattern, "cluster_sessions"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req, err := p.makeSessionsRequest(ctx, true /* excludeClosed */)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateSessionsTable(ctx, p, addRow, response)
	},
}

func populateSessionsTable(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	response *serverpb.ListSessionsResponse,
) error {
	shouldRedactOtherUserQuery := false
	canViewOtherUser := false
	// Check if the user is admin.
	if isAdmin, err := p.HasAdminRole(ctx); err != nil {
		return err
	} else if isAdmin {
		canViewOtherUser = true
	} else if !isAdmin {
		// If the user is not admin, check the individual VIEWACTIVITY and VIEWACTIVITYREDACTED
		// privileges.
		if hasPriv, shouldRedact, err := p.HasViewActivityOrViewActivityRedactedRole(ctx); err != nil {
			return err
		} else if hasPriv {
			canViewOtherUser = true
			if shouldRedact {
				// If the user has VIEWACTIVITYREDACTED, redact the query as it takes precedence
				// over VIEWACTIVITY.
				shouldRedactOtherUserQuery = true
			}
		}
	}
	for _, session := range response.Sessions {
		normalizedUser, err := username.MakeSQLUsernameFromUserInput(session.Username, username.PurposeValidation)
		if err != nil {
			return err
		}
		if !canViewOtherUser && normalizedUser != p.User() {
			continue
		}
		// Generate active_queries and active_query_start
		var activeQueries bytes.Buffer
		var activeQueryStart time.Time
		var activeQueryStartDatum tree.Datum

		for _, query := range session.ActiveQueries {
			// Note that the max length of ActiveQueries is 1.
			// The array is leftover from a time when we allowed multiple
			// queries to be executed at once in a session.
			activeQueryStart = query.Start
			sql := formatActiveQuery(query)
			// Never redact queries made by the same user.
			if shouldRedactOtherUserQuery && session.Username != p.User().Normalized() {
				sql = query.SqlNoConstants
			}
			activeQueries.WriteString(sql)
		}
		lastActiveQuery := session.LastActiveQuery
		// Never redact queries made by the same user.
		if shouldRedactOtherUserQuery && session.Username != p.User().Normalized() {
			lastActiveQuery = session.LastActiveQueryNoConstants
		}

		if activeQueryStart.IsZero() {
			activeQueryStartDatum = tree.DNull
		} else {
			activeQueryStartDatum, err = tree.MakeDTimestampTZ(activeQueryStart, time.Microsecond)
			if err != nil {
				return err
			}
		}

		kvTxnIDDatum := tree.DNull
		if session.ActiveTxn != nil {
			kvTxnIDDatum = tree.NewDString(session.ActiveTxn.ID.String())
		}

		sessionID := getSessionID(session)
		startTSDatum, err := tree.MakeDTimestampTZ(session.Start, time.Microsecond)
		if err != nil {
			return err
		}
		endTSDatum := tree.DNull
		if session.End != nil {
			endTSDatum, err = tree.MakeDTimestampTZ(*session.End, time.Microsecond)
			if err != nil {
				return err
			}
		}
		if err := addRow(
			tree.NewDInt(tree.DInt(session.NodeID)),
			sessionID,
			tree.NewDString(session.Username),
			tree.NewDString(session.ClientAddress),
			tree.NewDString(session.ApplicationName),
			tree.NewDString(activeQueries.String()),
			tree.NewDString(lastActiveQuery),
			tree.NewDInt(tree.DInt(session.NumTxnsExecuted)),
			startTSDatum,
			activeQueryStartDatum,
			kvTxnIDDatum,
			tree.NewDInt(tree.DInt(session.AllocBytes)),
			tree.NewDInt(tree.DInt(session.MaxAllocBytes)),
			tree.NewDString(session.Status.String()),
			endTSDatum,
			tree.NewDInt(tree.DInt(session.PGBackendPID)),
			tree.NewDInt(tree.DInt(session.TraceID)),
			tree.NewDInt(tree.DInt(session.GoroutineID)),
			tree.NewDString(string(session.AuthenticationMethod)),
		); err != nil {
			return err
		}
	}

	for _, rpcErr := range response.Errors {
		log.Warningf(ctx, "%v", rpcErr.Message)
		if rpcErr.NodeID != 0 {
			// Add a row with this node ID, error in active queries, and nulls
			// for all other columns.
			if err := addRow(
				tree.NewDInt(tree.DInt(rpcErr.NodeID)), // node ID
				tree.DNull,                             // session ID
				tree.DNull,                             // username
				tree.DNull,                             // client address
				tree.DNull,                             // application name
				tree.NewDString("-- "+rpcErr.Message),  // active queries
				tree.DNull,                             // last active query
				tree.DNull,                             // num txns executed
				tree.DNull,                             // session start
				tree.DNull,                             // active_query_start
				tree.DNull,                             // kv_txn
				tree.DNull,                             // alloc_bytes
				tree.DNull,                             // max_alloc_bytes
				tree.DNull,                             // status
				tree.DNull,                             // session_end
				tree.DNull,                             // pg_backend_pid
				tree.DNull,                             // trace_id
				tree.DNull,                             // goroutine_id
				tree.DNull,                             // authentication_method
			); err != nil {
				return err
			}
		}
	}

	return nil
}

var crdbInternalClusterContendedTablesView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.cluster_contended_tables (
  database_name,
  schema_name,
  table_name,
  num_contention_events
) AS
  SELECT
    database_name, schema_name, name, sum(num_contention_events)
  FROM
    (
      SELECT
        DISTINCT
        database_name,
        schema_name,
        name,
        index_id,
        num_contention_events
      FROM
        crdb_internal.cluster_contention_events
        JOIN crdb_internal.tables ON
            crdb_internal.cluster_contention_events.table_id
            = crdb_internal.tables.table_id
    )
  GROUP BY
    database_name, schema_name, name
`,
	resultColumns: colinfo.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "schema_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "num_contention_events", Typ: types.Int},
	},
}

var crdbInternalClusterContendedIndexesView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.cluster_contended_indexes (
  database_name,
  schema_name,
  table_name,
  index_name,
  num_contention_events
) AS
  SELECT
    DISTINCT
    database_name,
    schema_name,
    name,
    index_name,
    num_contention_events
  FROM
    crdb_internal.cluster_contention_events,
    crdb_internal.tables,
    crdb_internal.table_indexes
  WHERE
    crdb_internal.cluster_contention_events.index_id
    = crdb_internal.table_indexes.index_id
    AND crdb_internal.cluster_contention_events.table_id
      = crdb_internal.table_indexes.descriptor_id
    AND crdb_internal.cluster_contention_events.table_id
      = crdb_internal.tables.table_id
  ORDER BY
    num_contention_events DESC
`,
	resultColumns: colinfo.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "schema_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "index_name", Typ: types.String},
		{Name: "num_contention_events", Typ: types.Int},
	},
}

var crdbInternalClusterContendedKeysView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.cluster_contended_keys (
  database_name,
  schema_name,
  table_name,
  index_name,
  key,
  num_contention_events
) AS
  SELECT
    database_name,
    schema_name,
    name,
    index_name,
    crdb_internal.pretty_key(key, 0),
    sum(count)
  FROM
    crdb_internal.cluster_contention_events,
    crdb_internal.tables,
    crdb_internal.table_indexes
  WHERE
    crdb_internal.cluster_contention_events.index_id
    = crdb_internal.table_indexes.index_id
    AND crdb_internal.cluster_contention_events.table_id
      = crdb_internal.table_indexes.descriptor_id
    AND crdb_internal.cluster_contention_events.table_id
      = crdb_internal.tables.table_id
  GROUP BY
    database_name, schema_name, name, index_name, key
`,
	resultColumns: colinfo.ResultColumns{
		{Name: "database_name", Typ: types.String},
		{Name: "schema_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "index_name", Typ: types.String},
		{Name: "key", Typ: types.Bytes},
		{Name: "num_contention_events", Typ: types.Int},
	},
}

const contentionEventsSchemaPattern = `
CREATE TABLE crdb_internal.%s (
  table_id                   INT,
  index_id                   INT,
  num_contention_events      INT NOT NULL,
  cumulative_contention_time INTERVAL NOT NULL,
  key                        BYTES NOT NULL,
  txn_id                     UUID NOT NULL,
  count                      INT NOT NULL
)
`
const contentionEventsCommentPattern = `contention information %s

All of the contention information internally stored in three levels:
- on the highest, it is grouped by tableID/indexID pair
- on the middle, it is grouped by key
- on the lowest, it is grouped by txnID.
Each of the levels is maintained as an LRU cache with limited size, so
it is possible that not all of the contention information ever observed
is contained in this table.
`

// crdbInternalLocalContentionEventsTable exposes the list of contention events
// on the current node.
var crdbInternalLocalContentionEventsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(contentionEventsSchemaPattern, "node_contention_events"),
	comment: fmt.Sprintf(contentionEventsCommentPattern, "(RAM; local node only)"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		response, err := p.extendedEvalCtx.SQLStatusServer.ListLocalContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
		if err != nil {
			return err
		}
		return populateContentionEventsTable(ctx, p, addRow, response)
	},
}

// crdbInternalClusterContentionEventsTable exposes the list of contention
// events on the entire cluster.
var crdbInternalClusterContentionEventsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(contentionEventsSchemaPattern, "cluster_contention_events"),
	comment: fmt.Sprintf(contentionEventsCommentPattern, "(cluster RPC; expensive!)"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		response, err := p.extendedEvalCtx.SQLStatusServer.ListContentionEvents(ctx, &serverpb.ListContentionEventsRequest{})
		if err != nil {
			return err
		}
		return populateContentionEventsTable(ctx, p, addRow, response)
	},
}

func populateContentionEventsTable(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	response *serverpb.ListContentionEventsResponse,
) error {
	// Validate users have correct permission/role.
	hasPermission, shouldRedactKey, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
	if err != nil {
		return err
	}
	if !hasPermission {
		return noViewActivityOrViewActivityRedactedRoleError(p.User())
	}
	key := tree.NewDBytes("")
	for _, ice := range response.Events.IndexContentionEvents {
		for _, skc := range ice.Events {
			for _, stc := range skc.Txns {
				cumulativeContentionTime := tree.NewDInterval(
					duration.MakeDuration(ice.CumulativeContentionTime.Nanoseconds(), 0 /* days */, 0 /* months */),
					types.DefaultIntervalTypeMetadata,
				)
				if !shouldRedactKey {
					key = tree.NewDBytes(tree.DBytes(skc.Key))
				}
				if err := addRow(
					tree.NewDInt(tree.DInt(ice.TableID)),             // table_id
					tree.NewDInt(tree.DInt(ice.IndexID)),             // index_id
					tree.NewDInt(tree.DInt(ice.NumContentionEvents)), // num_contention_events
					cumulativeContentionTime,                         // cumulative_contention_time
					key,                                              // key
					tree.NewDUuid(tree.DUuid{UUID: stc.TxnID}),       // txn_id
					tree.NewDInt(tree.DInt(stc.Count)),               // count
				); err != nil {
					return err
				}
			}
		}
	}
	key = tree.NewDBytes("")
	for _, nkc := range response.Events.NonSQLKeysContention {
		for _, stc := range nkc.Txns {
			cumulativeContentionTime := tree.NewDInterval(
				duration.MakeDuration(nkc.CumulativeContentionTime.Nanoseconds(), 0 /* days */, 0 /* months */),
				types.DefaultIntervalTypeMetadata,
			)
			if !shouldRedactKey {
				key = tree.NewDBytes(tree.DBytes(nkc.Key))
			}
			if err := addRow(
				tree.DNull, // table_id
				tree.DNull, // index_id
				tree.NewDInt(tree.DInt(nkc.NumContentionEvents)), // num_contention_events
				cumulativeContentionTime,                         // cumulative_contention_time
				key,                                              // key
				tree.NewDUuid(tree.DUuid{UUID: stc.TxnID}), // txn_id
				tree.NewDInt(tree.DInt(stc.Count)),         // count
			); err != nil {
				return err
			}
		}
	}
	for _, rpcErr := range response.Errors {
		log.Warningf(ctx, "%v", rpcErr.Message)
	}
	return nil
}

const distSQLFlowsSchemaPattern = `
CREATE TABLE crdb_internal.%s (
  flow_id UUID NOT NULL,
  node_id INT NOT NULL,
  stmt    STRING NULL,
  since   TIMESTAMPTZ NOT NULL
)
`

const distSQLFlowsCommentPattern = `DistSQL remote flows information %s

This virtual table contains all of the remote flows of the DistSQL execution
that are currently running on %s. The local flows (those that are
running on the same node as the query originated on) are not included.
`

var crdbInternalLocalDistSQLFlowsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(distSQLFlowsSchemaPattern, "node_distsql_flows"),
	comment: fmt.Sprintf(distSQLFlowsCommentPattern, "(RAM; local node only)", "this node"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		response, err := p.extendedEvalCtx.SQLStatusServer.ListLocalDistSQLFlows(ctx, &serverpb.ListDistSQLFlowsRequest{})
		if err != nil {
			return err
		}
		return populateDistSQLFlowsTable(ctx, addRow, response)
	},
}

var crdbInternalClusterDistSQLFlowsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(distSQLFlowsSchemaPattern, "cluster_distsql_flows"),
	comment: fmt.Sprintf(distSQLFlowsCommentPattern, "(cluster RPC; expensive!)", "any node in the cluster"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		response, err := p.extendedEvalCtx.SQLStatusServer.ListDistSQLFlows(ctx, &serverpb.ListDistSQLFlowsRequest{})
		if err != nil {
			return err
		}
		return populateDistSQLFlowsTable(ctx, addRow, response)
	},
}

func populateDistSQLFlowsTable(
	ctx context.Context,
	addRow func(...tree.Datum) error,
	response *serverpb.ListDistSQLFlowsResponse,
) error {
	for _, f := range response.Flows {
		flowID := tree.NewDUuid(tree.DUuid{UUID: f.FlowID.UUID})
		for _, info := range f.Infos {
			nodeID := tree.NewDInt(tree.DInt(info.NodeID))
			stmt := tree.NewDString(info.Stmt)
			since, err := tree.MakeDTimestampTZ(info.Timestamp, time.Millisecond)
			if err != nil {
				return err
			}
			if err = addRow(flowID, nodeID, stmt, since); err != nil {
				return err
			}
		}
	}
	for _, rpcErr := range response.Errors {
		log.Warningf(ctx, "%v", rpcErr.Message)
	}
	return nil
}

// crdbInternalLocalMetricsTable exposes a snapshot of the metrics on the
// current node.
var crdbInternalLocalMetricsTable = virtualSchemaTable{
	comment: "current values for metrics (RAM; local node only)",
	schema: `CREATE TABLE crdb_internal.node_metrics (
  store_id 	         INT NULL,         -- the store, if any, for this metric
  name               STRING NOT NULL,  -- name of the metric
  value							 FLOAT NOT NULL    -- value of the metric
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}

		mr := p.ExecCfg().MetricsRecorder
		if mr == nil {
			return nil
		}
		nodeStatus := mr.GenerateNodeStatus(ctx)
		for i := 0; i <= len(nodeStatus.StoreStatuses); i++ {
			storeID := tree.DNull
			mtr := nodeStatus.Metrics
			if i > 0 {
				storeID = tree.NewDInt(tree.DInt(nodeStatus.StoreStatuses[i-1].Desc.StoreID))
				mtr = nodeStatus.StoreStatuses[i-1].Metrics
			}
			for name, value := range mtr {
				if err := addRow(
					storeID,
					tree.NewDString(name),
					tree.NewDFloat(tree.DFloat(value)),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

// crdbInternalBuiltinFunctionsTable exposes the built-in function
// metadata.
var crdbInternalBuiltinFunctionsTable = virtualSchemaTable{
	comment: "built-in functions (RAM/static)",
	schema: `
CREATE TABLE crdb_internal.builtin_functions (
  function  STRING NOT NULL,
  signature STRING NOT NULL,
  category  STRING NOT NULL,
  details   STRING NOT NULL,
  schema    STRING NOT NULL,
  oid       OID NOT NULL
)`,
	populate: func(ctx context.Context, _ *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, name := range builtins.AllBuiltinNames() {
			props, overloads := builtinsregistry.GetBuiltinProperties(name)
			schema := catconstants.PgCatalogName
			const crdbInternal = catconstants.CRDBInternalSchemaName + "."
			const infoSchema = catconstants.InformationSchemaName + "."
			if strings.HasPrefix(name, crdbInternal) {
				name = name[len(crdbInternal):]
				schema = catconstants.CRDBInternalSchemaName
			} else if strings.HasPrefix(name, infoSchema) {
				name = name[len(infoSchema):]
				schema = catconstants.InformationSchemaName
			}
			for _, f := range overloads {
				if err := addRow(
					tree.NewDString(name),
					tree.NewDString(f.Signature(false /* simplify */)),
					tree.NewDString(props.Category),
					tree.NewDString(f.Info),
					tree.NewDString(schema),
					tree.NewDOid(f.Oid),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

// crdbInternalBuiltinFunctionCommentsTable exposes the built-in function
// comments, for use in pg_catalog.
var crdbInternalBuiltinFunctionCommentsTable = virtualSchemaTable{
	comment: "built-in functions (RAM/static)",
	schema:  vtable.CrdbInternalBuiltinFunctionComments,
	populate: func(ctx context.Context, _ *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, name := range builtins.AllBuiltinNames() {
			_, overloads := builtinsregistry.GetBuiltinProperties(name)
			for _, f := range overloads {
				if f.Info == "" {
					continue
				}
				if err := addRow(
					tree.NewDOid(f.Oid),
					tree.NewDString(f.Info),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

func writeCreateTypeDescRow(
	ctx context.Context,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	p *planner,
	typeDesc catalog.TypeDescriptor,
	addRow func(...tree.Datum) error,
) (written bool, err error) {
	var typeVariety tree.CreateTypeVariety
	var typeList []tree.CompositeTypeElem
	var enumLabels tree.EnumValueList
	enumLabelsDatum := tree.NewDArray(types.String)
	resolver := p.semaCtx.TypeResolver
	descriptors := p.descCollection

	if typeDesc.AsAliasTypeDescriptor() != nil {
		// Alias types are created implicitly, so we don't have create
		// statements for them.
		return false, nil
	}
	if e := typeDesc.AsEnumTypeDescriptor(); e != nil {
		if e.AsRegionEnumTypeDescriptor() != nil {
			// Multi-region enums are created implicitly, so we don't have create
			// statements for them.
			return false, nil
		}

		for i := 0; i < e.NumEnumMembers(); i++ {
			rep := e.GetMemberLogicalRepresentation(i)
			enumLabels = append(enumLabels, tree.EnumValue(rep))
			if err := enumLabelsDatum.Append(tree.NewDString(rep)); err != nil {
				return false, err
			}
		}
		typeVariety = tree.Enum
	} else if c := typeDesc.AsCompositeTypeDescriptor(); c != nil {
		typeList = make([]tree.CompositeTypeElem, c.NumElements())
		for i := 0; i < c.NumElements(); i++ {
			t := c.GetElementType(i)
			if err := typedesc.EnsureTypeIsHydrated(
				ctx, t, resolver.(catalog.TypeDescriptorResolver),
			); err != nil {
				return false, err
			}
			typeList[i].Type = t
			typeList[i].Label = tree.Name(c.GetElementLabel(i))
		}
		typeVariety = tree.Composite
	} else {
		return false, errors.AssertionFailedf("unknown type descriptor kind %s", typeDesc.GetKind())
	}

	name, err := tree.NewUnresolvedObjectName(2, [3]string{typeDesc.GetName(), sc.GetName()}, 0)
	if err != nil {
		return false, err
	}
	node := &tree.CreateType{
		Variety:           typeVariety,
		TypeName:          name,
		CompositeTypeList: typeList,
		EnumLabels:        enumLabels,
	}

	createStatement := tree.AsString(node)

	comment, ok := descriptors.GetTypeComment(typeDesc.GetID())
	if ok {
		commentOnType := tree.CommentOnType{Comment: &comment, Name: name}
		createStatement += ";\n" + tree.AsString(&commentOnType)
	}

	return true, addRow(
		tree.NewDInt(tree.DInt(db.GetID())),       // database_id
		tree.NewDString(db.GetName()),             // database_name
		tree.NewDString(sc.GetName()),             // schema_name
		tree.NewDInt(tree.DInt(typeDesc.GetID())), // descriptor_id
		tree.NewDString(typeDesc.GetName()),       // descriptor_name
		tree.NewDString(createStatement),          // create_statement
		enumLabelsDatum,                           // empty for composite types
	)
}

var crdbInternalCreateTypeStmtsTable = virtualSchemaTable{
	comment: "CREATE statements for all user defined types accessible by the current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.create_type_statements (
	database_id        INT,
  database_name      STRING,
  schema_name        STRING,
  descriptor_id      INT,
  descriptor_name    STRING,
  create_statement   STRING,
  enum_members       STRING[], -- populated only for ENUM types
  INDEX (descriptor_id)
)
`,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTypeDesc(ctx, p, db, func(ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, typeDesc catalog.TypeDescriptor) error {
			_, err := writeCreateTypeDescRow(ctx, db, sc, p, typeDesc, addRow)

			return err
		})
	},
	indexes: []virtualIndex{
		{
			populate: func(
				ctx context.Context,
				unwrappedConstraint tree.Datum,
				p *planner,
				db catalog.DatabaseDescriptor,
				addRow func(...tree.Datum) error,
			) (matched bool, err error) {
				id := descpb.ID(tree.MustBeDInt(unwrappedConstraint))
				scName, typDesc, err := getSchemaAndTypeByTypeID(ctx, p, id)
				if err != nil || typDesc == nil {
					return false, err
				}

				// Handle special cases for the database descriptor provided. There are
				// two scenarios:
				// 1) A database descriptor is provided, but the type belongs to a
				//    different database. In this case, the row will be filtered out as
				//    it doesn't pertain to the requested database.
				// 2) The database descriptor is nil. This occurs when requesting types
				//    across all databases (e.g., SELECT .. FROM "".crdb_internal.create_type_statements).
				//    In this case, the row will be returned, but a database lookup is
				//    needed to populate columns that depend on the database descriptor.
				if db != nil && typDesc.GetParentID() != db.GetID() {
					return false, nil
				} else if db == nil {
					db, err = p.byIDGetterBuilder().WithoutDropped().Get().Database(ctx, typDesc.GetParentID())
					if err != nil {
						return false, err
					}
				}
				return writeCreateTypeDescRow(ctx, db, scName, p, typDesc, addRow)
			},
		},
	},
}

var crdbInternalCreateSchemaStmtsTable = virtualSchemaTable{
	comment: "CREATE statements for all user defined schemas accessible by the current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.create_schema_statements (
	database_id        INT,
  database_name      STRING,
  schema_name        STRING,
  descriptor_id      INT,
  create_statement   STRING
)
`,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		var dbDescs []catalog.DatabaseDescriptor
		if db == nil {
			var err error
			dbDescs, err = p.Descriptors().GetAllDatabaseDescriptors(ctx, p.Txn())
			if err != nil {
				return err
			}
		} else {
			dbDescs = append(dbDescs, db)
		}
		for _, db := range dbDescs {
			return forEachSchema(ctx, p, db, true /* requiresPrivileges */, func(ctx context.Context, schemaDesc catalog.SchemaDescriptor) error {
				switch schemaDesc.SchemaKind() {
				case catalog.SchemaUserDefined:
					node := &tree.CreateSchema{
						Schema: tree.ObjectNamePrefix{
							SchemaName:     tree.Name(schemaDesc.GetName()),
							ExplicitSchema: true,
						},
					}

					createStatement := tree.AsString(node)

					comment, ok := p.Descriptors().GetSchemaComment(schemaDesc.GetID())
					if ok {
						commentOnSchema := tree.CommentOnSchema{Comment: &comment, Name: tree.ObjectNamePrefix{SchemaName: tree.Name(schemaDesc.GetName()), ExplicitSchema: true}}
						createStatement += ";\n" + tree.AsString(&commentOnSchema)
					}

					if err := addRow(
						tree.NewDInt(tree.DInt(db.GetID())),         // database_id
						tree.NewDString(db.GetName()),               // database_name
						tree.NewDString(schemaDesc.GetName()),       // schema_name
						tree.NewDInt(tree.DInt(schemaDesc.GetID())), // descriptor_id (schema_id)
						tree.NewDString(createStatement),            // create_statement
					); err != nil {
						return err
					}
				}
				return nil
			})
		}
		return nil
	},
}

func createRoutinePopulate(
	procedure bool,
) func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
	return func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		var dbDescs []catalog.DatabaseDescriptor
		if db == nil {
			var err error
			dbDescs, err = p.Descriptors().GetAllDatabaseDescriptors(ctx, p.Txn())
			if err != nil {
				return err
			}
		} else {
			dbDescs = append(dbDescs, db)
		}
		var fnIDs []descpb.ID
		fnIDToScName := make(map[descpb.ID]string)
		fnIDToScID := make(map[descpb.ID]descpb.ID)
		fnIDToDBName := make(map[descpb.ID]string)
		fnIDToDBID := make(map[descpb.ID]descpb.ID)
		for _, curDB := range dbDescs {
			err := forEachSchema(ctx, p, curDB, true /* requiresPrivileges */, func(ctx context.Context, sc catalog.SchemaDescriptor) error {
				return sc.ForEachFunctionSignature(func(sig descpb.SchemaDescriptor_FunctionSignature) error {
					fnIDs = append(fnIDs, sig.ID)
					fnIDToScName[sig.ID] = sc.GetName()
					fnIDToScID[sig.ID] = sc.GetID()
					fnIDToDBName[sig.ID] = curDB.GetName()
					fnIDToDBID[sig.ID] = curDB.GetID()
					return nil
				})
			})
			if err != nil {
				return err
			}
		}

		fnDescs, err := p.Descriptors().ByIDWithoutLeased(p.txn).WithoutNonPublic().Get().Descs(ctx, fnIDs)
		if err != nil {
			return err
		}

		for _, desc := range fnDescs {
			fnDesc := desc.(catalog.FunctionDescriptor)
			if procedure != fnDesc.IsProcedure() {
				// Skip functions if procedure is true, and skip procedures
				// otherwise.
				continue
			}
			treeNode, err := fnDesc.ToCreateExpr()
			treeNode.Name.ObjectNamePrefix = tree.ObjectNamePrefix{
				ExplicitSchema: true,
				SchemaName:     tree.Name(fnIDToScName[fnDesc.GetID()]),
			}
			if err != nil {
				return err
			}
			for i := range treeNode.Options {
				if body, ok := treeNode.Options[i].(tree.RoutineBodyStr); ok {
					bodyStr := string(body)
					bodyStr, err = formatFunctionQueryTypesForDisplay(ctx, p.EvalContext(), &p.semaCtx, p.SessionData(), bodyStr, fnDesc.GetLanguage())
					if err != nil {
						return err
					}
					bodyStr, err = formatQuerySequencesForDisplay(ctx, &p.semaCtx, bodyStr, true /* multiStmt */, fnDesc.GetLanguage())
					if err != nil {
						return err
					}
					bodyStr = strings.TrimSpace(bodyStr)
					stmtStrs := strings.Split(bodyStr, "\n")
					for i := range stmtStrs {
						if stmtStrs[i] != "" {
							stmtStrs[i] = "\t" + stmtStrs[i]
						}
					}
					p := &treeNode.Options[i]
					// Add two new lines just for better formatting.
					*p = tree.RoutineBodyStr("\n" + strings.Join(stmtStrs, "\n") + "\n")
				}
			}

			err = addRow(
				tree.NewDInt(tree.DInt(fnIDToDBID[fnDesc.GetID()])), // database_id
				tree.NewDString(fnIDToDBName[fnDesc.GetID()]),       // database_name
				tree.NewDInt(tree.DInt(fnIDToScID[fnDesc.GetID()])), // schema_id
				tree.NewDString(fnIDToScName[fnDesc.GetID()]),       // schema_name
				tree.NewDInt(tree.DInt(fnDesc.GetID())),             // function_id
				tree.NewDString(fnDesc.GetName()),                   // function_name
				tree.NewDString(tree.AsString(treeNode)),            // create_statement
			)
			if err != nil {
				return err
			}
		}
		return nil
	}
}

var crdbInternalCreateFunctionStmtsTable = virtualSchemaTable{
	comment: "CREATE statements for all user-defined functions",
	schema: `
CREATE TABLE crdb_internal.create_function_statements (
  database_id INT,
  database_name STRING,
  schema_id INT,
  schema_name STRING,
  function_id INT,
  function_name STRING,
  create_statement STRING
)
`,
	populate: createRoutinePopulate(false /* procedure */),
}

var crdbInternalCreateProcedureStmtsTable = virtualSchemaTable{
	comment: "CREATE statements for all user-defined procedures",
	schema: `
CREATE TABLE crdb_internal.create_procedure_statements (
  database_id INT,
  database_name STRING,
  schema_id INT,
  schema_name STRING,
  procedure_id INT,
  procedure_name STRING,
  create_statement STRING
)
`,
	populate: createRoutinePopulate(true /* procedure */),
}

// Prepare the row populate function.
var typeView = tree.NewDString("view")
var typeTable = tree.NewDString("table")
var typeSequence = tree.NewDString("sequence")

// crdbInternalCreateStmtsTable exposes the CREATE TABLE/CREATE VIEW
// statements.
//
// TODO(tbg): prefix with kv_.
var crdbInternalCreateStmtsTable = makeAllRelationsVirtualTableWithDescriptorIDIndex(
	`CREATE and ALTER statements for all tables accessible by current user in current database (KV scan)`,
	`
CREATE TABLE crdb_internal.create_statements (
  database_id                   INT,
  database_name                 STRING,
  schema_name                   STRING NOT NULL,
  descriptor_id                 INT,
  descriptor_type               STRING NOT NULL,
  descriptor_name               STRING NOT NULL,
  create_statement              STRING NOT NULL,
  state                         STRING NOT NULL,
  create_nofks                  STRING NOT NULL,
  policy_statements             STRING[] NOT NULL,
  alter_statements              STRING[] NOT NULL,
  validate_statements           STRING[] NOT NULL,
  create_redactable             STRING NOT NULL,
  has_partitions                BOOL NOT NULL,
  is_multi_region               BOOL NOT NULL,
  is_virtual                    BOOL NOT NULL,
  is_temporary                  BOOL NOT NULL,
  INDEX(descriptor_id)
)
`, virtualCurrentDB, false, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor,
		table catalog.TableDescriptor, lookup simpleSchemaResolver, addRow func(...tree.Datum) error,
	) error {
		contextName := ""
		parentNameStr := tree.DNull
		if db != nil {
			contextName = db.GetName()
			parentNameStr = tree.NewDString(contextName)
		}
		scNameStr := tree.NewDString(sc.GetName())

		var descType tree.Datum
		var stmt, createNofk, createRedactable string
		alterStmts := tree.NewDArray(types.String)
		policyStmts := tree.NewDArray(types.String)
		validateStmts := tree.NewDArray(types.String)
		namePrefix := tree.ObjectNamePrefix{SchemaName: tree.Name(sc.GetName()), ExplicitSchema: true}
		name := tree.MakeTableNameFromPrefix(namePrefix, tree.Name(table.GetName()))
		var err error
		if table.IsView() {
			descType = typeView
			stmt, err = ShowCreateView(
				ctx, p.EvalContext(), &p.semaCtx, p.SessionData(), &name, table, false, /* redactableValues */
			)
			if err != nil {
				return err
			}
			createRedactable, err = ShowCreateView(
				ctx, p.EvalContext(), &p.semaCtx, p.SessionData(), &name, table, true, /* redactableValues */
			)
		} else if table.IsSequence() {
			descType = typeSequence
			stmt, err = ShowCreateSequence(ctx, &name, table)
			createRedactable = stmt
		} else {
			descType = typeTable
			displayOptions := ShowCreateDisplayOptions{
				FKDisplayMode:       OmitFKClausesFromCreate,
				IgnoreRLSStatements: true,
			}
			createNofk, err = ShowCreateTable(ctx, p, &name, contextName, table, lookup, displayOptions)
			if err != nil {
				return err
			}

			if err := showAlterStatement(ctx, &name, contextName, lookup, table, alterStmts, validateStmts); err != nil {
				return err
			}

			if err = showPolicyStatements(ctx, &name, table, p.EvalContext(), &p.semaCtx, p.SessionData(), policyStmts); err != nil {
				return err
			}

			displayOptions.FKDisplayMode = IncludeFkClausesInCreate
			displayOptions.IgnoreRLSStatements = false
			stmt, err = ShowCreateTable(ctx, p, &name, contextName, table, lookup, displayOptions)
			if err != nil {
				return err
			}
			displayOptions.RedactableValues = true
			createRedactable, err = ShowCreateTable(
				ctx, p, &name, contextName, table, lookup, displayOptions,
			)
		}
		if err != nil {
			return err
		}

		descID := tree.NewDInt(tree.DInt(table.GetID()))
		dbDescID := tree.NewDInt(tree.DInt(table.GetParentID()))
		if createNofk == "" {
			createNofk = stmt
		}
		hasPartitions := nil != catalog.FindIndex(table, catalog.IndexOpts{}, func(idx catalog.Index) bool {
			return idx.PartitioningColumnCount() != 0
		})
		return addRow(
			dbDescID,
			parentNameStr,
			scNameStr,
			descID,
			descType,
			tree.NewDString(table.GetName()),
			tree.NewDString(stmt),
			tree.NewDString(table.GetState().String()),
			tree.NewDString(createNofk),
			policyStmts,
			alterStmts,
			validateStmts,
			tree.NewDString(createRedactable),
			tree.MakeDBool(tree.DBool(hasPartitions)),
			tree.MakeDBool(tree.DBool(db != nil && db.IsMultiRegion())),
			tree.MakeDBool(tree.DBool(table.IsVirtualTable())),
			tree.MakeDBool(tree.DBool(table.IsTemporary())),
		)
	},
	nil)

// showPolicyStatements adds the RLS policy statements to the policy_statements column.
func showPolicyStatements(
	ctx context.Context,
	tn *tree.TableName,
	table catalog.TableDescriptor,
	evalCtx *eval.Context,
	semaCtx *tree.SemaContext,
	sessionData *sessiondata.SessionData,
	policyStmts *tree.DArray,
) error {
	for _, policy := range table.GetPolicies() {
		if policyStatement, err := showPolicyStatement(ctx, tn, table, evalCtx, semaCtx, sessionData, policy, false); err != nil {
			return err
		} else if len(policyStatement) != 0 {
			if err := policyStmts.Append(tree.NewDString(policyStatement)); err != nil {
				return err
			}
		}
	}

	return nil
}

func showAlterStatement(
	ctx context.Context,
	tn *tree.TableName,
	contextName string,
	lCtx simpleSchemaResolver,
	table catalog.TableDescriptor,
	alterStmts *tree.DArray,
	validateStmts *tree.DArray,
) error {
	for _, fk := range table.OutboundForeignKeys() {
		f := tree.NewFmtCtx(tree.FmtSimple)
		f.WriteString("ALTER TABLE ")
		f.FormatNode(tn)
		f.WriteString(" ADD CONSTRAINT ")
		f.FormatName(fk.GetName())
		f.WriteByte(' ')
		// Passing in EmptySearchPath causes the schema name to show up in the
		// constraint definition, which we need for `cockroach dump` output to be
		// usable.
		if err := showForeignKeyConstraint(
			&f.Buffer,
			contextName,
			table,
			fk.ForeignKeyDesc(),
			lCtx,
			sessiondata.EmptySearchPath,
		); err != nil {
			return err
		}
		if err := alterStmts.Append(tree.NewDString(f.CloseAndGetString())); err != nil {
			return err
		}

		f = tree.NewFmtCtx(tree.FmtSimple)
		f.WriteString("ALTER TABLE ")
		f.FormatNode(tn)
		f.WriteString(" VALIDATE CONSTRAINT ")
		f.FormatName(fk.GetName())

		if err := validateStmts.Append(tree.NewDString(f.CloseAndGetString())); err != nil {
			return err
		}
	}

	// Add the row level security ALTER statements to the alter_statements column.
	if alterRLSStatements, err := showRLSAlterStatement(tn, table, false); err != nil {
		return err
	} else if len(alterRLSStatements) != 0 {
		if err = alterStmts.Append(tree.NewDString(alterRLSStatements)); err != nil {
			return err
		}
	}

	return nil
}

// crdbInternalTableColumnsTable exposes the column descriptors.
//
// TODO(tbg): prefix with kv_.
var crdbInternalTableColumnsTable = virtualSchemaTable{
	comment: "details for all columns accessible by current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.table_columns (
  descriptor_id    INT,
  descriptor_name  STRING NOT NULL,
  column_id        INT NOT NULL,
  column_name      STRING NOT NULL,
  column_type      STRING NOT NULL,
  nullable         BOOL NOT NULL,
  default_expr     STRING,
  hidden           BOOL NOT NULL
)
`,
	generator: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		const numDatums = 8
		row := make(tree.Datums, numDatums)
		worker := func(ctx context.Context, pusher rowPusher) error {
			opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: true}
			return forEachTableDesc(ctx, p, dbContext, opts,
				func(ctx context.Context, descCtx tableDescContext) error {
					table := descCtx.table
					tableID := tree.NewDInt(tree.DInt(table.GetID()))
					tableName := tree.NewDString(table.GetName())
					columns := table.PublicColumns()
					for _, col := range columns {
						defStr := tree.DNull
						if col.HasDefault() {
							defExpr, err := schemaexpr.FormatExprForDisplay(
								ctx, table, col.GetDefaultExpr(), p.EvalContext(), &p.semaCtx, p.SessionData(), tree.FmtParsable,
							)
							if err != nil {
								return err
							}
							defStr = tree.NewDString(defExpr)
						}
						row = append(row[:0],
							tableID,
							tableName,
							tree.NewDInt(tree.DInt(col.GetID())),
							tree.NewDString(col.GetName()),
							tree.NewDString(col.GetType().DebugString()),
							tree.MakeDBool(tree.DBool(col.IsNullable())),
							defStr,
							tree.MakeDBool(tree.DBool(col.IsHidden())),
						)
						if buildutil.CrdbTestBuild {
							if len(row) != numDatums {
								return errors.AssertionFailedf("expected %d datums, got %d", numDatums, len(row))
							}
						}
						if err := pusher.pushRow(row...); err != nil {
							return err
						}
					}
					return nil
				},
			)
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// crdbInternalTableIndexesTable exposes the index descriptors.
var crdbInternalTableIndexesTable = virtualSchemaTable{
	comment: "indexes accessible by current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.table_indexes (
  descriptor_id       INT,
  descriptor_name     STRING NOT NULL,
  index_id            INT NOT NULL,
  index_name          STRING NOT NULL,
  index_type          STRING NOT NULL,
  is_unique           BOOL NOT NULL,
  is_inverted         BOOL NOT NULL,
  is_sharded          BOOL NOT NULL,
  is_visible          BOOL NOT NULL,
  visibility          FLOAT NOT NULL,
  shard_bucket_count  INT,
  created_at          TIMESTAMP,
  create_statement    STRING NOT NULL
)
`,
	generator: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		primary := tree.NewDString("primary")
		secondary := tree.NewDString("secondary")
		const numDatums = 13
		row := make([]tree.Datum, numDatums)
		worker := func(ctx context.Context, pusher rowPusher) error {
			alloc := &tree.DatumAlloc{}
			opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: true}
			return forEachTableDesc(ctx, p, dbContext, opts,
				func(ctx context.Context, descCtx tableDescContext) error {
					sc, table := descCtx.schema, descCtx.table
					tableID := tree.NewDInt(tree.DInt(table.GetID()))
					tableName := tree.NewDString(table.GetName())
					// We report the primary index of non-physical tables here. These
					// indexes are not reported as a part of ForeachIndex.
					return catalog.ForEachIndex(table, catalog.IndexOpts{
						NonPhysicalPrimaryIndex: true,
					}, func(idx catalog.Index) error {
						idxType := secondary
						if idx.Primary() {
							idxType = primary
						}
						createdAt := tree.DNull
						if ts := idx.CreatedAt(); !ts.IsZero() {
							tsDatum, err := tree.MakeDTimestamp(ts, time.Nanosecond)
							if err != nil {
								log.Warningf(ctx, "failed to construct timestamp for index: %v", err)
							} else {
								createdAt = tsDatum
							}
						}
						shardBucketCnt := tree.DNull
						if idx.IsSharded() {
							shardBucketCnt = tree.NewDInt(tree.DInt(idx.GetSharded().ShardBuckets))
						}
						idxInvisibility := idx.GetInvisibility()
						namePrefix := tree.ObjectNamePrefix{SchemaName: tree.Name(sc.GetName()), ExplicitSchema: true}
						fullTableName := tree.MakeTableNameFromPrefix(namePrefix, tree.Name(table.GetName()))
						var partitionBuf bytes.Buffer
						if err := ShowCreatePartitioning(
							alloc,
							p.ExecCfg().Codec,
							table,
							idx,
							idx.GetPartitioning(),
							&partitionBuf,
							0,     /* indent */
							0,     /* colOffset */
							false, /* redactableValues */
						); err != nil {
							return err
						}
						createIndexStmt, err := catformat.IndexForDisplay(
							ctx,
							table,
							&fullTableName,
							idx,
							partitionBuf.String(),
							tree.FmtSimple,
							p.EvalContext(),
							p.SemaCtx(),
							p.SessionData(),
							catformat.IndexDisplayShowCreate,
						)
						if err != nil {
							return err
						}
						row = append(row[:0],
							tableID,
							tableName,
							tree.NewDInt(tree.DInt(idx.GetID())),
							tree.NewDString(idx.GetName()),
							idxType,
							tree.MakeDBool(tree.DBool(idx.IsUnique())),
							tree.MakeDBool(idx.GetType() == idxtype.INVERTED),
							tree.MakeDBool(tree.DBool(idx.IsSharded())),
							tree.MakeDBool(idxInvisibility == 0.0),
							tree.NewDFloat(tree.DFloat(1-idxInvisibility)),
							shardBucketCnt,
							createdAt,
							tree.NewDString(createIndexStmt),
						)
						if buildutil.CrdbTestBuild {
							if len(row) != numDatums {
								return errors.AssertionFailedf("expected %d datums, got %d", numDatums, len(row))
							}
						}
						return pusher.pushRow(row...)
					})
				},
			)
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// crdbInternalIndexColumnsTable exposes the index columns.
//
// TODO(tbg): prefix with kv_.
var crdbInternalIndexColumnsTable = virtualSchemaTable{
	comment: "index columns for all indexes accessible by current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.index_columns (
  descriptor_id    INT,
  descriptor_name  STRING NOT NULL,
  index_id         INT NOT NULL,
  index_name       STRING NOT NULL,
  column_type      STRING NOT NULL,
  column_id        INT NOT NULL,
  column_name      STRING,
  column_direction STRING,
  implicit         BOOL
)
`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		key := tree.NewDString("key")
		storing := tree.NewDString("storing")
		extra := tree.NewDString("extra")
		composite := tree.NewDString("composite")
		idxDirMap := map[catenumpb.IndexColumn_Direction]tree.Datum{
			catenumpb.IndexColumn_ASC:  tree.NewDString(catenumpb.IndexColumn_ASC.String()),
			catenumpb.IndexColumn_DESC: tree.NewDString(catenumpb.IndexColumn_DESC.String()),
		}

		opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: true}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				parent, table := descCtx.database, descCtx.table
				tableID := tree.NewDInt(tree.DInt(table.GetID()))
				parentName := parent.GetName()
				tableName := tree.NewDString(table.GetName())

				reportIndex := func(idx catalog.Index) error {
					idxID := tree.NewDInt(tree.DInt(idx.GetID()))
					idxName := tree.NewDString(idx.GetName())

					// Report the main (key) columns.
					for i := 0; i < idx.NumKeyColumns(); i++ {
						c := idx.GetKeyColumnID(i)
						colName := tree.DNull
						colDir := tree.DNull
						if i >= len(idx.IndexDesc().KeyColumnNames) {
							// We log an error here, instead of reporting an error
							// to the user, because we really want to see the
							// erroneous data in the virtual table.
							log.Errorf(ctx, "index descriptor for [%d@%d] (%s.%s@%s) has more key column IDs (%d) than names (%d) (corrupted schema?)",
								table.GetID(), idx.GetID(), parentName, table.GetName(), idx.GetName(),
								len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnNames))
						} else {
							colName = tree.NewDString(idx.GetKeyColumnName(i))
						}
						if i >= len(idx.IndexDesc().KeyColumnDirections) {
							// See comment above.
							log.Errorf(ctx, "index descriptor for [%d@%d] (%s.%s@%s) has more key column IDs (%d) than directions (%d) (corrupted schema?)",
								table.GetID(), idx.GetID(), parentName, table.GetName(), idx.GetName(),
								len(idx.IndexDesc().KeyColumnIDs), len(idx.IndexDesc().KeyColumnDirections))
						} else {
							colDir = idxDirMap[idx.GetKeyColumnDirection(i)]
						}

						if err := addRow(
							tableID, tableName, idxID, idxName,
							key, tree.NewDInt(tree.DInt(c)), colName, colDir,
							tree.MakeDBool(i < idx.ExplicitColumnStartIdx()),
						); err != nil {
							return err
						}
					}

					notImplicit := tree.DBoolFalse

					// Report the stored columns.
					for i := 0; i < idx.NumSecondaryStoredColumns(); i++ {
						c := idx.GetStoredColumnID(i)
						if err := addRow(
							tableID, tableName, idxID, idxName,
							storing, tree.NewDInt(tree.DInt(c)), tree.DNull, tree.DNull,
							notImplicit,
						); err != nil {
							return err
						}
					}

					// Report the extra columns.
					for i := 0; i < idx.NumKeySuffixColumns(); i++ {
						c := idx.GetKeySuffixColumnID(i)
						if err := addRow(
							tableID, tableName, idxID, idxName,
							extra, tree.NewDInt(tree.DInt(c)), tree.DNull, tree.DNull,
							notImplicit,
						); err != nil {
							return err
						}
					}

					// Report the composite columns
					for i := 0; i < idx.NumCompositeColumns(); i++ {
						c := idx.GetCompositeColumnID(i)
						if err := addRow(
							tableID, tableName, idxID, idxName,
							composite, tree.NewDInt(tree.DInt(c)), tree.DNull, tree.DNull,
							notImplicit,
						); err != nil {
							return err
						}
					}

					return nil
				}

				return catalog.ForEachIndex(table, catalog.IndexOpts{NonPhysicalPrimaryIndex: true}, reportIndex)
			})
	},
}

// crdbInternalBackwardDependenciesTable exposes the backward
// inter-descriptor dependencies.
//
// TODO(tbg): prefix with kv_.
var crdbInternalBackwardDependenciesTable = virtualSchemaTable{
	comment: "backward inter-descriptor dependencies starting from tables accessible by current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.backward_dependencies (
  descriptor_id      INT,
  descriptor_name    STRING NOT NULL,
  index_id           INT,
  column_id          INT,
  dependson_id       INT NOT NULL,
  dependson_type     STRING NOT NULL,
  dependson_index_id INT,
  dependson_name     STRING,
  dependson_details  STRING
)
`,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor,
		addRow func(...tree.Datum) error,
	) error {
		fkDep := tree.NewDString("fk")
		viewDep := tree.NewDString("view")
		sequenceDep := tree.NewDString("sequence")

		opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: true}
		return forEachTableDesc(ctx, p, dbContext, opts, func(
			ctx context.Context, descCtx tableDescContext) error {
			table, tableLookup := descCtx.table, descCtx.tableLookup
			tableID := tree.NewDInt(tree.DInt(table.GetID()))
			tableName := tree.NewDString(table.GetName())

			for _, fk := range table.OutboundForeignKeys() {
				refTbl, err := tableLookup.getTableByID(fk.GetReferencedTableID())
				if err != nil {
					return err
				}
				refConstraint, err := catalog.FindFKReferencedUniqueConstraint(refTbl, fk)
				if err != nil {
					return err
				}
				var refIdxID descpb.IndexID
				if refIdx := refConstraint.AsUniqueWithIndex(); refIdx != nil {
					refIdxID = refIdx.GetID()
				}
				if err := addRow(
					tableID, tableName,
					tree.DNull,
					tree.DNull,
					tree.NewDInt(tree.DInt(fk.GetReferencedTableID())),
					fkDep,
					tree.NewDInt(tree.DInt(refIdxID)),
					tree.NewDString(fk.GetName()),
					tree.DNull,
				); err != nil {
					return err
				}
			}

			// Record the view dependencies.
			for _, tIdx := range table.GetDependsOn() {
				if err := addRow(
					tableID, tableName,
					tree.DNull,
					tree.DNull,
					tree.NewDInt(tree.DInt(tIdx)),
					viewDep,
					tree.DNull,
					tree.DNull,
					tree.DNull,
				); err != nil {
					return err
				}
			}
			for _, tIdx := range table.GetDependsOnTypes() {
				if err := addRow(
					tableID, tableName,
					tree.DNull,
					tree.DNull,
					tree.NewDInt(tree.DInt(tIdx)),
					viewDep,
					tree.DNull,
					tree.DNull,
					tree.DNull,
				); err != nil {
					return err
				}
			}

			// Record sequence dependencies.
			for _, col := range table.PublicColumns() {
				for i := 0; i < col.NumUsesSequences(); i++ {
					sequenceID := col.GetUsesSequenceID(i)
					if err := addRow(
						tableID, tableName,
						tree.DNull,
						tree.NewDInt(tree.DInt(col.GetID())),
						tree.NewDInt(tree.DInt(sequenceID)),
						sequenceDep,
						tree.DNull,
						tree.DNull,
						tree.DNull,
					); err != nil {
						return err
					}
				}
			}
			return nil
		})
	},
}

// crdbInternalFeatureUsage exposes the telemetry counters.
var crdbInternalFeatureUsage = virtualSchemaTable{
	comment: "telemetry counters (RAM; local node only)",
	schema: `
CREATE TABLE crdb_internal.feature_usage (
  feature_name          STRING NOT NULL,
  usage_count           INT NOT NULL
)
`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for feature, count := range telemetry.GetFeatureCounts(telemetry.Raw, telemetry.ReadOnly) {
			if count == 0 {
				// Skip over empty counters to avoid polluting the output.
				continue
			}
			if err := addRow(
				tree.NewDString(feature),
				tree.NewDInt(tree.DInt(int64(count))),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalForwardDependenciesTable exposes the forward
// inter-descriptor dependencies.
//
// TODO(tbg): prefix with kv_.
var crdbInternalForwardDependenciesTable = virtualSchemaTable{
	comment: "forward inter-descriptor dependencies starting from tables accessible by current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.forward_dependencies (
  descriptor_id         INT,
  descriptor_name       STRING NOT NULL,
  index_id              INT,
  dependedonby_id       INT NOT NULL,
  dependedonby_type     STRING NOT NULL,
  dependedonby_index_id INT,
  dependedonby_name     STRING,
  dependedonby_details  STRING
)
`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		fkDep := tree.NewDString("fk")
		viewDep := tree.NewDString("view")
		sequenceDep := tree.NewDString("sequence")
		opts := forEachTableDescOptions{
			virtualOpts: hideVirtual, /* virtual tables have no backward/forward dependencies*/
			allowAdding: true}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				table := descCtx.table
				tableID := tree.NewDInt(tree.DInt(table.GetID()))
				tableName := tree.NewDString(table.GetName())
				for _, fk := range table.InboundForeignKeys() {
					if err := addRow(
						tableID, tableName,
						tree.DNull,
						tree.NewDInt(tree.DInt(fk.GetOriginTableID())),
						fkDep,
						tree.DNull,
						tree.DNull,
						tree.DNull,
					); err != nil {
						return err
					}
				}

				reportDependedOnBy := func(
					dep *descpb.TableDescriptor_Reference, depTypeString *tree.DString,
				) error {
					return addRow(
						tableID, tableName,
						tree.DNull,
						tree.NewDInt(tree.DInt(dep.ID)),
						depTypeString,
						tree.NewDInt(tree.DInt(dep.IndexID)),
						tree.DNull,
						tree.NewDString(fmt.Sprintf("Columns: %v", dep.ColumnIDs)),
					)
				}

				if table.IsTable() || table.IsView() {
					return table.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
						return reportDependedOnBy(dep, viewDep)
					})
				} else if table.IsSequence() {
					return table.ForeachDependedOnBy(func(dep *descpb.TableDescriptor_Reference) error {
						return reportDependedOnBy(dep, sequenceDep)
					})
				}
				return nil
			})
	},
}

func listColumnNames(cols colinfo.ResultColumns) string {
	var buf strings.Builder
	comma := ""
	for _, c := range cols {
		buf.WriteString(comma)
		buf.WriteString(c.Name)
		comma = ","
	}
	return buf.String()
}

// crdbInternalRangesView exposes system ranges.
var crdbInternalRangesView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.ranges AS SELECT ` +
		// We'd like to use `*` here but it's not supported yet by our
		// dialect.
		listColumnNames(colinfo.RangesNoLeases) + `,` +
		// Extra columns that are "expensive to compute".
		colinfo.RangesExtraRenders +
		`FROM crdb_internal.ranges_no_leases`,
	resultColumns: colinfo.Ranges,
	comment:       "ranges is a view which queries ranges_no_leases for system ranges",
}

// descriptorsByType is a utility function that iterates through a slice of
// descriptors and, using the provided privilege checker function, categorizes
// the privileged descriptors for easy lookup of their human-readable names by IDs.
// As such, it returns maps of privileged descriptor IDs to names for database
// descriptors, table descriptors, schema descriptors, as well as indexes (per
// table). It also returns maps of table descriptor IDs to the parent schema ID
// and the parent (database) descriptor ID, to aid in necessary lookups.
func descriptorsByType(
	descs []catalog.Descriptor, privCheckerFunc func(desc catalog.Descriptor) (bool, error),
) (
	hasPermission bool,
	dbNames map[uint32]string,
	tableNames map[uint32]string,
	schemaNames map[uint32]string,
	indexNames map[uint32]map[uint32]string,
	schemaParents map[uint32]uint32,
	parents map[uint32]uint32,
	retErr error,
) {
	// TODO(knz): maybe this could use internalLookupCtx.
	dbNames = make(map[uint32]string)
	tableNames = make(map[uint32]string)
	schemaNames = make(map[uint32]string)
	indexNames = make(map[uint32]map[uint32]string)
	schemaParents = make(map[uint32]uint32)
	parents = make(map[uint32]uint32)
	hasPermission = false
	for _, desc := range descs {
		id := uint32(desc.GetID())
		if ok, err := privCheckerFunc(desc); err != nil {
			retErr = err
			return
		} else if !ok {
			continue
		}
		hasPermission = true
		switch desc := desc.(type) {
		case catalog.TableDescriptor:
			parents[id] = uint32(desc.GetParentID())
			schemaParents[id] = uint32(desc.GetParentSchemaID())
			tableNames[id] = desc.GetName()
			indexNames[id] = make(map[uint32]string)
			for _, idx := range desc.PublicNonPrimaryIndexes() {
				indexNames[id][uint32(idx.GetID())] = idx.GetName()
			}
		case catalog.DatabaseDescriptor:
			dbNames[id] = desc.GetName()
		case catalog.SchemaDescriptor:
			schemaNames[id] = desc.GetName()
		}
	}

	return hasPermission, dbNames, tableNames, schemaNames, indexNames, schemaParents, parents, nil
}

// lookupNamesByKey is a utility function that, given a key, utilizes the maps
// of descriptor IDs to names (and parents) returned by the descriptorsByType
// function to determine the table ID, database name, schema name, table name,
// and index name that the key belongs to.
func lookupNamesByKey(
	p *planner,
	key roachpb.Key,
	dbNames, tableNames, schemaNames map[uint32]string,
	indexNames map[uint32]map[uint32]string,
	schemaParents, parents map[uint32]uint32,
) (tableID uint32, dbName string, schemaName string, tableName string, indexName string) {
	var err error
	if _, tableID, err = p.ExecCfg().Codec.DecodeTablePrefix(key); err == nil {
		schemaParent := schemaParents[tableID]
		if schemaParent != 0 {
			schemaName = schemaNames[schemaParent]
		} else {
			// This case shouldn't happen - all schema ids should be available in the
			// schemaParents map. If it's not, just assume the name of the schema
			// is public to avoid problems.
			schemaName = string(catconstants.PublicSchemaName)
		}
		parent := parents[tableID]
		if parent != 0 {
			tableName = tableNames[tableID]
			dbName = dbNames[parent]
			if _, _, idxID, err := p.ExecCfg().Codec.DecodeIndexPrefix(key); err == nil {
				indexName = indexNames[tableID][idxID]
			}
		} else {
			dbName = dbNames[tableID]
		}
	}

	return tableID, dbName, schemaName, tableName, indexName
}

// crdbInternalRangesNoLeasesTable exposes all ranges in the system without the
// `lease_holder` information.
//
// TODO(tbg): prefix with kv_.
var crdbInternalRangesNoLeasesTable = virtualSchemaTable{
	comment: `range metadata without leaseholder details (KV join; expensive!)`,
	// NB 1: The `replicas` column is the union of `voting_replicas` and
	// `non_voting_replicas` and does not include `learner_replicas`.
	// NB 2: All the values in the `*replicas` columns correspond to store IDs.
	schema: `
CREATE TABLE crdb_internal.ranges_no_leases (
  range_id             INT NOT NULL,
  start_key            BYTES NOT NULL,
  start_pretty         STRING NOT NULL,
  end_key              BYTES NOT NULL,
  end_pretty           STRING NOT NULL,
  replicas             INT[] NOT NULL,
  replica_localities   STRING[] NOT NULL,
  voting_replicas      INT[] NOT NULL,
  non_voting_replicas  INT[] NOT NULL,
  learner_replicas     INT[] NOT NULL,
  split_enforced_until TIMESTAMP
)
`,
	resultColumns: colinfo.RangesNoLeases,
	generator: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, _ *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		hasAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return nil, nil, err
		}
		viewActOrViewActRedact, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return nil, nil, err
		}
		var descs catalog.Descriptors
		// Admin or viewActivity roles have access to all information, so
		// don't bother fetching all the descriptors unecessarily.
		if !hasAdmin && !viewActOrViewActRedact {
			all, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
			if err != nil {
				return nil, nil, err
			}
			descs = all.OrderedDescriptors()
		}

		hasPermission := hasAdmin || viewActOrViewActRedact
		for _, desc := range descs {
			if ok, err := p.HasPrivilege(ctx, desc, privilege.ZONECONFIG, p.User()); err != nil {
				return nil, nil, err
			} else if ok {
				hasPermission = true
				break
			}
		}
		// if the user has no VIEWACTIVITY or VIEWACTIVITYREDACTED or ZONECONFIG privilege on any table/schema/database
		if !hasPermission {
			return nil, nil, pgerror.Newf(pgcode.InsufficientPrivilege, "only users with the VIEWACTIVITY or VIEWACTIVITYREDACTED or ZONECONFIG privilege or the admin role can read crdb_internal.ranges_no_leases")
		}

		execCfg := p.ExecCfg()
		rangeDescIterator, err := execCfg.RangeDescIteratorFactory.NewIterator(ctx, execCfg.Codec.TenantSpan())
		if err != nil {
			return nil, nil, err
		}

		return func() (tree.Datums, error) {
			if !rangeDescIterator.Valid() {
				return nil, nil
			}

			rangeDesc := rangeDescIterator.CurRangeDescriptor()

			rangeDescIterator.Next()

			replicas := rangeDesc.Replicas()
			votersAndNonVoters := append([]roachpb.ReplicaDescriptor(nil),
				replicas.VoterAndNonVoterDescriptors()...)
			var learnerReplicaStoreIDs []int
			for _, rd := range replicas.LearnerDescriptors() {
				learnerReplicaStoreIDs = append(learnerReplicaStoreIDs, int(rd.StoreID))
			}
			sort.Slice(votersAndNonVoters, func(i, j int) bool {
				return votersAndNonVoters[i].StoreID < votersAndNonVoters[j].StoreID
			})
			sort.Ints(learnerReplicaStoreIDs)
			votersAndNonVotersArr := tree.NewDArray(types.Int)
			for _, replica := range votersAndNonVoters {
				if err := votersAndNonVotersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return nil, err
				}
			}
			votersArr := tree.NewDArray(types.Int)
			for _, replica := range replicas.VoterDescriptors() {
				if err := votersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return nil, err
				}
			}
			nonVotersArr := tree.NewDArray(types.Int)
			for _, replica := range replicas.NonVoterDescriptors() {
				if err := nonVotersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return nil, err
				}
			}
			learnersArr := tree.NewDArray(types.Int)
			for _, replica := range learnerReplicaStoreIDs {
				if err := learnersArr.Append(tree.NewDInt(tree.DInt(replica))); err != nil {
					return nil, err
				}
			}

			replicaLocalityArr := tree.NewDArray(types.String)
			for _, replica := range votersAndNonVoters {
				// The table should still be rendered even if node locality is unavailable,
				// so use NULL if nodeDesc is not found.
				// See https://github.com/cockroachdb/cockroach/issues/92915.
				replicaLocalityDatum := tree.DNull
				nodeDesc, err := p.ExecCfg().NodeDescs.GetNodeDescriptor(replica.NodeID)
				if err != nil {
					if !errors.HasType(err, &kvpb.DescNotFoundError{}) {
						return nil, err
					}
				} else {
					replicaLocalityDatum = tree.NewDString(nodeDesc.Locality.String())
				}
				if err := replicaLocalityArr.Append(replicaLocalityDatum); err != nil {
					return nil, err
				}
			}

			splitEnforcedUntil := tree.DNull
			if !rangeDesc.StickyBit.IsEmpty() {
				splitEnforcedUntil = eval.TimestampToInexactDTimestamp(rangeDesc.StickyBit)
			}

			return tree.Datums{
				tree.NewDInt(tree.DInt(rangeDesc.RangeID)),
				tree.NewDBytes(tree.DBytes(rangeDesc.StartKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, rangeDesc.StartKey.AsRawKey())),
				tree.NewDBytes(tree.DBytes(rangeDesc.EndKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, rangeDesc.EndKey.AsRawKey())),
				votersAndNonVotersArr,
				replicaLocalityArr,
				votersArr,
				nonVotersArr,
				learnersArr,
				splitEnforcedUntil,
			}, nil
		}, nil, nil
	},
}

// getAllNames returns a map from ID to namespaceKey for every entry in
// system.namespace.
func (p *planner) getAllNames(ctx context.Context) (map[descpb.ID]catalog.NameKey, error) {
	return getAllNames(ctx, p.InternalSQLTxn())
}

// TestingGetAllNames is a wrapper for getAllNames.
func TestingGetAllNames(ctx context.Context, txn isql.Txn) (map[descpb.ID]catalog.NameKey, error) {
	return getAllNames(ctx, txn)
}

// getAllNames is the testable implementation of getAllNames.
// It is public so that it can be tested outside the sql package.
func getAllNames(ctx context.Context, txn isql.Txn) (map[descpb.ID]catalog.NameKey, error) {
	namespace := map[descpb.ID]catalog.NameKey{}
	it, err := txn.QueryIteratorEx(
		ctx, "get-all-names", txn.KV(),
		sessiondata.NodeUserSessionDataOverride,
		`SELECT id, "parentID", "parentSchemaID", name FROM system.namespace`,
	)
	if err != nil {
		return nil, err
	}
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		r := it.Cur()
		id, parentID, parentSchemaID, name := tree.MustBeDInt(r[0]), tree.MustBeDInt(r[1]), tree.MustBeDInt(r[2]), tree.MustBeDString(r[3])
		namespace[descpb.ID(id)] = descpb.NameInfo{
			ParentID:       descpb.ID(parentID),
			ParentSchemaID: descpb.ID(parentSchemaID),
			Name:           string(name),
		}
	}
	if err != nil {
		return nil, err
	}

	return namespace, nil
}

// crdbInternalZonesTable decodes and exposes the zone configs in the
// system.zones table.
//
// TODO(tbg): prefix with kv_.
var crdbInternalZonesTable = virtualSchemaTable{
	comment: "decoded zone configurations from system.zones (KV scan)",
	schema: `
CREATE TABLE crdb_internal.zones (
  zone_id              INT NOT NULL,
  subzone_id           INT NOT NULL,
  target               STRING,
  range_name           STRING,
  database_name        STRING,
  schema_name          STRING,
  table_name           STRING,
  index_name           STRING,
  partition_name       STRING,
  raw_config_yaml      STRING NOT NULL,
  raw_config_sql       STRING, -- this column can be NULL if there is no specifier syntax
                               -- possible (e.g. the object was deleted).
	raw_config_protobuf  BYTES NOT NULL,
	full_config_yaml     STRING NOT NULL,
	full_config_sql      STRING
)
`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		namespace, err := p.getAllNames(ctx)
		if err != nil {
			return err
		}
		resolveID := func(id uint32) (parentID, parentSchemaID uint32, name string, err error) {
			// TODO(richardjcai): Remove logic for keys.PublicSchemaID in 22.2.
			if id == keys.PublicSchemaID {
				return 0, 0, string(catconstants.PublicSchemaName), nil
			}
			if entry, ok := namespace[descpb.ID(id)]; ok {
				return uint32(entry.GetParentID()), uint32(entry.GetParentSchemaID()), entry.GetName(), nil
			}
			return 0, 0, "", errors.AssertionFailedf(
				"object with ID %d does not exist", errors.Safe(id))
		}

		// For some reason, if we use the iterator API here, "concurrent txn use
		// detected" error might occur, so we buffer up all zones first.
		rows, err := p.InternalSQLTxn().QueryBufferedEx(
			ctx, "crdb-internal-zones-table", p.txn, sessiondata.NodeUserSessionDataOverride,
			`SELECT id, config FROM system.zones`,
		)
		if err != nil {
			return err
		}
		values := make(tree.Datums, len(showZoneConfigColumns))
		for _, r := range rows {
			id := uint32(tree.MustBeDInt(r[0]))

			var zoneSpecifier *tree.ZoneSpecifier
			zs, err := zonepb.ZoneSpecifierFromID(id, resolveID)
			if err != nil {
				// We can have valid zoneSpecifiers whose table/database has been
				// deleted because zoneSpecifiers are collected asynchronously.
				// In this case, just don't show the zoneSpecifier in the
				// output of the table.
				continue
			}
			zoneSpecifier = &zs

			configBytes := []byte(*r[1].(*tree.DBytes))
			var configProto zonepb.ZoneConfig
			if err := protoutil.Unmarshal(configBytes, &configProto); err != nil {
				return err
			}
			subzones := configProto.Subzones

			// Inherit full information about this zone.
			fullZone := configProto
			zcHelper := descs.AsZoneConfigHydrationHelper(p.Descriptors())
			if err := completeZoneConfig(
				ctx, &fullZone, p.Txn(), zcHelper, descpb.ID(tree.MustBeDInt(r[0])),
			); err != nil {
				return err
			}

			var table catalog.TableDescriptor
			if zs.Database != "" {
				database, err := p.Descriptors().ByIDWithoutLeased(p.txn).Get().Database(ctx, descpb.ID(id))
				if err != nil {
					return err
				}
				if ok, err := p.HasAnyPrivilege(ctx, database); err != nil {
					return err
				} else if !ok {
					continue
				}
			} else if zoneSpecifier.TableOrIndex.Table.ObjectName != "" {
				tableEntry, err := p.Descriptors().ByIDWithoutLeased(p.txn).Get().Table(ctx, descpb.ID(id))
				if err != nil {
					return err
				}
				if ok, err := p.HasAnyPrivilege(ctx, tableEntry); err != nil {
					return err
				} else if !ok {
					continue
				}
				table = tableEntry
			}

			// Write down information about the zone in the table.
			// TODO (rohany): We would like to just display information about these
			//  subzone placeholders, but there are a few tests that depend on this
			//  behavior, so leave it in for now.
			if !configProto.IsSubzonePlaceholder() {
				// Ensure subzones don't infect the value of the config_proto column.
				configProto.Subzones = nil
				configProto.SubzoneSpans = nil

				if err := generateZoneConfigIntrospectionValues(
					values,
					r[0],
					tree.NewDInt(tree.DInt(0)),
					zoneSpecifier,
					&configProto,
					&fullZone,
				); err != nil {
					return err
				}

				if err := addRow(values...); err != nil {
					return err
				}
			}

			if len(subzones) > 0 {
				if table == nil {
					return errors.AssertionFailedf(
						"object id %d with #subzones %d is not a table",
						id,
						len(subzones),
					)
				}

				for i, s := range subzones {
					index := catalog.FindActiveIndex(table, func(idx catalog.Index) bool {
						return idx.GetID() == descpb.IndexID(s.IndexID)
					})
					if index == nil {
						// If we can't find an active index that corresponds to this index
						// ID then continue, as the index is being dropped, or is already
						// dropped and in the MVCC GC queue.
						continue
					}

					zs := zs
					zs.TableOrIndex.Index = tree.UnrestrictedName(index.GetName())
					zs.Partition = tree.Name(s.PartitionName)
					zoneSpecifier = &zs

					// Generate information about full / inherited constraints.
					// There are two cases -- the subzone we are looking at refers
					// to an index, or to a partition.
					subZoneConfig := s.Config

					// In this case, we have an index. Inherit from the parent zone.
					if s.PartitionName == "" {
						subZoneConfig.InheritFromParent(&fullZone)
					} else {
						// We have a partition. Get the parent index partition from the zone and
						// have it inherit constraints.
						if indexSubzone := fullZone.GetSubzone(uint32(index.GetID()), ""); indexSubzone != nil {
							subZoneConfig.InheritFromParent(&indexSubzone.Config)
						}
						// Inherit remaining fields from the full parent zone.
						subZoneConfig.InheritFromParent(&fullZone)
					}

					if err := generateZoneConfigIntrospectionValues(
						values,
						r[0],
						tree.NewDInt(tree.DInt(i+1)),
						zoneSpecifier,
						&s.Config,
						&subZoneConfig,
					); err != nil {
						return err
					}

					if err := addRow(values...); err != nil {
						return err
					}
				}
			}
		}
		return nil
	},
}

func getAllNodeDescriptors(p *planner) ([]roachpb.NodeDescriptor, error) {
	g, err := p.ExecCfg().Gossip.OptionalErr(47899)
	if err != nil {
		return nil, err
	}
	var descriptors []roachpb.NodeDescriptor
	if err := g.IterateInfos(gossip.KeyNodeDescPrefix, func(key string, i gossip.Info) error {
		bytes, err := i.Value.GetBytes()
		if err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to extract bytes for key %q", key)
		}

		var d roachpb.NodeDescriptor
		if err := protoutil.Unmarshal(bytes, &d); err != nil {
			return errors.NewAssertionErrorWithWrappedErrf(err,
				"failed to parse value for key %q", key)
		}

		// Don't use node descriptors with NodeID 0, because that's meant to
		// indicate that the node has been removed from the cluster.
		if d.NodeID != 0 {
			descriptors = append(descriptors, d)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return descriptors, nil
}

// crdbInternalGossipNodesTable exposes local information about the cluster nodes.
var crdbInternalGossipNodesTable = virtualSchemaTable{
	comment: "locally known gossiped node details (RAM; local node only)",
	schema: `
CREATE TABLE crdb_internal.gossip_nodes (
  node_id               INT NOT NULL,
  network               STRING NOT NULL,
  address               STRING NOT NULL,
  advertise_address     STRING NOT NULL,
  sql_network           STRING NOT NULL,
  sql_address           STRING NOT NULL,
  advertise_sql_address STRING NOT NULL,
  attrs                 JSON NOT NULL,
  locality              STRING NOT NULL,
  cluster_name          STRING NOT NULL,
  server_version        STRING NOT NULL,
  build_tag             STRING NOT NULL,
  started_at            TIMESTAMP NOT NULL,
  is_live               BOOL NOT NULL,
  ranges                INT NOT NULL,
  leases                INT NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}

		g, err := p.ExecCfg().Gossip.OptionalErr(47899)
		if err != nil {
			return err
		}

		descriptors, err := getAllNodeDescriptors(p)
		if err != nil {
			return err
		}

		alive := make(map[roachpb.NodeID]tree.DBool)
		now := timeutil.Now()
		for _, d := range descriptors {
			var gossipLiveness livenesspb.Liveness
			if err := g.GetInfoProto(gossip.MakeNodeLivenessKey(d.NodeID), &gossipLiveness); err == nil {
				if now.Before(gossipLiveness.Expiration.ToTimestamp().GoTime()) {
					// TODO(baptist): This isn't the right way to check livenesses.
					alive[d.NodeID] = true
				}
			}
		}

		sort.Slice(descriptors, func(i, j int) bool {
			return descriptors[i].NodeID < descriptors[j].NodeID
		})

		type nodeStats struct {
			ranges int32
			leases int32
		}

		stats := make(map[roachpb.NodeID]nodeStats)
		if err := g.IterateInfos(gossip.KeyStoreDescPrefix, func(key string, i gossip.Info) error {
			bytes, err := i.Value.GetBytes()
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to extract bytes for key %q", key)
			}

			var desc roachpb.StoreDescriptor
			if err := protoutil.Unmarshal(bytes, &desc); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to parse value for key %q", key)
			}

			s := stats[desc.Node.NodeID]
			s.ranges += desc.Capacity.RangeCount
			s.leases += desc.Capacity.LeaseCount
			stats[desc.Node.NodeID] = s
			return nil
		}); err != nil {
			return err
		}

		for _, d := range descriptors {
			attrs := json.NewArrayBuilder(len(d.Attrs.Attrs))
			for _, a := range d.Attrs.Attrs {
				attrs.Add(json.FromString(a))
			}

			listenAddrRPC := d.Address
			listenAddrSQL := d.CheckedSQLAddress()

			advAddrRPC, _, err := g.GetNodeIDAddress(d.NodeID)
			if err != nil {
				return err
			}
			advAddrSQL, _, err := g.GetNodeIDSQLAddress(d.NodeID)
			if err != nil {
				return err
			}

			startTSDatum, err := tree.MakeDTimestamp(timeutil.Unix(0, d.StartedAt), time.Microsecond)
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(d.NodeID)),
				tree.NewDString(listenAddrRPC.NetworkField),
				tree.NewDString(listenAddrRPC.AddressField),
				tree.NewDString(advAddrRPC.String()),
				tree.NewDString(listenAddrSQL.NetworkField),
				tree.NewDString(listenAddrSQL.AddressField),
				tree.NewDString(advAddrSQL.String()),
				tree.NewDJSON(attrs.Build()),
				tree.NewDString(d.Locality.String()),
				tree.NewDString(d.ClusterName),
				tree.NewDString(d.ServerVersion.String()),
				tree.NewDString(d.BuildTag),
				startTSDatum,
				tree.MakeDBool(alive[d.NodeID]),
				tree.NewDInt(tree.DInt(stats[d.NodeID].ranges)),
				tree.NewDInt(tree.DInt(stats[d.NodeID].leases)),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalKVNodeLivenessTable exposes local information about the nodes'
// liveness, reading directly from KV. It's guaranteed to be up-to-date.
var crdbInternalKVNodeLivenessTable = virtualSchemaTable{
	comment: "node liveness status, as seen by kv",
	schema: `
CREATE TABLE crdb_internal.kv_node_liveness (
  node_id          INT NOT NULL,
  epoch            INT NOT NULL,
  expiration       STRING NOT NULL,
  draining         BOOL NOT NULL,
  membership       STRING NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}

		nl, err := p.ExecCfg().NodeLiveness.OptionalErr(47900)
		if err != nil {
			return err
		}

		nodeVitality, err := nl.ScanNodeVitalityFromKV(ctx)
		if err != nil {
			return err
		}

		var livenesses []livenesspb.Liveness
		for _, v := range nodeVitality {
			// We want the generated liveness which will simulate the expiration if
			// the liveness record is not being updated.
			livenesses = append(livenesses, v.GenLiveness())
		}

		sort.Slice(livenesses, func(i, j int) bool {
			return livenesses[i].NodeID < livenesses[j].NodeID
		})

		for i := range livenesses {
			l := &livenesses[i]
			if err := addRow(
				tree.NewDInt(tree.DInt(l.NodeID)),
				tree.NewDInt(tree.DInt(l.Epoch)),
				tree.NewDString(l.Expiration.String()),
				tree.MakeDBool(tree.DBool(l.Draining)),
				tree.NewDString(l.Membership.String()),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalGossipLivenessTable exposes local information about the nodes'
// liveness. The data exposed in this table can be stale/incomplete because
// gossip doesn't provide guarantees around freshness or consistency.
//
// TODO(irfansharif): Remove this decommissioning field in v21.1. It's retained
// for compatibility with v20.1 binaries where the `cockroach node` cli
// processes make use of it.
var crdbInternalGossipLivenessTable = virtualSchemaTable{
	comment: "locally known gossiped node liveness (RAM; local node only)",
	schema: `
CREATE TABLE crdb_internal.gossip_liveness (
  node_id          INT NOT NULL,
  epoch            INT NOT NULL,
  expiration       STRING NOT NULL,
  draining         BOOL NOT NULL,
  decommissioning  BOOL NOT NULL,
  membership       STRING NOT NULL,
  updated_at       TIMESTAMP
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// ATTENTION: The contents of this table should only access gossip data
		// which is highly available. DO NOT CALL functions which require the
		// cluster to be healthy, such as NodesStatusServer.ListNodesInternal().

		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}

		g, err := p.ExecCfg().Gossip.OptionalErr(47899)
		if err != nil {
			return err
		}

		type nodeInfo struct {
			liveness  livenesspb.Liveness
			updatedAt int64
		}

		var nodes []nodeInfo
		if err := g.IterateInfos(gossip.KeyNodeLivenessPrefix, func(key string, i gossip.Info) error {
			bytes, err := i.Value.GetBytes()
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to extract bytes for key %q", key)
			}

			var l livenesspb.Liveness
			if err := protoutil.Unmarshal(bytes, &l); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to parse value for key %q", key)
			}
			nodes = append(nodes, nodeInfo{
				liveness:  l,
				updatedAt: i.OrigStamp,
			})
			return nil
		}); err != nil {
			return err
		}

		sort.Slice(nodes, func(i, j int) bool {
			return nodes[i].liveness.NodeID < nodes[j].liveness.NodeID
		})

		for i := range nodes {
			n := &nodes[i]
			l := &n.liveness
			updatedTSDatum, err := tree.MakeDTimestamp(timeutil.Unix(0, n.updatedAt), time.Microsecond)
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(l.NodeID)),
				tree.NewDInt(tree.DInt(l.Epoch)),
				tree.NewDString(l.Expiration.String()),
				tree.MakeDBool(tree.DBool(l.Draining)),
				tree.MakeDBool(tree.DBool(!l.Membership.Active())),
				tree.NewDString(l.Membership.String()),
				updatedTSDatum,
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalGossipAlertsTable exposes current health alerts in the cluster.
var crdbInternalGossipAlertsTable = virtualSchemaTable{
	comment: "locally known gossiped health alerts (RAM; local node only)",
	schema: `
CREATE TABLE crdb_internal.gossip_alerts (
  node_id         INT NOT NULL,
  store_id        INT NULL,        -- null for alerts not associated to a store
  category        STRING NOT NULL, -- type of alert, usually by subsystem
  description     STRING NOT NULL, -- name of the alert (depends on subsystem)
  value           FLOAT NOT NULL   -- value of the alert (depends on subsystem, can be NaN)
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}

		g, err := p.ExecCfg().Gossip.OptionalErr(47899)
		if err != nil {
			return err
		}

		type resultWithNodeID struct {
			roachpb.NodeID
			statuspb.HealthCheckResult
		}
		var results []resultWithNodeID
		if err := g.IterateInfos(gossip.KeyNodeHealthAlertPrefix, func(key string, i gossip.Info) error {
			bytes, err := i.Value.GetBytes()
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to extract bytes for key %q", key)
			}

			var d statuspb.HealthCheckResult
			if err := protoutil.Unmarshal(bytes, &d); err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to parse value for key %q", key)
			}
			nodeID, err := gossip.DecodeNodeDescKey(key, gossip.KeyNodeHealthAlertPrefix)
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to parse node ID from key %q", key)
			}
			results = append(results, resultWithNodeID{nodeID, d})
			return nil
		}); err != nil {
			return err
		}

		for _, result := range results {
			for _, alert := range result.Alerts {
				storeID := tree.DNull
				if alert.StoreID != 0 {
					storeID = tree.NewDInt(tree.DInt(alert.StoreID))
				}
				if err := addRow(
					tree.NewDInt(tree.DInt(result.NodeID)),
					storeID,
					tree.NewDString(strings.ToLower(alert.Category.String())),
					tree.NewDString(alert.Description),
					tree.NewDFloat(tree.DFloat(alert.Value)),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

// crdbInternalGossipNetwork exposes the local view of the gossip network (i.e
// the gossip client connections from source_id node to target_id node).
var crdbInternalGossipNetworkTable = virtualSchemaTable{
	comment: "locally known edges in the gossip network (RAM; local node only)",
	schema: `
CREATE TABLE crdb_internal.gossip_network (
  source_id       INT NOT NULL,    -- source node of a gossip connection
  target_id       INT NOT NULL     -- target node of a gossip connection
)
`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		p.BufferClientNotice(ctx, pgnotice.Newf("This table is no longer supported/populated, and will be removed in a future version."))
		return nil
	},
}

// addPartitioningRows adds the rows in crdb_internal.partitions for each partition.
// None of the arguments can be nil, and it is used recursively when a list partition
// has subpartitions. In that case, the colOffset argument is incremented to represent
// how many columns of the index have been partitioned already.
func addPartitioningRows(
	ctx context.Context,
	p *planner,
	database string,
	table catalog.TableDescriptor,
	index catalog.Index,
	partitioning catalog.Partitioning,
	parentName tree.Datum,
	colOffset int,
	addRow func(...tree.Datum) error,
) error {
	tableID := tree.NewDInt(tree.DInt(table.GetID()))
	indexID := tree.NewDInt(tree.DInt(index.GetID()))
	numColumns := tree.NewDInt(tree.DInt(partitioning.NumColumns()))

	var buf bytes.Buffer
	for i := uint32(colOffset); i < uint32(colOffset+partitioning.NumColumns()); i++ {
		if i != uint32(colOffset) {
			buf.WriteString(`, `)
		}
		buf.WriteString(index.GetKeyColumnName(int(i)))
	}
	colNames := tree.NewDString(buf.String())

	var datumAlloc tree.DatumAlloc

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	// This produces the list_value column.
	err := partitioning.ForEachList(func(name string, values [][]byte, subPartitioning catalog.Partitioning) error {
		var buf bytes.Buffer
		for j, values := range values {
			if j != 0 {
				buf.WriteString(`, `)
			}
			tuple, _, err := rowenc.DecodePartitionTuple(
				&datumAlloc, p.ExecCfg().Codec, table, index, partitioning, values, fakePrefixDatums,
			)
			if err != nil {
				return err
			}
			buf.WriteString(tuple.String())
		}

		partitionValue := tree.NewDString(buf.String())
		nameDString := tree.NewDString(name)

		// Figure out which zone and subzone this partition should correspond to.
		zoneID, zone, subzone, err := GetZoneConfigInTxn(
			ctx, p.txn, p.Descriptors(), table.GetID(), index, name, false /* getInheritedDefault */)
		if err != nil {
			return err
		}
		subzoneID := base.SubzoneID(0)
		if subzone != nil {
			for i, s := range zone.Subzones {
				if s.IndexID == subzone.IndexID && s.PartitionName == subzone.PartitionName {
					subzoneID = base.SubzoneIDFromIndex(i)
				}
			}
		}

		if err := addRow(
			tableID,
			indexID,
			parentName,
			nameDString,
			numColumns,
			colNames,
			partitionValue,
			tree.DNull, /* null value for partition range */
			tree.NewDInt(tree.DInt(zoneID)),
			tree.NewDInt(tree.DInt(subzoneID)),
		); err != nil {
			return err
		}
		return addPartitioningRows(ctx, p, database, table, index, subPartitioning, nameDString,
			colOffset+partitioning.NumColumns(), addRow)
	})
	if err != nil {
		return err
	}

	// This produces the range_value column.
	err = partitioning.ForEachRange(func(name string, from, to []byte) error {
		var buf bytes.Buffer
		fromTuple, _, err := rowenc.DecodePartitionTuple(
			&datumAlloc, p.ExecCfg().Codec, table, index, partitioning, from, fakePrefixDatums,
		)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := rowenc.DecodePartitionTuple(
			&datumAlloc, p.ExecCfg().Codec, table, index, partitioning, to, fakePrefixDatums,
		)
		if err != nil {
			return err
		}
		buf.WriteString(toTuple.String())
		partitionRange := tree.NewDString(buf.String())

		// Figure out which zone and subzone this partition should correspond to.
		zoneID, zone, subzone, err := GetZoneConfigInTxn(
			ctx, p.txn, p.Descriptors(), table.GetID(), index, name, false /* getInheritedDefault */)
		if err != nil {
			return err
		}
		subzoneID := base.SubzoneID(0)
		if subzone != nil {
			for i, s := range zone.Subzones {
				if s.IndexID == subzone.IndexID && s.PartitionName == subzone.PartitionName {
					subzoneID = base.SubzoneIDFromIndex(i)
				}
			}
		}

		return addRow(
			tableID,
			indexID,
			parentName,
			tree.NewDString(name),
			numColumns,
			colNames,
			tree.DNull, /* null value for partition list */
			partitionRange,
			tree.NewDInt(tree.DInt(zoneID)),
			tree.NewDInt(tree.DInt(subzoneID)),
		)
	})

	return err
}

// crdbInternalPartitionsTable decodes and exposes the partitions of each
// table.
//
// TODO(tbg): prefix with cluster_.
var crdbInternalPartitionsTable = virtualSchemaTable{
	comment: "defined partitions for all tables/indexes accessible by the current user in the current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.partitions (
	table_id    INT NOT NULL,
	index_id    INT NOT NULL,
	parent_name STRING,
	name        STRING NOT NULL,
	columns     INT NOT NULL,
	column_names STRING,
	list_value  STRING,
	range_value STRING,
	zone_id INT, -- references a zone id in the crdb_internal.zones table
	subzone_id INT -- references a subzone id in the crdb_internal.zones table
)
	`,
	generator: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		dbName := ""
		if dbContext != nil {
			dbName = dbContext.GetName()
		}
		worker := func(ctx context.Context, pusher rowPusher) error {
			opts := forEachTableDescOptions{virtualOpts: hideVirtual /* virtual tables have no partitions*/, allowAdding: true}
			return forEachTableDesc(ctx, p, dbContext, opts,
				func(ctx context.Context, descCtx tableDescContext) error {
					table := descCtx.table
					return catalog.ForEachIndex(table, catalog.IndexOpts{
						AddMutations: true,
					}, func(index catalog.Index) error {
						return addPartitioningRows(ctx, p, dbName, table, index, index.GetPartitioning(),
							tree.DNull /* parentName */, 0 /* colOffset */, pusher.pushRow)
					})
				})
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// crdbInternalRegionsTable exposes available regions in the cluster.
var crdbInternalRegionsTable = virtualSchemaTable{
	comment: "available regions for the cluster",
	schema: `
CREATE TABLE crdb_internal.regions (
	region STRING NOT NULL,
	zones STRING[] NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		resp, err := p.InternalSQLTxn().Regions().GetRegions(ctx)
		if err != nil {
			return err
		}
		for regionName, regionMeta := range resp.Regions {
			zones := tree.NewDArray(types.String)
			for _, zone := range regionMeta.Zones {
				if err := zones.Append(tree.NewDString(zone)); err != nil {
					return err
				}
			}
			if err := addRow(
				tree.NewDString(regionName),
				zones,
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalKVNodeStatusTable exposes information from the status server about the cluster nodes.
//
// TODO(tbg): s/kv_/cluster_/
var crdbInternalKVNodeStatusTable = virtualSchemaTable{
	comment: "node details across the entire cluster (cluster RPC; expensive!)",
	schema: `
CREATE TABLE crdb_internal.kv_node_status (
  node_id        INT NOT NULL,
  network        STRING NOT NULL,
  address        STRING NOT NULL,
  attrs          JSON NOT NULL,
  locality       STRING NOT NULL,
  server_version STRING NOT NULL,
  go_version     STRING NOT NULL,
  tag            STRING NOT NULL,
  time           STRING NOT NULL,
  revision       STRING NOT NULL,
  cgo_compiler   STRING NOT NULL,
  platform       STRING NOT NULL,
  distribution   STRING NOT NULL,
  type           STRING NOT NULL,
  dependencies   STRING NOT NULL,
  started_at     TIMESTAMP NOT NULL,
  updated_at     TIMESTAMP NOT NULL,
  metrics        JSON NOT NULL,
  args           JSON NOT NULL,
  env            JSON NOT NULL,
  activity       JSON NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}
		ss, err := p.extendedEvalCtx.NodesStatusServer.OptionalNodesStatusServer()
		if err != nil {
			return err
		}
		response, err := ss.ListNodesInternal(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return err
		}

		for _, n := range response.Nodes {
			attrs := json.NewArrayBuilder(len(n.Desc.Attrs.Attrs))
			for _, a := range n.Desc.Attrs.Attrs {
				attrs.Add(json.FromString(a))
			}

			var dependencies string
			if n.BuildInfo.Dependencies == nil {
				dependencies = ""
			} else {
				dependencies = *(n.BuildInfo.Dependencies)
			}

			metrics := json.NewObjectBuilder(len(n.Metrics))
			for k, v := range n.Metrics {
				metric, err := json.FromFloat64(v)
				if err != nil {
					return err
				}
				metrics.Add(k, metric)
			}

			args := json.NewArrayBuilder(len(n.Args))
			for _, a := range n.Args {
				args.Add(json.FromString(a))
			}

			env := json.NewArrayBuilder(len(n.Env))
			for _, v := range n.Env {
				env.Add(json.FromString(v))
			}

			activity := json.NewObjectBuilder(len(n.Activity))
			for nodeID, values := range n.Activity {
				b := json.NewObjectBuilder(3)
				b.Add("latency", json.FromInt64(values.Latency))
				activity.Add(nodeID.String(), b.Build())
			}

			startTSDatum, err := tree.MakeDTimestamp(timeutil.Unix(0, n.StartedAt), time.Microsecond)
			if err != nil {
				return err
			}
			endTSDatum, err := tree.MakeDTimestamp(timeutil.Unix(0, n.UpdatedAt), time.Microsecond)
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(n.Desc.NodeID)),
				tree.NewDString(n.Desc.Address.NetworkField),
				tree.NewDString(n.Desc.Address.AddressField),
				tree.NewDJSON(attrs.Build()),
				tree.NewDString(n.Desc.Locality.String()),
				tree.NewDString(n.Desc.ServerVersion.String()),
				tree.NewDString(n.BuildInfo.GoVersion),
				tree.NewDString(n.BuildInfo.Tag),
				tree.NewDString(n.BuildInfo.Time),
				tree.NewDString(n.BuildInfo.Revision),
				tree.NewDString(n.BuildInfo.CgoCompiler),
				tree.NewDString(n.BuildInfo.Platform),
				tree.NewDString(n.BuildInfo.Distribution),
				tree.NewDString(n.BuildInfo.Type),
				tree.NewDString(dependencies),
				startTSDatum,
				endTSDatum,
				tree.NewDJSON(metrics.Build()),
				tree.NewDJSON(args.Build()),
				tree.NewDJSON(env.Build()),
				tree.NewDJSON(activity.Build()),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalKVStoreStatusTable exposes information about the cluster stores.
//
// TODO(tbg): s/kv_/cluster_/
var crdbInternalKVStoreStatusTable = virtualSchemaTable{
	comment: "store details and status (cluster RPC; expensive!)",
	schema: `
CREATE TABLE crdb_internal.kv_store_status (
  node_id            INT NOT NULL,
  store_id           INT NOT NULL,
  attrs              JSON NOT NULL,
  capacity           INT NOT NULL,
  available          INT NOT NULL,
  used               INT NOT NULL,
  logical_bytes      INT NOT NULL,
  range_count        INT NOT NULL,
  lease_count        INT NOT NULL,
  writes_per_second  FLOAT NOT NULL,
  bytes_per_replica  JSON NOT NULL,
  writes_per_replica JSON NOT NULL,
  metrics            JSON NOT NULL,
  properties         JSON NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}
		ss, err := p.ExecCfg().NodesStatusServer.OptionalNodesStatusServer()
		if err != nil {
			return err
		}
		response, err := ss.ListNodesInternal(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return err
		}

		for _, n := range response.Nodes {
			for _, s := range n.StoreStatuses {
				attrs := json.NewArrayBuilder(len(s.Desc.Attrs.Attrs))
				for _, a := range s.Desc.Attrs.Attrs {
					attrs.Add(json.FromString(a))
				}

				metrics := json.NewObjectBuilder(len(s.Metrics))
				for k, v := range s.Metrics {
					metric, err := json.FromFloat64(v)
					if err != nil {
						return err
					}
					metrics.Add(k, metric)
				}

				properties := json.NewObjectBuilder(3)
				properties.Add("read_only", json.FromBool(s.Desc.Properties.ReadOnly))
				properties.Add("encrypted", json.FromBool(s.Desc.Properties.Encrypted))
				if fsprops := s.Desc.Properties.FileStoreProperties; fsprops != nil {
					jprops := json.NewObjectBuilder(5)
					jprops.Add("path", json.FromString(fsprops.Path))
					jprops.Add("fs_type", json.FromString(fsprops.FsType))
					jprops.Add("mount_point", json.FromString(fsprops.MountPoint))
					jprops.Add("mount_options", json.FromString(fsprops.MountOptions))
					jprops.Add("block_device", json.FromString(fsprops.BlockDevice))
					properties.Add("file_store_properties", jprops.Build())
				}

				percentilesToJSON := func(ps roachpb.Percentiles) (json.JSON, error) {
					b := json.NewObjectBuilder(5)
					v, err := json.FromFloat64(ps.P10)
					if err != nil {
						return nil, err
					}
					b.Add("P10", v)
					v, err = json.FromFloat64(ps.P25)
					if err != nil {
						return nil, err
					}
					b.Add("P25", v)
					v, err = json.FromFloat64(ps.P50)
					if err != nil {
						return nil, err
					}
					b.Add("P50", v)
					v, err = json.FromFloat64(ps.P75)
					if err != nil {
						return nil, err
					}
					b.Add("P75", v)
					v, err = json.FromFloat64(ps.P90)
					if err != nil {
						return nil, err
					}
					b.Add("P90", v)
					v, err = json.FromFloat64(ps.PMax)
					if err != nil {
						return nil, err
					}
					b.Add("PMax", v)
					return b.Build(), nil
				}

				bytesPerReplica, err := percentilesToJSON(s.Desc.Capacity.BytesPerReplica)
				if err != nil {
					return err
				}
				writesPerReplica, err := percentilesToJSON(s.Desc.Capacity.WritesPerReplica)
				if err != nil {
					return err
				}

				if err := addRow(
					tree.NewDInt(tree.DInt(s.Desc.Node.NodeID)),
					tree.NewDInt(tree.DInt(s.Desc.StoreID)),
					tree.NewDJSON(attrs.Build()),
					tree.NewDInt(tree.DInt(s.Desc.Capacity.Capacity)),
					tree.NewDInt(tree.DInt(s.Desc.Capacity.Available)),
					tree.NewDInt(tree.DInt(s.Desc.Capacity.Used)),
					tree.NewDInt(tree.DInt(s.Desc.Capacity.LogicalBytes)),
					tree.NewDInt(tree.DInt(s.Desc.Capacity.RangeCount)),
					tree.NewDInt(tree.DInt(s.Desc.Capacity.LeaseCount)),
					tree.NewDFloat(tree.DFloat(s.Desc.Capacity.WritesPerSecond)),
					tree.NewDJSON(bytesPerReplica),
					tree.NewDJSON(writesPerReplica),
					tree.NewDJSON(metrics.Build()),
					tree.NewDJSON(properties.Build()),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

var crdbInternalCatalogDescriptorTable = virtualSchemaTable{
	comment: `like system.descriptor but overlaid with in-txn in-memory changes and including virtual objects`,
	schema: `
CREATE TABLE crdb_internal.kv_catalog_descriptor (
  id            INT NOT NULL,
  descriptor    JSON NOT NULL
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		all, err := p.Descriptors().GetAll(ctx, p.Txn())
		if err != nil {
			return err
		}
		// Delegate privilege check to system table.
		{
			sysTable := all.LookupDescriptor(systemschema.DescriptorTable.GetID())
			if ok, err := p.HasPrivilege(ctx, sysTable, privilege.SELECT, p.User()); err != nil {
				return err
			} else if !ok {
				return nil
			}
		}
		return all.ForEachDescriptor(func(desc catalog.Descriptor) error {
			j, err := protoreflect.MessageToJSON(desc.DescriptorProto(), protoreflect.FmtFlags{})
			if err != nil {
				return err
			}
			return addRow(
				tree.NewDInt(tree.DInt(int64(desc.GetID()))),
				tree.NewDJSON(j),
			)
		})
	},
}

var crdbInternalCatalogZonesTable = virtualSchemaTable{
	comment: `like system.zones but overlaid with in-txn in-memory changes`,
	schema: `
CREATE TABLE crdb_internal.kv_catalog_zones (
  id        INT NOT NULL,
  config    JSON NOT NULL
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		all, err := p.Descriptors().GetAll(ctx, p.Txn())
		if err != nil {
			return err
		}
		// Delegate privilege check to system table.
		{
			sysTable := all.LookupDescriptor(systemschema.ZonesTable.GetID())
			if ok, err := p.HasPrivilege(ctx, sysTable, privilege.SELECT, p.User()); err != nil {
				return err
			} else if !ok {
				return nil
			}
		}
		// Loop over all zone configs.
		return all.ForEachZoneConfig(func(id descpb.ID, zc catalog.ZoneConfig) error {
			j, err := protoreflect.MessageToJSON(zc.ZoneConfigProto(), protoreflect.FmtFlags{})
			if err != nil {
				return err
			}
			return addRow(
				tree.NewDInt(tree.DInt(int64(id))),
				tree.NewDJSON(j),
			)
		})
	},
}

var crdbInternalCatalogNamespaceTable = virtualSchemaTable{
	comment: `like system.namespace but overlaid with in-txn in-memory changes`,
	schema: `
CREATE TABLE crdb_internal.kv_catalog_namespace (
  parent_id        INT NOT NULL,
  parent_schema_id INT NOT NULL,
  name             STRING NOT NULL,
  id               INT NOT NULL
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		all, err := p.Descriptors().GetAll(ctx, p.Txn())
		if err != nil {
			return err
		}
		// Delegate privilege check to system table.
		{
			sysTable := all.LookupDescriptor(systemschema.NamespaceTable.GetID())
			if ok, err := p.HasPrivilege(ctx, sysTable, privilege.SELECT, p.User()); err != nil {
				return err
			} else if !ok {
				return nil
			}
		}
		// Loop over all namespace entries.
		return all.ForEachNamespaceEntry(func(e nstree.NamespaceEntry) error {
			return addRow(
				tree.NewDInt(tree.DInt(int64(e.GetParentID()))),
				tree.NewDInt(tree.DInt(int64(e.GetParentSchemaID()))),
				tree.NewDString(e.GetName()),
				tree.NewDInt(tree.DInt(int64(e.GetID()))),
			)
		})
	},
}

var crdbInternalCatalogCommentsTable = virtualSchemaTable{
	comment: `like system.comments but overlaid with in-txn in-memory changes and including virtual objects`,
	schema:  vtable.CrdbInternalCatalogComments,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		h := makeOidHasher()
		all, err := p.Descriptors().GetAllComments(ctx, p.Txn(), dbContext)
		if err != nil {
			return err
		}
		if err := all.ForEachComment(func(key catalogkeys.CommentKey, cmt string) error {
			classOid := tree.DNull
			objOid := tree.DNull
			objSubID := tree.DZero
			switch key.CommentType {
			case catalogkeys.DatabaseCommentType:
				classOid = tree.NewDOid(catconstants.PgCatalogDatabaseTableID)
				objOid = tree.NewDOid(oid.Oid(key.ObjectID))

			case catalogkeys.SchemaCommentType:
				classOid = tree.NewDOid(catconstants.PgCatalogNamespaceTableID)
				objOid = tree.NewDOid(oid.Oid(key.ObjectID))

			case catalogkeys.TypeCommentType:
				classOid = tree.NewDOid(catconstants.PgCatalogTypeTableID)
				objOid = tree.NewDOid(oid.Oid(key.ObjectID))

			case catalogkeys.ColumnCommentType, catalogkeys.TableCommentType:
				classOid = tree.NewDOid(catconstants.PgCatalogClassTableID)
				objOid = tree.NewDOid(oid.Oid(key.ObjectID))
				objSubID = tree.NewDInt(tree.DInt(key.SubID))

			case catalogkeys.IndexCommentType:
				classOid = tree.NewDOid(catconstants.PgCatalogClassTableID)
				objOid = h.IndexOid(descpb.ID(key.ObjectID), descpb.IndexID(key.SubID))

			case catalogkeys.ConstraintCommentType:
				// We can use a leased descriptor here because we're only looking up
				// the constraint by ID, and that won't change during the lifetime of
				// the table.
				tableDesc, err := p.Descriptors().ByIDWithLeased(p.txn).Get().Table(ctx, descpb.ID(key.ObjectID))
				if err != nil {
					return err
				}
				c, err := catalog.MustFindConstraintByID(tableDesc, descpb.ConstraintID(key.SubID))
				if err != nil {
					// However, a leased descriptor can be stale, and may not include the
					// constraint if it was just added. So if we can't find it, we'll
					// try a non-leased descriptor.
					var innerErr error
					tableDesc, innerErr = p.Descriptors().ByIDWithoutLeased(p.txn).Get().Table(ctx, descpb.ID(key.ObjectID))
					if innerErr != nil {
						return errors.CombineErrors(innerErr, err)
					}
					c, innerErr = catalog.MustFindConstraintByID(tableDesc, descpb.ConstraintID(key.SubID))
					if innerErr != nil {
						return errors.CombineErrors(innerErr, err)
					}
				}
				classOid = tree.NewDOid(catconstants.PgCatalogConstraintTableID)
				objOid = getOIDFromConstraint(c, tableDesc.GetParentID(), tableDesc.GetParentSchemaID(), tableDesc)
			}

			return addRow(
				classOid,
				objOid,
				objSubID,
				tree.NewDString(cmt))
		}); err != nil {
			return err
		}
		return nil
	},
}

type marshaledJobMetadata struct {
	status                      *tree.DString
	payloadBytes, progressBytes *tree.DBytes
}

func (mj marshaledJobMetadata) size() (n int64) {
	return int64(8 + mj.status.Size() + mj.progressBytes.Size() + mj.payloadBytes.Size())
}

type marshaledJobMetadataMap map[jobspb.JobID]marshaledJobMetadata

// GetJobMetadata implements the jobs.JobMetadataGetter interface.
func (m marshaledJobMetadataMap) GetJobMetadata(
	jobID jobspb.JobID,
) (md *jobs.JobMetadata, err error) {
	ujm, found := m[jobID]
	if !found {
		return nil, errors.New("job not found")
	}
	md = &jobs.JobMetadata{ID: jobID}
	if ujm.status == nil {
		return nil, errors.New("missing status")
	}
	md.State = jobs.State(*ujm.status)
	md.Payload, err = jobs.UnmarshalPayload(ujm.payloadBytes)
	if err != nil {
		return nil, errors.Wrap(err, "corrupt payload bytes")
	}
	md.Progress, err = jobs.UnmarshalProgress(ujm.progressBytes)
	if err != nil {
		return nil, errors.Wrap(err, "corrupt progress bytes")
	}
	return md, nil
}

func getPayloadAndProgressFromJobsRecord(
	p *planner, job *jobs.Record,
) (progressBytes *tree.DBytes, payloadBytes *tree.DBytes, err error) {
	progressMarshalled, err := protoutil.Marshal(&jobspb.Progress{
		ModifiedMicros: p.txn.ReadTimestamp().GoTime().UnixMicro(),
		Details:        jobspb.WrapProgressDetails(job.Progress),
		StatusMessage:  string(job.StatusMessage),
	})
	if err != nil {
		return nil, nil, err
	}
	progressBytes = tree.NewDBytes(tree.DBytes(progressMarshalled))
	payloadMarshalled, err := protoutil.Marshal(&jobspb.Payload{
		Description:   job.Description,
		Statement:     job.Statements,
		UsernameProto: job.Username.EncodeProto(),
		Details:       jobspb.WrapPayloadDetails(job.Details),
		Noncancelable: job.NonCancelable,
	})
	if err != nil {
		return nil, nil, err
	}
	payloadBytes = tree.NewDBytes(tree.DBytes(payloadMarshalled))
	return progressBytes, payloadBytes, nil
}

func collectMarshaledJobMetadataMap(
	ctx context.Context, p *planner, acct *mon.BoundAccount, descs []catalog.Descriptor,
) (marshaledJobMetadataMap, error) {
	// Collect all job IDs referenced in descs.
	referencedJobIDs := map[jobspb.JobID]struct{}{}
	for _, desc := range descs {
		tbl, ok := desc.(catalog.TableDescriptor)
		if !ok {
			continue
		}
		for _, j := range tbl.GetMutationJobs() {
			referencedJobIDs[j.JobID] = struct{}{}
		}
	}
	if len(referencedJobIDs) == 0 {
		return nil, nil
	}
	// Build job map with referenced job IDs.
	m := make(marshaledJobMetadataMap)

	// Be careful to query against the empty database string, which avoids taking
	// a lease against the current database (in case it's currently unavailable).
	query := `
SELECT id, status, payload, progress FROM "".crdb_internal.system_jobs
`
	it, err := p.InternalSQLTxn().QueryIteratorEx(
		ctx, "crdb-internal-jobs-table", p.Txn(),
		sessiondata.NodeUserSessionDataOverride,
		query)
	if err != nil {
		return nil, err
	}
	for {
		ok, err := it.Next(ctx)
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		r := it.Cur()
		id, status, payloadBytes, progressBytes := r[0], r[1], r[2], r[3]
		jobID := jobspb.JobID(*id.(*tree.DInt))
		if _, isReferencedByDesc := referencedJobIDs[jobID]; !isReferencedByDesc {
			continue
		}
		mj := marshaledJobMetadata{
			status:        status.(*tree.DString),
			payloadBytes:  payloadBytes.(*tree.DBytes),
			progressBytes: progressBytes.(*tree.DBytes),
		}
		m[jobID] = mj
		if err := acct.Grow(ctx, mj.size()); err != nil {
			return nil, err
		}
	}
	if err := it.Close(); err != nil {
		return nil, err
	}
	if err := p.ExtendedEvalContext().jobs.forEachToCreate(func(record *jobs.Record) error {
		progressBytes, payloadBytes, err := getPayloadAndProgressFromJobsRecord(p, record)
		if err != nil {
			return err
		}
		mj := marshaledJobMetadata{
			status:        tree.NewDString(string(record.StatusMessage)),
			payloadBytes:  payloadBytes,
			progressBytes: progressBytes,
		}
		m[record.JobID] = mj
		return nil
	}); err != nil {
		return nil, err
	}
	return m, nil
}

var crdbInternalInvalidDescriptorsTable = virtualSchemaTable{
	comment: `virtual table to validate descriptors`,
	schema: `
CREATE TABLE crdb_internal.invalid_objects (
  id            INT,
  database_name STRING,
  schema_name   STRING,
  obj_name      STRING,
  error         STRING,
  error_redactable STRING NOT VISIBLE
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		// The internalLookupContext will only have descriptors in the current
		// database. To deal with this, we fall through.
		c, err := p.Descriptors().GetAllFromStorageUnvalidated(ctx, p.txn)
		if err != nil {
			return err
		}
		descs := c.OrderedDescriptors()
		// Collect all marshaled job metadata and account for its memory usage.
		acct := p.Mon().MakeBoundAccount()
		defer acct.Close(ctx)
		jmg, err := collectMarshaledJobMetadataMap(ctx, p, &acct, descs)
		if err != nil {
			return err
		}
		// Our validation logic is "cluster version aware" in that newly added
		// validation is silenced if the cluster is in a mixed version state (this
		// helps ensure tightened validation does not, all of a sudden, start
		// screaming about existing corruptions and blocking previously running user
		// workload). However, in such a mixed version state, we still want this
		// `invalid_objects` vtable to report any corruptions caught by the newly
		// added validation, so, we supply the "latest" cluster version to the
		// validation logic.
		version := clusterversion.ClusterVersion{Version: clusterversion.Latest.Version()}

		addValidationErrorRow := func(ne catalog.NameEntry, validationError error, lCtx tableLookupFn) error {
			if validationError == nil {
				return nil
			}
			dbName := fmt.Sprintf("[%d]", ne.GetParentID())
			scName := fmt.Sprintf("[%d]", ne.GetParentSchemaID())
			if n, ok := lCtx.dbNames[ne.GetParentID()]; ok {
				dbName = n
			}
			if n, err := lCtx.getSchemaNameByID(ne.GetParentSchemaID()); err == nil {
				scName = n
			}
			objName := ne.GetName()
			if ne.GetParentSchemaID() == descpb.InvalidID {
				scName = objName
				objName = ""
				if ne.GetParentID() == descpb.InvalidID {
					dbName = scName
					scName = ""
				}
			}
			return addRow(
				tree.NewDInt(tree.DInt(ne.GetID())),
				tree.NewDString(dbName),
				tree.NewDString(scName),
				tree.NewDString(objName),
				tree.NewDString(validationError.Error()),
				tree.NewDString(string(redact.Sprint(validationError))),
			)
		}

		doDescriptorValidationErrors := func(ctx context.Context, descriptor catalog.Descriptor, lCtx tableLookupFn) (err error) {
			if descriptor == nil {
				return nil
			}
			doError := func(validationError error) {
				if err != nil {
					return
				}
				err = addValidationErrorRow(descriptor, validationError, lCtx)
			}
			ve := c.ValidateWithRecover(ctx, version, descriptor)
			for _, validationError := range ve {
				doError(validationError)
			}
			roleExists := func(username username.SQLUsername) (bool, error) {
				if username.IsRootUser() ||
					username.IsAdminRole() ||
					username.IsNodeUser() ||
					username.IsPublicRole() {
					return true, nil
				}
				err := p.CheckRoleExists(ctx, username)
				if err != nil && pgerror.GetPGCode(err) == pgcode.UndefinedObject {
					err = nil
					return false, err
				} else if err != nil {
					return false, err
				}
				return true, nil
			}
			doError(catalog.ValidateRolesInDescriptor(descriptor, roleExists))
			if dbDesc, ok := descriptor.(catalog.DatabaseDescriptor); ok {
				doError(
					catalog.ValidateRolesInDefaultPrivilegeDescriptor(
						dbDesc.GetDefaultPrivilegeDescriptor(), roleExists))
			} else if schemaDesc, ok := descriptor.(catalog.SchemaDescriptor); ok {
				doError(
					catalog.ValidateRolesInDefaultPrivilegeDescriptor(
						schemaDesc.GetDefaultPrivilegeDescriptor(), roleExists))
			}
			jobs.ValidateJobReferencesInDescriptor(descriptor, jmg, doError)
			return err
		}

		// Validate table descriptors
		const allowAdding = true
		opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: allowAdding}
		if err := forEachTableDescFromDescriptors(
			ctx, p, dbContext, c, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				descriptor, lCtx := descCtx.table, descCtx.tableLookup
				return doDescriptorValidationErrors(ctx, descriptor, lCtx)
			}); err != nil {
			return err
		}

		// Validate type descriptors.
		if err := forEachTypeDescWithTableLookupInternalFromDescriptors(
			ctx, p, dbContext, allowAdding, c, func(
				ctx context.Context, _ catalog.DatabaseDescriptor, _ catalog.SchemaDescriptor, descriptor catalog.TypeDescriptor, lCtx tableLookupFn,
			) error {
				return doDescriptorValidationErrors(ctx, descriptor, lCtx)
			}); err != nil {
			return err
		}

		// Validate database & schema descriptors, and namespace entries.
		{
			lCtx := newInternalLookupCtx(c.OrderedDescriptors(), dbContext)

			if err := c.ForEachDescriptor(func(desc catalog.Descriptor) error {
				switch d := desc.(type) {
				case catalog.DatabaseDescriptor:
					if dbContext != nil && d.GetID() != dbContext.GetID() {
						return nil
					}
				case catalog.SchemaDescriptor:
					if dbContext != nil && d.GetParentID() != dbContext.GetID() {
						return nil
					}
				case catalog.FunctionDescriptor:
					if dbContext != nil && d.GetParentID() != dbContext.GetID() {
						return nil
					}
				default:
					return nil
				}
				return doDescriptorValidationErrors(ctx, desc, lCtx)
			}); err != nil {
				return err
			}

			return c.ForEachNamespaceEntry(func(ne nstree.NamespaceEntry) error {
				if dbContext != nil {
					if ne.GetParentID() == descpb.InvalidID {
						if ne.GetID() != dbContext.GetID() {
							return nil
						}
					} else if ne.GetParentID() != dbContext.GetID() {
						return nil
					}
				}
				return addValidationErrorRow(ne, c.ValidateNamespaceEntry(ne), lCtx)
			})
		}
	},
}

var crdbInternalClusterDatabasePrivilegesTable = virtualSchemaTable{
	comment: `virtual table with database privileges`,
	schema: `
CREATE TABLE crdb_internal.cluster_database_privileges (
	database_name   STRING NOT NULL,
	grantee         STRING NOT NULL,
	privilege_type  STRING NOT NULL,
	is_grantable    STRING,
	INDEX(database_name)
)`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			makeClusterDatabasePrivilegesFromDescriptor(ctx, p, addRow))
	},
	indexes: []virtualIndex{
		{populate: func(ctx context.Context, unwrappedConstraint tree.Datum, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
			if unwrappedConstraint == tree.DNull {
				return false, nil
			}
			dbName := string(tree.MustBeDString(unwrappedConstraint))
			if dbContext != nil && dbContext.GetName() != dbName {
				return false, nil
			}

			dbDesc, err := p.Descriptors().ByName(p.Txn()).MaybeGet().Database(ctx, dbName)
			if err != nil || dbDesc == nil {
				return false, err
			}
			hasPriv, err := userCanSeeDescriptor(
				ctx, p, dbDesc, nil /* parentDBDesc */, false /* allowAdding */, false /* includeDropped */)

			if err != nil || !hasPriv {
				return false, err
			}
			var called bool
			if err := makeClusterDatabasePrivilegesFromDescriptor(
				ctx, p, func(datum ...tree.Datum) error {
					called = true
					return addRow(datum...)
				},
			)(ctx, dbDesc); err != nil {
				return false, err
			}
			return called, nil
		}},
	},
}

func makeClusterDatabasePrivilegesFromDescriptor(
	ctx context.Context, p *planner, addRow func(...tree.Datum) error,
) func(context.Context, catalog.DatabaseDescriptor) error {
	return func(ctx context.Context, db catalog.DatabaseDescriptor) error {
		privs, err := db.GetPrivileges().Show(privilege.Database, true /* showImplicitOwnerPrivs */)
		if err != nil {
			return err
		}
		dbNameStr := tree.NewDString(db.GetName())
		// TODO(knz): This should filter for the current user, see
		// https://github.com/cockroachdb/cockroach/issues/35572
		for _, u := range privs {
			userNameStr := tree.NewDString(u.User.Normalized())
			for _, priv := range u.Privileges {
				// We use this function to check for the grant option so that the
				// object owner also gets is_grantable=true.
				isGrantable, err := p.CheckGrantOptionsForUser(
					ctx, db.GetPrivileges(), db, []privilege.Kind{priv.Kind}, u.User,
				)
				if err != nil {
					return err
				}
				if err := addRow(
					dbNameStr,   // database_name
					userNameStr, // grantee
					tree.NewDString(string(priv.Kind.DisplayName())), // privilege_type
					yesOrNoDatum(isGrantable),
				); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

var crdbInternalCrossDbReferences = virtualSchemaTable{
	comment: `virtual table with cross db references`,
	schema: `
CREATE TABLE crdb_internal.cross_db_references (
	object_database
		STRING NOT NULL,
	object_schema
		STRING NOT NULL,
	object_name
		STRING NOT NULL,
	referenced_object_database
		STRING NOT NULL,
	referenced_object_schema
		STRING NOT NULL,
	referenced_object_name
		STRING NOT NULL,
	cross_database_reference_description
		STRING NOT NULL
);`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: true}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				sc, table, lookupFn := descCtx.schema, descCtx.table, descCtx.tableLookup
				// For tables detect if foreign key references point at a different
				// database. Additionally, check if any of the columns have sequence
				// references to a different database.
				if table.IsTable() {
					objectDatabaseName := lookupFn.getDatabaseName(table)
					for _, fk := range table.OutboundForeignKeys() {
						referencedTable, err := lookupFn.getTableByID(fk.GetReferencedTableID())
						if err != nil {
							return err
						}
						if referencedTable.GetParentID() != table.GetParentID() {
							refSchemaName, err := lookupFn.getSchemaNameByID(referencedTable.GetParentSchemaID())
							if err != nil {
								return err
							}
							refDatabaseName := lookupFn.getDatabaseName(referencedTable)

							if err := addRow(tree.NewDString(objectDatabaseName),
								tree.NewDString(sc.GetName()),
								tree.NewDString(table.GetName()),
								tree.NewDString(refDatabaseName),
								tree.NewDString(refSchemaName),
								tree.NewDString(referencedTable.GetName()),
								tree.NewDString("table foreign key reference")); err != nil {
								return err
							}
						}
					}

					// Check for sequence dependencies
					for _, col := range table.PublicColumns() {
						for i := 0; i < col.NumUsesSequences(); i++ {
							sequenceID := col.GetUsesSequenceID(i)
							seqDesc, err := lookupFn.getTableByID(sequenceID)
							if err != nil {
								return err
							}
							if seqDesc.GetParentID() != table.GetParentID() {
								seqSchemaName, err := lookupFn.getSchemaNameByID(seqDesc.GetParentSchemaID())
								if err != nil {
									return err
								}
								refDatabaseName := lookupFn.getDatabaseName(seqDesc)
								if err := addRow(tree.NewDString(objectDatabaseName),
									tree.NewDString(sc.GetName()),
									tree.NewDString(table.GetName()),
									tree.NewDString(refDatabaseName),
									tree.NewDString(seqSchemaName),
									tree.NewDString(seqDesc.GetName()),
									tree.NewDString("table column refers to sequence")); err != nil {
									return err
								}
							}
						}
					}
				} else if table.IsView() {
					// For views check if we depend on tables in a different database.
					dependsOn := table.GetDependsOn()
					for _, dependency := range dependsOn {
						dependentTable, err := lookupFn.getTableByID(dependency)
						if err != nil {
							return err
						}
						if dependentTable.GetParentID() != table.GetParentID() {
							objectDatabaseName := lookupFn.getDatabaseName(table)
							refSchemaName, err := lookupFn.getSchemaNameByID(dependentTable.GetParentSchemaID())
							if err != nil {
								return err
							}
							refDatabaseName := lookupFn.getDatabaseName(dependentTable)

							if err := addRow(tree.NewDString(objectDatabaseName),
								tree.NewDString(sc.GetName()),
								tree.NewDString(table.GetName()),
								tree.NewDString(refDatabaseName),
								tree.NewDString(refSchemaName),
								tree.NewDString(dependentTable.GetName()),
								tree.NewDString("view references table")); err != nil {
								return err
							}
						}
					}

					// For views check if we depend on types in a different database.
					dependsOnTypes := table.GetDependsOnTypes()
					for _, dependency := range dependsOnTypes {
						dependentType, err := lookupFn.getTypeByID(dependency)
						if err != nil {
							return err
						}
						if dependentType.GetParentID() != table.GetParentID() {
							objectDatabaseName := lookupFn.getDatabaseName(table)
							refSchemaName, err := lookupFn.getSchemaNameByID(dependentType.GetParentSchemaID())
							if err != nil {
								return err
							}
							refDatabaseName := lookupFn.getDatabaseName(dependentType)

							if err := addRow(tree.NewDString(objectDatabaseName),
								tree.NewDString(sc.GetName()),
								tree.NewDString(table.GetName()),
								tree.NewDString(refDatabaseName),
								tree.NewDString(refSchemaName),
								tree.NewDString(dependentType.GetName()),
								tree.NewDString("view references type")); err != nil {
								return err
							}
						}
					}

				} else if table.IsSequence() {
					// For sequences check if the sequence is owned by
					// a different database.
					sequenceOpts := table.GetSequenceOpts()
					if sequenceOpts.SequenceOwner.OwnerTableID != descpb.InvalidID {
						ownerTable, err := lookupFn.getTableByID(sequenceOpts.SequenceOwner.OwnerTableID)
						if err != nil {
							return err
						}
						if ownerTable.GetParentID() != table.GetParentID() {
							objectDatabaseName := lookupFn.getDatabaseName(table)
							refSchemaName, err := lookupFn.getSchemaNameByID(ownerTable.GetParentSchemaID())
							if err != nil {
								return err
							}
							refDatabaseName := lookupFn.getDatabaseName(ownerTable)

							if err := addRow(tree.NewDString(objectDatabaseName),
								tree.NewDString(sc.GetName()),
								tree.NewDString(table.GetName()),
								tree.NewDString(refDatabaseName),
								tree.NewDString(refSchemaName),
								tree.NewDString(ownerTable.GetName()),
								tree.NewDString("sequences owning table")); err != nil {
								return err
							}
						}
					}
				}
				return nil
			})
	},
}

var crdbLostTableDescriptors = virtualSchemaTable{
	comment: `virtual table with table descriptors that still have data`,
	schema: `
CREATE TABLE crdb_internal.lost_descriptors_with_data (
	descID
		INTEGER NOT NULL
);`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		minID := descpb.ID(keys.MaxReservedDescID + 1)
		maxID, err := p.ExecCfg().DescIDGenerator.PeekNextUniqueDescID(ctx)
		if err != nil {
			return err
		}
		if err != nil {
			return err
		}
		if minID >= maxID {
			return nil
		}
		// Get all descriptors which will be used to determine
		// which ones are missing.
		c, err := p.Descriptors().GetAllFromStorageUnvalidated(ctx, p.txn)
		if err != nil {
			return err
		}
		// shouldCheck returns true iff we expect no data to exist with that
		// table ID prefix.
		shouldCheck := func(id descpb.ID) bool {
			return minID <= id && id < maxID && c.LookupDescriptor(id) == nil
		}
		// hasData returns true iff there exists at least one row with a prefix for
		// a table ID in [startID, endID[.
		hasData := func(startID, endID descpb.ID) (found bool, _ error) {
			startPrefix := p.extendedEvalCtx.Codec.TablePrefix(uint32(startID))
			endPrefix := p.extendedEvalCtx.Codec.TablePrefix(uint32(endID - 1)).PrefixEnd()
			b := p.Txn().NewBatch()
			b.Header.MaxSpanRequestKeys = 1
			scanRequest := kvpb.NewScan(startPrefix, endPrefix).(*kvpb.ScanRequest)
			scanRequest.ScanFormat = kvpb.BATCH_RESPONSE
			b.AddRawRequest(scanRequest)
			err = p.execCfg.DB.Run(ctx, b)
			if err != nil {
				return false, err
			}
			res := b.RawResponse().Responses[0].GetScan()
			return res.NumKeys > 0, nil
		}
		// Loop through all allocated, non-reserved descriptor IDs.
		for startID, endID := minID, minID; endID <= maxID; endID++ {
			// Identify spans to check via discontinuities in shouldCheck.
			if shouldCheck(endID) == shouldCheck(endID-1) {
				continue
			}
			// Handle span start.
			if shouldCheck(endID) && !shouldCheck(endID-1) {
				startID = endID
				continue
			}
			// Handle span end.
			// Check that the span [startID, endID[ is empty.
			if found, err := hasData(startID, endID); err != nil {
				return err
			} else if !found {
				// This is the expected outcome.
				continue
			}
			// If the span is unexpectedly not empty, refine the search by checking
			// each individual descriptor ID in the span for data.
			for id := startID; id < endID; id++ {
				if found, err := hasData(id, id+1); err != nil {
					return err
				} else if !found {
					continue
				}
				if err := addRow(tree.NewDInt(tree.DInt(id))); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

// crdbInternalRepairableCatalogCorruptions lists all corruptions in the
// catalog which can be automatically repaired. By type:
//   - 'descriptor' corruptions denote corruptions in the system.descriptor
//     table which can be repaired by applying
//     crdb_internal.unsafe_upsert_descriptor to the output of
//     crdb_internal.repaired_descriptor,
//   - 'namespace' corruptions denote corruptions in the system.namespace
//     table which can be repaired by removing the corresponding record with
//     crdb_internal.unsafe_delete_namespace_entry.
var crdbInternalRepairableCatalogCorruptions = virtualSchemaView{
	comment: "known corruptions in the catalog which can be repaired using builtin functions like " +
		"crdb_internal.repaired_descriptor",
	resultColumns: colinfo.ResultColumns{
		{Name: "parent_id", Typ: types.Int},
		{Name: "parent_schema_id", Typ: types.Int},
		{Name: "name", Typ: types.String},
		{Name: "id", Typ: types.Int},
		{Name: "corruption", Typ: types.String},
	},
	schema: `
CREATE VIEW crdb_internal.kv_repairable_catalog_corruptions (
	parent_id,
	parent_schema_id,
	name,
	id,
	corruption
) AS
	WITH
		data
			AS (
				SELECT
					ns."parentID" AS parent_id,
					ns."parentSchemaID" AS parent_schema_id,
					ns.name,
					COALESCE(ns.id, d.id) AS id,
					d.descriptor,
					crdb_internal.descriptor_with_post_deserialization_changes(d.descriptor)
						AS updated_descriptor,
					crdb_internal.repaired_descriptor(
						d.descriptor,
						(SELECT array_agg(id) AS desc_id_array FROM system.descriptor),
						(
							SELECT
								array_agg(id) AS job_id_array
							FROM
								system.jobs
							WHERE
								status NOT IN ('failed', 'succeeded', 'canceled', 'revert-failed')
						),
						( SELECT
							array_agg(username) as username_array FROM
							(SELECT username
							FROM system.users UNION
							SELECT 'public' as username UNION
							SELECT 'node' as username)
						)
					)
						AS repaired_descriptor
				FROM
					system.namespace AS ns FULL JOIN system.descriptor AS d ON ns.id = d.id
			),
		diag
			AS (
				SELECT
					*,
					CASE
					WHEN descriptor IS NULL AND id != 29 THEN 'namespace'
					WHEN updated_descriptor != repaired_descriptor THEN 'descriptor'
					ELSE NULL
					END
						AS corruption
				FROM
					data
			)
	SELECT
		parent_id, parent_schema_id, name, id, corruption
	FROM
		diag
	WHERE
		corruption IS NOT NULL
	ORDER BY
		parent_id, parent_schema_id, name, id
`,
}

var crdbInternalDefaultPrivilegesTable = virtualSchemaTable{
	comment: `virtual table with default privileges`,
	schema: `
CREATE TABLE crdb_internal.default_privileges (
	database_name   STRING NOT NULL,
	schema_name     STRING,
	role            STRING,
	for_all_roles   BOOL,
	object_type     STRING NOT NULL,
	grantee         STRING NOT NULL,
	privilege_type  STRING NOT NULL,
	is_grantable    BOOL
);`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {

		// Cache roles ahead of time to avoid role lookup inside loop.
		var roles []catpb.DefaultPrivilegesRole
		if err := forEachRoleAtCacheReadTS(ctx, p, func(ctx context.Context, userName username.SQLUsername, isRole bool, options roleOptions, settings tree.Datum) error {
			// Skip the internal node user, since it can't be modified and just adds noise.
			if userName.IsNodeUser() {
				return nil
			}
			roles = append(roles, catpb.DefaultPrivilegesRole{
				Role: userName,
			})
			return nil
		}); err != nil {
			return err
		}
		// Handle ForAllRoles outside forEachRole since it is a pseudo role.
		roles = append(roles, catpb.DefaultPrivilegesRole{
			ForAllRoles: true,
		})

		return forEachDatabaseDesc(ctx, p, nil /* all databases */, true, /* requiresPrivileges */
			func(ctx context.Context, databaseDesc catalog.DatabaseDescriptor) error {
				database := tree.NewDString(databaseDesc.GetName())
				addRowsForSchema := func(defaultPrivilegeDescriptor catalog.DefaultPrivilegeDescriptor, schema tree.Datum) error {
					for _, role := range roles {
						defaultPrivilegesForRole, found := defaultPrivilegeDescriptor.GetDefaultPrivilegesForRole(role)
						if !found {
							// If an entry is not found for the role, the role still has
							// the default set of default privileges.
							newDefaultPrivilegesForRole := catpb.InitDefaultPrivilegesForRole(role, defaultPrivilegeDescriptor.GetDefaultPrivilegeDescriptorType())
							defaultPrivilegesForRole = &newDefaultPrivilegesForRole
						}
						role := tree.DNull
						forAllRoles := tree.DBoolTrue
						if defaultPrivilegesForRole.IsExplicitRole() {
							role = tree.NewDString(defaultPrivilegesForRole.GetExplicitRole().UserProto.Decode().Normalized())
							forAllRoles = tree.DBoolFalse
						}

						for objectType, privs := range defaultPrivilegesForRole.DefaultPrivilegesPerObject {
							objectTypeDatum := tree.NewDString(objectType.String())
							privilegeObjectType := targetObjectToPrivilegeObject[objectType]
							for _, userPrivs := range privs.Users {
								grantee := tree.NewDString(userPrivs.User().Normalized())
								privList, err := privilege.ListFromBitField(userPrivs.Privileges, privilegeObjectType)
								if err != nil {
									return err
								}
								for _, priv := range privList {
									if err := addRow(
										database, // database_name
										// When the schema_name is NULL, that means the default
										// privileges are defined at the database level.
										schema,          // schema_name
										role,            // role
										forAllRoles,     // for_all_roles
										objectTypeDatum, // object_type
										grantee,         // grantee
										tree.NewDString(string(priv.DisplayName())),                         // privilege_type
										tree.MakeDBool(tree.DBool(priv.IsSetIn(userPrivs.WithGrantOption))), // is_grantable
									); err != nil {
										return err
									}
								}
							}
						}

						if schema == tree.DNull {
							for _, objectType := range privilege.GetTargetObjectTypes() {
								if catprivilege.GetRoleHasAllPrivilegesOnTargetObject(defaultPrivilegesForRole, objectType) {
									if err := addRow(
										database,                             // database_name
										schema,                               // schema_name
										role,                                 // role
										forAllRoles,                          // for_all_roles
										tree.NewDString(objectType.String()), // object_type
										role,                                 // grantee
										tree.NewDString(string(privilege.ALL.DisplayName())), // privilege_type
										tree.DBoolTrue, // is_grantable
									); err != nil {
										return err
									}
								}
							}
							if catprivilege.GetPublicHasUsageOnTypes(defaultPrivilegesForRole) {
								if err := addRow(
									database,    // database_name
									schema,      // schema_name
									role,        // role
									forAllRoles, // for_all_roles
									tree.NewDString(privilege.Types.String()),               // object_type
									tree.NewDString(username.PublicRoleName().Normalized()), // grantee
									tree.NewDString(string(privilege.USAGE.DisplayName())),  // privilege_type
									tree.DBoolFalse, // is_grantable
								); err != nil {
									return err
								}
							}
							if catprivilege.GetPublicHasExecuteOnFunctions(defaultPrivilegesForRole) {
								if err := addRow(
									database,    // database_name
									schema,      // schema_name
									role,        // role
									forAllRoles, // for_all_roles
									tree.NewDString(privilege.Routines.String()),             // object_type
									tree.NewDString(username.PublicRoleName().Normalized()),  // grantee
									tree.NewDString(string(privilege.EXECUTE.DisplayName())), // privilege_type
									tree.DBoolFalse, // is_grantable
								); err != nil {
									return err
								}
							}
						}
					}
					return nil
				}

				// Add default privileges for default privileges defined on the
				// database.
				if err := addRowsForSchema(databaseDesc.GetDefaultPrivilegeDescriptor(), tree.DNull); err != nil {
					return err
				}

				return forEachSchema(ctx, p, databaseDesc, true /* requiresPrivileges */, func(ctx context.Context, schema catalog.SchemaDescriptor) error {
					return addRowsForSchema(schema.GetDefaultPrivilegeDescriptor(), tree.NewDString(schema.GetName()))
				})
			})
	},
}

var crdbInternalIndexUsageStatistics = virtualSchemaTable{
	comment: `cluster-wide index usage statistics (in-memory, not durable).` +
		`Querying this table is an expensive operation since it creates a` +
		`cluster-wide RPC fanout.`,
	schema: `
CREATE TABLE crdb_internal.index_usage_statistics (
  table_id        INT NOT NULL,
  index_id        INT NOT NULL,
  total_reads     INT NOT NULL,
  last_read       TIMESTAMPTZ
);`,
	generator: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		// Perform RPC Fanout.
		stats, err :=
			p.extendedEvalCtx.SQLStatusServer.IndexUsageStatistics(ctx, &serverpb.IndexUsageStatisticsRequest{})
		if err != nil {
			return nil, nil, err
		}
		indexStats := idxusage.NewLocalIndexUsageStatsFromExistingStats(&idxusage.Config{}, stats.Statistics)

		const numDatums = 4
		row := make(tree.Datums, numDatums)
		worker := func(ctx context.Context, pusher rowPusher) error {
			opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: true}
			return forEachTableDesc(ctx, p, dbContext, opts,
				func(ctx context.Context, descCtx tableDescContext) error {
					table := descCtx.table
					tableID := table.GetID()
					return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(idx catalog.Index) error {
						indexID := idx.GetID()
						stats := indexStats.Get(roachpb.TableID(tableID), roachpb.IndexID(indexID))
						lastScanTs := tree.DNull
						if !stats.LastRead.IsZero() {
							lastScanTs, err = tree.MakeDTimestampTZ(stats.LastRead, time.Nanosecond)
							if err != nil {
								return err
							}
						}
						row = append(row[:0],
							tree.NewDInt(tree.DInt(tableID)),              // tableID
							tree.NewDInt(tree.DInt(indexID)),              // indexID
							tree.NewDInt(tree.DInt(stats.TotalReadCount)), // total_reads
							lastScanTs, // last_scan
						)
						if buildutil.CrdbTestBuild {
							if len(row) != numDatums {
								return errors.AssertionFailedf("expected %d datums, got %d", numDatums, len(row))
							}
						}
						return pusher.pushRow(row...)
					})
				})
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// crdb_internal.cluster_statement_statistics contains cluster-wide statement statistics
// that have not yet been flushed to disk.
var crdbInternalClusterStmtStatsTable = virtualSchemaTable{
	comment: `cluster-wide statement statistics that have not yet been flushed ` +
		`to system tables. Querying this table is a somewhat expensive operation ` +
		`since it creates a cluster-wide RPC-fanout.`,
	schema: `
CREATE TABLE crdb_internal.cluster_statement_statistics (
    aggregated_ts              TIMESTAMPTZ NOT NULL,
    fingerprint_id             BYTES NOT NULL,
    transaction_fingerprint_id BYTES NOT NULL,
    plan_hash                  BYTES NOT NULL,
    app_name                   STRING NOT NULL,
    metadata                   JSONB NOT NULL,
    statistics                 JSONB NOT NULL,
    sampled_plan               JSONB NOT NULL,
    aggregation_interval       INTERVAL NOT NULL,
    index_recommendations      STRING[] NOT NULL
);`,
	generator: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		// TODO(azhng): we want to eventually implement memory accounting within the
		//  RPC handlers. See #69032.
		acc := p.Mon().MakeBoundAccount()
		defer acc.Close(ctx)

		// Perform RPC fanout.
		stats, err :=
			p.extendedEvalCtx.SQLStatusServer.Statements(ctx, &serverpb.StatementsRequest{
				FetchMode: serverpb.StatementsRequest_StmtStatsOnly,
			})
		if err != nil {
			return nil, nil, err
		}

		statsMemSize := stats.Size()
		if err = acc.Grow(ctx, int64(statsMemSize)); err != nil {
			return nil, nil, err
		}

		memSQLStats, err := sslocal.NewTempSQLStatsFromExistingStmtStats(stats.Statements)
		if err != nil {
			return nil, nil, err
		}

		s := p.extendedEvalCtx.statsProvider
		curAggTs := s.ComputeAggregatedTs()
		aggInterval := s.GetAggregationInterval()

		const numDatums = 10
		row := make(tree.Datums, numDatums)
		worker := func(ctx context.Context, pusher rowPusher) error {
			return memSQLStats.IterateStatementStats(ctx, sqlstats.IteratorOptions{
				SortedAppNames: true,
				SortedKey:      true,
			}, func(ctx context.Context, statistics *appstatspb.CollectedStatementStatistics) error {

				aggregatedTs, err := tree.MakeDTimestampTZ(curAggTs, time.Microsecond)
				if err != nil {
					return err
				}

				fingerprintID := tree.NewDBytes(
					tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(statistics.ID))))

				transactionFingerprintID := tree.NewDBytes(
					tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(statistics.Key.TransactionFingerprintID))))

				planHash := tree.NewDBytes(
					tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(statistics.Key.PlanHash)))

				metadataJSON, err := sqlstatsutil.BuildStmtMetadataJSON(statistics)
				if err != nil {
					return err
				}
				statisticsJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&statistics.Stats)
				if err != nil {
					return err
				}
				plan := sqlstatsutil.ExplainTreePlanNodeToJSON(&statistics.Stats.SensitiveInfo.MostRecentPlanDescription)

				aggInterval := tree.NewDInterval(
					duration.MakeDuration(aggInterval.Nanoseconds(), 0, 0),
					types.DefaultIntervalTypeMetadata)

				indexRecommendations := tree.NewDArray(types.String)
				for _, recommendation := range statistics.Stats.IndexRecommendations {
					if err := indexRecommendations.Append(tree.NewDString(recommendation)); err != nil {
						return err
					}
				}

				row = append(row[:0],
					aggregatedTs,                        // aggregated_ts
					fingerprintID,                       // fingerprint_id
					transactionFingerprintID,            // transaction_fingerprint_id
					planHash,                            // plan_hash
					tree.NewDString(statistics.Key.App), // app_name
					tree.NewDJSON(metadataJSON),         // metadata
					tree.NewDJSON(statisticsJSON),       // statistics
					tree.NewDJSON(plan),                 // plan
					aggInterval,                         // aggregation_interval
					indexRecommendations,                // index_recommendations
				)
				if buildutil.CrdbTestBuild {
					if len(row) != numDatums {
						return errors.AssertionFailedf("expected %d datums, got %d", numDatums, len(row))
					}
				}
				return pusher.pushRow(row...)
			})
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// crdb_internal.statement_statistics view merges in-memory cluster statement statistics from
// the crdb_internal.cluster_statement_statistics virtual table with persisted statement
// statistics from the system table. Equivalent stats between tables are aggregated. This
// view is primarily used to query statement stats info by date range.
var crdbInternalStmtStatsView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.statement_statistics AS
SELECT
  aggregated_ts,
  fingerprint_id,
  transaction_fingerprint_id,
  plan_hash,
  app_name,
  max(metadata) as metadata,
  merge_statement_stats(DISTINCT statistics),
  max(sampled_plan),
  aggregation_interval,
  array_remove(array_cat_agg(index_recommendations), NULL) AS index_recommendations
FROM (
  SELECT
      aggregated_ts,
      fingerprint_id,
      transaction_fingerprint_id,
      plan_hash,
      app_name,
      metadata,
      statistics,
      sampled_plan,
      aggregation_interval,
      index_recommendations
  FROM
      crdb_internal.cluster_statement_statistics
  UNION ALL
      SELECT
          aggregated_ts,
          fingerprint_id,
          transaction_fingerprint_id,
          plan_hash,
          app_name,
          metadata,
          statistics,
          plan,
          agg_interval,
          index_recommendations
      FROM
          system.statement_statistics
)
GROUP BY
  aggregated_ts,
  fingerprint_id,
  transaction_fingerprint_id,
  plan_hash,
  app_name,
  aggregation_interval`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "transaction_fingerprint_id", Typ: types.Bytes},
		{Name: "plan_hash", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
		{Name: "sampled_plan", Typ: types.Jsonb},
		{Name: "aggregation_interval", Typ: types.Interval},
		{Name: "index_recommendations", Typ: types.StringArray},
	},
}

// crdb_internal.statement_statistics_persisted view selects persisted statement
// statistics from the system table. This view is primarily used to query statement
// stats info by date range.
var crdbInternalStmtStatsPersistedView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.statement_statistics_persisted AS
      SELECT
          aggregated_ts,
          fingerprint_id,
          transaction_fingerprint_id,
          plan_hash,
          app_name,
          node_id,
          agg_interval,
          metadata,
          statistics,
          plan,
          index_recommendations,
          indexes_usage,
          execution_count,
          service_latency,
          cpu_sql_nanos,
          contention_time,
          total_estimated_execution_time,
          p99_latency
      FROM
          system.statement_statistics`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "transaction_fingerprint_id", Typ: types.Bytes},
		{Name: "plan_hash", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "node_id", Typ: types.Int},
		{Name: "agg_interval", Typ: types.Interval},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
		{Name: "plan", Typ: types.Jsonb},
		{Name: "index_recommendations", Typ: types.StringArray},
		{Name: "indexes_usage", Typ: types.Jsonb},
		{Name: "execution_count", Typ: types.Int},
		{Name: "service_latency", Typ: types.Float},
		{Name: "cpu_sql_nanos", Typ: types.Float},
		{Name: "contention_time", Typ: types.Float},
		{Name: "total_estimated_execution_time", Typ: types.Float},
		{Name: "p99_latency", Typ: types.Float},
	},
}

// crdb_internal.statement_activity view to give permission to non-admins
// to access the system.statement_activity table
var crdbInternalStmtActivityView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.statement_activity AS
      SELECT
				aggregated_ts,
				fingerprint_id,
				transaction_fingerprint_id,
				plan_hash,
				app_name,
				agg_interval,
				metadata,
				statistics,
				plan,
				index_recommendations,
				execution_count,
				execution_total_seconds,
				execution_total_cluster_seconds,
				contention_time_avg_seconds,
				cpu_sql_avg_nanos,
				service_latency_avg_seconds,
				service_latency_p99_seconds
      FROM
          system.statement_activity`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "transaction_fingerprint_id", Typ: types.Bytes},
		{Name: "plan_hash", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "agg_interval", Typ: types.Interval},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
		{Name: "plan", Typ: types.Jsonb},
		{Name: "index_recommendations", Typ: types.StringArray},
		{Name: "execution_count", Typ: types.Int},
		{Name: "execution_total_seconds", Typ: types.Float},
		{Name: "execution_total_cluster_seconds", Typ: types.Float},
		{Name: "contention_time_avg_seconds", Typ: types.Float},
		{Name: "cpu_sql_avg_nanos", Typ: types.Float},
		{Name: "service_latency_avg_seconds", Typ: types.Float},
		{Name: "service_latency_p99_seconds", Typ: types.Float},
	},
}

// crdb_internal.transaction_activity is a view to give permission to non-admins
// to access the system.transaction_activity table
var crdbInternalTxnActivityView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.transaction_activity AS
      SELECT
				aggregated_ts,
				fingerprint_id,
				app_name,
				agg_interval,
				metadata,
				statistics,
				query,
				execution_count,
				execution_total_seconds,
				execution_total_cluster_seconds,
				contention_time_avg_seconds,
				cpu_sql_avg_nanos,
				service_latency_avg_seconds,
				service_latency_p99_seconds
      FROM
        system.transaction_activity`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "agg_interval", Typ: types.Interval},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
		{Name: "query", Typ: types.String},
		{Name: "execution_count", Typ: types.Int},
		{Name: "execution_total_seconds", Typ: types.Float},
		{Name: "execution_total_cluster_seconds", Typ: types.Float},
		{Name: "contention_time_avg_seconds", Typ: types.Float},
		{Name: "cpu_sql_avg_nanos", Typ: types.Float},
		{Name: "service_latency_avg_seconds", Typ: types.Float},
		{Name: "service_latency_p99_seconds", Typ: types.Float},
	},
}

// crdb_internal.statement_statistics_persisted_v22_2 view selects persisted statement
// statistics from the system table. This view is primarily used to query statement
// stats info by date range. This view is created to be used in mixed version state cluster.
var crdbInternalStmtStatsPersistedViewV22_2 = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.statement_statistics_persisted_v22_2 AS
      SELECT
          aggregated_ts,
          fingerprint_id,
          transaction_fingerprint_id,
          plan_hash,
          app_name,
          node_id,
          agg_interval,
          metadata,
          statistics,
          plan,
          index_recommendations
      FROM
          system.statement_statistics`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "transaction_fingerprint_id", Typ: types.Bytes},
		{Name: "plan_hash", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "node_id", Typ: types.Int},
		{Name: "agg_interval", Typ: types.Interval},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
		{Name: "plan", Typ: types.Jsonb},
		{Name: "index_recommendations", Typ: types.StringArray},
	},
}

var crdbInternalActiveRangeFeedsTable = virtualSchemaTable{
	comment: `node-level table listing all currently running range feeds`,
	schema: `
CREATE TABLE crdb_internal.active_range_feeds (
  id INT,
  tags STRING,
  start_after DECIMAL,
  diff BOOL,
  node_id INT,
  range_id INT,
  created TIMESTAMPTZ,
  range_start STRING,
  range_end STRING,
  resolved DECIMAL,
  resolved_age INTERVAL,
  last_event TIMESTAMPTZ,
  catchup BOOL,
  num_errs INT,
  last_err STRING
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return p.execCfg.DistSender.ForEachActiveRangeFeed(
			func(rfCtx kvcoord.RangeFeedContext, rf kvcoord.PartialRangeFeed) error {
				now := p.EvalContext().GetStmtTimestamp()
				timestampOrNull := func(t time.Time) (tree.Datum, error) {
					if t.IsZero() {
						return tree.DNull, nil
					}
					return tree.MakeDTimestampTZ(t, time.Microsecond)
				}
				age := func(t time.Time) tree.Datum {
					if t.Unix() == 0 {
						return tree.DNull
					}
					return tree.NewDInterval(duration.Age(now, t), types.DefaultIntervalTypeMetadata)
				}

				lastEvent, err := timestampOrNull(rf.LastValueReceived)
				if err != nil {
					return err
				}
				createdAt, err := timestampOrNull(rf.CreatedTime)
				if err != nil {
					return err
				}

				var lastErr tree.Datum
				if rf.LastErr == nil {
					lastErr = tree.DNull
				} else {
					lastErr = tree.NewDString(rf.LastErr.Error())
				}

				return addRow(
					tree.NewDInt(tree.DInt(rfCtx.ID)),
					tree.NewDString(rfCtx.CtxTags),
					eval.TimestampToDecimalDatum(rf.StartAfter),
					tree.MakeDBool(tree.DBool(rfCtx.WithDiff)),
					tree.NewDInt(tree.DInt(rf.NodeID)),
					tree.NewDInt(tree.DInt(rf.RangeID)),
					createdAt,
					tree.NewDString(keys.PrettyPrint(nil /* valDirs */, rf.Span.Key)),
					tree.NewDString(keys.PrettyPrint(nil /* valDirs */, rf.Span.EndKey)),
					eval.TimestampToDecimalDatum(rf.Resolved),
					age(rf.Resolved.GoTime()),
					lastEvent,
					tree.MakeDBool(tree.DBool(rf.InCatchup)),
					tree.NewDInt(tree.DInt(rf.NumErrs)),
					lastErr,
				)
			},
		)
	},
}

// crdb_internal.cluster_transaction_statistics contains cluster-wide transaction statistics
// that have not yet been flushed to disk.
var crdbInternalClusterTxnStatsTable = virtualSchemaTable{
	comment: `cluster-wide transaction statistics that have not yet been flushed ` +
		`to system tables. Querying this table is a somewhat expensive operation ` +
		`since it creates a cluster-wide RPC-fanout.`,
	schema: `
CREATE TABLE crdb_internal.cluster_transaction_statistics (
    aggregated_ts         TIMESTAMPTZ NOT NULL,
    fingerprint_id        BYTES NOT NULL,
    app_name              STRING NOT NULL,
    metadata              JSONB NOT NULL,
    statistics            JSONB NOT NULL,
    aggregation_interval  INTERVAL NOT NULL
);`,
	generator: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		// TODO(azhng): we want to eventually implement memory accounting within the
		//  RPC handlers. See #69032.
		acc := p.Mon().MakeBoundAccount()
		defer acc.Close(ctx)

		// Perform RPC fanout.
		stats, err :=
			p.extendedEvalCtx.SQLStatusServer.Statements(ctx, &serverpb.StatementsRequest{
				FetchMode: serverpb.StatementsRequest_TxnStatsOnly,
			})

		if err != nil {
			return nil, nil, err
		}

		statsMemSize := stats.Size()
		if err = acc.Grow(ctx, int64(statsMemSize)); err != nil {
			return nil, nil, err
		}

		memSQLStats, err :=
			sslocal.NewTempSQLStatsFromExistingTxnStats(stats.Transactions)
		if err != nil {
			return nil, nil, err
		}

		s := p.extendedEvalCtx.statsProvider
		curAggTs := s.ComputeAggregatedTs()
		aggInterval := s.GetAggregationInterval()

		const numDatums = 6
		row := make(tree.Datums, numDatums)
		worker := func(ctx context.Context, pusher rowPusher) error {
			return memSQLStats.IterateTransactionStats(ctx, sqlstats.IteratorOptions{
				SortedAppNames: true,
				SortedKey:      true,
			}, func(
				ctx context.Context,
				statistics *appstatspb.CollectedTransactionStatistics) error {

				aggregatedTs, err := tree.MakeDTimestampTZ(curAggTs, time.Microsecond)
				if err != nil {
					return err
				}

				fingerprintID := tree.NewDBytes(
					tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(statistics.TransactionFingerprintID))))

				metadataJSON, err := sqlstatsutil.BuildTxnMetadataJSON(statistics)
				if err != nil {
					return err
				}
				statisticsJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(statistics)
				if err != nil {
					return err
				}

				aggInterval := tree.NewDInterval(
					duration.MakeDuration(aggInterval.Nanoseconds(), 0, 0),
					types.DefaultIntervalTypeMetadata)

				row = append(row[:0],
					aggregatedTs,                    // aggregated_ts
					fingerprintID,                   // fingerprint_id
					tree.NewDString(statistics.App), // app_name
					tree.NewDJSON(metadataJSON),     // metadata
					tree.NewDJSON(statisticsJSON),   // statistics
					aggInterval,                     // aggregation_interval
				)
				if buildutil.CrdbTestBuild {
					if len(row) != numDatums {
						return errors.AssertionFailedf("expected %d datums, got %d", numDatums, len(row))
					}
				}
				return pusher.pushRow(row...)
			})
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// crdb_internal.transaction_statistics view merges in-memory cluster transactions statistics
// from the crdb_internal.cluster_transaction_statistics virtual table with persisted statement
// statistics from the system table. Equivalent stats between tables are aggregated. This
// view is primarily used to query statement stats info by date range.
var crdbInternalTxnStatsView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.transaction_statistics AS
SELECT
  aggregated_ts,
  fingerprint_id,
  app_name,
  max(metadata),
  merge_transaction_stats(statistics),
  aggregation_interval
FROM (
  SELECT
    aggregated_ts,
    fingerprint_id,
    app_name,
    metadata,
    statistics,
    aggregation_interval
  FROM
    crdb_internal.cluster_transaction_statistics
  UNION ALL
      SELECT
        aggregated_ts,
        fingerprint_id,
        app_name,
        metadata,
        statistics,
        agg_interval
      FROM
        system.transaction_statistics
)
GROUP BY
  aggregated_ts,
  fingerprint_id,
  app_name,
  aggregation_interval`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
		{Name: "aggregation_interval", Typ: types.Interval},
	},
}

// crdb_internal.transaction_statistics_persisted view selects persisted transaction
// statistics from the system table. This view is primarily used to query transaction
// stats info by date range.
var crdbInternalTxnStatsPersistedView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.transaction_statistics_persisted AS
      SELECT
        aggregated_ts,
        fingerprint_id,
        app_name,
        node_id,
        agg_interval,
        metadata,
        statistics,
        execution_count,
        service_latency,
        cpu_sql_nanos,
        contention_time,
        total_estimated_execution_time,
        p99_latency
      FROM
        system.transaction_statistics`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "node_id", Typ: types.Int},
		{Name: "agg_interval", Typ: types.Interval},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
		{Name: "execution_count", Typ: types.Int},
		{Name: "service_latency", Typ: types.Float},
		{Name: "cpu_sql_nanos", Typ: types.Float},
		{Name: "contention_time", Typ: types.Float},
		{Name: "total_estimated_execution_time", Typ: types.Float},
		{Name: "p99_latency", Typ: types.Float},
	},
}

// crdb_internal.transaction_statistics_persisted_v22_2 view selects persisted transaction
// statistics from the system table. This view is primarily used to query transaction
// stats info by date range. This view is created to be used in mixed version state cluster.
var crdbInternalTxnStatsPersistedViewV22_2 = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.transaction_statistics_persisted_v22_2 AS
      SELECT
        aggregated_ts,
        fingerprint_id,
        app_name,
        node_id,
        agg_interval,
        metadata,
        statistics
      FROM
        system.transaction_statistics`,
	resultColumns: colinfo.ResultColumns{
		{Name: "aggregated_ts", Typ: types.TimestampTZ},
		{Name: "fingerprint_id", Typ: types.Bytes},
		{Name: "app_name", Typ: types.String},
		{Name: "node_id", Typ: types.Int},
		{Name: "agg_interval", Typ: types.Interval},
		{Name: "metadata", Typ: types.Jsonb},
		{Name: "statistics", Typ: types.Jsonb},
	},
}

var crdbInternalDroppedRelationsView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.kv_dropped_relations AS
WITH
	dropped_relations
		AS (
			SELECT
				id,
				(descriptor->'table'->>'name') AS name,
				(descriptor->'table'->'parentId')::INT8 AS parent_id,
				(descriptor->'table'->'unexposedParentSchemaId')::INT8
					AS parent_schema_id,
				to_timestamp(
					((descriptor->'table'->>'dropTime')::DECIMAL * 0.000000001)::FLOAT8
				)
					AS drop_time
			FROM
				crdb_internal.kv_catalog_descriptor
			WHERE
				descriptor->'table'->>'state' = 'DROP'
		),
	gc_ttl
		AS (
			SELECT
				id, (config->'gc'->'ttlSeconds')::INT8 AS ttl
			FROM
				crdb_internal.kv_catalog_zones
		)
SELECT
	dr.parent_id,
	dr.parent_schema_id,
	dr.name,
	dr.id,
	dr.drop_time,
	COALESCE(gc.ttl, db_gc.ttl, root_gc.ttl) * '1 second'::INTERVAL AS ttl
FROM
	dropped_relations AS dr
	LEFT JOIN gc_ttl AS gc ON gc.id = dr.id
	LEFT JOIN gc_ttl AS db_gc ON db_gc.id = dr.parent_id
	LEFT JOIN gc_ttl AS root_gc ON root_gc.id = 0
ORDER BY
	parent_id, parent_schema_id, id`,
	resultColumns: colinfo.ResultColumns{
		{Name: "parent_id", Typ: types.Int},
		{Name: "parent_schema_id", Typ: types.Int},
		{Name: "name", Typ: types.String},
		{Name: "id", Typ: types.Int},
		{Name: "drop_time", Typ: types.Timestamp},
		{Name: "ttl", Typ: types.Interval},
	},
	comment: "kv_dropped_relations contains all dropped relations waiting for garbage collection",
}

// crdbInternalTenantUsageDetailsView, exposes system ranges.
var crdbInternalTenantUsageDetailsView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.tenant_usage_details AS
  SELECT
    tenant_id,
    (j->>'rU')::FLOAT8 AS total_ru,
    (j->>'readBytes')::INT8 AS total_read_bytes,
    (j->>'readRequests')::INT8 AS total_read_requests,
    (j->>'writeBytes')::INT8 AS total_write_bytes,
    (j->>'writeRequests')::INT8 AS total_write_requests,
    (j->>'sqlPodsCpuSeconds')::FLOAT8 AS total_sql_pod_seconds,
    (j->>'pgwireEgressBytes')::INT8 AS total_pgwire_egress_bytes,
    (j->>'externalIOIngressBytes')::INT8 AS total_external_io_ingress_bytes,
    (j->>'externalIOIngressBytes')::INT8 AS total_external_io_ingress_bytes,
    (j->>'kvRU')::FLOAT8 AS total_kv_ru,
    (j->>'crossRegionNetworkRU')::FLOAT8 AS total_cross_region_network_ru
  FROM
    (
      SELECT
        tenant_id,
        crdb_internal.pb_to_json('cockroach.roachpb.TenantConsumption', total_consumption) AS j
      FROM
        system.tenant_usage
      WHERE
        instance_id = 0
    )
`,
	resultColumns: colinfo.ResultColumns{
		{Name: "tenant_id", Typ: types.Int},
		{Name: "total_ru", Typ: types.Float},
		{Name: "total_read_bytes", Typ: types.Int},
		{Name: "total_read_requests", Typ: types.Int},
		{Name: "total_write_bytes", Typ: types.Int},
		{Name: "total_write_requests", Typ: types.Int},
		{Name: "total_sql_pod_seconds", Typ: types.Float},
		{Name: "total_pgwire_egress_bytes", Typ: types.Int},
		{Name: "total_external_io_ingress_bytes", Typ: types.Int},
		{Name: "total_external_io_egress_bytes", Typ: types.Int},
		{Name: "total_kv_ru", Typ: types.Float},
		{Name: "total_cross_region_network_ru", Typ: types.Float},
	},
}

var crdbInternalTransactionContentionEventsTable = virtualSchemaTable{
	comment: `cluster-wide transaction contention events. Querying this table is an
		expensive operation since it creates a cluster-wide RPC-fanout.`,
	schema: `
CREATE TABLE crdb_internal.transaction_contention_events (
    collection_ts                TIMESTAMPTZ NOT NULL,

    blocking_txn_id              UUID NOT NULL,
    blocking_txn_fingerprint_id  BYTES NOT NULL,

    waiting_txn_id               UUID NOT NULL,
    waiting_txn_fingerprint_id   BYTES NOT NULL,

    contention_duration          INTERVAL NOT NULL,
    contending_key               BYTES NOT NULL,
    contending_pretty_key     	 STRING NOT NULL,

    waiting_stmt_id              string NOT NULL,
    waiting_stmt_fingerprint_id  BYTES NOT NULL,

    database_name                STRING NOT NULL,
    schema_name                  STRING NOT NULL,
    table_name                   STRING NOT NULL,
    index_name                   STRING,
    contention_type              STRING NOT NULL
);`,
	generator: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		// Check permission first before making RPC fanout.
		// If a user has VIEWACTIVITYREDACTED role option but the user does not
		// have the ADMIN role option, then the contending key should be redacted.
		hasPermission, shouldRedactContendingKey, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return nil, nil, err
		}
		if !hasPermission {
			return nil, nil, noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		// Account for memory used by the RPC fanout.
		acc := p.Mon().MakeBoundAccount()
		defer acc.Close(ctx)

		resp, err := p.extendedEvalCtx.SQLStatusServer.TransactionContentionEvents(
			ctx, &serverpb.TransactionContentionEventsRequest{})

		if err != nil {
			return nil, nil, err
		}

		respSize := resp.Size()
		if err = acc.Grow(ctx, int64(respSize)); err != nil {
			return nil, nil, err
		}

		const numDatums = 15
		row := make(tree.Datums, numDatums)
		worker := func(ctx context.Context, pusher rowPusher) error {
			for i := range resp.Events {
				collectionTs, err := tree.MakeDTimestampTZ(resp.Events[i].CollectionTs, time.Microsecond)
				if err != nil {
					return err
				}
				blockingFingerprintID := tree.NewDBytes(
					tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(resp.Events[i].BlockingTxnFingerprintID))))

				waitingFingerprintID := tree.NewDBytes(
					tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(resp.Events[i].WaitingTxnFingerprintID))))

				contentionDuration := tree.NewDInterval(
					duration.MakeDuration(
						resp.Events[i].BlockingEvent.Duration.Nanoseconds(),
						0, /* days */
						0, /* months */
					),
					types.DefaultIntervalTypeMetadata,
				)

				contendingPrettyKey := tree.NewDString("")
				contendingKey := tree.NewDBytes("")
				if !shouldRedactContendingKey {
					decodedKey, _, _ := keys.DecodeTenantPrefix(resp.Events[i].BlockingEvent.Key)
					contendingPrettyKey = tree.NewDString(keys.PrettyPrint(nil /* valDirs */, decodedKey))
					contendingKey = tree.NewDBytes(
						tree.DBytes(decodedKey))
				}

				waitingStmtFingerprintID := tree.NewDBytes(
					tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(resp.Events[i].WaitingStmtFingerprintID))))

				waitingStmtId := tree.NewDString(hex.EncodeToString(resp.Events[i].WaitingStmtID.GetBytes()))

				dbName, schemaName, tableName, indexName := getContentionEventInfo(ctx, p, resp.Events[i])
				row = append(row[:0],
					collectionTs, // collection_ts
					tree.NewDUuid(tree.DUuid{UUID: resp.Events[i].BlockingEvent.TxnMeta.ID}), // blocking_txn_id
					blockingFingerprintID, // blocking_fingerprint_id
					tree.NewDUuid(tree.DUuid{UUID: resp.Events[i].WaitingTxnID}), // waiting_txn_id
					waitingFingerprintID,        // waiting_fingerprint_id
					contentionDuration,          // contention_duration
					contendingKey,               // contending_key,
					contendingPrettyKey,         // contending_pretty_key
					waitingStmtId,               // waiting_stmt_id
					waitingStmtFingerprintID,    // waiting_stmt_fingerprint_id
					tree.NewDString(dbName),     // database_name
					tree.NewDString(schemaName), // schema_name
					tree.NewDString(tableName),  // table_name
					tree.NewDString(indexName),  // index_name
					tree.NewDString(resp.Events[i].ContentionType.String()),
				)
				if buildutil.CrdbTestBuild {
					if len(row) != numDatums {
						return errors.AssertionFailedf("expected %d datums, got %d", numDatums, len(row))
					}
				}
				if err = pusher.pushRow(row...); err != nil {
					return err
				}
			}
			return nil
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

var crdbInternalIndexSpansTable = virtualSchemaTable{
	comment: `key spans per table index`,
	schema: `
CREATE TABLE crdb_internal.index_spans (
  descriptor_id INT NOT NULL,
  index_id      INT NOT NULL,
  start_key     BYTES NOT NULL,
  end_key       BYTES NOT NULL,
  INDEX(descriptor_id)
);`,
	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, constraint tree.Datum, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				descID := catid.DescID(tree.MustBeDInt(constraint))
				var table catalog.TableDescriptor
				// We need to include offline tables, like in
				// forEachTableDescAll() below. So we can't use p.LookupByID()
				// which only considers online tables.
				p.runWithOptions(resolveFlags{skipCache: true}, func() {
					table, err = p.byIDGetterBuilder().Get().Table(ctx, descID)
				})
				if err != nil {
					return false, err
				}
				return true, generateIndexSpans(ctx, p, table, addRow)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{virtualOpts: hideVirtual, allowAdding: true}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				table := descCtx.table
				return generateIndexSpans(ctx, p, table, addRow)
			})
	},
}

func generateIndexSpans(
	ctx context.Context, p *planner, table catalog.TableDescriptor, addRow func(...tree.Datum) error,
) error {
	tabID := table.GetID()
	return catalog.ForEachIndex(table, catalog.IndexOpts{}, func(idx catalog.Index) error {
		indexID := idx.GetID()
		start := roachpb.Key(rowenc.MakeIndexKeyPrefix(p.ExecCfg().Codec, tabID, indexID))
		end := start.PrefixEnd()
		return addRow(
			tree.NewDInt(tree.DInt(tabID)),
			tree.NewDInt(tree.DInt(indexID)),
			tree.NewDBytes(tree.DBytes(start)),
			tree.NewDBytes(tree.DBytes(end)),
		)
	})
}

var crdbInternalTableSpansTable = virtualSchemaTable{
	comment: `key spans per SQL object`,
	schema: `
CREATE TABLE crdb_internal.table_spans (
  descriptor_id INT NOT NULL,
  start_key     BYTES NOT NULL,
  end_key       BYTES NOT NULL,
  dropped       BOOL NOT NULL,
  INDEX(descriptor_id)
);`,
	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, constraint tree.Datum, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				descID := catid.DescID(tree.MustBeDInt(constraint))
				var table catalog.TableDescriptor
				// We need to include offline tables, like in
				// forEachTableDescAll() below. So we can't use p.LookupByID()
				// which only considers online tables.
				p.runWithOptions(resolveFlags{skipCache: true}, func() {
					table, err = p.byIDGetterBuilder().Get().Table(ctx, descID)
				})
				if err != nil {
					return false, err
				}
				return true, generateTableSpan(ctx, p, table, addRow)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		opts := forEachTableDescOptions{
			virtualOpts:    hideVirtual,
			allowAdding:    true,
			includeDropped: true,
		}
		return forEachTableDesc(ctx, p, dbContext, opts,
			func(ctx context.Context, descCtx tableDescContext) error {
				table := descCtx.table
				return generateTableSpan(ctx, p, table, addRow)
			})
	},
}

func generateTableSpan(
	ctx context.Context, p *planner, table catalog.TableDescriptor, addRow func(...tree.Datum) error,
) error {
	tabID := table.GetID()
	start := p.ExecCfg().Codec.TablePrefix(uint32(tabID))
	end := start.PrefixEnd()
	return addRow(
		tree.NewDInt(tree.DInt(tabID)),
		tree.NewDBytes(tree.DBytes(start)),
		tree.NewDBytes(tree.DBytes(end)),
		tree.MakeDBool(tree.DBool(table.Dropped())),
	)
}

// crdbInternalClusterLocksTable exposes the state of locks, as well as lock waiters,
// in range lock tables across the cluster.
var crdbInternalClusterLocksTable = virtualSchemaTable{
	comment: `cluster-wide locks held in lock tables. Querying this table is an
		expensive operation since it creates a cluster-wide RPC-fanout.`,
	schema: `
CREATE TABLE crdb_internal.cluster_locks (
    range_id            INT NOT NULL,
    table_id            INT NOT NULL,
    database_name       STRING NOT NULL,
    schema_name         STRING NOT NULL,
    table_name          STRING NOT NULL,
    index_name          STRING,
    lock_key            BYTES NOT NULL,
    lock_key_pretty     STRING NOT NULL,
    txn_id              UUID,
    ts                  TIMESTAMP,
    lock_strength       STRING,
    durability          STRING,
    granted             BOOL,
    contended           BOOL NOT NULL,
    duration            INTERVAL,
    isolation_level     STRING,
    INDEX(table_id),
    INDEX(database_name),
    INDEX(table_name),
    INDEX(contended)
);`,
	indexes: []virtualIndex{
		{
			populate: genPopulateClusterLocksWithIndex("table_id" /* idxColumnName */, func(filters *clusterLocksFilters, idxConstraint tree.Datum) {
				if tableID, ok := tree.AsDInt(idxConstraint); ok {
					filters.tableID = (*int64)(&tableID)
				}
			}),
		},
		{
			populate: genPopulateClusterLocksWithIndex("database_name" /* idxColumnName */, func(filters *clusterLocksFilters, idxConstraint tree.Datum) {
				if dbName, ok := tree.AsDString(idxConstraint); ok {
					filters.databaseName = (*string)(&dbName)
				}
			}),
		},
		{
			populate: genPopulateClusterLocksWithIndex("table_name" /* idxColumnName */, func(filters *clusterLocksFilters, idxConstraint tree.Datum) {
				if tableName, ok := tree.AsDString(idxConstraint); ok {
					filters.tableName = (*string)(&tableName)
				}
			}),
		},
		{
			populate: genPopulateClusterLocksWithIndex("contended" /* idxColumnName */, func(filters *clusterLocksFilters, idxConstraint tree.Datum) {
				if contended, ok := tree.AsDBool(idxConstraint); ok {
					filters.contended = (*bool)(&contended)
				}
			}),
		},
	},
	generator: genClusterLocksGenerator(clusterLocksFilters{}),
}

type clusterLocksFilters struct {
	tableID      *int64
	databaseName *string
	tableName    *string
	contended    *bool
}

func genClusterLocksGenerator(
	filters clusterLocksFilters,
) func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
	return func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, _ *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		hasAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return nil, nil, err
		}
		hasViewActivityOrViewActivityRedacted, shouldRedactKeys, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return nil, nil, err
		}
		if !hasViewActivityOrViewActivityRedacted {
			return nil, nil, noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		all, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return nil, nil, err
		}
		descs := all.OrderedDescriptors()

		privCheckerFunc := func(desc catalog.Descriptor) (bool, error) {
			if hasAdmin {
				return true, nil
			}
			return p.HasAnyPrivilege(ctx, desc)
		}

		_, dbNames, tableNames, schemaNames, indexNames, schemaParents, parents, err :=
			descriptorsByType(descs, privCheckerFunc)
		if err != nil {
			return nil, nil, err
		}

		var spansToQuery roachpb.Spans
		for _, desc := range descs {
			if ok, err := privCheckerFunc(desc); err != nil {
				return nil, nil, err
			} else if !ok {
				continue
			}
			switch desc := desc.(type) {
			case catalog.TableDescriptor:
				if filters.tableName != nil && *filters.tableName != desc.GetName() {
					continue
				}
				if filters.tableID != nil && descpb.ID(*filters.tableID) != desc.GetID() {
					continue
				}
				if filters.databaseName != nil && *filters.databaseName != dbNames[uint32(desc.GetParentID())] {
					continue
				}
				if desc.ExternalRowData() != nil {
					continue
				}
				spansToQuery = append(spansToQuery, desc.TableSpan(p.execCfg.Codec))
			}
		}

		spanIdx := 0
		spansRemain := func() bool {
			return spanIdx < len(spansToQuery)
		}
		getNextSpan := func() *roachpb.Span {
			if !spansRemain() {
				return nil
			}

			nextSpan := spansToQuery[spanIdx]
			spanIdx++
			return &nextSpan
		}

		var resp *kvpb.QueryLocksResponse
		var locks []roachpb.LockStateInfo
		var resumeSpan *roachpb.Span

		fetchLocks := func(key, endKey roachpb.Key) error {
			b := p.Txn().NewBatch()
			queryLocksRequest := &kvpb.QueryLocksRequest{
				RequestHeader: kvpb.RequestHeader{
					Key:    key,
					EndKey: endKey,
				},
				IncludeUncontended: true,
			}
			if filters.contended != nil && *filters.contended {
				queryLocksRequest.IncludeUncontended = false
			}

			b.AddRawRequest(queryLocksRequest)

			b.Header.MaxSpanRequestKeys = int64(rowinfra.ProductionKVBatchSize)
			b.Header.TargetBytes = int64(rowinfra.GetDefaultBatchBytesLimit(p.extendedEvalCtx.TestingKnobs.ForceProductionValues))

			err := p.txn.Run(ctx, b)
			if err != nil {
				return err
			}

			if len(b.RawResponse().Responses) != 1 {
				return errors.AssertionFailedf(
					"unexpected response length of %d for QueryLocksRequest", len(b.RawResponse().Responses),
				)
			}

			resp = b.RawResponse().Responses[0].GetQueryLocks()
			locks = resp.Locks
			resumeSpan = resp.ResumeSpan
			return nil
		}

		lockIdx := 0
		getNextLock := func() (*roachpb.LockStateInfo, error) {
			// If we don't yet have a response or the current response is exhausted,
			// look for a span to query. This may be a ResumeSpan, in the case we
			// need to fetch the next page of results for the current span, or it
			// may be the next span in our list.
			for lockIdx >= len(locks) && (spansRemain() || resumeSpan != nil) {
				var spanToQuery *roachpb.Span
				if resumeSpan != nil {
					spanToQuery = resumeSpan
				} else {
					spanToQuery = getNextSpan()
				}

				if spanToQuery != nil {
					err := fetchLocks(spanToQuery.Key, spanToQuery.EndKey)
					if err != nil {
						return nil, err
					}
					lockIdx = 0
				}
			}

			if lockIdx < len(locks) {
				nextLock := locks[lockIdx]
				lockIdx++
				return &nextLock, nil
			}

			return nil, nil
		}

		var curLock *roachpb.LockStateInfo
		// Used to deduplicate waiter information from shared locks.
		var prevLock *roachpb.LockStateInfo
		var fErr error
		waiterIdx := -1
		// Flatten response such that both lock holders and lock waiters are each
		// individual rows in the final output. As such, we iterate through the
		// locks received in the response and first output the lock holder, then
		// each waiter, prior to moving onto the next lock (or fetching additional
		// results as necessary).
		return func() (tree.Datums, error) {
			lockHasWaitersDeduped := false
			if curLock == nil || waiterIdx >= len(curLock.Waiters) {
				prevLock = curLock
				curLock, fErr = getNextLock()
				if prevLock != nil && curLock != nil && prevLock.Key.Equal(curLock.Key) {
					lockHasWaitersDeduped = len(curLock.Waiters) > 0
					curLock.Waiters = curLock.Waiters[:0]
				}
				waiterIdx = -1
			}

			// If we couldn't get any more locks from getNextLock(), we have finished
			// generating result rows.
			if curLock == nil || fErr != nil {
				return nil, fErr
			}
			txnIDDatum := tree.DNull
			tsDatum := tree.DNull
			isolationLevelDatum := tree.DNull
			strengthDatum := tree.DNull
			durationDatum := tree.DNull
			granted := false
			// Utilize -1 to indicate that the row represents the lock holder.
			if waiterIdx < 0 {
				if curLock.LockHolder != nil {
					txnIDDatum = tree.NewDUuid(tree.DUuid{UUID: curLock.LockHolder.ID})
					tsDatum = eval.TimestampToInexactDTimestamp(curLock.LockHolder.WriteTimestamp)
					isolationLevelDatum = tree.NewDString(tree.FromKVIsoLevel(curLock.LockHolder.IsoLevel).String())
					strengthDatum = tree.NewDString(curLock.LockStrength.String())
					durationDatum = tree.NewDInterval(
						duration.MakeDuration(curLock.HoldDuration.Nanoseconds(), 0 /* days */, 0 /* months */),
						types.DefaultIntervalTypeMetadata,
					)
					granted = true
				}
			} else {
				waiter := curLock.Waiters[waiterIdx]
				if waiter.WaitingTxn != nil {
					txnIDDatum = tree.NewDUuid(tree.DUuid{UUID: waiter.WaitingTxn.ID})
					tsDatum = eval.TimestampToInexactDTimestamp(waiter.WaitingTxn.WriteTimestamp)
					isolationLevelDatum = tree.NewDString(tree.FromKVIsoLevel(waiter.WaitingTxn.IsoLevel).String())
				}
				strengthDatum = tree.NewDString(waiter.Strength.String())
				durationDatum = tree.NewDInterval(
					duration.MakeDuration(waiter.WaitDuration.Nanoseconds(), 0 /* days */, 0 /* months */),
					types.DefaultIntervalTypeMetadata,
				)
			}

			waiterIdx++

			tableID, dbName, schemaName, tableName, indexName := lookupNamesByKey(
				p, curLock.Key, dbNames, tableNames, schemaNames,
				indexNames, schemaParents, parents,
			)

			var keyOrRedacted roachpb.Key
			var prettyKeyOrRedacted string
			if !shouldRedactKeys {
				keyOrRedacted, _, _ = keys.DecodeTenantPrefix(curLock.Key)
				prettyKeyOrRedacted = keys.PrettyPrint(nil /* valDirs */, keyOrRedacted)
			}
			contented := lockHasWaitersDeduped || len(curLock.Waiters) > 0
			return tree.Datums{
				tree.NewDInt(tree.DInt(curLock.RangeID)),     /* range_id */
				tree.NewDInt(tree.DInt(tableID)),             /* table_id */
				tree.NewDString(dbName),                      /* database_name */
				tree.NewDString(schemaName),                  /* schema_name */
				tree.NewDString(tableName),                   /* table_name */
				tree.NewDString(indexName),                   /* index_name */
				tree.NewDBytes(tree.DBytes(keyOrRedacted)),   /* lock_key */
				tree.NewDString(prettyKeyOrRedacted),         /* lock_key_pretty */
				txnIDDatum,                                   /* txn_id */
				tsDatum,                                      /* ts */
				strengthDatum,                                /* lock_strength */
				tree.NewDString(curLock.Durability.String()), /* durability */
				tree.MakeDBool(tree.DBool(granted)),          /* granted */
				tree.MakeDBool(tree.DBool(contented)),        /* contended */
				durationDatum,                                /* duration */
				isolationLevelDatum,                          /* isolation_level */
			}, nil

		}, nil, nil
	}
}

func genPopulateClusterLocksWithIndex(
	idxColumnName string, setFilters func(filters *clusterLocksFilters, idxConstraint tree.Datum),
) func(context.Context, tree.Datum, *planner, catalog.DatabaseDescriptor, func(...tree.Datum) error) (bool, error) {
	return func(ctx context.Context, idxConstraint tree.Datum, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
		var filters clusterLocksFilters
		setFilters(&filters, idxConstraint)

		if filters.tableID == nil && filters.databaseName == nil && filters.tableName == nil && filters.contended == nil {
			return false, errors.AssertionFailedf("unexpected type %T for %s column in virtual table crdb_internal.cluster_locks", idxConstraint, idxColumnName)
		}

		return populateClusterLocksWithFilter(ctx, p, db, addRow, filters)
	}
}

func populateClusterLocksWithFilter(
	ctx context.Context,
	p *planner,
	db catalog.DatabaseDescriptor,
	addRow func(...tree.Datum) error,
	filters clusterLocksFilters,
) (matched bool, err error) {
	var rowGenerator virtualTableGenerator
	generator := genClusterLocksGenerator(filters)
	rowGenerator, _, err = generator(ctx, p, db, nil /* stopper */)
	if err != nil {
		return false, err
	}
	var row tree.Datums
	row, err = rowGenerator()
	for row != nil && err == nil {
		err = addRow(row...)
		if err != nil {
			break
		}
		matched = true

		row, err = rowGenerator()
	}
	return matched, err
}

// This is the table structure for both {cluster,node}_txn_execution_insights.
const txnExecutionInsightsSchemaPattern = `
CREATE TABLE crdb_internal.%s (
  txn_id                     UUID NOT NULL,
  txn_fingerprint_id         BYTES NOT NULL,
  query                      STRING NOT NULL,
  implicit_txn               BOOL NOT NULL,
  session_id                 STRING NOT NULL,
  start_time                 TIMESTAMP NOT NULL,
  end_time                   TIMESTAMP NOT NULL,
  user_name                  STRING NOT NULL,
  app_name                   STRING NOT NULL,
  rows_read                  INT8 NOT NULL,
  rows_written               INT8 NOT NULL,
  priority                   STRING NOT NULL,
  retries                    INT8 NOT NULL,
  last_retry_reason          STRING,
  contention                 INTERVAL,
  problems                   STRING[] NOT NULL,
  causes                     STRING[] NOT NULL,
  stmt_execution_ids         STRING[] NOT NULL,
  cpu_sql_nanos              INT8,
  last_error_code            STRING,
  last_error_redactable      STRING,
  status                     STRING NOT NULL
)`

var crdbInternalClusterTxnExecutionInsightsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(txnExecutionInsightsSchemaPattern, "cluster_txn_execution_insights"),
	comment: `Cluster transaction execution insights`,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (err error) {
		return populateTxnExecutionInsights(ctx, p, addRow, &serverpb.ListExecutionInsightsRequest{})
	},
}

var crdbInternalNodeTxnExecutionInsightsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(txnExecutionInsightsSchemaPattern, "node_txn_execution_insights"),
	comment: `Node transaction execution insights`,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (err error) {
		return populateTxnExecutionInsights(ctx, p, addRow, &serverpb.ListExecutionInsightsRequest{NodeID: "local"})
	},
}

func populateTxnExecutionInsights(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	request *serverpb.ListExecutionInsightsRequest,
) (err error) {
	// Check if the user has sufficient privileges.
	hasPrivs, shouldRedactError, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
	if err != nil {
		return err
	} else if !hasPrivs {
		return noViewActivityOrViewActivityRedactedRoleError(p.User())
	}

	acc := p.Mon().MakeBoundAccount()
	defer acc.Close(ctx)

	response, err := p.extendedEvalCtx.SQLStatusServer.ListExecutionInsights(ctx, request)
	if err != nil {
		return err
	}

	if err := acc.Grow(ctx, int64(response.Size())); err != nil {
		return err
	}

	// We should truncate the query if it surpasses some absurd limit.
	queryMax := 5000
	for _, insight := range response.Insights {
		if insight.Transaction == nil {
			continue
		}

		var queryBuilder strings.Builder
		for i := range insight.Statements {
			// Build query string.
			remaining := queryMax - queryBuilder.Len()
			if len(insight.Statements[i].Query) > remaining {
				if remaining > 0 {
					queryBuilder.WriteString(insight.Statements[i].Query[:remaining] + "...")
				}
				break
			}
			if i > 0 {
				queryBuilder.WriteString(" ; ")
			}
			queryBuilder.WriteString(insight.Statements[i].Query)
		}

		errorCode := tree.DNull
		if insight.Transaction.LastErrorCode != "" {
			errorCode = tree.NewDString(insight.Transaction.LastErrorCode)
		}

		var errorMsg tree.Datum
		if shouldRedactError {
			errorMsg = tree.NewDString(string(insight.Transaction.LastErrorMsg.Redact()))
		} else {
			errorMsg = tree.NewDString(string(insight.Transaction.LastErrorMsg))
		}

		problems := tree.NewDArray(types.String)
		for i := range insight.Transaction.Problems {
			if err = problems.Append(tree.NewDString(insight.Transaction.Problems[i].String())); err != nil {
				return err
			}
		}

		causes := tree.NewDArray(types.String)
		for i := range insight.Transaction.Causes {
			if err = causes.Append(tree.NewDString(insight.Transaction.Causes[i].String())); err != nil {
				return err
			}
		}

		var startTimestamp *tree.DTimestamp
		startTimestamp, err = tree.MakeDTimestamp(insight.Transaction.StartTime, time.Nanosecond)
		if err != nil {
			return err
		}

		var endTimestamp *tree.DTimestamp
		endTimestamp, err = tree.MakeDTimestamp(insight.Transaction.EndTime, time.Nanosecond)
		if err != nil {
			return err
		}

		autoRetryReason := tree.DNull
		if insight.Transaction.AutoRetryReason != "" {
			autoRetryReason = tree.NewDString(insight.Transaction.AutoRetryReason)
		}

		contentionTime := tree.DNull
		if insight.Transaction.Contention != nil {
			contentionTime = tree.NewDInterval(
				duration.MakeDuration(insight.Transaction.Contention.Nanoseconds(), 0, 0),
				types.DefaultIntervalTypeMetadata,
			)
		}

		stmtIDs := tree.NewDArray(types.String)
		for _, id := range insight.Transaction.StmtExecutionIDs {
			if err = stmtIDs.Append(tree.NewDString(hex.EncodeToString(id.GetBytes()))); err != nil {
				return err
			}
		}

		err = errors.CombineErrors(err, addRow(
			tree.NewDUuid(tree.DUuid{UUID: insight.Transaction.ID}),
			tree.NewDBytes(tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(insight.Transaction.FingerprintID)))),
			tree.NewDString(queryBuilder.String()),
			tree.MakeDBool(tree.DBool(insight.Transaction.ImplicitTxn)),
			tree.NewDString(hex.EncodeToString(insight.Session.ID.GetBytes())),
			startTimestamp,
			endTimestamp,
			tree.NewDString(insight.Transaction.User),
			tree.NewDString(insight.Transaction.ApplicationName),
			tree.NewDInt(tree.DInt(insight.Transaction.RowsRead)),
			tree.NewDInt(tree.DInt(insight.Transaction.RowsWritten)),
			tree.NewDString(insight.Transaction.UserPriority),
			tree.NewDInt(tree.DInt(insight.Transaction.RetryCount)),
			autoRetryReason,
			contentionTime,
			problems,
			causes,
			stmtIDs,
			tree.NewDInt(tree.DInt(insight.Transaction.CPUSQLNanos)),
			errorCode,
			errorMsg,
			tree.NewDString(insight.Transaction.Status.String()),
		))

		if err != nil {
			return err
		}
	}
	return
}

// This is the table structure for both cluster_execution_insights and node_execution_insights.
// Both contain statement execution insights.
const executionInsightsSchemaPattern = `
CREATE TABLE crdb_internal.%s (
	session_id                 STRING NOT NULL,
	txn_id                     UUID NOT NULL,
	txn_fingerprint_id         BYTES NOT NULL,
	stmt_id                    STRING NOT NULL,
	stmt_fingerprint_id        BYTES NOT NULL,
	problem                    STRING NOT NULL,
	causes                     STRING[] NOT NULL,
	query                      STRING NOT NULL,
	status                     STRING NOT NULL,
	start_time                 TIMESTAMP NOT NULL,
	end_time                   TIMESTAMP NOT NULL,
	full_scan                  BOOL NOT NULL,
	user_name                  STRING NOT NULL,
	app_name                   STRING NOT NULL,
	database_name              STRING NOT NULL,
	plan_gist                  STRING NOT NULL,
	rows_read                  INT8 NOT NULL,
	rows_written               INT8 NOT NULL,
	priority                   STRING NOT NULL,
	retries                    INT8 NOT NULL,
	last_retry_reason          STRING,
	exec_node_ids              INT[] NOT NULL,
	kv_node_ids                INT[] NOT NULL,
	contention                 INTERVAL,
	index_recommendations      STRING[] NOT NULL,
	implicit_txn               BOOL NOT NULL,
	cpu_sql_nanos              INT8,
	error_code                 STRING,
	last_error_redactable      STRING
)`

var crdbInternalClusterExecutionInsightsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(executionInsightsSchemaPattern, "cluster_execution_insights"),
	comment: `Cluster-wide statement execution insights`,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (err error) {
		return populateStmtInsights(ctx, p, addRow, &serverpb.ListExecutionInsightsRequest{})
	},
}

var crdbInternalNodeExecutionInsightsTable = virtualSchemaTable{
	schema:  fmt.Sprintf(executionInsightsSchemaPattern, "node_execution_insights"),
	comment: `Node statement execution insights`,
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (err error) {
		return populateStmtInsights(ctx, p, addRow, &serverpb.ListExecutionInsightsRequest{NodeID: "local"})
	},
}

func noViewActivityOrViewActivityRedactedRoleError(user username.SQLUsername) error {
	return pgerror.Newf(
		pgcode.InsufficientPrivilege,
		"user %s does not have %s or %s privilege",
		user,
		roleoption.VIEWACTIVITY,
		roleoption.VIEWACTIVITYREDACTED,
	)
}

func populateStmtInsights(
	ctx context.Context,
	p *planner,
	addRow func(...tree.Datum) error,
	request *serverpb.ListExecutionInsightsRequest,
) (err error) {
	// Check if the user has sufficient privileges.
	hasPrivs, shouldRedactError, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
	if err != nil {
		return err
	} else if !hasPrivs {
		return noViewActivityOrViewActivityRedactedRoleError(p.User())
	}

	acct := p.Mon().MakeBoundAccount()
	defer acct.Close(ctx)

	response, err := p.extendedEvalCtx.SQLStatusServer.ListExecutionInsights(ctx, request)
	if err != nil {
		return err
	}

	if err := acct.Grow(ctx, int64(response.Size())); err != nil {
		return err
	}

	for _, insight := range response.Insights {
		// We don't expect the transaction to be null here, but we should provide
		// this check to ensure we only show valid data.
		if insight.Transaction == nil {
			continue
		}

		for _, s := range insight.Statements {

			causes := tree.NewDArray(types.String)
			for _, cause := range s.Causes {
				if err = causes.Append(tree.NewDString(cause.String())); err != nil {
					return err
				}
			}

			var startTimestamp *tree.DTimestamp
			startTimestamp, err = tree.MakeDTimestamp(s.StartTime, time.Nanosecond)
			if err != nil {
				return err
			}

			var endTimestamp *tree.DTimestamp
			endTimestamp, err = tree.MakeDTimestamp(s.EndTime, time.Nanosecond)
			if err != nil {
				return err
			}

			execNodeIDs := tree.NewDArray(types.Int)
			for _, nodeID := range s.Nodes {
				if err = execNodeIDs.Append(tree.NewDInt(tree.DInt(nodeID))); err != nil {
					return err
				}
			}
			kvNodeIDs := tree.NewDArray(types.Int)
			for _, kvNodeID := range s.KVNodeIDs {
				if err = kvNodeIDs.Append(tree.NewDInt(tree.DInt(kvNodeID))); err != nil {
					return err
				}
			}

			autoRetryReason := tree.DNull
			if s.AutoRetryReason != "" {
				autoRetryReason = tree.NewDString(s.AutoRetryReason)
			}

			contentionTime := tree.DNull
			if s.Contention != nil {
				contentionTime = tree.NewDInterval(
					duration.MakeDuration(s.Contention.Nanoseconds(), 0, 0),
					types.DefaultIntervalTypeMetadata,
				)
			}

			indexRecommendations := tree.NewDArray(types.String)
			for _, recommendation := range s.IndexRecommendations {
				if err = indexRecommendations.Append(tree.NewDString(recommendation)); err != nil {
					return err
				}
			}

			errorCode := tree.DNull
			errorMsg := tree.DNull
			if s.ErrorCode != "" {
				errorCode = tree.NewDString(s.ErrorCode)
				if shouldRedactError {
					errorMsg = tree.NewDString(string(s.ErrorMsg.Redact()))
				} else {
					errorMsg = tree.NewDString(string(s.ErrorMsg))
				}
			}

			err = errors.CombineErrors(err, addRow(
				tree.NewDString(hex.EncodeToString(insight.Session.ID.GetBytes())),
				tree.NewDUuid(tree.DUuid{UUID: insight.Transaction.ID}),
				tree.NewDBytes(tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(insight.Transaction.FingerprintID)))),
				tree.NewDString(hex.EncodeToString(s.ID.GetBytes())),
				tree.NewDBytes(tree.DBytes(sqlstatsutil.EncodeUint64ToBytes(uint64(s.FingerprintID)))),
				tree.NewDString(s.Problem.String()),
				causes,
				tree.NewDString(s.Query),
				tree.NewDString(s.Status.String()),
				startTimestamp,
				endTimestamp,
				tree.MakeDBool(tree.DBool(s.FullScan)),
				tree.NewDString(insight.Transaction.User),
				tree.NewDString(insight.Transaction.ApplicationName),
				tree.NewDString(s.Database),
				tree.NewDString(s.PlanGist),
				tree.NewDInt(tree.DInt(s.RowsRead)),
				tree.NewDInt(tree.DInt(s.RowsWritten)),
				tree.NewDString(insight.Transaction.UserPriority),
				tree.NewDInt(tree.DInt(s.Retries)),
				autoRetryReason,
				execNodeIDs,
				kvNodeIDs,
				contentionTime,
				indexRecommendations,
				tree.MakeDBool(tree.DBool(insight.Transaction.ImplicitTxn)),
				tree.NewDInt(tree.DInt(s.CPUSQLNanos)),
				errorCode,
				errorMsg,
			))
		}
	}
	return
}

// getContentionEventInfo performs a best-effort decoding of the key on which
// the contention occurred into the corresponding db, schema, table, and index
// names. If the key doesn't belong to the SQL data, then returned strings will
// contain a hint about that.
// TODO(#101826): we should teach this function to properly handle non-SQL keys.
func getContentionEventInfo(
	ctx context.Context, p *planner, contentionEvent contentionpb.ExtendedContentionEvent,
) (dbName, schemaName, tableName, indexName string) {
	// Strip the tenant prefix right away if present.
	key, err := p.ExecCfg().Codec.StripTenantPrefix(contentionEvent.BlockingEvent.Key)
	if err != nil {
		// We really don't want to return errors, so we'll include the error
		// details as the table name.
		tableName = err.Error()
		return "", "", tableName, ""
	}
	if keys.TableDataMin.Compare(key) > 0 || keys.TableDataMax.Compare(key) < 0 {
		// Non-SQL keys are handled separately.
		tableName = fmt.Sprintf("%q", key)
		return "", "", tableName, ""
	}
	_, tableID, indexID, err := keys.DecodeTableIDIndexID(key)
	if err != nil {
		// We really don't want to return errors, so we'll include the error
		// details in the table name.
		tableName = err.Error()
		return "", "", tableName, ""
	}

	desc := p.Descriptors()
	var tableDesc catalog.TableDescriptor
	tableDesc, err = desc.ByIDWithLeased(p.txn).WithoutNonPublic().Get().Table(ctx, descpb.ID(tableID))
	if err != nil {
		return "", "", fmt.Sprintf("[dropped table id: %d]", tableID), "[dropped index]" //nolint:returnerrcheck
	}

	idxDesc, err := catalog.MustFindIndexByID(tableDesc, descpb.IndexID(indexID))
	if err != nil {
		indexName = fmt.Sprintf("[dropped index id: %d]", indexID)
	} else if idxDesc != nil {
		indexName = idxDesc.GetName()
	}

	dbDesc, err := desc.ByIDWithLeased(p.txn).WithoutNonPublic().Get().Database(ctx, tableDesc.GetParentID())
	if err != nil {
		dbName = "[dropped database]"
	} else if dbDesc != nil {
		dbName = dbDesc.GetName()
	}

	schemaDesc, err := desc.ByIDWithLeased(p.txn).WithoutNonPublic().Get().Schema(ctx, tableDesc.GetParentSchemaID())
	if err != nil {
		schemaName = "[dropped schema]"
	} else if schemaDesc != nil {
		schemaName = schemaDesc.GetName()
	}

	return dbName, schemaName, tableDesc.GetName(), indexName
}

var crdbInternalNodeMemoryMonitors = virtualSchemaTable{
	comment: `node-level table listing all currently active memory monitors`,
	schema: `
CREATE TABLE crdb_internal.node_memory_monitors (
  level             INT8,
  name              STRING,
  id                INT8,
  parent_id         INT8,
  used              INT8,
  reserved_used     INT8,
  reserved_reserved INT8,
  stopped           BOOL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// The memory monitors' names can expose some information about the
		// activity on the node, so we require VIEWACTIVITY or
		// VIEWACTIVITYREDACTED permissions.
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		monitorStateCb := func(monitor mon.MonitorState) error {
			return addRow(
				tree.NewDInt(tree.DInt(monitor.Level)),
				tree.NewDString(monitor.Name.String()),
				tree.NewDInt(tree.DInt(monitor.ID)),
				tree.NewDInt(tree.DInt(monitor.ParentID)),
				tree.NewDInt(tree.DInt(monitor.Used)),
				tree.NewDInt(tree.DInt(monitor.ReservedUsed)),
				tree.NewDInt(tree.DInt(monitor.ReservedReserved)),
				tree.MakeDBool(tree.DBool(monitor.Stopped)),
			)
		}
		return p.extendedEvalCtx.ExecCfg.RootMemoryMonitor.TraverseTree(monitorStateCb)
	},
}

var crdbInternalShowTenantCapabilitiesCache = virtualSchemaTable{
	comment: `eventually consistent in-memory tenant capability cache for this node`,
	schema: `
CREATE TABLE crdb_internal.node_tenant_capabilities_cache (
  tenant_id        INT,
  capability_name  STRING,
  capability_value STRING
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		const op = "node_tenant_capabilities_cache"
		if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.VIEWCLUSTERMETADATA); err != nil {
			return err
		}
		tenantCapabilitiesReader, err := p.ExecCfg().TenantCapabilitiesReader.Get(op)
		if err != nil {
			return err
		}
		type tenantCapabilitiesEntry struct {
			tenantID           roachpb.TenantID
			tenantCapabilities *tenantcapabilitiespb.TenantCapabilities
		}
		tenantCapabilitiesMap := tenantCapabilitiesReader.GetGlobalCapabilityState()
		tenantCapabilitiesEntries := make([]tenantCapabilitiesEntry, 0, len(tenantCapabilitiesMap))
		for tenantID, tenantCapabilities := range tenantCapabilitiesMap {
			tenantCapabilitiesEntries = append(
				tenantCapabilitiesEntries,
				tenantCapabilitiesEntry{
					tenantID:           tenantID,
					tenantCapabilities: tenantCapabilities,
				},
			)
		}
		// Sort by tenant ID.
		sort.Slice(tenantCapabilitiesEntries, func(i, j int) bool {
			return tenantCapabilitiesEntries[i].tenantID.ToUint64() <
				tenantCapabilitiesEntries[j].tenantID.ToUint64()
		})
		for _, tenantCapabilitiesEntry := range tenantCapabilitiesEntries {
			tenantID := tree.NewDInt(tree.DInt(tenantCapabilitiesEntry.tenantID.ToUint64()))
			for _, capabilityID := range tenantcapabilities.IDs {
				value := tenantcapabilities.MustGetValueByID(
					tenantCapabilitiesEntry.tenantCapabilities,
					capabilityID,
				)
				if err := addRow(
					tenantID,
					tree.NewDString(capabilityID.String()),
					tree.NewDString(value.String()),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

var crdbInternalInheritedRoleMembers = virtualSchemaTable{
	comment: `table listing transitive closure of system.role_members`,
	schema: `
CREATE TABLE crdb_internal.kv_inherited_role_members (
  role               STRING,
  inheriting_member  STRING,
  member_is_explicit BOOL,
  member_is_admin    BOOL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (retErr error) {
		// Populate explicitMemberships with the contents of system.role_members.
		// This data structure maps each role to the set of roles it's explicitly
		// a member of. Consider for example:
		//     CREATE ROLE a;
		//     CREATE ROLE b;
		//     CREATE ROLE c;
		//     GRANT a TO b;
		//     GRANT b TO c;
		// The role `a` is explicitly a member of `b` and the role `c` is
		// explicitly a member of `b`. The role `c` is also a member of `a`,
		// but not explicitly, because it inherits the membership through `b`.
		explicitMemberships := make(map[username.SQLUsername]map[username.SQLUsername]bool)
		if err := forEachRoleMembershipAtCacheReadTS(ctx, p, func(ctx context.Context, role, member username.SQLUsername, isAdmin bool) error {
			if _, found := explicitMemberships[member]; !found {
				explicitMemberships[member] = make(map[username.SQLUsername]bool)
			}
			explicitMemberships[member][role] = true
			return nil
		}); err != nil {
			return err
		}
		// Iterate through all members in order for stability.
		members := make([]username.SQLUsername, 0, len(explicitMemberships))
		for m := range explicitMemberships {
			members = append(members, m)
		}
		sort.Slice(members, func(i, j int) bool {
			return members[i].Normalized() < members[j].Normalized()
		})
		for _, member := range members {
			memberOf, err := p.MemberOfWithAdminOption(ctx, member)
			if err != nil {
				return err
			}
			explicitRoles := explicitMemberships[member]
			// Iterate through all roles in order for stability.
			roles := make([]username.SQLUsername, 0, len(memberOf))
			for r := range memberOf {
				roles = append(roles, r)
			}
			sort.Slice(roles, func(i, j int) bool {
				return roles[i].Normalized() < roles[j].Normalized()
			})
			for _, r := range roles {
				if err := addRow(
					tree.NewDString(r.Normalized()),              // role
					tree.NewDString(member.Normalized()),         // inheriting_member
					tree.MakeDBool(tree.DBool(explicitRoles[r])), // member_is_explicit
					tree.MakeDBool(tree.DBool(memberOf[r])),      // member_is_admin
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

func resultColsFromColDescs(colDescs []descpb.ColumnDescriptor) colinfo.ResultColumns {
	result := make(colinfo.ResultColumns, 0, len(colDescs))
	for _, colDesc := range colDescs {
		result = append(result, colinfo.ResultColumn{
			Name: colDesc.Name,
			Typ:  colDesc.Type,
		})
	}
	return result
}

var crdbInternalKVSystemPrivileges = virtualSchemaView{
	comment: `this vtable is a view on system.privileges with the root user and is used to back SHOW SYSTEM GRANTS`,
	schema: `
CREATE VIEW crdb_internal.kv_system_privileges AS
SELECT username, path,
       crdb_internal.privilege_name(privileges) AS privileges,
       crdb_internal.privilege_name(grant_options) AS grant_options,
       user_id
FROM system.privileges;`,
	resultColumns: resultColsFromColDescs(systemschema.SystemPrivilegeTable.TableDesc().Columns),
}

var crdbInternalKVFlowController = virtualSchemaTable{
	comment: `node-level view of the kv flow controller, its active streams and available tokens state`,
	schema: `
CREATE TABLE crdb_internal.kv_flow_controller (
  tenant_id                INT NOT NULL,
  store_id                 INT NOT NULL,
  available_regular_tokens INT NOT NULL,
  available_elastic_tokens INT NOT NULL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowController(ctx, &kvflowinspectpb.ControllerRequest{})
		if err != nil {
			return err
		}
		for _, stream := range resp.Streams {
			if err := addRow(
				tree.NewDInt(tree.DInt(stream.TenantID.ToUint64())),
				tree.NewDInt(tree.DInt(stream.StoreID)),
				tree.NewDInt(tree.DInt(stream.AvailableEvalRegularTokens)),
				tree.NewDInt(tree.DInt(stream.AvailableEvalElasticTokens)),
			); err != nil {
				return err
			}
		}
		return nil
	},
}
var crdbInternalKVFlowControllerV2 = virtualSchemaTable{
	comment: `node-level view of the kv flow controller v2, its active streams and available tokens state`,
	schema: `
CREATE TABLE crdb_internal.kv_flow_controller_v2 (
  tenant_id                     INT NOT NULL,
  store_id                      INT NOT NULL,
  available_eval_regular_tokens INT NOT NULL,
  available_eval_elastic_tokens INT NOT NULL,
  available_send_regular_tokens INT NOT NULL,
  available_send_elastic_tokens INT NOT NULL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowControllerV2(ctx, &kvflowinspectpb.ControllerRequest{})
		if err != nil {
			return err
		}
		for _, stream := range resp.Streams {
			if err := addRow(
				tree.NewDInt(tree.DInt(stream.TenantID.ToUint64())),
				tree.NewDInt(tree.DInt(stream.StoreID)),
				tree.NewDInt(tree.DInt(stream.AvailableEvalRegularTokens)),
				tree.NewDInt(tree.DInt(stream.AvailableEvalElasticTokens)),
				tree.NewDInt(tree.DInt(stream.AvailableSendRegularTokens)),
				tree.NewDInt(tree.DInt(stream.AvailableSendElasticTokens)),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var crdbInternalKVFlowHandles = virtualSchemaTable{
	comment: `node-level view of active kv flow control handles, their underlying streams, and tracked state`,
	schema: `
CREATE TABLE crdb_internal.kv_flow_control_handles (
  range_id                 INT NOT NULL,
  tenant_id                INT NOT NULL,
  store_id                 INT NOT NULL,
  total_tracked_tokens     INT NOT NULL,
  INDEX(range_id)
);`,

	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, constraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
				if err != nil {
					return false, err
				}
				if !hasRoleOption {
					return false, noViewActivityOrViewActivityRedactedRoleError(p.User())
				}

				rangeID := roachpb.RangeID(tree.MustBeDInt(constraint))
				resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandles(
					ctx, &kvflowinspectpb.HandlesRequest{
						RangeIDs: []roachpb.RangeID{rangeID},
					})
				if err != nil {
					return false, err
				}
				return true, populateFlowHandlesResponse(resp, addRow)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandles(ctx, &kvflowinspectpb.HandlesRequest{})
		if err != nil {
			return err
		}
		return populateFlowHandlesResponse(resp, addRow)
	},
}
var crdbInternalKVFlowHandlesV2 = virtualSchemaTable{
	comment: `node-level view of active kv flow range controllers, their underlying streams, and tracked state`,
	schema: `
CREATE TABLE crdb_internal.kv_flow_control_handles_v2 (
  range_id                   INT NOT NULL,
  tenant_id                  INT NOT NULL,
  store_id                   INT NOT NULL,
  total_tracked_tokens       INT NOT NULL,
  total_eval_deducted_tokens INT NOT NULL,
  total_send_deducted_tokens INT NOT NULL,
  INDEX(range_id)
);`,

	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, constraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
				if err != nil {
					return false, err
				}
				if !hasRoleOption {
					return false, noViewActivityOrViewActivityRedactedRoleError(p.User())
				}

				rangeID := roachpb.RangeID(tree.MustBeDInt(constraint))
				resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandlesV2(
					ctx, &kvflowinspectpb.HandlesRequest{
						RangeIDs: []roachpb.RangeID{rangeID},
					})
				if err != nil {
					return false, err
				}
				return true, populateFlowHandlesResponseV2(resp, addRow)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandlesV2(ctx, &kvflowinspectpb.HandlesRequest{})
		if err != nil {
			return err
		}
		return populateFlowHandlesResponseV2(resp, addRow)
	},
}

func populateFlowHandlesResponse(
	resp *kvflowinspectpb.HandlesResponse, addRow func(...tree.Datum) error,
) error {
	for _, handle := range resp.Handles {
		for _, connected := range handle.ConnectedStreams {
			totalTrackedTokens := int64(0)
			for _, tracked := range connected.TrackedDeductions {
				totalTrackedTokens += tracked.Tokens
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(handle.RangeID)),
				tree.NewDInt(tree.DInt(connected.Stream.TenantID.ToUint64())),
				tree.NewDInt(tree.DInt(connected.Stream.StoreID)),
				tree.NewDInt(tree.DInt(totalTrackedTokens)),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func populateFlowHandlesResponseV2(
	resp *kvflowinspectpb.HandlesResponse, addRow func(...tree.Datum) error,
) error {
	for _, handle := range resp.Handles {
		for _, connected := range handle.ConnectedStreams {
			totalTrackedTokens := int64(0)
			for _, tracked := range connected.TrackedDeductions {
				totalTrackedTokens += tracked.Tokens
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(handle.RangeID)),
				tree.NewDInt(tree.DInt(connected.Stream.TenantID.ToUint64())),
				tree.NewDInt(tree.DInt(connected.Stream.StoreID)),
				tree.NewDInt(tree.DInt(totalTrackedTokens)),
				tree.NewDInt(tree.DInt(connected.TotalEvalDeductedTokens)),
				tree.NewDInt(tree.DInt(connected.TotalSendDeductedTokens)),
			); err != nil {
				return err
			}
		}
	}
	return nil
}

var crdbInternalKVFlowTokenDeductions = virtualSchemaTable{
	comment: `node-level view of tracked kv flow tokens`,
	schema: `
CREATE TABLE crdb_internal.kv_flow_token_deductions (
  range_id  INT NOT NULL,
  tenant_id INT NOT NULL,
  store_id  INT NOT NULL,
  priority  STRING NOT NULL,
  log_term  INT NOT NULL,
  log_index INT NOT NULL,
  tokens    INT NOT NULL,
  INDEX(range_id)
);`,

	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, constraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
				if err != nil {
					return false, err
				}
				if !hasRoleOption {
					return false, noViewActivityOrViewActivityRedactedRoleError(p.User())
				}

				rangeID := roachpb.RangeID(tree.MustBeDInt(constraint))
				resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandles(
					ctx, &kvflowinspectpb.HandlesRequest{
						RangeIDs: []roachpb.RangeID{rangeID},
					})
				if err != nil {
					return false, err
				}
				return true, populateFlowTokensResponse(resp, addRow)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandles(ctx, &kvflowinspectpb.HandlesRequest{})
		if err != nil {
			return err
		}
		return populateFlowTokensResponse(resp, addRow)
	},
}

var crdbInternalKVFlowTokenDeductionsV2 = virtualSchemaTable{
	comment: `node-level view of tracked kv flow tokens`,
	schema: `
CREATE TABLE crdb_internal.kv_flow_token_deductions_v2 (
  range_id  INT NOT NULL,
  tenant_id INT NOT NULL,
  store_id  INT NOT NULL,
  priority  STRING NOT NULL,
  log_term  INT NOT NULL,
  log_index INT NOT NULL,
  tokens    INT NOT NULL,
  INDEX(range_id)
);`,

	indexes: []virtualIndex{
		{
			populate: func(ctx context.Context, constraint tree.Datum, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
				hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
				if err != nil {
					return false, err
				}
				if !hasRoleOption {
					return false, noViewActivityOrViewActivityRedactedRoleError(p.User())
				}

				rangeID := roachpb.RangeID(tree.MustBeDInt(constraint))
				resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandlesV2(
					ctx, &kvflowinspectpb.HandlesRequest{
						RangeIDs: []roachpb.RangeID{rangeID},
					})
				if err != nil {
					return false, err
				}
				return true, populateFlowTokensResponse(resp, addRow)
			},
		},
	},
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.KVFlowHandlesV2(ctx, &kvflowinspectpb.HandlesRequest{})
		if err != nil {
			return err
		}
		return populateFlowTokensResponse(resp, addRow)
	},
}

func populateFlowTokensResponse(
	resp *kvflowinspectpb.HandlesResponse, addRow func(...tree.Datum) error,
) error {
	for _, handle := range resp.Handles {
		for _, connected := range handle.ConnectedStreams {
			for _, deduction := range connected.TrackedDeductions {
				if err := addRow(
					tree.NewDInt(tree.DInt(handle.RangeID)),
					tree.NewDInt(tree.DInt(connected.Stream.TenantID.ToUint64())),
					tree.NewDInt(tree.DInt(connected.Stream.StoreID)),
					tree.NewDString(admissionpb.WorkPriority(deduction.Priority).String()),
					tree.NewDInt(tree.DInt(deduction.RaftLogPosition.Term)),
					tree.NewDInt(tree.DInt(deduction.RaftLogPosition.Index)),
					tree.NewDInt(tree.DInt(deduction.Tokens)),
				); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

var crdbInternalClusterReplicationResolvedView = virtualSchemaView{
	schema: `
		CREATE VIEW crdb_internal.cluster_replication_spans AS WITH spans AS (
			SELECT
				j.id AS job_id, jsonb_array_elements(crdb_internal.pb_to_json('progress', i.value)->'streamIngest'->'checkpoint'->'resolvedSpans') AS s
			FROM system.jobs j LEFT JOIN system.job_info i ON j.id = i.job_id AND i.info_key = 'legacy_progress'
			WHERE j.job_type = 'REPLICATION STREAM INGESTION'
			) SELECT
				job_id,
				crdb_internal.pretty_key(decode(s->'span'->>'key', 'base64'), 0) AS start_key,
				crdb_internal.pretty_key(decode(s->'span'->>'endKey', 'base64'), 0) AS end_key,
				((s->'timestamp'->>'wallTime')||'.'||COALESCE((s->'timestamp'->'logical'), '0'))::decimal AS resolved,
				date_trunc('second', ((cluster_logical_timestamp() - (s->'timestamp'->>'wallTime')::int) /1e9)::interval) AS resolved_age
			FROM spans`,
	resultColumns: colinfo.ResultColumns{
		{Name: "job_id", Typ: types.Int},
		{Name: "start_key", Typ: types.String},
		{Name: "end_key", Typ: types.String},
		{Name: "resolved", Typ: types.Decimal},
		{Name: "resolved_age", Typ: types.Interval},
	},
}

var crdbInternalLogicalReplicationResolvedView = virtualSchemaView{
	schema: `
		CREATE VIEW crdb_internal.logical_replication_spans AS WITH spans AS (
			SELECT
				j.id AS job_id, jsonb_array_elements(crdb_internal.pb_to_json('progress', i.value)->'LogicalReplication'->'checkpoint'->'resolvedSpans') AS s
			FROM system.jobs j LEFT JOIN system.job_info i ON j.id = i.job_id AND i.info_key = 'legacy_progress'
			WHERE j.job_type = 'LOGICAL REPLICATION'
			) SELECT
				job_id,
				crdb_internal.pretty_key(decode(s->'span'->>'key', 'base64'), 0) AS start_key,
				crdb_internal.pretty_key(decode(s->'span'->>'endKey', 'base64'), 0) AS end_key,
				((s->'timestamp'->>'wallTime')||'.'||COALESCE((s->'timestamp'->'logical'), '0'))::decimal AS resolved,
				date_trunc('second', ((cluster_logical_timestamp() - (s->'timestamp'->>'wallTime')::int) /1e9)::interval) AS resolved_age
			FROM spans`,
	resultColumns: colinfo.ResultColumns{
		{Name: "job_id", Typ: types.Int},
		{Name: "start_key", Typ: types.String},
		{Name: "end_key", Typ: types.String},
		{Name: "resolved", Typ: types.Decimal},
		{Name: "resolved_age", Typ: types.Interval},
	},
}

var crdbInternalPCRStreamsTable = virtualSchemaTable{
	comment: `node-level table listing all currently running cluster replication production streams`,
	// NB: startTS is exclusive; consider renaming to startAfter.
	schema: `
CREATE TABLE crdb_internal.cluster_replication_node_streams (
	stream_id INT,
	consumer STRING,
	spans INT,
	initial_ts DECIMAL NOT VISIBLE,
	prev_ts DECIMAL NOT VISIBLE,
	state STRING,
	read INTERVAL,
	emit INTERVAL,
	last_read_ms INT,
	last_emit_ms INT,
	seq INT,
	chkpts INT,
	last_chkpt INTERVAL,
	batches INT,
	batches_full INT NOT VISIBLE,
	batches_ready INT NOT VISIBLE,
	batches_checkpoint INT NOT VISIBLE,
	megabytes INT,
	last_kb INT,
	rf_chk INT,
	rf_adv INT,
	rf_last_adv INTERVAL,
	resolved DECIMAL NOT VISIBLE,
	resolved_age INTERVAL
	);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		sm, err := p.EvalContext().StreamManagerFactory.GetReplicationStreamManager(ctx)
		if err != nil {
			// A non-CCL binary can't have anything to inspect so just return empty.
			if err.Error() == "replication streaming requires a CCL binary" {
				return nil
			}
			return err
		}
		now := p.EvalContext().GetStmtTimestamp()

		// Transform `.0000000000` to `.0` to shorted/de-noise HLCs.
		shortenLogical := func(d *tree.DDecimal) *tree.DDecimal {
			var tmp apd.Decimal
			d.Modf(nil, &tmp)
			if tmp.IsZero() {
				if _, err := tree.DecimalCtx.Quantize(&tmp, &d.Decimal, -1); err == nil {
					d.Decimal = tmp
				}
			}
			return d
		}

		dur := func(nanos int64) tree.Datum {
			// Round to seconds to save some width.
			return tree.NewDInterval(duration.MakeDuration((nanos/1e9)*1e9, 0, 0), types.DefaultIntervalTypeMetadata)
		}
		age := func(t time.Time) tree.Datum {
			if t.Unix() == 0 {
				return tree.DNull
			}
			return dur(now.Sub(t).Nanoseconds())
		}

		for _, s := range sm.DebugGetProducerStatuses(ctx) {
			resolved := time.UnixMicro(s.RF.ResolvedMicros)
			resolvedDatum := tree.DNull
			if resolved.Unix() != 0 {
				resolvedDatum = shortenLogical(eval.TimestampToDecimalDatum(hlc.Timestamp{WallTime: resolved.UnixNano()}))
			}

			curState := s.State
			currentWait := duration.Age(now, time.UnixMicro(s.LastPolledMicros)).Nanos()

			curOrLast := func(currentNanos int64, lastNanos int64, statePredicate streampb.ProducerState) tree.Datum {
				if curState == statePredicate {
					return tree.NewDInt(tree.DInt(time.Duration(currentNanos).Milliseconds()))
				}
				return tree.NewDInt(tree.DInt(time.Duration(lastNanos).Milliseconds()))
			}
			currentWaitWithState := func(statePredicate streampb.ProducerState) int64 {
				if curState == statePredicate {
					return currentWait
				}
				return 0
			}
			flushFull, flushReady, flushCheckpoint := s.Flushes.Full, s.Flushes.Ready, s.Flushes.Forced

			if err := addRow(
				tree.NewDInt(tree.DInt(s.StreamID)),                                              // stream_id
				tree.NewDString(fmt.Sprintf("%d[%d]", s.Spec.ConsumerNode, s.Spec.ConsumerProc)), // consumer
				tree.NewDInt(tree.DInt(len(s.Spec.Spans))),                                       // spans
				shortenLogical(eval.TimestampToDecimalDatum(s.Spec.InitialScanTimestamp)),        // initial_ts
				shortenLogical(eval.TimestampToDecimalDatum(s.Spec.PreviousReplicatedTimestamp)), //prev_ts
				tree.NewDString(curState.String()),                                               // state
				dur(s.Flushes.ProduceWaitNanos+currentWaitWithState(streampb.Producing)),         // read
				dur(s.Flushes.EmitWaitNanos+currentWaitWithState(streampb.Emitting)),             // emit
				curOrLast(currentWait, s.Flushes.LastProduceWaitNanos, streampb.Producing),       // last_read_ms
				curOrLast(currentWait, s.Flushes.LastEmitWaitNanos, streampb.Emitting),           // last_emit_ms
				tree.NewDInt(tree.DInt(s.SeqNum)),                                                // seq
				tree.NewDInt(tree.DInt(s.Flushes.Checkpoints)),                                   // checkpoints
				age(time.UnixMicro(s.LastCheckpoint.Micros)),                                     // last_chkpt
				tree.NewDInt(tree.DInt(s.Flushes.Batches)),                                       // batches
				tree.NewDInt(tree.DInt(flushFull)),                                               // batches_full
				tree.NewDInt(tree.DInt(flushReady)),                                              // batches_ready
				tree.NewDInt(tree.DInt(flushCheckpoint)),                                         // batches_checkpoint
				tree.NewDInt(tree.DInt(s.Flushes.Bytes)/(1<<20)),                                 // megabytes
				tree.NewDInt(tree.DInt(s.Flushes.LastSize/1024)),                                 // last_kb
				tree.NewDInt(tree.DInt(s.RF.Checkpoints)),                                        // rf_chk
				tree.NewDInt(tree.DInt(s.RF.Advances)),                                           // rf_adv
				age(time.UnixMicro(s.RF.LastAdvanceMicros)),                                      // rf_last_adv
				resolvedDatum, // resolved not visible.
				age(resolved), // resolved_age.
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var crdbInternalPCRStreamSpansTable = virtualSchemaTable{
	comment: `node-level table listing all currently running cluster replication production stream spec spans`,
	schema: `
CREATE TABLE crdb_internal.cluster_replication_node_stream_spans (
	stream_id INT,
	consumer STRING,
	span_start STRING,
	span_end STRING
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		sm, err := p.EvalContext().StreamManagerFactory.GetReplicationStreamManager(ctx)
		if err != nil {
			// A non-CCL binary can't have anything to inspect so just return empty.
			if err.Error() == "replication streaming requires a CCL binary" {
				return nil
			}
			return err
		}
		for _, status := range sm.DebugGetProducerStatuses(ctx) {
			for _, s := range status.Spec.Spans {
				if err := addRow(
					tree.NewDInt(tree.DInt(status.StreamID)),
					tree.NewDString(fmt.Sprintf("%d[%d]", status.Spec.ConsumerNode, status.Spec.ConsumerProc)),
					tree.NewDString(keys.PrettyPrint(nil /* valDirs */, s.Key)),
					tree.NewDString(keys.PrettyPrint(nil /* valDirs */, s.EndKey)),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

var crdbInternalPCRStreamCheckpointsTable = virtualSchemaTable{
	comment: `node-level table listing all currently running cluster replication production stream checkpoint spans`,
	schema: `
CREATE TABLE crdb_internal.cluster_replication_node_stream_checkpoints (
	stream_id INT,
	consumer STRING,
	span_start STRING,
	span_end STRING,
	resolved DECIMAL,
	resolved_age INTERVAL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		sm, err := p.EvalContext().StreamManagerFactory.GetReplicationStreamManager(ctx)
		if err != nil {
			// A non-CCL binary can't have anything to inspect so just return empty.
			if err.Error() == "replication streaming requires a CCL binary" {
				return nil
			}
			return err
		}
		for _, status := range sm.DebugGetProducerStatuses(ctx) {
			for _, s := range status.LastCheckpoint.Spans {
				if err := addRow(
					tree.NewDInt(tree.DInt(status.StreamID)),
					tree.NewDString(fmt.Sprintf("%d[%d]", status.Spec.ConsumerNode, status.Spec.ConsumerProc)),
					tree.NewDString(keys.PrettyPrint(nil /* valDirs */, s.Span.Key)),
					tree.NewDString(keys.PrettyPrint(nil /* valDirs */, s.Span.EndKey)),
					eval.TimestampToDecimalDatum(s.Timestamp),
					tree.NewDInterval(duration.Age(
						p.EvalContext().GetStmtTimestamp(), s.Timestamp.GoTime()), types.DefaultIntervalTypeMetadata,
					),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}
var crdbInternalLDRProcessorTable = virtualSchemaTable{
	comment: `node-level table listing all currently running logical replication writer processors`,
	schema: `
CREATE TABLE crdb_internal.logical_replication_node_processors (
	stream_id INT,
	consumer STRING,
	state STRING,
	recv_time INTERVAL,
	last_recv_time INTERVAL,
	ingest_time INTERVAL,
	flush_time INTERVAL,
	flush_count INT,
	flush_kvs INT,
	flush_bytes INT,
	flush_batches INT,
	last_flush_time INTERVAL,
	chunks_running INT,
	chunks_done INT,
	last_kvs_done INT,
	last_kvs_todo INT,
	last_batches INT,
	last_slowest INTERVAL,
	last_checkpoint INTERVAL,
	checkpoints INT,
	retry_size INT,
	resolved_age INTERVAL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		sm, err := p.EvalContext().StreamManagerFactory.GetReplicationStreamManager(ctx)
		if err != nil {
			// A non-CCL binary can't have anything to inspect so just return empty.
			if err.Error() == "replication streaming requires a CCL binary" {
				return nil
			}
			return err
		}
		now := p.EvalContext().GetStmtTimestamp()
		dur := func(nanos int64) tree.Datum {
			return tree.NewDInterval(duration.MakeDuration(nanos, 0, 0), types.DefaultIntervalTypeMetadata)
		}
		nanosSince := func(t time.Time) int64 {
			if t.IsZero() {
				return 0
			}
			return now.Sub(t).Nanoseconds()
		}
		age := func(t time.Time) tree.Datum {
			if t.Unix() == 0 {
				return tree.DNull
			}
			return tree.NewDInterval(duration.Age(now, t), types.DefaultIntervalTypeMetadata)
		}

		for _, container := range sm.DebugGetLogicalConsumerStatuses(ctx) {
			status := container.GetStats()
			curOrLast := func(currentNanos int64, lastNanos int64, currentState streampb.LogicalConsumerState) tree.Datum {
				if status.CurrentState == currentState {
					return dur(currentNanos)
				}
				return dur(lastNanos)
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(container.StreamID)),
				tree.NewDString(fmt.Sprintf("%d[%d]", p.extendedEvalCtx.ExecCfg.JobRegistry.ID(), container.ProcessorID)),
				tree.NewDString(status.CurrentState.String()),                                                                   // current_state
				dur(status.Recv.TotalWaitNanos+nanosSince(status.Recv.CurReceiveStart)),                                         // recv_time
				curOrLast(nanosSince(status.Recv.CurReceiveStart), status.Recv.LastWaitNanos, streampb.Waiting),                 // last_recv_time
				dur(status.Ingest.TotalIngestNanos+nanosSince(status.Ingest.CurIngestStart)),                                    // ingest_time
				dur(status.Flushes.Nanos+nanosSince(status.Flushes.Last.CurFlushStart)),                                         // flush_time
				tree.NewDInt(tree.DInt(status.Flushes.Count)),                                                                   // flush_count
				tree.NewDInt(tree.DInt(status.Flushes.KVs)),                                                                     // flush_kvs
				tree.NewDInt(tree.DInt(status.Flushes.Bytes)),                                                                   // flush_bytes
				tree.NewDInt(tree.DInt(status.Flushes.Batches)),                                                                 // flush_batches
				curOrLast(nanosSince(status.Flushes.Last.CurFlushStart), status.Flushes.Last.LastFlushNanos, streampb.Flushing), // last_flush_time
				tree.NewDInt(tree.DInt(status.Flushes.Last.ChunksRunning)),                                                      // chunks_running
				tree.NewDInt(tree.DInt(status.Flushes.Last.ChunksDone)),                                                         // chunks_done
				tree.NewDInt(tree.DInt(status.Flushes.Last.TotalKVs)),                                                           // last_kvs_done
				tree.NewDInt(tree.DInt(status.Flushes.Last.ProcessedKVs)),                                                       // last_kvs_todo
				tree.NewDInt(tree.DInt(status.Flushes.Last.Batches)),                                                            // last_batches
				dur(status.Flushes.Last.SlowestBatchNanos),                                                                      // last_slowest
				age(status.Checkpoints.LastCheckpoint),                                                                          // last_checkpoint
				tree.NewDInt(tree.DInt(status.Checkpoints.Count)),                                                               // checkpoints
				tree.NewDInt(tree.DInt(status.Purgatory.CurrentCount)),                                                          // retry_size
				age(status.Checkpoints.Resolved),                                                                                // resolved_age
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalFullyQualifiedNamesView is a view on system.namespace that
// provides fully qualified names for objects in the cluster. A row is only
// visible if the querying user has the CONNECT privilege on the database.
var crdbInternalFullyQualifiedNamesView = virtualSchemaView{
	schema: `
		CREATE VIEW crdb_internal.fully_qualified_names (
			object_id,
			schema_id,
			database_id,
			object_name,
			schema_name,
			database_name,
			fq_name
		) AS
			SELECT
				t.id, sc.id, db.id,
				t.name, sc.name, db.name,
				quote_ident(db.name) || '.' || quote_ident(sc.name) || '.' || quote_ident(t.name)
			FROM system.namespace t
			JOIN system.namespace sc ON t."parentSchemaID" = sc.id
			JOIN system.namespace db on t."parentID" = db.id
			-- Filter out the synthetic public schema for the system database.
			WHERE db."parentID" = 0
			-- Filter rows that the user should not be able to see. This check matches
			-- how metadata visibility works for pg_catalog tables.
			AND pg_catalog.has_database_privilege(db.name, 'CONNECT')`,
	resultColumns: colinfo.ResultColumns{
		{Name: "object_id", Typ: types.Int},
		{Name: "schema_id", Typ: types.Int},
		{Name: "database_id", Typ: types.Int},
		{Name: "object_name", Typ: types.String},
		{Name: "schema_name", Typ: types.String},
		{Name: "database_name", Typ: types.String},
		{Name: "fq_name", Typ: types.String},
	},
}

var crdbInternalStoreLivenessSupportFromTable = virtualSchemaTable{
	comment: `node-level view of store liveness support from other stores`,
	schema: `
CREATE TABLE crdb_internal.store_liveness_support_from (
  node_id               INT NOT NULL,
  store_id              INT NOT NULL,
  support_from_node_id  INT NOT NULL,
  support_from_store_id INT NOT NULL,
  support_epoch         INT NOT NULL,
  support_expiration    TIMESTAMP NOT NULL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.StoreLivenessSupportFrom(ctx, &slpb.InspectStoreLivenessRequest{})
		if err != nil {
			return err
		}
		return populateStoreLivenessSupportResponse(resp, addRow)
	},
}

var crdbInternalStoreLivenessSupportForTable = virtualSchemaTable{
	comment: `node-level view of store liveness support for other stores`,
	schema: `
CREATE TABLE crdb_internal.store_liveness_support_for (
  node_id               INT NOT NULL,
  store_id              INT NOT NULL,
  support_for_node_id  INT NOT NULL,
  support_for_store_id INT NOT NULL,
  support_epoch         INT NOT NULL,
  support_expiration    TIMESTAMP NOT NULL
);`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasRoleOption, _, err := p.HasViewActivityOrViewActivityRedactedRole(ctx)
		if err != nil {
			return err
		}
		if !hasRoleOption {
			return noViewActivityOrViewActivityRedactedRoleError(p.User())
		}

		resp, err := p.extendedEvalCtx.ExecCfg.InspectzServer.StoreLivenessSupportFor(ctx, &slpb.InspectStoreLivenessRequest{})
		if err != nil {
			return err
		}
		return populateStoreLivenessSupportResponse(resp, addRow)
	},
}

func populateStoreLivenessSupportResponse(
	resp *slpb.InspectStoreLivenessResponse, addRow func(...tree.Datum) error,
) error {
	for _, ssps := range resp.SupportStatesPerStore {
		for _, ss := range ssps.SupportStates {
			if err := addRow(
				tree.NewDInt(tree.DInt(ssps.StoreID.NodeID)),
				tree.NewDInt(tree.DInt(ssps.StoreID.StoreID)),
				tree.NewDInt(tree.DInt(ss.Target.NodeID)),
				tree.NewDInt(tree.DInt(ss.Target.StoreID)),
				tree.NewDInt(tree.DInt(ss.Epoch)),
				eval.TimestampToInexactDTimestamp(ss.Expiration),
			); err != nil {
				return err
			}
		}
	}
	return nil
}
