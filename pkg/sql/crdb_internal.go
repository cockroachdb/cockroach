// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catformat"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
)

// CrdbInternalName is the name of the crdb_internal schema.
const CrdbInternalName = catconstants.CRDBInternalSchemaName

// Naming convention:
// - if the response is served from memory, prefix with node_
// - if the response is served via a kv request, prefix with kv_
// - if the response is not from kv requests but is cluster-wide (i.e. the
//    answer isn't specific to the sql connection being used, prefix with cluster_.
//
// Adding something new here will require an update to `pkg/cli` for inclusion in
// a `debug zip`; the unit tests will guide you.
//
// Many existing tables don't follow the conventions above, but please apply
// them to future additions.
var crdbInternal = virtualSchema{
	name: CrdbInternalName,
	tableDefs: map[descpb.ID]virtualSchemaDef{
		catconstants.CrdbInternalBackwardDependenciesTableID:      crdbInternalBackwardDependenciesTable,
		catconstants.CrdbInternalBuildInfoTableID:                 crdbInternalBuildInfoTable,
		catconstants.CrdbInternalBuiltinFunctionsTableID:          crdbInternalBuiltinFunctionsTable,
		catconstants.CrdbInternalClusterContendedIndexesViewID:    crdbInternalClusterContendedIndexesView,
		catconstants.CrdbInternalClusterContendedKeysViewID:       crdbInternalClusterContendedKeysView,
		catconstants.CrdbInternalClusterContendedTablesViewID:     crdbInternalClusterContendedTablesView,
		catconstants.CrdbInternalClusterContentionEventsTableID:   crdbInternalClusterContentionEventsTable,
		catconstants.CrdbInternalClusterDistSQLFlowsTableID:       crdbInternalClusterDistSQLFlowsTable,
		catconstants.CrdbInternalClusterQueriesTableID:            crdbInternalClusterQueriesTable,
		catconstants.CrdbInternalClusterTransactionsTableID:       crdbInternalClusterTxnsTable,
		catconstants.CrdbInternalClusterSessionsTableID:           crdbInternalClusterSessionsTable,
		catconstants.CrdbInternalClusterSettingsTableID:           crdbInternalClusterSettingsTable,
		catconstants.CrdbInternalCreateStmtsTableID:               crdbInternalCreateStmtsTable,
		catconstants.CrdbInternalCreateTypeStmtsTableID:           crdbInternalCreateTypeStmtsTable,
		catconstants.CrdbInternalDatabasesTableID:                 crdbInternalDatabasesTable,
		catconstants.CrdbInternalFeatureUsageID:                   crdbInternalFeatureUsage,
		catconstants.CrdbInternalForwardDependenciesTableID:       crdbInternalForwardDependenciesTable,
		catconstants.CrdbInternalGossipNodesTableID:               crdbInternalGossipNodesTable,
		catconstants.CrdbInternalKVNodeLivenessTableID:            crdbInternalKVNodeLivenessTable,
		catconstants.CrdbInternalGossipAlertsTableID:              crdbInternalGossipAlertsTable,
		catconstants.CrdbInternalGossipLivenessTableID:            crdbInternalGossipLivenessTable,
		catconstants.CrdbInternalGossipNetworkTableID:             crdbInternalGossipNetworkTable,
		catconstants.CrdbInternalIndexColumnsTableID:              crdbInternalIndexColumnsTable,
		catconstants.CrdbInternalInflightTraceSpanTableID:         crdbInternalInflightTraceSpanTable,
		catconstants.CrdbInternalJobsTableID:                      crdbInternalJobsTable,
		catconstants.CrdbInternalKVNodeStatusTableID:              crdbInternalKVNodeStatusTable,
		catconstants.CrdbInternalKVStoreStatusTableID:             crdbInternalKVStoreStatusTable,
		catconstants.CrdbInternalLeasesTableID:                    crdbInternalLeasesTable,
		catconstants.CrdbInternalLocalContentionEventsTableID:     crdbInternalLocalContentionEventsTable,
		catconstants.CrdbInternalLocalDistSQLFlowsTableID:         crdbInternalLocalDistSQLFlowsTable,
		catconstants.CrdbInternalLocalQueriesTableID:              crdbInternalLocalQueriesTable,
		catconstants.CrdbInternalLocalTransactionsTableID:         crdbInternalLocalTxnsTable,
		catconstants.CrdbInternalLocalSessionsTableID:             crdbInternalLocalSessionsTable,
		catconstants.CrdbInternalLocalMetricsTableID:              crdbInternalLocalMetricsTable,
		catconstants.CrdbInternalPartitionsTableID:                crdbInternalPartitionsTable,
		catconstants.CrdbInternalPredefinedCommentsTableID:        crdbInternalPredefinedCommentsTable,
		catconstants.CrdbInternalRangesNoLeasesTableID:            crdbInternalRangesNoLeasesTable,
		catconstants.CrdbInternalRangesViewID:                     crdbInternalRangesView,
		catconstants.CrdbInternalRuntimeInfoTableID:               crdbInternalRuntimeInfoTable,
		catconstants.CrdbInternalSchemaChangesTableID:             crdbInternalSchemaChangesTable,
		catconstants.CrdbInternalSessionTraceTableID:              crdbInternalSessionTraceTable,
		catconstants.CrdbInternalSessionVariablesTableID:          crdbInternalSessionVariablesTable,
		catconstants.CrdbInternalStmtStatsTableID:                 crdbInternalStmtStatsTable,
		catconstants.CrdbInternalTableColumnsTableID:              crdbInternalTableColumnsTable,
		catconstants.CrdbInternalTableIndexesTableID:              crdbInternalTableIndexesTable,
		catconstants.CrdbInternalTablesTableLastStatsID:           crdbInternalTablesTableLastStats,
		catconstants.CrdbInternalTablesTableID:                    crdbInternalTablesTable,
		catconstants.CrdbInternalTransactionStatsTableID:          crdbInternalTransactionStatisticsTable,
		catconstants.CrdbInternalTxnStatsTableID:                  crdbInternalTxnStatsTable,
		catconstants.CrdbInternalZonesTableID:                     crdbInternalZonesTable,
		catconstants.CrdbInternalInvalidDescriptorsTableID:        crdbInternalInvalidDescriptorsTable,
		catconstants.CrdbInternalClusterDatabasePrivilegesTableID: crdbInternalClusterDatabasePrivilegesTable,
		catconstants.CrdbInternalInterleaved:                      crdbInternalInterleaved,
		catconstants.CrdbInternalCrossDbRefrences:                 crdbInternalCrossDbReferences,
		catconstants.CrdbInternalLostTableDescriptors:             crdbLostTableDescriptors,
		catconstants.CrdbInternalClusterInflightTracesTable:       crdbInternalClusterInflightTracesTable,
	},
	validWithNoDatabaseContext: true,
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
		nodeID, _ := execCfg.NodeID.OptionalNodeID() // zero if not available

		info := build.GetInfo()
		for k, v := range map[string]string{
			"Name":         "CockroachDB",
			"ClusterID":    execCfg.ClusterID().String(),
			"Organization": execCfg.Organization(),
			"Build":        info.Short(),
			"Version":      info.Tag,
			"Channel":      info.Channel,
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
		if err := p.RequireAdminRole(ctx, "access the node runtime information"); err != nil {
			return err
		}

		node := p.ExecCfg().NodeInfo

		nodeID, _ := node.NodeID.OptionalNodeID() // zero if not available
		dbURL, err := node.PGURL(url.User(security.RootUser))
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
	regions STRING[],
	survival_goal STRING,
	create_statement STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /* all databases */, true, /* requiresPrivileges */
			func(db catalog.DatabaseDescriptor) error {
				var survivalGoal tree.Datum = tree.DNull
				var primaryRegion tree.Datum = tree.DNull
				regions := tree.NewDArray(types.String)

				createNode := tree.CreateDatabase{}
				createNode.ConnectionLimit = -1
				createNode.Name = tree.Name(db.GetName())
				if db.IsMultiRegion() {
					primaryRegion = tree.NewDString(string(db.GetRegionConfig().PrimaryRegion))

					createNode.PrimaryRegion = tree.Name(db.GetRegionConfig().PrimaryRegion)

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

					createNode.SurvivalGoal = tree.SurvivalGoalDefault
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

				return addRow(
					tree.NewDInt(tree.DInt(db.GetID())),            // id
					tree.NewDString(db.GetName()),                  // name
					tree.NewDName(getOwnerOfDesc(db).Normalized()), // owner
					primaryRegion,                        // primary_region
					regions,                              // regions
					survivalGoal,                         // survival_goal
					tree.NewDString(createNode.String()), // create_statement
				)
			})
	},
}

// TODO(tbg): prefix with kv_.
var crdbInternalTablesTable = virtualSchemaTable{
	comment: `table descriptors accessible by current user, including non-public and virtual (KV scan; expensive!)`,
	schema: `
CREATE TABLE crdb_internal.tables (
  table_id                 INT NOT NULL,
  parent_id                INT NOT NULL,
  name                     STRING NOT NULL,
  database_name            STRING,
  version                  INT NOT NULL,
  mod_time                 TIMESTAMP NOT NULL,
  mod_time_logical         DECIMAL NOT NULL,
  format_version           STRING NOT NULL,
  state                    STRING NOT NULL,
  sc_lease_node_id         INT,
  sc_lease_expiration_time TIMESTAMP,
  drop_time                TIMESTAMP,
  audit_mode               STRING NOT NULL,
  schema_name              STRING NOT NULL,
  parent_schema_id         INT NOT NULL,
  locality                 TEXT
)`,
	generator: func(ctx context.Context, p *planner, dbDesc catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		row := make(tree.Datums, 14)
		worker := func(pusher rowPusher) error {
			descs, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
			if err != nil {
				return err
			}
			dbNames := make(map[descpb.ID]string)
			scNames := make(map[descpb.ID]string)
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

			addDesc := func(table catalog.TableDescriptor, dbName tree.Datum, scName string) error {
				leaseNodeDatum := tree.DNull
				leaseExpDatum := tree.DNull
				if lease := table.GetLease(); lease != nil {
					leaseNodeDatum = tree.NewDInt(tree.DInt(int64(lease.NodeID)))
					leaseExpDatum, err = tree.MakeDTimestamp(
						timeutil.Unix(0, lease.ExpirationTime), time.Nanosecond,
					)
					if err != nil {
						return err
					}
				}
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
					if err := tabledesc.FormatTableLocalityConfig(c, f); err != nil {
						return err
					}
					locality = tree.NewDString(f.String())
				}
				row = row[:0]
				row = append(row,
					tree.NewDInt(tree.DInt(int64(table.GetID()))),
					tree.NewDInt(tree.DInt(int64(table.GetParentID()))),
					tree.NewDString(table.GetName()),
					dbName,
					tree.NewDInt(tree.DInt(int64(table.GetVersion()))),
					tree.TimestampToInexactDTimestamp(table.GetModificationTime()),
					tree.TimestampToDecimalDatum(table.GetModificationTime()),
					tree.NewDString(table.GetFormatVersion().String()),
					tree.NewDString(table.GetState().String()),
					leaseNodeDatum,
					leaseExpDatum,
					dropTimeDatum,
					tree.NewDString(table.GetAuditMode().String()),
					tree.NewDString(scName),
					tree.NewDInt(tree.DInt(int64(table.GetParentSchemaID()))),
					locality,
				)
				return pusher.pushRow(row...)
			}

			// Note: we do not use forEachTableDesc() here because we want to
			// include added and dropped descriptors.
			for _, desc := range descs {
				table, ok := desc.(catalog.TableDescriptor)
				if !ok || p.CheckAnyPrivilege(ctx, table) != nil {
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
		}
		return setupGenerator(ctx, worker, stopper)
	},
}

// statsAsOfTimeClusterMode controls the cluster setting for the duration which
// is used to define the AS OF time for querying the system.table_statistics
// table when building crdb_internal.table_row_statistics.
var statsAsOfTimeClusterMode = settings.RegisterDurationSetting(
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
	populate: func(ctx context.Context, p *planner, db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
		statRows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryBufferedEx(
			ctx, "crdb-internal-statistics-table", nil,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
		return forEachTableDescAll(ctx, p, db, virtualMany,
			func(db catalog.DatabaseDescriptor, _ string, table catalog.TableDescriptor) error {
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
		descs, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, ok := desc.(catalog.TableDescriptor)
			if !ok || p.CheckAnyPrivilege(ctx, table) != nil {
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

// TODO(tbg): prefix with node_.
var crdbInternalLeasesTable = virtualSchemaTable{
	comment: `acquired table leases (RAM; local node only)`,
	schema: `
CREATE TABLE crdb_internal.leases (
  node_id     INT NOT NULL,
  table_id    INT NOT NULL,
  name        STRING NOT NULL,
  parent_id   INT NOT NULL,
  expiration  TIMESTAMP NOT NULL,
  deleted     BOOL NOT NULL
)`,
	populate: func(
		ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) (err error) {
		nodeID, _ := p.execCfg.NodeID.OptionalNodeID() // zero if not available
		p.LeaseMgr().VisitLeases(func(desc catalog.Descriptor, takenOffline bool, _ int, expiration tree.DTimestamp) (wantMore bool) {
			if p.CheckAnyPrivilege(ctx, desc) != nil {
				// TODO(ajwerner): inspect what type of error got returned.
				return true
			}

			err = addRow(
				tree.NewDInt(tree.DInt(nodeID)),
				tree.NewDInt(tree.DInt(int64(desc.GetID()))),
				tree.NewDString(desc.GetName()),
				tree.NewDInt(tree.DInt(int64(desc.GetParentID()))),
				&expiration,
				tree.MakeDBool(tree.DBool(takenOffline)),
			)
			return err == nil
		})
		return err
	},
}

func tsOrNull(micros int64) (tree.Datum, error) {
	if micros == 0 {
		return tree.DNull, nil
	}
	ts := timeutil.Unix(0, micros*time.Microsecond.Nanoseconds())
	return tree.MakeDTimestamp(ts, time.Microsecond)
}

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
  created               TIMESTAMP,
  started               TIMESTAMP,
  finished              TIMESTAMP,
  modified              TIMESTAMP,
  fraction_completed    FLOAT,
  high_water_timestamp  DECIMAL,
  error                 STRING,
  coordinator_id        INT,
  trace_id              INT
)`,
	comment: `decoded job metadata from system.jobs (KV scan)`,
	generator: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, _ *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		currentUser := p.SessionData().User()
		isAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return nil, nil, err
		}

		hasControlJob, err := p.HasRoleOption(ctx, roleoption.CONTROLJOB)
		if err != nil {
			return nil, nil, err
		}

		// Beware: we're querying system.jobs as root; we need to be careful to filter
		// out results that the current user is not able to see.
		query := `SELECT id, status, created, payload, progress, claim_session_id, claim_instance_id FROM system.jobs`
		it, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryIteratorEx(
			ctx, "crdb-internal-jobs-table", p.txn,
			sessiondata.InternalExecutorOverride{User: security.RootUserName()},
			query)
		if err != nil {
			return nil, nil, err
		}

		cleanup := func() {
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

		// We'll reuse this container on each loop.
		container := make(tree.Datums, 0, 16)
		return func() (datums tree.Datums, e error) {
			// Loop while we need to skip a row.
			for {
				ok, err := it.Next(ctx)
				if !ok {
					return nil, err
				}
				r := it.Cur()
				id, status, created, payloadBytes, progressBytes, sessionIDBytes, instanceID :=
					r[0], r[1], r[2], r[3], r[4], r[5], r[6]

				var jobType, description, statement, username, descriptorIDs, started, runningStatus,
					finished, modified, fractionCompleted, highWaterTimestamp, errorStr, coordinatorID,
					traceID = tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull,
					tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull,
					tree.DNull

				// Extract data from the payload.
				payload, err := jobs.UnmarshalPayload(payloadBytes)

				// We filter out masked rows before we allocate all the
				// datums. Needless allocate when not necessary.
				ownedByAdmin := false
				var sqlUsername security.SQLUsername
				if payload != nil {
					sqlUsername = payload.UsernameProto.Decode()
					ownedByAdmin, err = p.UserHasAdminRole(ctx, sqlUsername)
					if err != nil {
						errorStr = tree.NewDString(fmt.Sprintf("error decoding payload: %v", err))
					}
				}
				if sessionID, ok := sessionIDBytes.(*tree.DBytes); ok {
					if isAlive, err := p.EvalContext().SQLLivenessReader.IsAlive(
						ctx, sqlliveness.SessionID(*sessionID),
					); err != nil {
						// Silently swallow the error for checking for liveness.
					} else if instanceID, ok := instanceID.(*tree.DInt); ok && isAlive {
						coordinatorID = instanceID
					}
				}

				sameUser := payload != nil && sqlUsername == currentUser
				// The user can access the row if the meet one of the conditions:
				//  1. The user is an admin.
				//  2. The job is owned by the user.
				//  3. The user has CONTROLJOB privilege and the job is not owned by
				//      an admin.
				if canAccess := isAdmin || !ownedByAdmin && hasControlJob || sameUser; !canAccess {
					continue
				}

				if err != nil {
					errorStr = tree.NewDString(fmt.Sprintf("error decoding payload: %v", err))
				} else {
					jobType = tree.NewDString(payload.Type().String())
					description = tree.NewDString(payload.Description)
					statement = tree.NewDString(strings.Join(payload.Statement, "; "))
					username = tree.NewDString(sqlUsername.Normalized())
					descriptorIDsArr := tree.NewDArray(types.Int)
					for _, descID := range payload.DescriptorIDs {
						if err := descriptorIDsArr.Append(tree.NewDInt(tree.DInt(int(descID)))); err != nil {
							return nil, err
						}
					}
					descriptorIDs = descriptorIDsArr
					started, err = tsOrNull(payload.StartedMicros)
					if err != nil {
						return nil, err
					}
					finished, err = tsOrNull(payload.FinishedMicros)
					if err != nil {
						return nil, err
					}
					errorStr = tree.NewDString(payload.Error)
				}

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
							highWaterTimestamp = tree.TimestampToDecimalDatum(*highwater)
						} else {
							fractionCompleted = tree.NewDFloat(tree.DFloat(progress.GetFractionCompleted()))
						}
						modified, err = tsOrNull(progress.ModifiedMicros)
						if err != nil {
							return nil, err
						}

						if len(progress.RunningStatus) > 0 {
							if s, ok := status.(*tree.DString); ok {
								if jobs.Status(string(*s)) == jobs.StatusRunning {
									runningStatus = tree.NewDString(progress.RunningStatus)
								}
							}
						}
						traceID = tree.NewDInt(tree.DInt(progress.TraceID))
					}
				}

				container = container[:0]
				container = append(container,
					id,
					jobType,
					description,
					statement,
					username,
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
				)
				return container, nil
			}
		}, cleanup, nil
	},
}

// execStatAvg is a helper for execution stats shown in virtual tables. Returns
// NULL when the count is 0, or the mean of the given NumericStat.
func execStatAvg(count int64, n roachpb.NumericStat) tree.Datum {
	if count == 0 {
		return tree.DNull
	}
	return tree.NewDFloat(tree.DFloat(n.Mean))
}

// execStatVar is a helper for execution stats shown in virtual tables. Returns
// NULL when the count is 0, or the variance of the given NumericStat.
func execStatVar(count int64, n roachpb.NumericStat) tree.Datum {
	if count == 0 {
		return tree.DNull
	}
	return tree.NewDFloat(tree.DFloat(n.GetVariance(count)))
}

// getSQLStats retrieves a sqlStats storage subsystem from the planner or
// returns an error if not available. virtualTableName specifies the virtual
// table for which this sqlStats object is needed.
func getSQLStats(p *planner, virtualTableName string) (sqlstats.Storage, error) {
	if p.extendedEvalCtx.statsStorage == nil {
		return nil, errors.Newf("%s cannot be used in this context", virtualTableName)
	}
	return p.extendedEvalCtx.statsStorage, nil
}

// ExplainTreePlanNodeToJSON builds a formatted JSON object from the explain tree nodes.
func ExplainTreePlanNodeToJSON(node *roachpb.ExplainTreePlanNode) json.JSON {

	// Create a new json.ObjectBuilder with key-value pairs for the node's name (1),
	// node's attributes (len(node.Attrs)), and the node's children (1).
	nodePlan := json.NewObjectBuilder(len(node.Attrs) + 2 /* numAddsHint */)
	nodeChildren := json.NewArrayBuilder(len(node.Children))

	nodePlan.Add("Name", json.FromString(node.Name))

	for _, attr := range node.Attrs {
		nodePlan.Add(strings.Title(attr.Key), json.FromString(attr.Value))
	}

	for _, childNode := range node.Children {
		nodeChildren.Add(ExplainTreePlanNodeToJSON(childNode))
	}

	nodePlan.Add("Children", nodeChildren.Build())
	return nodePlan.Build()
}

var crdbInternalStmtStatsTable = virtualSchemaTable{
	comment: `statement statistics (in-memory, not durable; local node only). ` +
		`This table is wiped periodically (by default, at least every two hours)`,
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
  rows_avg            FLOAT NOT NULL,
  rows_var            FLOAT NOT NULL,
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
  implicit_txn        BOOL NOT NULL,
  full_scan           BOOL NOT NULL,
  sample_plan         JSONB,
  database_name       STRING NOT NULL,
  exec_node_ids       INT[] NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasViewActivity, err := p.HasRoleOption(ctx, roleoption.VIEWACTIVITY)
		if err != nil {
			return err
		}
		if !hasViewActivity {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s does not have %s privilege", p.User(), roleoption.VIEWACTIVITY)
		}

		sqlStats, err := getSQLStats(p, "crdb_internal.node_statement_statistics")
		if err != nil {
			return err
		}

		nodeID, _ := p.execCfg.NodeID.OptionalNodeID() // zero if not available

		statementVisitor := func(stats *roachpb.CollectedStatementStatistics) error {
			anonymized := tree.DNull
			anonStr, ok := scrubStmtStatKey(p.getVirtualTabler(), stats.Key.Query)
			if ok {
				anonymized = tree.NewDString(anonStr)
			}

			errString := tree.DNull
			if stats.Stats.SensitiveInfo.LastErr != "" {
				errString = tree.NewDString(stats.Stats.SensitiveInfo.LastErr)
			}
			var flags string
			if stats.Key.DistSQL {
				flags = "+"
			}
			if stats.Key.Failed {
				flags = "!" + flags
			}

			samplePlan := ExplainTreePlanNodeToJSON(&stats.Stats.SensitiveInfo.MostRecentPlanDescription)

			execNodeIDs := tree.NewDArray(types.Int)
			for _, nodeID := range stats.Stats.Nodes {
				if err := execNodeIDs.Append(tree.NewDInt(tree.DInt(nodeID))); err != nil {
					return err
				}
			}

			err := addRow(
				tree.NewDInt(tree.DInt(nodeID)),                           // node_id
				tree.NewDString(stats.Key.App),                            // application_name
				tree.NewDString(flags),                                    // flags
				tree.NewDString(strconv.FormatUint(uint64(stats.ID), 10)), // statement_id
				tree.NewDString(stats.Key.Query),                          // key
				anonymized,                                                // anonymized
				tree.NewDInt(tree.DInt(stats.Stats.Count)),                // count
				tree.NewDInt(tree.DInt(stats.Stats.FirstAttemptCount)),    // first_attempt_count
				tree.NewDInt(tree.DInt(stats.Stats.MaxRetries)),           // max_retries
				errString, // last_error
				tree.NewDFloat(tree.DFloat(stats.Stats.NumRows.Mean)),                               // rows_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.NumRows.GetVariance(stats.Stats.Count))),     // rows_var
				tree.NewDFloat(tree.DFloat(stats.Stats.ParseLat.Mean)),                              // parse_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.ParseLat.GetVariance(stats.Stats.Count))),    // parse_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.PlanLat.Mean)),                               // plan_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.PlanLat.GetVariance(stats.Stats.Count))),     // plan_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.RunLat.Mean)),                                // run_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.RunLat.GetVariance(stats.Stats.Count))),      // run_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.Mean)),                            // service_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.GetVariance(stats.Stats.Count))),  // service_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.OverheadLat.Mean)),                           // overhead_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.OverheadLat.GetVariance(stats.Stats.Count))), // overhead_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.BytesRead.Mean)),                             // bytes_read_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.BytesRead.GetVariance(stats.Stats.Count))),   // bytes_read_var
				tree.NewDFloat(tree.DFloat(stats.Stats.RowsRead.Mean)),                              // rows_read_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.RowsRead.GetVariance(stats.Stats.Count))),    // rows_read_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),        // network_bytes_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),        // network_bytes_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),     // network_msgs_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),     // network_msgs_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),         // max_mem_usage_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),         // max_mem_usage_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),        // max_disk_usage_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),        // max_disk_usage_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),      // contention_time_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),      // contention_time_var
				tree.MakeDBool(tree.DBool(stats.Key.ImplicitTxn)),                                   // implicit_txn
				tree.MakeDBool(tree.DBool(stats.Key.FullScan)),                                      // full_scan
				tree.NewDJSON(samplePlan),           // sample_plan
				tree.NewDString(stats.Key.Database), // database_name
				execNodeIDs,                         // exec_node_ids
			)
			if err != nil {
				return err
			}

			return nil
		}

		return sqlStats.IterateStatementStats(ctx, &sqlstats.IteratorOptions{
			SortedAppNames: true,
			SortedKey:      true,
		}, statementVisitor)
	},
}

// TODO(arul): Explore updating the schema below to have key be an INT and
// statement_ids be INT[] now that we've moved to having uint64 as the type of
// StmtFingerprintID and TxnKey. Issue #55284
var crdbInternalTransactionStatisticsTable = virtualSchemaTable{
	comment: `finer-grained transaction statistics (in-memory, not durable; local node only). ` +
		`This table is wiped periodically (by default, at least every two hours)`,
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
  contention_time_var FLOAT
)
`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasViewActivity, err := p.HasRoleOption(ctx, roleoption.VIEWACTIVITY)
		if err != nil {
			return err
		}
		if !hasViewActivity {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"user %s does not have %s privilege", p.User(), roleoption.VIEWACTIVITY)
		}
		sqlStats, err := getSQLStats(p, "crdb_internal.node_transaction_statistics")
		if err != nil {
			return err
		}

		nodeID, _ := p.execCfg.NodeID.OptionalNodeID() // zero if not available

		transactionVisitor := func(txnFingerprintID roachpb.TransactionFingerprintID, stats *roachpb.CollectedTransactionStatistics) error {
			stmtFingerprintIDsDatum := tree.NewDArray(types.String)
			for _, stmtFingerprintID := range stats.StatementFingerprintIDs {
				if err := stmtFingerprintIDsDatum.Append(tree.NewDString(strconv.FormatUint(uint64(stmtFingerprintID), 10))); err != nil {
					return err
				}
			}

			err := addRow(
				tree.NewDInt(tree.DInt(nodeID)),                                                    // node_id
				tree.NewDString(stats.App),                                                         // application_name
				tree.NewDString(strconv.FormatUint(uint64(txnFingerprintID), 10)),                  // key
				stmtFingerprintIDsDatum,                                                            // statement_ids
				tree.NewDInt(tree.DInt(stats.Stats.Count)),                                         // count
				tree.NewDInt(tree.DInt(stats.Stats.MaxRetries)),                                    // max_retries
				tree.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.Mean)),                           // service_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.ServiceLat.GetVariance(stats.Stats.Count))), // service_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.RetryLat.Mean)),                             // retry_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.RetryLat.GetVariance(stats.Stats.Count))),   // retry_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.CommitLat.Mean)),                            // commit_lat_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.CommitLat.GetVariance(stats.Stats.Count))),  // commit_lat_var
				tree.NewDFloat(tree.DFloat(stats.Stats.NumRows.Mean)),                              // rows_read_avg
				tree.NewDFloat(tree.DFloat(stats.Stats.NumRows.GetVariance(stats.Stats.Count))),    // rows_read_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),       // network_bytes_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkBytes),       // network_bytes_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),    // network_msgs_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.NetworkMessages),    // network_msgs_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),        // max_mem_usage_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxMemUsage),        // max_mem_usage_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),       // max_disk_usage_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.MaxDiskUsage),       // max_disk_usage_var
				execStatAvg(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),     // contention_time_avg
				execStatVar(stats.Stats.ExecStats.Count, stats.Stats.ExecStats.ContentionTime),     // contention_time_var
			)

			if err != nil {
				return err
			}

			return nil
		}

		return sqlStats.IterateTransactionStats(ctx, &sqlstats.IteratorOptions{
			SortedAppNames: true,
			SortedKey:      true,
		}, transactionVisitor)
	},
}

var crdbInternalTxnStatsTable = virtualSchemaTable{
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
		if err := p.RequireAdminRole(ctx, "access application statistics"); err != nil {
			return err
		}

		sqlStats, err := getSQLStats(p, "crdb_internal.node_txn_stats")
		if err != nil {
			return err
		}

		nodeID, _ := p.execCfg.NodeID.OptionalNodeID() // zero if not available

		appTxnStatsVisitor := func(appName string, stats *roachpb.TxnStats) error {
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

		return sqlStats.IterateAggregatedTransactionStats(ctx, &sqlstats.IteratorOptions{
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
// for a trace_id.
//
// crdbInternalClusterInflightTracesTable is an indexed, virtual table that only
// returns rows when accessed with an index constraint specifying the trace_id
// for which inflight spans need to be aggregated from all nodes in the cluster.
//
// Each row in the virtual table corresponds to a single `tracing.Recording` on
// a particular node. A `tracing.Recording` is the trace of a single operation
// rooted at a root span on that node. Under the hood, the virtual table
// contacts all "live" nodes in the cluster via the trace collector which
// streams back a recording at a time.
//
// The underlying trace collector only buffers recordings one node at a time.
// The virtual table also produces rows lazily, i.e. as and when they are
// consumed by the consumer. Therefore, the memory overhead of querying this
// table will be the size of all the `tracing.Recordings` of a particular
// `trace_id` on a single node in the cluster. Each `tracing.Recording` has its
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
	indexes: []virtualIndex{{populate: func(ctx context.Context, constraint tree.Datum, p *planner,
		db catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) (matched bool, err error) {
		var traceID uint64
		d := tree.UnwrapDatum(p.EvalContext(), constraint)
		if d == tree.DNull {
			return false, nil
		}
		switch t := d.(type) {
		case *tree.DInt:
			traceID = uint64(*t)
		default:
			return false, errors.AssertionFailedf(
				"unexpected type %T for trace_id column in virtual table crdb_internal.cluster_inflight_traces", d)
		}

		traceCollector := p.ExecCfg().TraceCollector
		for iter := traceCollector.StartIter(ctx, traceID); iter.Valid(); iter.Next() {
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
		// We only want to generate rows when an index constraint is provided on the
		// query accessing this vtable. This index constraint will provide the
		// trace_id for which we will collect inflight trace spans from the cluster.
		return nil
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
		hasAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}
		if !hasAdmin {
			return pgerror.Newf(pgcode.InsufficientPrivilege,
				"only users with the admin role are allowed to read crdb_internal.node_inflight_trace_spans")
		}
		return p.ExecCfg().Settings.Tracer.VisitSpans(func(span *tracing.Span) error {
			for _, rec := range span.GetRecording() {
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
  description   STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		hasAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return err
		}
		if !hasAdmin {
			hasModify, err := p.HasRoleOption(ctx, roleoption.MODIFYCLUSTERSETTING)
			if err != nil {
				return err
			}
			if !hasModify {
				return pgerror.Newf(pgcode.InsufficientPrivilege,
					"only users with the %s privilege are allowed to read "+
						"crdb_internal.cluster_settings", roleoption.MODIFYCLUSTERSETTING)
			}
		}
		for _, k := range settings.Keys() {
			if !hasAdmin && settings.AdminOnly(k) {
				continue
			}
			setting, _ := settings.Lookup(k, settings.LookupForLocalAccess)
			strVal := setting.String(&p.ExecCfg().Settings.SV)
			isPublic := setting.Visibility() == settings.Public
			desc := setting.Description()
			if err := addRow(
				tree.NewDString(k),
				tree.NewDString(strVal),
				tree.NewDString(setting.Typ()),
				tree.MakeDBool(tree.DBool(isPublic)),
				tree.NewDString(desc),
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
			value := gen.Get(&p.extendedEvalCtx)
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
  id UUID,                 -- the unique ID of the transaction
  node_id INT,             -- the ID of the node running the transaction
  session_id STRING,       -- the ID of the session
  start TIMESTAMP,         -- the start time of the transaction
  txn_string STRING,       -- the string representation of the transcation
  application_name STRING, -- the name of the application as per SET application_name
  num_stmts INT,           -- the number of statements executed so far
  num_retries INT,         -- the number of times the transaction was restarted
  num_auto_retries INT     -- the number of times the transaction was automatically restarted
)`

var crdbInternalLocalTxnsTable = virtualSchemaTable{
	comment: "running user transactions visible by the current user (RAM; local node only)",
	schema:  fmt.Sprintf(txnsSchemaPattern, "node_transactions"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.node_transactions"); err != nil {
			return err
		}
		req, err := p.makeSessionsRequest(ctx)
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
		if err := p.RequireAdminRole(ctx, "read crdb_internal.cluster_transactions"); err != nil {
			return err
		}
		req, err := p.makeSessionsRequest(ctx)
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
  start            TIMESTAMP,      -- the start time of the query
  query            STRING,         -- the SQL code of the query
  client_address   STRING,         -- the address of the client that issued the query
  application_name STRING,         -- the name of the application as per SET application_name
  distributed      BOOL,           -- whether the query is running distributed
  phase            STRING          -- the current execution phase
)`

func (p *planner) makeSessionsRequest(ctx context.Context) (serverpb.ListSessionsRequest, error) {
	req := serverpb.ListSessionsRequest{Username: p.SessionData().User().Normalized()}
	hasAdmin, err := p.HasAdminRole(ctx)
	if err != nil {
		return serverpb.ListSessionsRequest{}, err
	}
	if hasAdmin {
		req.Username = ""
	} else {
		hasViewActivity, err := p.HasRoleOption(ctx, roleoption.VIEWACTIVITY)
		if err != nil {
			return serverpb.ListSessionsRequest{}, err
		}
		if hasViewActivity {
			req.Username = ""
		}
	}
	return req, nil
}

func getSessionID(session serverpb.Session) tree.Datum {
	// TODO(knz): serverpb.Session is always constructed with an ID
	// set from a 16-byte session ID. Yet we get crash reports
	// that fail in BytesToClusterWideID() with a byte slice that's
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
		clusterSessionID := BytesToClusterWideID(session.ID)
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
		req, err := p.makeSessionsRequest(ctx)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateQueriesTable(ctx, addRow, response)
	},
}

// crdbInternalClusterQueriesTable exposes the list of running queries
// on the entire cluster. The result is dependent on the current user.
var crdbInternalClusterQueriesTable = virtualSchemaTable{
	comment: "running queries visible by current user (cluster RPC; expensive!)",
	schema:  fmt.Sprintf(queriesSchemaPattern, "cluster_queries"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req, err := p.makeSessionsRequest(ctx)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateQueriesTable(ctx, addRow, response)
	},
}

func populateQueriesTable(
	ctx context.Context, addRow func(...tree.Datum) error, response *serverpb.ListSessionsResponse,
) error {
	for _, session := range response.Sessions {
		sessionID := getSessionID(session)
		for _, query := range session.ActiveQueries {
			isDistributedDatum := tree.DNull
			phase := strings.ToLower(query.Phase.String())
			if phase == "executing" {
				isDistributedDatum = tree.DBoolFalse
				if query.IsDistributed {
					isDistributedDatum = tree.DBoolTrue
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

			ts, err := tree.MakeDTimestamp(query.Start, time.Microsecond)
			if err != nil {
				return err
			}
			if err := addRow(
				tree.NewDString(query.ID),
				txnID,
				tree.NewDInt(tree.DInt(session.NodeID)),
				sessionID,
				tree.NewDString(session.Username),
				ts,
				tree.NewDString(query.Sql),
				tree.NewDString(session.ClientAddress),
				tree.NewDString(session.ApplicationName),
				isDistributedDatum,
				tree.NewDString(phase),
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
			); err != nil {
				return err
			}
		}
	}
	return nil
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
  session_start      TIMESTAMP,      -- the time when the session was opened
  oldest_query_start TIMESTAMP,      -- the time when the oldest query in the session was started
  kv_txn             STRING,         -- the ID of the current KV transaction
  alloc_bytes        INT,            -- the number of bytes allocated by the session
  max_alloc_bytes    INT             -- the high water mark of bytes allocated by the session
)
`

// crdbInternalLocalSessionsTable exposes the list of running sessions
// on the current node. The results are dependent on the current user.
var crdbInternalLocalSessionsTable = virtualSchemaTable{
	comment: "running sessions visible by current user (RAM; local node only)",
	schema:  fmt.Sprintf(sessionsSchemaPattern, "node_sessions"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req, err := p.makeSessionsRequest(ctx)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateSessionsTable(ctx, addRow, response)
	},
}

// crdbInternalClusterSessionsTable exposes the list of running sessions
// on the entire cluster. The result is dependent on the current user.
var crdbInternalClusterSessionsTable = virtualSchemaTable{
	comment: "running sessions visible to current user (cluster RPC; expensive!)",
	schema:  fmt.Sprintf(sessionsSchemaPattern, "cluster_sessions"),
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req, err := p.makeSessionsRequest(ctx)
		if err != nil {
			return err
		}
		response, err := p.extendedEvalCtx.SQLStatusServer.ListSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateSessionsTable(ctx, addRow, response)
	},
}

func populateSessionsTable(
	ctx context.Context, addRow func(...tree.Datum) error, response *serverpb.ListSessionsResponse,
) error {
	for _, session := range response.Sessions {
		// Generate active_queries and oldest_query_start
		var activeQueries bytes.Buffer
		var oldestStart time.Time
		var oldestStartDatum tree.Datum

		for idx, query := range session.ActiveQueries {
			if idx > 0 {
				activeQueries.WriteString("; ")
			}
			activeQueries.WriteString(query.Sql)

			if oldestStart.IsZero() || query.Start.Before(oldestStart) {
				oldestStart = query.Start
			}
		}

		var err error
		if oldestStart.IsZero() {
			oldestStartDatum = tree.DNull
		} else {
			oldestStartDatum, err = tree.MakeDTimestamp(oldestStart, time.Microsecond)
			if err != nil {
				return err
			}
		}

		kvTxnIDDatum := tree.DNull
		if session.ActiveTxn != nil {
			kvTxnIDDatum = tree.NewDString(session.ActiveTxn.ID.String())
		}

		sessionID := getSessionID(session)
		startTSDatum, err := tree.MakeDTimestamp(session.Start, time.Microsecond)
		if err != nil {
			return err
		}
		if err := addRow(
			tree.NewDInt(tree.DInt(session.NodeID)),
			sessionID,
			tree.NewDString(session.Username),
			tree.NewDString(session.ClientAddress),
			tree.NewDString(session.ApplicationName),
			tree.NewDString(activeQueries.String()),
			tree.NewDString(session.LastActiveQuery),
			startTSDatum,
			oldestStartDatum,
			kvTxnIDDatum,
			tree.NewDInt(tree.DInt(session.AllocBytes)),
			tree.NewDInt(tree.DInt(session.MaxAllocBytes)),
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
				tree.DNull,                             // session start
				tree.DNull,                             // oldest_query_start
				tree.DNull,                             // kv_txn
				tree.DNull,                             // alloc_bytes
				tree.DNull,                             // max_alloc_bytes
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
      = crdb_internal.tables.table_id
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
		return populateContentionEventsTable(ctx, addRow, response)
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
		return populateContentionEventsTable(ctx, addRow, response)
	},
}

func populateContentionEventsTable(
	ctx context.Context,
	addRow func(...tree.Datum) error,
	response *serverpb.ListContentionEventsResponse,
) error {
	for _, ice := range response.Events.IndexContentionEvents {
		for _, skc := range ice.Events {
			for _, stc := range skc.Txns {
				cumulativeContentionTime := tree.NewDInterval(
					duration.MakeDuration(ice.CumulativeContentionTime.Nanoseconds(), 0 /* days */, 0 /* months */),
					types.DefaultIntervalTypeMetadata,
				)
				if err := addRow(
					tree.NewDInt(tree.DInt(ice.TableID)),             // table_id
					tree.NewDInt(tree.DInt(ice.IndexID)),             // index_id
					tree.NewDInt(tree.DInt(ice.NumContentionEvents)), // num_contention_events
					cumulativeContentionTime,                         // cumulative_contention_time
					tree.NewDBytes(tree.DBytes(skc.Key)),             // key
					tree.NewDUuid(tree.DUuid{UUID: stc.TxnID}),       // txn_id
					tree.NewDInt(tree.DInt(stc.Count)),               // count
				); err != nil {
					return err
				}
			}
		}
	}
	for _, nkc := range response.Events.NonSQLKeysContention {
		for _, stc := range nkc.Txns {
			cumulativeContentionTime := tree.NewDInterval(
				duration.MakeDuration(nkc.CumulativeContentionTime.Nanoseconds(), 0 /* days */, 0 /* months */),
				types.DefaultIntervalTypeMetadata,
			)
			if err := addRow(
				tree.DNull, // table_id
				tree.DNull, // index_id
				tree.NewDInt(tree.DInt(nkc.NumContentionEvents)), // num_contention_events
				cumulativeContentionTime,                         // cumulative_contention_time
				tree.NewDBytes(tree.DBytes(nkc.Key)),             // key
				tree.NewDUuid(tree.DUuid{UUID: stc.TxnID}),       // txn_id
				tree.NewDInt(tree.DInt(stc.Count)),               // count
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
  since   TIMESTAMPTZ NOT NULL,
  status  STRING NOT NULL
)
`

const distSQLFlowsCommentPattern = `DistSQL remote flows information %s

This virtual table contains all of the remote flows of the DistSQL execution
that are currently running or queued on %s. The local
flows (those that are running on the same node as the query originated on)
are not included.
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
			since, err := tree.MakeDTimestampTZ(info.Timestamp, time.Millisecond)
			if err != nil {
				return err
			}
			status := tree.NewDString(strings.ToLower(info.Status.String()))
			if err = addRow(flowID, nodeID, since, status); err != nil {
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
		if err := p.RequireAdminRole(ctx, "read crdb_internal.node_metrics"); err != nil {
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
  details   STRING NOT NULL
)`,
	populate: func(ctx context.Context, _ *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		for _, name := range builtins.AllBuiltinNames {
			props, overloads := builtins.GetBuiltinProperties(name)
			for _, f := range overloads {
				if err := addRow(
					tree.NewDString(name),
					tree.NewDString(f.Signature(false /* simplify */)),
					tree.NewDString(props.Category),
					tree.NewDString(f.Info),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
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
		return forEachTypeDesc(ctx, p, db, func(db catalog.DatabaseDescriptor, sc string, typeDesc catalog.TypeDescriptor) error {
			switch typeDesc.GetKind() {
			case descpb.TypeDescriptor_ENUM:
				var enumLabels tree.EnumValueList
				enumLabelsDatum := tree.NewDArray(types.String)
				for i := 0; i < typeDesc.NumEnumMembers(); i++ {
					rep := typeDesc.GetMemberLogicalRepresentation(i)
					enumLabels = append(enumLabels, tree.EnumValue(rep))
					if err := enumLabelsDatum.Append(tree.NewDString(rep)); err != nil {
						return err
					}
				}
				name, err := tree.NewUnresolvedObjectName(2, [3]string{typeDesc.GetName(), sc}, 0)
				if err != nil {
					return err
				}
				node := &tree.CreateType{
					Variety:    tree.Enum,
					TypeName:   name,
					EnumLabels: enumLabels,
				}
				if err := addRow(
					tree.NewDInt(tree.DInt(db.GetID())),       // database_id
					tree.NewDString(db.GetName()),             // database_name
					tree.NewDString(sc),                       // schema_name
					tree.NewDInt(tree.DInt(typeDesc.GetID())), // descriptor_id
					tree.NewDString(typeDesc.GetName()),       // descriptor_name
					tree.NewDString(tree.AsString(node)),      // create_statement
					enumLabelsDatum,
				); err != nil {
					return err
				}
			case descpb.TypeDescriptor_MULTIREGION_ENUM:
				// Multi-region enums are created implicitly, so we don't have create
				// statements for them.
			case descpb.TypeDescriptor_ALIAS:
			// Alias types are created implicitly, so we don't have create
			// statements for them.
			default:
				return errors.AssertionFailedf("unknown type descriptor kind %s", typeDesc.GetKind().String())
			}
			return nil
		})
	},
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
  alter_statements              STRING[] NOT NULL,
  validate_statements           STRING[] NOT NULL,
  has_partitions                BOOL NOT NULL,
  is_multi_region               BOOL NOT NULL,
  is_virtual                    BOOL NOT NULL,
  is_temporary                  BOOL NOT NULL,
  INDEX(descriptor_id)
)
`, virtualCurrentDB, false, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db catalog.DatabaseDescriptor, scName string,
		table catalog.TableDescriptor, lookup simpleSchemaResolver, addRow func(...tree.Datum) error) error {
		contextName := ""
		parentNameStr := tree.DNull
		if db != nil {
			contextName = db.GetName()
			parentNameStr = tree.NewDString(contextName)
		}
		scNameStr := tree.NewDString(scName)

		var descType tree.Datum
		var stmt, createNofk string
		alterStmts := tree.NewDArray(types.String)
		validateStmts := tree.NewDArray(types.String)
		namePrefix := tree.ObjectNamePrefix{SchemaName: tree.Name(scName), ExplicitSchema: true}
		name := tree.MakeTableNameFromPrefix(namePrefix, tree.Name(table.GetName()))
		var err error
		if table.IsView() {
			descType = typeView
			stmt, err = ShowCreateView(ctx, &p.semaCtx, &name, table)
		} else if table.IsSequence() {
			descType = typeSequence
			stmt, err = ShowCreateSequence(ctx, &name, table)
		} else {
			descType = typeTable
			displayOptions := ShowCreateDisplayOptions{
				FKDisplayMode: OmitFKClausesFromCreate,
			}
			createNofk, err = ShowCreateTable(ctx, p, &name, contextName, table, lookup, displayOptions)
			if err != nil {
				return err
			}
			if err := showAlterStatementWithInterleave(ctx, &name, contextName, lookup, table.PublicNonPrimaryIndexes(), table, alterStmts,
				validateStmts, &p.semaCtx); err != nil {
				return err
			}
			displayOptions.FKDisplayMode = IncludeFkClausesInCreate
			stmt, err = ShowCreateTable(ctx, p, &name, contextName, table, lookup, displayOptions)
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
			return idx.GetPartitioning().NumColumns() != 0
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
			alterStmts,
			validateStmts,
			tree.MakeDBool(tree.DBool(hasPartitions)),
			tree.MakeDBool(tree.DBool(db != nil && db.IsMultiRegion())),
			tree.MakeDBool(tree.DBool(table.IsVirtualTable())),
			tree.MakeDBool(tree.DBool(table.IsTemporary())),
		)
	})

func showAlterStatementWithInterleave(
	ctx context.Context,
	tn *tree.TableName,
	contextName string,
	lCtx simpleSchemaResolver,
	allIdx []catalog.Index,
	table catalog.TableDescriptor,
	alterStmts *tree.DArray,
	validateStmts *tree.DArray,
	semaCtx *tree.SemaContext,
) error {
	if err := table.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
		f := tree.NewFmtCtx(tree.FmtSimple)
		f.WriteString("ALTER TABLE ")
		f.FormatNode(tn)
		f.WriteString(" ADD CONSTRAINT ")
		f.FormatNameP(&fk.Name)
		f.WriteByte(' ')
		// Passing in EmptySearchPath causes the schema name to show up in the
		// constraint definition, which we need for `cockroach dump` output to be
		// usable.
		if err := showForeignKeyConstraint(
			&f.Buffer,
			contextName,
			table,
			fk,
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
		f.FormatNameP(&fk.Name)

		return validateStmts.Append(tree.NewDString(f.CloseAndGetString()))
	}); err != nil {
		return err
	}

	for _, idx := range allIdx {
		// Create CREATE INDEX commands for INTERLEAVE tables. These commands
		// are included in the ALTER TABLE statements.
		if idx.NumInterleaveAncestors() > 0 {
			f := tree.NewFmtCtx(tree.FmtSimple)
			parentTableID := idx.GetInterleaveAncestor(idx.NumInterleaveAncestors() - 1).TableID
			var err error
			var parentName tree.TableName
			if lCtx != nil {
				parentName, err = getParentAsTableName(lCtx, parentTableID, contextName)
				if err != nil {
					return err
				}
			} else {
				parentName = tree.MakeTableNameWithSchema(tree.Name(""), tree.PublicSchemaName, tree.Name(fmt.Sprintf("[%d as parent]", parentTableID)))
				parentName.ExplicitCatalog = false
				parentName.ExplicitSchema = false
			}

			var tableName tree.TableName
			if lCtx != nil {
				tableName, err = getTableNameFromTableDescriptor(lCtx, table, contextName)
				if err != nil {
					return err
				}
			} else {
				tableName = tree.MakeTableNameWithSchema(tree.Name(""), tree.PublicSchemaName, tree.Name(fmt.Sprintf("[%d as parent]", table.GetID())))
				tableName.ExplicitCatalog = false
				tableName.ExplicitSchema = false
			}
			var sharedPrefixLen int
			for i := 0; i < idx.NumInterleaveAncestors(); i++ {
				sharedPrefixLen += int(idx.GetInterleaveAncestor(i).SharedPrefixLen)
			}
			// Write the CREATE INDEX statements.
			if err := showCreateIndexWithInterleave(ctx, f, table, idx, tableName, parentName, sharedPrefixLen, semaCtx); err != nil {
				return err
			}
			if err := alterStmts.Append(tree.NewDString(f.CloseAndGetString())); err != nil {
				return err
			}
		}
	}
	return nil
}

func showCreateIndexWithInterleave(
	ctx context.Context,
	f *tree.FmtCtx,
	table catalog.TableDescriptor,
	idx catalog.Index,
	tableName tree.TableName,
	parentName tree.TableName,
	sharedPrefixLen int,
	semaCtx *tree.SemaContext,
) error {
	f.WriteString("CREATE ")
	idxStr, err := catformat.IndexForDisplay(
		ctx, table, &tableName, idx, "" /* partition */, "" /* interleave */, semaCtx,
	)
	if err != nil {
		return err
	}
	f.WriteString(idxStr)
	f.WriteString(" INTERLEAVE IN PARENT ")
	parentName.Format(f)
	f.WriteString(" (")
	// Get all of the columns and write them.
	comma := ""
	for i := 0; i < sharedPrefixLen; i++ {
		name := idx.GetKeyColumnName(i)
		f.WriteString(comma)
		f.FormatNameP(&name)
		comma = ", "
	}
	f.WriteString(")")
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
		row := make(tree.Datums, 8)
		worker := func(pusher rowPusher) error {
			return forEachTableDescAll(ctx, p, dbContext, hideVirtual,
				func(db catalog.DatabaseDescriptor, _ string, table catalog.TableDescriptor) error {
					tableID := tree.NewDInt(tree.DInt(table.GetID()))
					tableName := tree.NewDString(table.GetName())
					columns := table.PublicColumns()
					for _, col := range columns {
						defStr := tree.DNull
						if col.HasDefault() {
							defExpr, err := schemaexpr.FormatExprForDisplay(ctx, table, col.GetDefaultExpr(), &p.semaCtx, tree.FmtParsable)
							if err != nil {
								return err
							}
							defStr = tree.NewDString(defExpr)
						}
						row = row[:0]
						row = append(row,
							tableID,
							tableName,
							tree.NewDInt(tree.DInt(col.GetID())),
							tree.NewDString(col.GetName()),
							tree.NewDString(col.GetType().DebugString()),
							tree.MakeDBool(tree.DBool(col.IsNullable())),
							defStr,
							tree.MakeDBool(tree.DBool(col.IsHidden())),
						)
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
//
// TODO(tbg): prefix with kv_.
var crdbInternalTableIndexesTable = virtualSchemaTable{
	comment: "indexes accessible by current user in current database (KV scan)",
	schema: `
CREATE TABLE crdb_internal.table_indexes (
  descriptor_id    INT,
  descriptor_name  STRING NOT NULL,
  index_id         INT NOT NULL,
  index_name       STRING NOT NULL,
  index_type       STRING NOT NULL,
  is_unique        BOOL NOT NULL,
  is_inverted      BOOL NOT NULL
)
`,
	generator: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, stopper *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		primary := tree.NewDString("primary")
		secondary := tree.NewDString("secondary")
		row := make(tree.Datums, 7)
		worker := func(pusher rowPusher) error {
			return forEachTableDescAll(ctx, p, dbContext, hideVirtual,
				func(db catalog.DatabaseDescriptor, _ string, table catalog.TableDescriptor) error {
					tableID := tree.NewDInt(tree.DInt(table.GetID()))
					tableName := tree.NewDString(table.GetName())
					// We report the primary index of non-physical tables here. These
					// indexes are not reported as a part of ForeachIndex.
					return catalog.ForEachIndex(table, catalog.IndexOpts{
						NonPhysicalPrimaryIndex: true,
					}, func(idx catalog.Index) error {
						row = row[:0]
						idxType := secondary
						if idx.Primary() {
							idxType = primary
						}
						row = append(row,
							tableID,
							tableName,
							tree.NewDInt(tree.DInt(idx.GetID())),
							tree.NewDString(idx.GetName()),
							idxType,
							tree.MakeDBool(tree.DBool(idx.IsUnique())),
							tree.MakeDBool(idx.GetType() == descpb.IndexDescriptor_INVERTED),
						)
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
		idxDirMap := map[descpb.IndexDescriptor_Direction]tree.Datum{
			descpb.IndexDescriptor_ASC:  tree.NewDString(descpb.IndexDescriptor_ASC.String()),
			descpb.IndexDescriptor_DESC: tree.NewDString(descpb.IndexDescriptor_DESC.String()),
		}

		return forEachTableDescAll(ctx, p, dbContext, hideVirtual,
			func(parent catalog.DatabaseDescriptor, _ string, table catalog.TableDescriptor) error {
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
		interleaveDep := tree.NewDString("interleave")

		return forEachTableDescAllWithTableLookup(ctx, p, dbContext, hideVirtual, func(
			db catalog.DatabaseDescriptor, _ string, table catalog.TableDescriptor, tableLookup tableLookupFn,
		) error {
			tableID := tree.NewDInt(tree.DInt(table.GetID()))
			tableName := tree.NewDString(table.GetName())

			reportIdxDeps := func(idx catalog.Index) error {
				for i := 0; i < idx.NumInterleaveAncestors(); i++ {
					interleaveParent := idx.GetInterleaveAncestor(i)
					if err := addRow(
						tableID, tableName,
						tree.NewDInt(tree.DInt(idx.GetID())),
						tree.DNull,
						tree.NewDInt(tree.DInt(interleaveParent.TableID)),
						interleaveDep,
						tree.NewDInt(tree.DInt(interleaveParent.IndexID)),
						tree.DNull,
						tree.NewDString(fmt.Sprintf("SharedPrefixLen: %d",
							interleaveParent.SharedPrefixLen)),
					); err != nil {
						return err
					}
				}
				return nil
			}
			if err := table.ForeachOutboundFK(func(fk *descpb.ForeignKeyConstraint) error {
				refTbl, err := tableLookup.getTableByID(fk.ReferencedTableID)
				if err != nil {
					return err
				}
				refConstraint, err := tabledesc.FindFKReferencedUniqueConstraint(refTbl, fk.ReferencedColumnIDs)
				if err != nil {
					return err
				}
				var refIdxID descpb.IndexID
				if refIdx, ok := refConstraint.(*descpb.IndexDescriptor); ok {
					refIdxID = refIdx.ID
				}
				return addRow(
					tableID, tableName,
					tree.DNull,
					tree.DNull,
					tree.NewDInt(tree.DInt(fk.ReferencedTableID)),
					fkDep,
					tree.NewDInt(tree.DInt(refIdxID)),
					tree.NewDString(fk.Name),
					tree.DNull,
				)
			}); err != nil {
				return err
			}

			// Record the backward references of the primary index.
			if err := catalog.ForEachIndex(table, catalog.IndexOpts{}, reportIdxDeps); err != nil {
				return err
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
		interleaveDep := tree.NewDString("interleave")
		sequenceDep := tree.NewDString("sequence")
		return forEachTableDescAll(ctx, p, dbContext, hideVirtual, /* virtual tables have no backward/forward dependencies*/
			func(db catalog.DatabaseDescriptor, _ string, table catalog.TableDescriptor) error {
				tableID := tree.NewDInt(tree.DInt(table.GetID()))
				tableName := tree.NewDString(table.GetName())

				reportIdxDeps := func(idx catalog.Index) error {
					for i := 0; i < idx.NumInterleavedBy(); i++ {
						interleaveRef := idx.GetInterleavedBy(i)
						if err := addRow(
							tableID, tableName,
							tree.NewDInt(tree.DInt(idx.GetID())),
							tree.NewDInt(tree.DInt(interleaveRef.Table)),
							interleaveDep,
							tree.NewDInt(tree.DInt(interleaveRef.Index)),
							tree.DNull,
							tree.NewDString(fmt.Sprintf("SharedPrefixLen: %d",
								interleaveRef.SharedPrefixLen)),
						); err != nil {
							return err
						}
					}
					return nil
				}
				if err := table.ForeachInboundFK(func(fk *descpb.ForeignKeyConstraint) error {
					return addRow(
						tableID, tableName,
						tree.DNull,
						tree.NewDInt(tree.DInt(fk.OriginTableID)),
						fkDep,
						tree.DNull,
						tree.DNull,
						tree.DNull,
					)
				}); err != nil {
					return err
				}

				// Record the backward references of the primary index.
				if err := catalog.ForEachIndex(table, catalog.IndexOpts{}, reportIdxDeps); err != nil {
					return err
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

// crdbInternalRangesView exposes system ranges.
var crdbInternalRangesView = virtualSchemaView{
	schema: `
CREATE VIEW crdb_internal.ranges AS SELECT
	range_id,
	start_key,
	start_pretty,
	end_key,
	end_pretty,
  table_id,
	database_name,
  schema_name,
	table_name,
	index_name,
	replicas,
	replica_localities,
	voting_replicas,
	non_voting_replicas,
	learner_replicas,
	split_enforced_until,
	crdb_internal.lease_holder(start_key) AS lease_holder,
	(crdb_internal.range_stats(start_key)->>'key_bytes')::INT +
	(crdb_internal.range_stats(start_key)->>'val_bytes')::INT AS range_size
FROM crdb_internal.ranges_no_leases
`,
	resultColumns: colinfo.ResultColumns{
		{Name: "range_id", Typ: types.Int},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_key", Typ: types.Bytes},
		{Name: "end_pretty", Typ: types.String},
		{Name: "table_id", Typ: types.Int},
		{Name: "database_name", Typ: types.String},
		{Name: "schema_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "index_name", Typ: types.String},
		{Name: "replicas", Typ: types.Int2Vector},
		{Name: "replica_localities", Typ: types.StringArray},
		{Name: "voting_replicas", Typ: types.Int2Vector},
		{Name: "non_voting_replicas", Typ: types.Int2Vector},
		{Name: "learner_replicas", Typ: types.Int2Vector},
		{Name: "split_enforced_until", Typ: types.Timestamp},
		{Name: "lease_holder", Typ: types.Int},
		{Name: "range_size", Typ: types.Int},
	},
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
  table_id             INT NOT NULL,
  database_name        STRING NOT NULL,
  schema_name          STRING NOT NULL,
  table_name           STRING NOT NULL,
  index_name           STRING NOT NULL,
  replicas             INT[] NOT NULL,
  replica_localities   STRING[] NOT NULL,
  voting_replicas      INT[] NOT NULL,
  non_voting_replicas  INT[] NOT NULL,
  learner_replicas     INT[] NOT NULL,
  split_enforced_until TIMESTAMP
)
`,
	generator: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, _ *stop.Stopper) (virtualTableGenerator, cleanupFunc, error) {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.ranges_no_leases"); err != nil {
			return nil, nil, err
		}
		descs, err := p.Descriptors().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return nil, nil, err
		}
		// TODO(knz): maybe this could use internalLookupCtx.
		dbNames := make(map[uint32]string)
		tableNames := make(map[uint32]string)
		schemaNames := make(map[uint32]string)
		indexNames := make(map[uint32]map[uint32]string)
		schemaParents := make(map[uint32]uint32)
		parents := make(map[uint32]uint32)
		for _, desc := range descs {
			id := uint32(desc.GetID())
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
		ranges, err := kvclient.ScanMetaKVs(ctx, p.txn, roachpb.Span{
			Key:    keys.MinKey,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			return nil, nil, err
		}

		// Map node descriptors to localities
		descriptors, err := getAllNodeDescriptors(p)
		if err != nil {
			return nil, nil, err
		}
		nodeIDToLocality := make(map[roachpb.NodeID]roachpb.Locality)
		for _, desc := range descriptors {
			nodeIDToLocality[desc.NodeID] = desc.Locality
		}

		var desc roachpb.RangeDescriptor

		i := 0

		return func() (tree.Datums, error) {
			if i >= len(ranges) {
				return nil, nil
			}

			r := ranges[i]
			i++

			if err := r.ValueProto(&desc); err != nil {
				return nil, err
			}

			votersAndNonVoters := append([]roachpb.ReplicaDescriptor(nil),
				desc.Replicas().VoterAndNonVoterDescriptors()...)
			var learnerReplicaStoreIDs []int
			for _, rd := range desc.Replicas().LearnerDescriptors() {
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
			for _, replica := range desc.Replicas().VoterDescriptors() {
				if err := votersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return nil, err
				}
			}
			nonVotersArr := tree.NewDArray(types.Int)
			for _, replica := range desc.Replicas().NonVoterDescriptors() {
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
				replicaLocality := nodeIDToLocality[replica.NodeID].String()
				if err := replicaLocalityArr.Append(tree.NewDString(replicaLocality)); err != nil {
					return nil, err
				}
			}

			var dbName, schemaName, tableName, indexName string
			var tableID uint32
			if _, tableID, err = p.ExecCfg().Codec.DecodeTablePrefix(desc.StartKey.AsRawKey()); err == nil {
				schemaParent := schemaParents[tableID]
				if schemaParent != 0 {
					schemaName = schemaNames[schemaParent]
				} else {
					// This case shouldn't happen - all schema ids should be available in the
					// schemaParents map. If it's not, just assume the name of the schema
					// is public to avoid problems.
					schemaName = string(tree.PublicSchemaName)
				}
				parent := parents[tableID]
				if parent != 0 {
					tableName = tableNames[tableID]
					dbName = dbNames[parent]
					if _, _, idxID, err := p.ExecCfg().Codec.DecodeIndexPrefix(desc.StartKey.AsRawKey()); err == nil {
						indexName = indexNames[tableID][idxID]
					}
				} else {
					dbName = dbNames[tableID]
				}
			}

			splitEnforcedUntil := tree.DNull
			if !desc.GetStickyBit().IsEmpty() {
				splitEnforcedUntil = tree.TimestampToInexactDTimestamp(*desc.StickyBit)
			}

			return tree.Datums{
				tree.NewDInt(tree.DInt(desc.RangeID)),
				tree.NewDBytes(tree.DBytes(desc.StartKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, desc.StartKey.AsRawKey())),
				tree.NewDBytes(tree.DBytes(desc.EndKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, desc.EndKey.AsRawKey())),
				tree.NewDInt(tree.DInt(tableID)),
				tree.NewDString(dbName),
				tree.NewDString(schemaName),
				tree.NewDString(tableName),
				tree.NewDString(indexName),
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
	return getAllNames(ctx, p.txn, p.ExtendedEvalContext().ExecCfg.InternalExecutor)
}

// TestingGetAllNames is a wrapper for getAllNames.
func TestingGetAllNames(
	ctx context.Context, txn *kv.Txn, executor *InternalExecutor,
) (map[descpb.ID]catalog.NameKey, error) {
	return getAllNames(ctx, txn, executor)
}

// getAllNames is the testable implementation of getAllNames.
// It is public so that it can be tested outside the sql package.
func getAllNames(
	ctx context.Context, txn *kv.Txn, executor *InternalExecutor,
) (map[descpb.ID]catalog.NameKey, error) {
	namespace := map[descpb.ID]catalog.NameKey{}
	it, err := executor.QueryIterator(
		ctx, "get-all-names", txn,
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
		if !p.ExecCfg().Codec.ForSystemTenant() {
			// Don't try to populate crdb_internal.zones if running in a multitenant
			// configuration.
			return nil
		}

		namespace, err := p.getAllNames(ctx)
		if err != nil {
			return err
		}
		resolveID := func(id uint32) (parentID, parentSchemaID uint32, name string, err error) {
			if id == keys.PublicSchemaID {
				return 0, 0, string(tree.PublicSchemaName), nil
			}
			if entry, ok := namespace[descpb.ID(id)]; ok {
				return uint32(entry.GetParentID()), uint32(entry.GetParentSchemaID()), entry.GetName(), nil
			}
			return 0, 0, "", errors.AssertionFailedf(
				"object with ID %d does not exist", errors.Safe(id))
		}

		getKey := func(key roachpb.Key) (*roachpb.Value, error) {
			kv, err := p.txn.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			return kv.Value, nil
		}

		// For some reason, if we use the iterator API here, "concurrent txn use
		// detected" error might occur, so we buffer up all zones first.
		rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryBuffered(
			ctx, "crdb-internal-zones-table", p.txn, `SELECT id, config FROM system.zones`)
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
			} else {
				zoneSpecifier = &zs
			}

			configBytes := []byte(*r[1].(*tree.DBytes))
			var configProto zonepb.ZoneConfig
			if err := protoutil.Unmarshal(configBytes, &configProto); err != nil {
				return err
			}
			subzones := configProto.Subzones

			// Inherit full information about this zone.
			fullZone := configProto
			if err := completeZoneConfig(&fullZone, config.SystemTenantObjectID(tree.MustBeDInt(r[0])), getKey); err != nil {
				return err
			}

			var table catalog.TableDescriptor
			if zs.Database != "" {
				_, database, err := p.Descriptors().GetImmutableDatabaseByID(ctx, p.txn, descpb.ID(id), tree.DatabaseLookupFlags{
					Required:    true,
					AvoidCached: true,
				})
				if err != nil {
					return err
				}
				if p.CheckAnyPrivilege(ctx, database) != nil {
					continue
				}
			} else if zoneSpecifier.TableOrIndex.Table.ObjectName != "" {
				tableEntry, err := p.LookupTableByID(ctx, descpb.ID(id))
				if err != nil {
					return err
				}
				if p.CheckAnyPrivilege(ctx, tableEntry) != nil {
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
						// dropped and in the GC queue.
						continue
					}
					if zoneSpecifier != nil {
						zs := zs
						zs.TableOrIndex.Index = tree.UnrestrictedName(index.GetName())
						zs.Partition = tree.Name(s.PartitionName)
						zoneSpecifier = &zs
					}

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
	if err := g.IterateInfos(gossip.KeyNodeIDPrefix, func(key string, i gossip.Info) error {
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
		if err := p.RequireAdminRole(ctx, "read crdb_internal.gossip_nodes"); err != nil {
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
		for _, d := range descriptors {
			if _, err := g.GetInfo(gossip.MakeGossipClientsKey(d.NodeID)); err == nil {
				alive[d.NodeID] = true
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
		if err := g.IterateInfos(gossip.KeyStorePrefix, func(key string, i gossip.Info) error {
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

			advAddrRPC, err := g.GetNodeIDAddress(d.NodeID)
			if err != nil {
				return err
			}
			advAddrSQL, err := g.GetNodeIDSQLAddress(d.NodeID)
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
		if err := p.RequireAdminRole(ctx, "read crdb_internal.node_liveness"); err != nil {
			return err
		}

		nl, err := p.ExecCfg().NodeLiveness.OptionalErr(47900)
		if err != nil {
			return err
		}

		livenesses, err := nl.GetLivenessesFromKV(ctx)
		if err != nil {
			return err
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
		// cluster to be healthy, such as NodesStatusServer.Nodes().

		if err := p.RequireAdminRole(ctx, "read crdb_internal.gossip_liveness"); err != nil {
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
		if err := p.RequireAdminRole(ctx, "read crdb_internal.gossip_alerts"); err != nil {
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
			nodeID, err := gossip.NodeIDFromKey(key, gossip.KeyNodeHealthAlertPrefix)
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
		if err := p.RequireAdminRole(ctx, "read crdb_internal.gossip_network"); err != nil {
			return err
		}

		g, err := p.ExecCfg().Gossip.OptionalErr(47899)
		if err != nil {
			return err
		}

		c := g.Connectivity()
		for _, conn := range c.ClientConns {
			if err := addRow(
				tree.NewDInt(tree.DInt(conn.SourceID)),
				tree.NewDInt(tree.DInt(conn.TargetID)),
			); err != nil {
				return err
			}
		}
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
	// Secondary tenants cannot set zone configs on individual objects, so they
	// have no ability to partition tables/indexes.
	// NOTE: we assume the system tenant below by casting object IDs directly to
	// config.SystemTenantObjectID.
	if !p.ExecCfg().Codec.ForSystemTenant() {
		return nil
	}

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

	var datumAlloc rowenc.DatumAlloc

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
			ctx, p.txn, config.SystemTenantObjectID(table.GetID()), index, name, false /* getInheritedDefault */)
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
			ctx, p.txn, config.SystemTenantObjectID(table.GetID()), index, name, false /* getInheritedDefault */)
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
		worker := func(pusher rowPusher) error {
			return forEachTableDescAll(ctx, p, dbContext, hideVirtual, /* virtual tables have no partitions*/
				func(db catalog.DatabaseDescriptor, _ string, table catalog.TableDescriptor) error {
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
		if err := p.RequireAdminRole(ctx, "read crdb_internal.kv_node_status"); err != nil {
			return err
		}
		ss, err := p.extendedEvalCtx.NodesStatusServer.OptionalNodesStatusServer(
			errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
		if err != nil {
			return err
		}
		response, err := ss.Nodes(ctx, &serverpb.NodesRequest{})
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
				b.Add("incoming", json.FromInt64(values.Incoming))
				b.Add("outgoing", json.FromInt64(values.Outgoing))
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
  metrics            JSON NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, _ catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.kv_store_status"); err != nil {
			return err
		}
		ss, err := p.ExecCfg().NodesStatusServer.OptionalNodesStatusServer(
			errorutil.FeatureNotAvailableToNonSystemTenantsIssue)
		if err != nil {
			return err
		}
		response, err := ss.Nodes(ctx, &serverpb.NodesRequest{})
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
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

// crdbInternalPredefinedComments exposes the predefined
// comments for virtual tables. This is used by SHOW TABLES WITH COMMENT
// as fall-back when system.comments is silent.
// TODO(knz): extend this with vtable column comments.
//
// TODO(tbg): prefix with node_.
var crdbInternalPredefinedCommentsTable = virtualSchemaTable{
	comment: `comments for predefined virtual tables (RAM/static)`,
	schema: `
CREATE TABLE crdb_internal.predefined_comments (
	TYPE      INT,
	OBJECT_ID INT,
	SUB_ID    INT,
	COMMENT   STRING
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		tableCommentKey := tree.NewDInt(keys.TableCommentType)
		vt := p.getVirtualTabler()
		vEntries := vt.getSchemas()
		vSchemaNames := vt.getSchemaNames()

		for _, virtSchemaName := range vSchemaNames {
			e := vEntries[virtSchemaName]

			for _, tName := range e.orderedDefNames {
				vTableEntry := e.defs[tName]
				table := vTableEntry.desc

				if vTableEntry.comment != "" {
					if err := addRow(
						tableCommentKey,
						tree.NewDInt(tree.DInt(table.GetID())),
						zeroVal,
						tree.NewDString(vTableEntry.comment)); err != nil {
						return err
					}
				}
			}
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
	md.Status = jobs.Status(*ujm.status)
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
			referencedJobIDs[jobspb.JobID(j.JobID)] = struct{}{}
		}
	}
	if len(referencedJobIDs) == 0 {
		return nil, nil
	}
	// Build job map with referenced job IDs.
	m := make(marshaledJobMetadataMap)
	query := `SELECT id, status, payload, progress FROM system.jobs`
	it, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryIteratorEx(
		ctx, "crdb-internal-jobs-table", p.Txn(),
		sessiondata.InternalExecutorOverride{User: security.RootUserName()},
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
  error         STRING
)`,
	populate: func(
		ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		// The internalLookupContext will only have descriptors in the current
		// database. To deal with this, we fall through.
		m, err := catalogkv.GetAllDescriptorsAndNamespaceEntriesUnvalidated(ctx, p.txn, p.extendedEvalCtx.Codec)
		if err != nil {
			return err
		}
		descs := m.OrderedDescriptors()
		// Collect all marshaled job metadata and account for its memory usage.
		acct := p.EvalContext().Mon.MakeBoundAccount()
		defer acct.Close(ctx)
		jmg, err := collectMarshaledJobMetadataMap(ctx, p, &acct, descs)
		if err != nil {
			return err
		}

		addRowsForObject := func(dbDesc catalog.DatabaseDescriptor, schema string, descriptor catalog.Descriptor) (err error) {
			if descriptor == nil {
				return nil
			}
			var dbName string
			if dbDesc != nil {
				dbName = dbDesc.GetName()
			}
			addValidationErrorRow := func(validationError error) {
				if err == nil {
					err = addRow(
						tree.NewDInt(tree.DInt(descriptor.GetID())),
						tree.NewDString(dbName),
						tree.NewDString(schema),
						tree.NewDString(descriptor.GetName()),
						tree.NewDString(validationError.Error()),
					)
				}
			}
			ve := catalog.ValidateWithRecover(ctx, m, catalog.ValidationLevelAllPreTxnCommit, descriptor)
			for _, validationError := range ve.Errors() {
				addValidationErrorRow(validationError)
			}
			jobs.ValidateJobReferencesInDescriptor(descriptor, jmg, addValidationErrorRow)
			return err
		}

		const allowAdding = true
		if err := forEachTableDescWithTableLookupInternalFromDescriptors(
			ctx, p, dbContext, hideVirtual, allowAdding, descs, func(
				dbDesc catalog.DatabaseDescriptor, schema string, descriptor catalog.TableDescriptor, _ tableLookupFn,
			) error {
				return addRowsForObject(dbDesc, schema, descriptor)
			}); err != nil {
			return err
		}

		// Validate type descriptors.
		return forEachTypeDescWithTableLookupInternalFromDescriptors(
			ctx, p, dbContext, allowAdding, descs, func(
				dbDesc catalog.DatabaseDescriptor, schema string, descriptor catalog.TypeDescriptor, _ tableLookupFn,
			) error {
				return addRowsForObject(dbDesc, schema, descriptor)
			})
	},
}

var crdbInternalClusterDatabasePrivilegesTable = virtualSchemaTable{
	comment: `virtual table with database privileges`,
	schema: `
CREATE TABLE crdb_internal.cluster_database_privileges (
	database_name   STRING NOT NULL,
	grantee         STRING NOT NULL,
	privilege_type  STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, dbContext, true, /* requiresPrivileges */
			func(db catalog.DatabaseDescriptor) error {
				privs := db.GetPrivileges().Show(privilege.Database)
				dbNameStr := tree.NewDString(db.GetName())
				// TODO(knz): This should filter for the current user, see
				// https://github.com/cockroachdb/cockroach/issues/35572
				for _, u := range privs {
					userNameStr := tree.NewDString(u.User.Normalized())
					for _, priv := range u.Privileges {
						if err := addRow(
							dbNameStr,             // database_name
							userNameStr,           // grantee
							tree.NewDString(priv), // privilege_type
						); err != nil {
							return err
						}
					}
				}
				return nil
			})
	},
}

var crdbInternalInterleaved = virtualSchemaTable{
	comment: `virtual table with interleaved table information`,
	schema: `
CREATE TABLE crdb_internal.interleaved (
	database_name
		STRING NOT NULL,
	schema_name
		STRING NOT NULL,
	table_name
		STRING NOT NULL,
	index_name
		STRING NOT NULL,
	parent_database_name
		STRING NOT NULL,
	parent_schema_name
		STRING NOT NULL,
	parent_table_name
		STRING NOT NULL
);`,
	populate: func(ctx context.Context, p *planner, dbContext catalog.DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTableDescAllWithTableLookup(ctx, p, dbContext, hideVirtual,
			func(db catalog.DatabaseDescriptor, schemaName string, table catalog.TableDescriptor, lookupFn tableLookupFn) error {
				if !table.IsInterleaved() {
					return nil
				}
				indexes := table.NonDropIndexes()
				for _, index := range indexes {
					if index.NumInterleaveAncestors() == 0 {
						continue
					}
					ancestor := index.GetInterleaveAncestor(index.NumInterleaveAncestors() - 1)
					parentTable, err := lookupFn.getTableByID(ancestor.TableID)
					if err != nil {
						return err
					}
					parentSchemaName, err := lookupFn.getSchemaNameByID(parentTable.GetParentSchemaID())
					if err != nil {
						return err
					}
					parentDatabase, err := lookupFn.getDatabaseByID(parentTable.GetParentID())
					if err != nil {
						return err
					}

					if err := addRow(tree.NewDString(db.GetName()),
						tree.NewDString(schemaName),
						tree.NewDString(table.GetName()),
						tree.NewDString(index.GetName()),
						tree.NewDString(parentDatabase.GetName()),
						tree.NewDString(parentSchemaName),
						tree.NewDString(parentTable.GetName())); err != nil {
						return err
					}
				}
				return nil
			})
	},
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
		return forEachTableDescAllWithTableLookup(ctx, p, dbContext, hideVirtual,
			func(db catalog.DatabaseDescriptor, schemaName string, table catalog.TableDescriptor, lookupFn tableLookupFn) error {
				// For tables detect if foreign key references point at a different
				// database. Additionally, check if any of the columns have sequence
				// references to a different database.
				if table.IsTable() {
					objectDatabaseName := lookupFn.getDatabaseName(table)
					err := table.ForeachOutboundFK(
						func(fk *descpb.ForeignKeyConstraint) error {
							referencedTable, err := lookupFn.getTableByID(fk.ReferencedTableID)
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
									tree.NewDString(schemaName),
									tree.NewDString(table.GetName()),
									tree.NewDString(refDatabaseName),
									tree.NewDString(refSchemaName),
									tree.NewDString(referencedTable.GetName()),
									tree.NewDString("table foreign key reference")); err != nil {
									return err
								}
							}
							return nil
						})
					if err != nil {
						return err
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
									tree.NewDString(schemaName),
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
								tree.NewDString(schemaName),
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
								tree.NewDString(schemaName),
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
								tree.NewDString(schemaName),
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
		maxDescIDKeyVal, err := p.extendedEvalCtx.DB.Get(context.Background(), p.extendedEvalCtx.Codec.DescIDSequenceKey())
		if err != nil {
			return err
		}
		maxDescID, err := maxDescIDKeyVal.Value.GetInt()
		if err != nil {
			return err
		}
		// Get all descriptors which will be used to determine
		// which ones are missing.
		dg, err := catalogkv.GetAllDescriptorsAndNamespaceEntriesUnvalidated(ctx, p.txn, p.extendedEvalCtx.Codec)
		if err != nil {
			return err
		}
		// Generate large batches of scans over the keyspace,
		// so that we minimize scans. We will issue individual
		// scans if there is data in a given descriptor range.
		unusedDescSpan := roachpb.Span{}
		descStart := 0
		descEnd := 0
		scanAndGenerateRows := func() error {
			if unusedDescSpan.Key == nil {
				return nil
			}
			b := kv.Batch{}
			b.Header.MaxSpanRequestKeys = 1
			scanRequest := roachpb.NewScan(unusedDescSpan.Key, unusedDescSpan.EndKey, false).(*roachpb.ScanRequest)
			scanRequest.ScanFormat = roachpb.BATCH_RESPONSE
			b.AddRawRequest(scanRequest)
			err = p.extendedEvalCtx.DB.Run(ctx, &b)
			if err != nil {
				return err
			}
			// Check the descriptors inside this range for
			// data.
			res := b.RawResponse().Responses[0].GetScan()
			if res.NumKeys > 0 {
				b = kv.Batch{}
				b.Header.MaxSpanRequestKeys = 1
				for descID := descStart; descID <= descEnd; descID++ {
					prefix := p.extendedEvalCtx.Codec.TablePrefix(uint32(descID))
					scanRequest := roachpb.NewScan(prefix, prefix.PrefixEnd(), false).(*roachpb.ScanRequest)
					scanRequest.ScanFormat = roachpb.BATCH_RESPONSE
					b.AddRawRequest(scanRequest)
				}
				err = p.extendedEvalCtx.DB.Run(ctx, &b)
				if err != nil {
					return err
				}
				for idx := range b.RawResponse().Responses {
					res := b.RawResponse().Responses[idx].GetScan()
					if res.NumKeys == 0 {
						continue
					}
					// Add a row if any key came back
					if err := addRow(tree.NewDInt(tree.DInt(idx + descStart))); err != nil {
						return err
					}
				}
			}
			unusedDescSpan = roachpb.Span{}
			descStart = 0
			descEnd = 0
			return nil
		}
		// Loop over every possible descriptor ID
		for id := keys.MinUserDescID; id < int(maxDescID); id++ {
			// Skip over descriptors that are known
			if _, ok := dg.Descriptors[descpb.ID(id)]; ok {
				err := scanAndGenerateRows()
				if err != nil {
					return err
				}
				continue
			}
			// Update our span range to include this
			// descriptor.
			prefix := p.extendedEvalCtx.Codec.TablePrefix(uint32(id))
			if unusedDescSpan.Key == nil {
				descStart = id
				unusedDescSpan.Key = prefix
			}
			descEnd = id
			unusedDescSpan.EndKey = prefix.PrefixEnd()

		}
		err = scanAndGenerateRows()
		if err != nil {
			return err
		}
		return nil
	},
}
