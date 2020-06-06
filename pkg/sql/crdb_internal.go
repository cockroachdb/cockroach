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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/status/statuspb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const crdbInternalName = sessiondata.CRDBInternalSchemaName

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
	name: crdbInternalName,
	tableDefs: map[sqlbase.ID]virtualSchemaDef{
		sqlbase.CrdbInternalBackwardDependenciesTableID: crdbInternalBackwardDependenciesTable,
		sqlbase.CrdbInternalBuildInfoTableID:            crdbInternalBuildInfoTable,
		sqlbase.CrdbInternalBuiltinFunctionsTableID:     crdbInternalBuiltinFunctionsTable,
		sqlbase.CrdbInternalClusterQueriesTableID:       crdbInternalClusterQueriesTable,
		sqlbase.CrdbInternalClusterTransactionsTableID:  crdbInternalClusterTxnsTable,
		sqlbase.CrdbInternalClusterSessionsTableID:      crdbInternalClusterSessionsTable,
		sqlbase.CrdbInternalClusterSettingsTableID:      crdbInternalClusterSettingsTable,
		sqlbase.CrdbInternalCreateStmtsTableID:          crdbInternalCreateStmtsTable,
		sqlbase.CrdbInternalCreateTypeStmtsTableID:      crdbInternalCreateTypeStmtsTable,
		sqlbase.CrdbInternalDatabasesTableID:            crdbInternalDatabasesTable,
		sqlbase.CrdbInternalFeatureUsageID:              crdbInternalFeatureUsage,
		sqlbase.CrdbInternalForwardDependenciesTableID:  crdbInternalForwardDependenciesTable,
		sqlbase.CrdbInternalGossipNodesTableID:          crdbInternalGossipNodesTable,
		sqlbase.CrdbInternalGossipAlertsTableID:         crdbInternalGossipAlertsTable,
		sqlbase.CrdbInternalGossipLivenessTableID:       crdbInternalGossipLivenessTable,
		sqlbase.CrdbInternalGossipNetworkTableID:        crdbInternalGossipNetworkTable,
		sqlbase.CrdbInternalIndexColumnsTableID:         crdbInternalIndexColumnsTable,
		sqlbase.CrdbInternalJobsTableID:                 crdbInternalJobsTable,
		sqlbase.CrdbInternalKVNodeStatusTableID:         crdbInternalKVNodeStatusTable,
		sqlbase.CrdbInternalKVStoreStatusTableID:        crdbInternalKVStoreStatusTable,
		sqlbase.CrdbInternalLeasesTableID:               crdbInternalLeasesTable,
		sqlbase.CrdbInternalLocalQueriesTableID:         crdbInternalLocalQueriesTable,
		sqlbase.CrdbInternalLocalTransactionsTableID:    crdbInternalLocalTxnsTable,
		sqlbase.CrdbInternalLocalSessionsTableID:        crdbInternalLocalSessionsTable,
		sqlbase.CrdbInternalLocalMetricsTableID:         crdbInternalLocalMetricsTable,
		sqlbase.CrdbInternalPartitionsTableID:           crdbInternalPartitionsTable,
		sqlbase.CrdbInternalPredefinedCommentsTableID:   crdbInternalPredefinedCommentsTable,
		sqlbase.CrdbInternalRangesNoLeasesTableID:       crdbInternalRangesNoLeasesTable,
		sqlbase.CrdbInternalRangesViewID:                crdbInternalRangesView,
		sqlbase.CrdbInternalRuntimeInfoTableID:          crdbInternalRuntimeInfoTable,
		sqlbase.CrdbInternalSchemaChangesTableID:        crdbInternalSchemaChangesTable,
		sqlbase.CrdbInternalSessionTraceTableID:         crdbInternalSessionTraceTable,
		sqlbase.CrdbInternalSessionVariablesTableID:     crdbInternalSessionVariablesTable,
		sqlbase.CrdbInternalStmtStatsTableID:            crdbInternalStmtStatsTable,
		sqlbase.CrdbInternalTableColumnsTableID:         crdbInternalTableColumnsTable,
		sqlbase.CrdbInternalTableIndexesTableID:         crdbInternalTableIndexesTable,
		sqlbase.CrdbInternalTablesTableID:               crdbInternalTablesTable,
		sqlbase.CrdbInternalTxnStatsTableID:             crdbInternalTxnStatsTable,
		sqlbase.CrdbInternalZonesTableID:                crdbInternalZonesTable,
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
	populate: func(_ context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "access the node runtime information"); err != nil {
			return err
		}

		node := p.ExecCfg().NodeInfo

		nodeID := tree.NewDInt(tree.DInt(int64(node.NodeID.Get())))
		dbURL, err := node.PGURL(url.User(security.RootUser))
		if err != nil {
			return err
		}

		for _, item := range []struct {
			component string
			url       *url.URL
		}{
			{"DB", dbURL}, {"UI", node.AdminURL()},
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
					nodeID,
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
	name STRING NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachDatabaseDesc(ctx, p, nil /* all databases */, true, /* requiresPrivileges */
			func(db *sqlbase.DatabaseDescriptor) error {
				return addRow(
					tree.NewDInt(tree.DInt(db.ID)), // id
					tree.NewDString(db.Name),       // name
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
  schema_name              STRING NOT NULL
)`,
	generator: func(ctx context.Context, p *planner, dbDesc *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		row := make(tree.Datums, 14)
		worker := func(pusher rowPusher) error {
			descs, err := p.Tables().GetAllDescriptors(ctx, p.txn)
			if err != nil {
				return err
			}
			dbNames := make(map[sqlbase.ID]string)
			// Record database descriptors for name lookups.
			for _, desc := range descs {
				db, ok := desc.(*sqlbase.DatabaseDescriptor)
				if ok {
					dbNames[db.ID] = db.Name
				}
			}

			addDesc := func(table *sqlbase.TableDescriptor, dbName tree.Datum, scName string) error {
				leaseNodeDatum := tree.DNull
				leaseExpDatum := tree.DNull
				if table.Lease != nil {
					leaseNodeDatum = tree.NewDInt(tree.DInt(int64(table.Lease.NodeID)))
					leaseExpDatum, err = tree.MakeDTimestamp(
						timeutil.Unix(0, table.Lease.ExpirationTime), time.Nanosecond,
					)
					if err != nil {
						return err
					}
				}
				dropTimeDatum := tree.DNull
				if table.DropTime != 0 {
					dropTimeDatum, err = tree.MakeDTimestamp(
						timeutil.Unix(0, table.DropTime), time.Nanosecond,
					)
					if err != nil {
						return err
					}
				}
				row = row[:0]
				row = append(row,
					tree.NewDInt(tree.DInt(int64(table.ID))),
					tree.NewDInt(tree.DInt(int64(table.GetParentID()))),
					tree.NewDString(table.Name),
					dbName,
					tree.NewDInt(tree.DInt(int64(table.Version))),
					tree.TimestampToInexactDTimestamp(table.ModificationTime),
					tree.TimestampToDecimal(table.ModificationTime),
					tree.NewDString(table.FormatVersion.String()),
					tree.NewDString(table.State.String()),
					leaseNodeDatum,
					leaseExpDatum,
					dropTimeDatum,
					tree.NewDString(table.AuditMode.String()),
					tree.NewDString(scName),
				)
				return pusher.pushRow(row...)
			}

			// Note: we do not use forEachTableDesc() here because we want to
			// include added and dropped descriptors.
			for _, desc := range descs {
				table, ok := desc.(*sqlbase.TableDescriptor)
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
				if err := addDesc(table, tree.NewDString(dbName), "public"); err != nil {
					return err
				}
			}

			// Also add all the virtual descriptors.
			vt := p.getVirtualTabler()
			vEntries := vt.getEntries()
			for _, virtSchemaName := range vt.getSchemaNames() {
				e := vEntries[virtSchemaName]
				for _, tName := range e.orderedDefNames {
					vTableEntry := e.defs[tName]
					if err := addDesc(vTableEntry.desc, tree.DNull, virtSchemaName); err != nil {
						return err
					}
				}
			}
			return nil
		}
		next, cleanup := setupGenerator(ctx, worker)
		return next, cleanup, nil
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		descs, err := p.Tables().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, ok := desc.(*sqlbase.TableDescriptor)
			if !ok || p.CheckAnyPrivilege(ctx, table) != nil {
				continue
			}
			tableID := tree.NewDInt(tree.DInt(int64(table.ID)))
			parentID := tree.NewDInt(tree.DInt(int64(table.GetParentID())))
			tableName := tree.NewDString(table.Name)
			for _, mut := range table.Mutations {
				mutType := "UNKNOWN"
				targetID := tree.DNull
				targetName := tree.DNull
				switch d := mut.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					mutType = "COLUMN"
					targetID = tree.NewDInt(tree.DInt(int64(d.Column.ID)))
					targetName = tree.NewDString(d.Column.Name)
				case *sqlbase.DescriptorMutation_Index:
					mutType = "INDEX"
					targetID = tree.NewDInt(tree.DInt(int64(d.Index.ID)))
					targetName = tree.NewDString(d.Index.Name)
				case *sqlbase.DescriptorMutation_Constraint:
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
		ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error,
	) (err error) {
		nodeID := tree.NewDInt(tree.DInt(int64(p.execCfg.NodeID.Get())))
		p.LeaseMgr().VisitLeases(func(desc sqlbase.TableDescriptor, dropped bool, _ int, expiration tree.DTimestamp) (wantMore bool) {
			if p.CheckAnyPrivilege(ctx, &desc) != nil {
				// TODO(ajwerner): inspect what type of error got returned.
				return true
			}

			err = addRow(
				nodeID,
				tree.NewDInt(tree.DInt(int64(desc.ID))),
				tree.NewDString(desc.Name),
				tree.NewDInt(tree.DInt(int64(desc.ParentID))),
				&expiration,
				tree.MakeDBool(tree.DBool(dropped)),
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
	job_id             		INT,
	job_type           		STRING,
	description        		STRING,
	statement          		STRING,
	user_name          		STRING,
	descriptor_ids     		INT[],
	status             		STRING,
	running_status     		STRING,
	created            		TIMESTAMP,
	started            		TIMESTAMP,
	finished           		TIMESTAMP,
	modified           		TIMESTAMP,
	fraction_completed 		FLOAT,
	high_water_timestamp	DECIMAL,
	error              		STRING,
	coordinator_id     		INT
)`,
	comment: `decoded job metadata from system.jobs (KV scan)`,
	generator: func(ctx context.Context, p *planner, _ *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		currentUser := p.SessionData().User
		isAdmin, err := p.HasAdminRole(ctx)
		if err != nil {
			return nil, nil, err
		}

		// Beware: we're querying system.jobs as root; we need to be careful to filter
		// out results that the current user is not able to see.
		query := `SELECT id, status, created, payload, progress FROM system.jobs`
		rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.QueryEx(
			ctx, "crdb-internal-jobs-table", p.txn,
			sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
			query)
		if err != nil {
			return nil, nil, err
		}

		// Attempt to account for the memory of the retrieved rows and the data
		// we're going to unmarshal and keep bufferred in RAM.
		//
		// TODO(ajwerner): This is a pretty terrible hack. Instead the internal
		// executor should be hooked into the memory monitor associated with this
		// conn executor. If we did that we would still want to account for the
		// unmarshaling. Additionally, it's probably a good idea to paginate this
		// and other virtual table queries but that's a bigger task.
		ba := p.ExtendedEvalContext().Mon.MakeBoundAccount()
		defer ba.Close(ctx)
		var totalMem int64
		for _, r := range rows {
			for _, d := range r {
				totalMem += int64(d.Size())
			}
		}
		if err := ba.Grow(ctx, totalMem); err != nil {
			return nil, nil, err
		}

		// We'll reuse this container on each loop.
		container := make(tree.Datums, 0, 16)
		return func() (datums tree.Datums, e error) {
			// Loop while we need to skip a row.
			for {
				if len(rows) == 0 {
					return nil, nil
				}
				r := rows[0]
				rows = rows[1:]
				id, status, created, payloadBytes, progressBytes := r[0], r[1], r[2], r[3], r[4]

				var jobType, description, statement, username, descriptorIDs, started, runningStatus,
					finished, modified, fractionCompleted, highWaterTimestamp, errorStr, leaseNode = tree.DNull,
					tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull,
					tree.DNull, tree.DNull, tree.DNull, tree.DNull, tree.DNull

				// Extract data from the payload.
				payload, err := jobs.UnmarshalPayload(payloadBytes)

				// We filter out masked rows before we allocate all the
				// datums. Needless allocate when not necessary.
				sameUser := payload != nil && payload.Username == currentUser
				if canAccess := isAdmin || sameUser; !canAccess {
					// This user is neither an admin nor the user who created the
					// job. They cannot see this row.
					continue
				}

				if err != nil {
					errorStr = tree.NewDString(fmt.Sprintf("error decoding payload: %v", err))
				} else {
					jobType = tree.NewDString(payload.Type().String())
					description = tree.NewDString(payload.Description)
					statement = tree.NewDString(payload.Statement)
					username = tree.NewDString(payload.Username)
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
					if payload.Lease != nil {
						leaseNode = tree.NewDInt(tree.DInt(payload.Lease.NodeID))
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
							highWaterTimestamp = tree.TimestampToDecimal(*highwater)
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
					leaseNode,
				)
				return container, nil
			}
		}, nil, nil
	},
}

type stmtList []stmtKey

func (s stmtList) Len() int {
	return len(s)
}
func (s stmtList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s stmtList) Less(i, j int) bool {
	return s[i].stmt < s[j].stmt
}

var crdbInternalStmtStatsTable = virtualSchemaTable{
	comment: `statement statistics (in-memory, not durable; local node only). ` +
		`This table is wiped periodically (by default, at least every two hours)`,
	schema: `
CREATE TABLE crdb_internal.node_statement_statistics (
  node_id             INT NOT NULL,
  application_name    STRING NOT NULL,
  flags               STRING NOT NULL,
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
  bytes_read          INT NOT NULL,
  rows_read           INT NOT NULL,
  implicit_txn        BOOL NOT NULL
)`,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "access application statistics"); err != nil {
			return err
		}

		sqlStats := p.extendedEvalCtx.sqlStatsCollector.sqlStats
		if sqlStats == nil {
			return errors.AssertionFailedf(
				"cannot access sql statistics from this context")
		}

		nodeID, _ := p.execCfg.NodeID.OptionalNodeID() // zero if not available

		// Retrieve the application names and sort them to ensure the
		// output is deterministic.
		var appNames []string
		sqlStats.Lock()
		for n := range sqlStats.apps {
			appNames = append(appNames, n)
		}
		sqlStats.Unlock()
		sort.Strings(appNames)

		// Now retrieve the application stats proper.
		for _, appName := range appNames {
			appStats := sqlStats.getStatsForApplication(appName)

			// Retrieve the statement keys and sort them to ensure the
			// output is deterministic.
			var stmtKeys stmtList
			appStats.Lock()
			for k := range appStats.stmts {
				stmtKeys = append(stmtKeys, k)
			}
			appStats.Unlock()
			sort.Sort(stmtKeys)

			// Now retrieve the per-stmt stats proper.
			for _, stmtKey := range stmtKeys {
				anonymized := tree.DNull
				anonStr, ok := scrubStmtStatKey(p.getVirtualTabler(), stmtKey.stmt)
				if ok {
					anonymized = tree.NewDString(anonStr)
				}

				s := appStats.getStatsForStmtWithKey(stmtKey, true /* createIfNonexistent */)

				s.Lock()
				errString := tree.DNull
				if s.data.SensitiveInfo.LastErr != "" {
					errString = tree.NewDString(s.data.SensitiveInfo.LastErr)
				}
				err := addRow(
					tree.NewDInt(tree.DInt(nodeID)),
					tree.NewDString(appName),
					tree.NewDString(stmtKey.flags()),
					tree.NewDString(stmtKey.stmt),
					anonymized,
					tree.NewDInt(tree.DInt(s.data.Count)),
					tree.NewDInt(tree.DInt(s.data.FirstAttemptCount)),
					tree.NewDInt(tree.DInt(s.data.MaxRetries)),
					errString,
					tree.NewDFloat(tree.DFloat(s.data.NumRows.Mean)),
					tree.NewDFloat(tree.DFloat(s.data.NumRows.GetVariance(s.data.Count))),
					tree.NewDFloat(tree.DFloat(s.data.ParseLat.Mean)),
					tree.NewDFloat(tree.DFloat(s.data.ParseLat.GetVariance(s.data.Count))),
					tree.NewDFloat(tree.DFloat(s.data.PlanLat.Mean)),
					tree.NewDFloat(tree.DFloat(s.data.PlanLat.GetVariance(s.data.Count))),
					tree.NewDFloat(tree.DFloat(s.data.RunLat.Mean)),
					tree.NewDFloat(tree.DFloat(s.data.RunLat.GetVariance(s.data.Count))),
					tree.NewDFloat(tree.DFloat(s.data.ServiceLat.Mean)),
					tree.NewDFloat(tree.DFloat(s.data.ServiceLat.GetVariance(s.data.Count))),
					tree.NewDFloat(tree.DFloat(s.data.OverheadLat.Mean)),
					tree.NewDFloat(tree.DFloat(s.data.OverheadLat.GetVariance(s.data.Count))),
					tree.NewDInt(tree.DInt(s.data.BytesRead)),
					tree.NewDInt(tree.DInt(s.data.RowsRead)),
					tree.MakeDBool(tree.DBool(stmtKey.implicitTxn)),
				)
				s.Unlock()
				if err != nil {
					return err
				}
			}
		}
		return nil
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "access application statistics"); err != nil {
			return err
		}

		sqlStats := p.extendedEvalCtx.sqlStatsCollector.sqlStats
		if sqlStats == nil {
			return errors.AssertionFailedf(
				"cannot access sql statistics from this context")
		}

		nodeID, _ := p.execCfg.NodeID.OptionalNodeID() // zero if not available

		// Retrieve the application names and sort them to ensure the
		// output is deterministic.
		var appNames []string
		sqlStats.Lock()
		for n := range sqlStats.apps {
			appNames = append(appNames, n)
		}
		sqlStats.Unlock()
		sort.Strings(appNames)

		for _, appName := range appNames {
			appStats := sqlStats.getStatsForApplication(appName)
			txnCount, txnTimeAvg, txnTimeVar, committedCount, implicitCount := appStats.txns.getStats()
			err := addRow(
				tree.NewDInt(tree.DInt(nodeID)),
				tree.NewDString(appName),
				tree.NewDInt(tree.DInt(txnCount)),
				tree.NewDFloat(tree.DFloat(txnTimeAvg)),
				tree.NewDFloat(tree.DFloat(txnTimeVar)),
				tree.NewDInt(tree.DInt(committedCount)),
				tree.NewDInt(tree.DInt(implicitCount)),
			)
			if err != nil {
				return err
			}
		}
		return nil
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.cluster_settings"); err != nil {
			return err
		}
		for _, k := range settings.Keys() {
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
  application_name STRING  -- the name of the application as per SET application_name
)`

var crdbInternalLocalTxnsTable = virtualSchemaTable{
	comment: "running user transactions visible by the current user (RAM; local node only)",
	schema:  fmt.Sprintf(txnsSchemaPattern, "node_transactions"),
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.node_transactions"); err != nil {
			return err
		}
		req := p.makeSessionsRequest(ctx)
		ss, err := p.extendedEvalCtx.StatusServer.OptionalErr()
		if err != nil {
			return err
		}
		response, err := ss.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateTransactionsTable(ctx, addRow, response)
	},
}

var crdbInternalClusterTxnsTable = virtualSchemaTable{
	comment: "running user transactions visible by the current user (cluster RPC; expensive!)",
	schema:  fmt.Sprintf(txnsSchemaPattern, "cluster_transactions"),
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.cluster_transactions"); err != nil {
			return err
		}
		req := p.makeSessionsRequest(ctx)
		ss, err := p.extendedEvalCtx.StatusServer.OptionalErr()
		if err != nil {
			return err
		}
		response, err := ss.ListSessions(ctx, &req)
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

func (p *planner) makeSessionsRequest(ctx context.Context) serverpb.ListSessionsRequest {
	req := serverpb.ListSessionsRequest{Username: p.SessionData().User}
	if err := p.RequireAdminRole(ctx, "list sessions"); err == nil {
		// The root user can see all sessions.
		req.Username = ""
	}
	return req
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req := p.makeSessionsRequest(ctx)
		ss, err := p.extendedEvalCtx.StatusServer.OptionalErr()
		if err != nil {
			return err
		}
		response, err := ss.ListLocalSessions(ctx, &req)
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req := p.makeSessionsRequest(ctx)
		ss, err := p.extendedEvalCtx.StatusServer.OptionalErr()
		if err != nil {
			return err
		}
		response, err := ss.ListSessions(ctx, &req)
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req := p.makeSessionsRequest(ctx)
		ss, err := p.extendedEvalCtx.StatusServer.OptionalErr()
		if err != nil {
			return err
		}
		response, err := ss.ListLocalSessions(ctx, &req)
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		req := p.makeSessionsRequest(ctx)
		ss, err := p.extendedEvalCtx.StatusServer.OptionalErr()
		if err != nil {
			return err
		}
		response, err := ss.ListSessions(ctx, &req)
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
		if session.KvTxnID != nil {
			kvTxnIDDatum = tree.NewDString(session.KvTxnID.String())
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

// crdbInternalLocalMetricsTable exposes a snapshot of the metrics on the
// current node.
var crdbInternalLocalMetricsTable = virtualSchemaTable{
	comment: "current values for metrics (RAM; local node only)",
	schema: `CREATE TABLE crdb_internal.node_metrics (
  store_id 	         INT NULL,         -- the store, if any, for this metric
  name               STRING NOT NULL,  -- name of the metric
  value							 FLOAT NOT NULL    -- value of the metric
)`,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	populate: func(ctx context.Context, _ *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	INDEX (descriptor_id)
)
`,
	populate: func(ctx context.Context, p *planner, db *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		return forEachTypeDesc(ctx, p, db, func(db *DatabaseDescriptor, sc string, typeDesc *TypeDescriptor) error {
			switch typeDesc.Kind {
			case sqlbase.TypeDescriptor_ENUM:
				var enumLabels []string
				for i := range typeDesc.EnumMembers {
					enumLabels = append(enumLabels, typeDesc.EnumMembers[i].LogicalRepresentation)
				}
				name, err := tree.NewUnresolvedObjectName(3, [3]string{typeDesc.Name, sc, db.Name}, 0)
				if err != nil {
					return err
				}
				node := &tree.CreateType{
					Variety:    tree.Enum,
					TypeName:   name,
					EnumLabels: enumLabels,
				}
				if err := addRow(
					tree.NewDInt(tree.DInt(db.ID)),       // database_id
					tree.NewDString(db.Name),             // database_name
					tree.NewDString(sc),                  // schema_name
					tree.NewDInt(tree.DInt(typeDesc.ID)), // descriptor_id
					tree.NewDString(typeDesc.Name),       // descriptor_name
					tree.NewDString(tree.AsString(node)), // create_statement
				); err != nil {
					return err
				}
			case sqlbase.TypeDescriptor_ALIAS:
			// Alias types are created implicitly, so we don't have create
			// statements for them.
			default:
				return errors.AssertionFailedf("unknown type descriptor kind %s", typeDesc.Kind.String())
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
  INDEX(descriptor_id)
)
`, virtualOnce, false, /* includesIndexEntries */
	func(ctx context.Context, p *planner, h oidHasher, db *sqlbase.DatabaseDescriptor, scName string,
		table *sqlbase.TableDescriptor, lookup simpleSchemaResolver, addRow func(...tree.Datum) error) error {
		contextName := ""
		parentNameStr := tree.DNull
		if db != nil {
			contextName = db.Name
			parentNameStr = tree.NewDString(db.Name)
		}
		scNameStr := tree.NewDString(scName)

		var descType tree.Datum
		var stmt, createNofk string
		alterStmts := tree.NewDArray(types.String)
		validateStmts := tree.NewDArray(types.String)
		var err error
		if table.IsView() {
			descType = typeView
			stmt, err = ShowCreateView(ctx, (*tree.Name)(&table.Name), table)
		} else if table.IsSequence() {
			descType = typeSequence
			stmt, err = ShowCreateSequence(ctx, (*tree.Name)(&table.Name), table)
		} else {
			descType = typeTable
			tn := (*tree.Name)(&table.Name)
			displayOptions := ShowCreateDisplayOptions{
				FKDisplayMode: OmitFKClausesFromCreate,
			}
			createNofk, err = ShowCreateTable(ctx, p, tn, contextName, table, lookup, displayOptions)
			if err != nil {
				return err
			}
			if err := showAlterStatementWithInterleave(ctx, tn, contextName, lookup, table.Indexes, table, alterStmts,
				validateStmts); err != nil {
				return err
			}
			displayOptions.FKDisplayMode = IncludeFkClausesInCreate
			stmt, err = ShowCreateTable(ctx, p, tn, contextName, table, lookup, displayOptions)
		}
		if err != nil {
			return err
		}

		descID := tree.NewDInt(tree.DInt(table.ID))
		dbDescID := tree.NewDInt(tree.DInt(table.GetParentID()))
		if createNofk == "" {
			createNofk = stmt
		}
		hasPartitions := false
		for i := range table.Indexes {
			if table.Indexes[i].Partitioning.NumColumns != 0 {
				hasPartitions = true
				break
			}
		}
		hasPartitions = hasPartitions || table.PrimaryIndex.Partitioning.NumColumns != 0
		return addRow(
			dbDescID,
			parentNameStr,
			scNameStr,
			descID,
			descType,
			tree.NewDString(table.Name),
			tree.NewDString(stmt),
			tree.NewDString(table.State.String()),
			tree.NewDString(createNofk),
			alterStmts,
			validateStmts,
			tree.MakeDBool(tree.DBool(hasPartitions)),
		)
	})

func showAlterStatementWithInterleave(
	ctx context.Context,
	tn *tree.Name,
	contextName string,
	lCtx simpleSchemaResolver,
	allIdx []sqlbase.IndexDescriptor,
	table *sqlbase.TableDescriptor,
	alterStmts *tree.DArray,
	validateStmts *tree.DArray,
) error {
	for i := range table.OutboundFKs {
		fk := &table.OutboundFKs[i]
		f := tree.NewFmtCtx(tree.FmtSimple)
		f.WriteString("ALTER TABLE ")
		f.FormatNode(tn)
		f.WriteString(" ADD CONSTRAINT ")
		f.FormatNameP(&fk.Name)
		f.WriteByte(' ')
		if err := showForeignKeyConstraint(&f.Buffer, contextName, table, fk, lCtx); err != nil {
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

		if err := validateStmts.Append(tree.NewDString(f.CloseAndGetString())); err != nil {
			return err
		}
	}

	for i := range allIdx {
		idx := &allIdx[i]
		// Create CREATE INDEX commands for INTERLEAVE tables. These commands
		// are included in the ALTER TABLE statements.
		if len(idx.Interleave.Ancestors) > 0 {
			f := tree.NewFmtCtx(tree.FmtSimple)
			intl := idx.Interleave
			parentTableID := intl.Ancestors[len(intl.Ancestors)-1].TableID
			var err error
			var parentName tree.TableName
			if lCtx != nil {
				parentName, err = getParentAsTableName(lCtx, parentTableID, contextName)
				if err != nil {
					return err
				}
			} else {
				parentName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as parent]", parentTableID)))
				parentName.ExplicitCatalog = false
				parentName.ExplicitSchema = false
			}

			var tableName tree.TableName
			if lCtx != nil {
				tableName, err = getTableAsTableName(lCtx, table, contextName)
				if err != nil {
					return err
				}
			} else {
				tableName = tree.MakeTableName(tree.Name(""), tree.Name(fmt.Sprintf("[%d as parent]", table.ID)))
				tableName.ExplicitCatalog = false
				tableName.ExplicitSchema = false
			}
			var sharedPrefixLen int
			for _, ancestor := range intl.Ancestors {
				sharedPrefixLen += int(ancestor.SharedPrefixLen)
			}
			// Write the CREATE INDEX statements.
			showCreateIndexWithInterleave(f, idx, tableName, parentName, sharedPrefixLen)
			if err := alterStmts.Append(tree.NewDString(f.CloseAndGetString())); err != nil {
				return err
			}
		}
	}
	return nil
}

func showCreateIndexWithInterleave(
	f *tree.FmtCtx,
	idx *sqlbase.IndexDescriptor,
	tableName tree.TableName,
	parentName tree.TableName,
	sharedPrefixLen int,
) {
	f.WriteString("CREATE ")
	f.WriteString(idx.SQLString(&tableName))
	f.WriteString(" INTERLEAVE IN PARENT ")
	parentName.Format(f)
	f.WriteString(" (")
	// Get all of the columns and write them.
	comma := ""
	for _, name := range idx.ColumnNames[:sharedPrefixLen] {
		f.WriteString(comma)
		f.FormatNameP(&name)
		comma = ", "
	}
	f.WriteString(")")
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
	generator: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		row := make(tree.Datums, 8)
		worker := func(pusher rowPusher) error {
			return forEachTableDescAll(ctx, p, dbContext, hideVirtual,
				func(db *DatabaseDescriptor, _ string, table *TableDescriptor) error {
					tableID := tree.NewDInt(tree.DInt(table.ID))
					tableName := tree.NewDString(table.Name)
					for i := range table.Columns {
						col := &table.Columns[i]
						defStr := tree.DNull
						if col.DefaultExpr != nil {
							def, err := schemaexpr.DeserializeTableDescExpr(ctx, &p.semaCtx, table, *col.DefaultExpr)
							if err != nil {
								return err
							}
							defStr = tree.NewDString(tree.SerializeForDisplay(def))
						}
						row = row[:0]
						row = append(row,
							tableID,
							tableName,
							tree.NewDInt(tree.DInt(col.ID)),
							tree.NewDString(col.Name),
							tree.NewDString(col.Type.DebugString()),
							tree.MakeDBool(tree.DBool(col.Nullable)),
							defStr,
							tree.MakeDBool(tree.DBool(col.Hidden)),
						)
						if err := pusher.pushRow(row...); err != nil {
							return err
						}
					}
					return nil
				},
			)
		}
		next, cleanup := setupGenerator(ctx, worker)
		return next, cleanup, nil
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
	generator: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		primary := tree.NewDString("primary")
		secondary := tree.NewDString("secondary")
		row := make(tree.Datums, 7)
		worker := func(pusher rowPusher) error {
			return forEachTableDescAll(ctx, p, dbContext, hideVirtual,
				func(db *DatabaseDescriptor, _ string, table *TableDescriptor) error {
					tableID := tree.NewDInt(tree.DInt(table.ID))
					tableName := tree.NewDString(table.Name)
					row = row[:0]
					row = append(row,
						tableID,
						tableName,
						tree.NewDInt(tree.DInt(table.PrimaryIndex.ID)),
						tree.NewDString(table.PrimaryIndex.Name),
						primary,
						tree.MakeDBool(tree.DBool(table.PrimaryIndex.Unique)),
						tree.MakeDBool(table.PrimaryIndex.Type == sqlbase.IndexDescriptor_INVERTED),
					)
					if err := pusher.pushRow(row...); err != nil {
						return err
					}
					for _, idx := range table.Indexes {
						row = row[:0]
						row = append(row,
							tableID,
							tableName,
							tree.NewDInt(tree.DInt(idx.ID)),
							tree.NewDString(idx.Name),
							secondary,
							tree.MakeDBool(tree.DBool(idx.Unique)),
							tree.MakeDBool(idx.Type == sqlbase.IndexDescriptor_INVERTED),
						)
						if err := pusher.pushRow(row...); err != nil {
							return err
						}
					}
					return nil
				},
			)
		}
		next, cleanup := setupGenerator(ctx, worker)
		return next, cleanup, nil
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
  column_direction STRING
)
`,
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		key := tree.NewDString("key")
		storing := tree.NewDString("storing")
		extra := tree.NewDString("extra")
		composite := tree.NewDString("composite")
		idxDirMap := map[sqlbase.IndexDescriptor_Direction]tree.Datum{
			sqlbase.IndexDescriptor_ASC:  tree.NewDString(sqlbase.IndexDescriptor_ASC.String()),
			sqlbase.IndexDescriptor_DESC: tree.NewDString(sqlbase.IndexDescriptor_DESC.String()),
		}

		return forEachTableDescAll(ctx, p, dbContext, hideVirtual,
			func(parent *DatabaseDescriptor, _ string, table *TableDescriptor) error {
				tableID := tree.NewDInt(tree.DInt(table.ID))
				parentName := parent.Name
				tableName := tree.NewDString(table.Name)

				reportIndex := func(idx *sqlbase.IndexDescriptor) error {
					idxID := tree.NewDInt(tree.DInt(idx.ID))
					idxName := tree.NewDString(idx.Name)

					// Report the main (key) columns.
					for i, c := range idx.ColumnIDs {
						colName := tree.DNull
						colDir := tree.DNull
						if i >= len(idx.ColumnNames) {
							// We log an error here, instead of reporting an error
							// to the user, because we really want to see the
							// erroneous data in the virtual table.
							log.Errorf(ctx, "index descriptor for [%d@%d] (%s.%s@%s) has more key column IDs (%d) than names (%d) (corrupted schema?)",
								table.ID, idx.ID, parentName, table.Name, idx.Name,
								len(idx.ColumnIDs), len(idx.ColumnNames))
						} else {
							colName = tree.NewDString(idx.ColumnNames[i])
						}
						if i >= len(idx.ColumnDirections) {
							// See comment above.
							log.Errorf(ctx, "index descriptor for [%d@%d] (%s.%s@%s) has more key column IDs (%d) than directions (%d) (corrupted schema?)",
								table.ID, idx.ID, parentName, table.Name, idx.Name,
								len(idx.ColumnIDs), len(idx.ColumnDirections))
						} else {
							colDir = idxDirMap[idx.ColumnDirections[i]]
						}

						if err := addRow(
							tableID, tableName, idxID, idxName,
							key, tree.NewDInt(tree.DInt(c)), colName, colDir,
						); err != nil {
							return err
						}
					}

					// Report the stored columns.
					for _, c := range idx.StoreColumnIDs {
						if err := addRow(
							tableID, tableName, idxID, idxName,
							storing, tree.NewDInt(tree.DInt(c)), tree.DNull, tree.DNull,
						); err != nil {
							return err
						}
					}

					// Report the extra columns.
					for _, c := range idx.ExtraColumnIDs {
						if err := addRow(
							tableID, tableName, idxID, idxName,
							extra, tree.NewDInt(tree.DInt(c)), tree.DNull, tree.DNull,
						); err != nil {
							return err
						}
					}

					// Report the composite columns
					for _, c := range idx.CompositeColumnIDs {
						if err := addRow(
							tableID, tableName, idxID, idxName,
							composite, tree.NewDInt(tree.DInt(c)), tree.DNull, tree.DNull,
						); err != nil {
							return err
						}
					}

					return nil
				}

				if err := reportIndex(&table.PrimaryIndex); err != nil {
					return err
				}
				for i := range table.Indexes {
					if err := reportIndex(&table.Indexes[i]); err != nil {
						return err
					}
				}
				return nil
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
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		fkDep := tree.NewDString("fk")
		viewDep := tree.NewDString("view")
		sequenceDep := tree.NewDString("sequence")
		interleaveDep := tree.NewDString("interleave")
		return forEachTableDescAllWithTableLookup(ctx, p, dbContext, hideVirtual,
			/* virtual tables have no backward/forward dependencies*/
			func(db *DatabaseDescriptor, _ string, table *TableDescriptor, tableLookup tableLookupFn) error {
				tableID := tree.NewDInt(tree.DInt(table.ID))
				tableName := tree.NewDString(table.Name)

				reportIdxDeps := func(idx *sqlbase.IndexDescriptor) error {
					for _, interleaveParent := range idx.Interleave.Ancestors {
						if err := addRow(
							tableID, tableName,
							tree.NewDInt(tree.DInt(idx.ID)),
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

				for i := range table.OutboundFKs {
					fk := &table.OutboundFKs[i]
					refTbl, err := tableLookup.getTableByID(fk.ReferencedTableID)
					if err != nil {
						return err
					}
					refIdx, err := sqlbase.FindFKReferencedIndex(refTbl, fk.ReferencedColumnIDs)
					if err != nil {
						return err
					}
					if err := addRow(
						tableID, tableName,
						tree.DNull,
						tree.DNull,
						tree.NewDInt(tree.DInt(fk.ReferencedTableID)),
						fkDep,
						tree.NewDInt(tree.DInt(refIdx.ID)),
						tree.NewDString(fk.Name),
						tree.DNull,
					); err != nil {
						return err
					}
				}

				// Record the backward references of the primary index.
				if err := reportIdxDeps(&table.PrimaryIndex); err != nil {
					return err
				}

				// Record the backward references of secondary indexes.
				for i := range table.Indexes {
					if err := reportIdxDeps(&table.Indexes[i]); err != nil {
						return err
					}
				}

				// Record the view dependencies.
				for _, tIdx := range table.DependsOn {
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
				for i := range table.Columns {
					col := &table.Columns[i]
					for _, sequenceID := range col.UsesSequenceIds {
						if err := addRow(
							tableID, tableName,
							tree.DNull,
							tree.NewDInt(tree.DInt(col.ID)),
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
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	populate: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		fkDep := tree.NewDString("fk")
		viewDep := tree.NewDString("view")
		interleaveDep := tree.NewDString("interleave")
		sequenceDep := tree.NewDString("sequence")
		return forEachTableDescAll(ctx, p, dbContext, hideVirtual, /* virtual tables have no backward/forward dependencies*/
			func(db *DatabaseDescriptor, _ string, table *TableDescriptor) error {
				tableID := tree.NewDInt(tree.DInt(table.ID))
				tableName := tree.NewDString(table.Name)

				reportIdxDeps := func(idx *sqlbase.IndexDescriptor) error {
					for _, interleaveRef := range idx.InterleavedBy {
						if err := addRow(
							tableID, tableName,
							tree.NewDInt(tree.DInt(idx.ID)),
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

				for i := range table.InboundFKs {
					fk := &table.InboundFKs[i]
					if err := addRow(
						tableID, tableName,
						tree.DNull,
						tree.NewDInt(tree.DInt(fk.OriginTableID)),
						fkDep,
						tree.DNull,
						tree.DNull,
						tree.DNull,
					); err != nil {
						return err
					}
				}

				// Record the backward references of the primary index.
				if err := reportIdxDeps(&table.PrimaryIndex); err != nil {
					return err
				}

				// Record the backward references of secondary indexes.
				for i := range table.Indexes {
					if err := reportIdxDeps(&table.Indexes[i]); err != nil {
						return err
					}
				}

				if table.IsTable() || table.IsView() {
					// Record the view dependencies.
					for _, dep := range table.DependedOnBy {
						if err := addRow(
							tableID, tableName,
							tree.DNull,
							tree.NewDInt(tree.DInt(dep.ID)),
							viewDep,
							tree.NewDInt(tree.DInt(dep.IndexID)),
							tree.DNull,
							tree.NewDString(fmt.Sprintf("Columns: %v", dep.ColumnIDs)),
						); err != nil {
							return err
						}
					}
				} else if table.IsSequence() {
					// Record the sequence dependencies.
					for _, dep := range table.DependedOnBy {
						if err := addRow(
							tableID, tableName,
							tree.DNull,
							tree.NewDInt(tree.DInt(dep.ID)),
							sequenceDep,
							tree.NewDInt(tree.DInt(dep.IndexID)),
							tree.DNull,
							tree.NewDString(fmt.Sprintf("Columns: %v", dep.ColumnIDs)),
						); err != nil {
							return err
						}
					}
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
	database_name,
	table_name,
	index_name,
	replicas,
	replica_localities,
	learner_replicas,
	split_enforced_until,
	crdb_internal.lease_holder(start_key) AS lease_holder,
	(crdb_internal.range_stats(start_key)->>'key_bytes')::INT +
	(crdb_internal.range_stats(start_key)->>'val_bytes')::INT AS range_size
FROM crdb_internal.ranges_no_leases
`,
	resultColumns: sqlbase.ResultColumns{
		{Name: "range_id", Typ: types.Int},
		{Name: "start_key", Typ: types.Bytes},
		{Name: "start_pretty", Typ: types.String},
		{Name: "end_key", Typ: types.Bytes},
		{Name: "end_pretty", Typ: types.String},
		{Name: "database_name", Typ: types.String},
		{Name: "table_name", Typ: types.String},
		{Name: "index_name", Typ: types.String},
		{Name: "replicas", Typ: types.Int2Vector},
		{Name: "replica_localities", Typ: types.StringArray},
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
	schema: `
CREATE TABLE crdb_internal.ranges_no_leases (
  range_id             INT NOT NULL,
  start_key            BYTES NOT NULL,
  start_pretty         STRING NOT NULL,
  end_key              BYTES NOT NULL,
  end_pretty           STRING NOT NULL,
  database_name        STRING NOT NULL,
  table_name           STRING NOT NULL,
  index_name           STRING NOT NULL,
  replicas             INT[] NOT NULL,
  replica_localities   STRING[] NOT NULL,
	learner_replicas     INT[] NOT NULL,
	split_enforced_until TIMESTAMP
)
`,
	generator: func(ctx context.Context, p *planner, _ *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.ranges_no_leases"); err != nil {
			return nil, nil, err
		}
		descs, err := p.Tables().GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return nil, nil, err
		}
		// TODO(knz): maybe this could use internalLookupCtx.
		dbNames := make(map[uint32]string)
		tableNames := make(map[uint32]string)
		indexNames := make(map[uint32]map[uint32]string)
		parents := make(map[uint32]uint32)
		for _, desc := range descs {
			id := uint32(desc.GetID())
			switch desc := desc.(type) {
			case *sqlbase.TableDescriptor:
				parents[id] = uint32(desc.ParentID)
				tableNames[id] = desc.GetName()
				indexNames[id] = make(map[uint32]string)
				for _, idx := range desc.Indexes {
					indexNames[id][uint32(idx.ID)] = idx.Name
				}
			case *sqlbase.DatabaseDescriptor:
				dbNames[id] = desc.GetName()
			}
		}
		ranges, err := ScanMetaKVs(ctx, p.txn, roachpb.Span{
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

			voterReplicas := append([]roachpb.ReplicaDescriptor(nil), desc.Replicas().Voters()...)
			var learnerReplicaStoreIDs []int
			for _, rd := range desc.Replicas().Learners() {
				learnerReplicaStoreIDs = append(learnerReplicaStoreIDs, int(rd.StoreID))
			}
			sort.Slice(voterReplicas, func(i, j int) bool {
				return voterReplicas[i].StoreID < voterReplicas[j].StoreID
			})
			sort.Ints(learnerReplicaStoreIDs)
			votersArr := tree.NewDArray(types.Int)
			for _, replica := range voterReplicas {
				if err := votersArr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
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
			for _, replica := range voterReplicas {
				replicaLocality := nodeIDToLocality[replica.NodeID].String()
				if err := replicaLocalityArr.Append(tree.NewDString(replicaLocality)); err != nil {
					return nil, err
				}
			}

			var dbName, tableName, indexName string
			if _, tableID, err := p.ExecCfg().Codec.DecodeTablePrefix(desc.StartKey.AsRawKey()); err == nil {
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
			if (desc.GetStickyBit() != hlc.Timestamp{}) {
				splitEnforcedUntil = tree.TimestampToInexactDTimestamp(*desc.StickyBit)
			}

			return tree.Datums{
				tree.NewDInt(tree.DInt(desc.RangeID)),
				tree.NewDBytes(tree.DBytes(desc.StartKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, desc.StartKey.AsRawKey())),
				tree.NewDBytes(tree.DBytes(desc.EndKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, desc.EndKey.AsRawKey())),
				tree.NewDString(dbName),
				tree.NewDString(tableName),
				tree.NewDString(indexName),
				votersArr,
				replicaLocalityArr,
				learnersArr,
				splitEnforcedUntil,
			}, nil
		}, nil, nil
	},
}

// NamespaceKey represents a key from the namespace table.
type NamespaceKey struct {
	ParentID sqlbase.ID
	// ParentSchemaID is not populated for rows under system.deprecated_namespace.
	// This table will no longer exist on 20.2 or later.
	ParentSchemaID sqlbase.ID
	Name           string
}

// getAllNames returns a map from ID to namespaceKey for every entry in
// system.namespace.
func (p *planner) getAllNames(ctx context.Context) (map[sqlbase.ID]NamespaceKey, error) {
	return getAllNames(ctx, p.txn, p.ExtendedEvalContext().ExecCfg.InternalExecutor)
}

// TestingGetAllNames is a wrapper for getAllNames.
func TestingGetAllNames(
	ctx context.Context, txn *kv.Txn, executor *InternalExecutor,
) (map[sqlbase.ID]NamespaceKey, error) {
	return getAllNames(ctx, txn, executor)
}

// getAllNames is the testable implementation of getAllNames.
// It is public so that it can be tested outside the sql package.
func getAllNames(
	ctx context.Context, txn *kv.Txn, executor *InternalExecutor,
) (map[sqlbase.ID]NamespaceKey, error) {
	namespace := map[sqlbase.ID]NamespaceKey{}
	if executor.s.cfg.Settings.Version.IsActive(ctx, clusterversion.VersionNamespaceTableWithSchemas) {
		rows, err := executor.Query(
			ctx, "get-all-names", txn,
			`SELECT id, "parentID", "parentSchemaID", name FROM system.namespace`,
		)
		if err != nil {
			return nil, err
		}
		for _, r := range rows {
			id, parentID, parentSchemaID, name := tree.MustBeDInt(r[0]), tree.MustBeDInt(r[1]), tree.MustBeDInt(r[2]), tree.MustBeDString(r[3])
			namespace[sqlbase.ID(id)] = NamespaceKey{
				ParentID:       sqlbase.ID(parentID),
				ParentSchemaID: sqlbase.ID(parentSchemaID),
				Name:           string(name),
			}
		}
	}

	// Also get all rows from namespace_deprecated, and add to the namespace map
	// if it is not already there yet.
	// If a row exists in both here and namespace, only use the one from namespace.
	// TODO(sqlexec): In 20.2, this can be removed.
	deprecatedRows, err := executor.Query(
		ctx, "get-all-names-deprecated-namespace", txn,
		fmt.Sprintf(`SELECT id, "parentID", name FROM [%d as namespace]`, keys.DeprecatedNamespaceTableID),
	)
	if err != nil {
		return nil, err
	}
	for _, r := range deprecatedRows {
		id, parentID, name := tree.MustBeDInt(r[0]), tree.MustBeDInt(r[1]), tree.MustBeDString(r[2])
		if _, ok := namespace[sqlbase.ID(id)]; !ok {
			namespace[sqlbase.ID(id)] = NamespaceKey{
				ParentID: sqlbase.ID(parentID),
				Name:     string(name),
			}
		}
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
  zone_id          INT NOT NULL,
  subzone_id       INT NOT NULL,
  target           STRING,
  range_name       STRING,
  database_name    STRING,
  table_name       STRING,
  index_name       STRING,
  partition_name   STRING,
  raw_config_yaml      STRING NOT NULL,
  raw_config_sql       STRING, -- this column can be NULL if there is no specifier syntax
                           -- possible (e.g. the object was deleted).
	raw_config_protobuf  BYTES NOT NULL,
	full_config_yaml STRING NOT NULL,
	full_config_sql STRING
)
`,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if !p.ExecCfg().Codec.ForSystemTenant() {
			return errorutil.UnsupportedWithMultiTenancy()
		}

		namespace, err := p.getAllNames(ctx)
		if err != nil {
			return err
		}
		resolveID := func(id uint32) (parentID uint32, name string, err error) {
			if entry, ok := namespace[sqlbase.ID(id)]; ok {
				return uint32(entry.ParentID), entry.Name, nil
			}
			return 0, "", errors.AssertionFailedf(
				"object with ID %d does not exist", errors.Safe(id))
		}

		getKey := func(key roachpb.Key) (*roachpb.Value, error) {
			kv, err := p.txn.Get(ctx, key)
			if err != nil {
				return nil, err
			}
			return kv.Value, nil
		}

		rows, err := p.ExtendedEvalContext().ExecCfg.InternalExecutor.Query(
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

			var table *TableDescriptor
			if zs.Database != "" {
				database, err := sqlbase.GetDatabaseDescFromID(ctx, p.txn, p.ExecCfg().Codec, sqlbase.ID(id))
				if err != nil {
					return err
				}
				if p.CheckAnyPrivilege(ctx, database) != nil {
					continue
				}
			} else if zoneSpecifier.TableOrIndex.Table.ObjectName != "" {
				table, err = sqlbase.GetTableDescFromID(ctx, p.txn, p.ExecCfg().Codec, sqlbase.ID(id))
				if err != nil {
					return err
				}
				if p.CheckAnyPrivilege(ctx, table) != nil {
					continue
				}
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
					index := table.FindActiveIndexByID(sqlbase.IndexID(s.IndexID))
					if index == nil {
						// If we can't find an active index that corresponds to this index
						// ID then continue, as the index is being dropped, or is already
						// dropped and in the GC queue.
						continue
					}
					if zoneSpecifier != nil {
						zs := zs
						zs.TableOrIndex.Index = tree.UnrestrictedName(index.Name)
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
						if indexSubzone := fullZone.GetSubzone(uint32(index.ID), ""); indexSubzone != nil {
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
			listenAddrSQL := d.SQLAddress
			if listenAddrSQL.IsEmpty() {
				// Pre-19.2 node or same address for both.
				listenAddrSQL = listenAddrRPC
			}

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

// crdbInternalGossipLivenessTable exposes local information about the nodes'
// liveness. The data exposed in this table can be stale/incomplete because
// gossip doesn't provide guarantees around freshness or consistency.
var crdbInternalGossipLivenessTable = virtualSchemaTable{
	comment: "locally known gossiped node liveness (RAM; local node only)",
	schema: `
CREATE TABLE crdb_internal.gossip_liveness (
  node_id         INT NOT NULL,
  epoch           INT NOT NULL,
  expiration      STRING NOT NULL,
  draining        BOOL NOT NULL,
  decommissioning BOOL NOT NULL,
  updated_at      TIMESTAMP
)
	`,
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		// ATTENTION: The contents of this table should only access gossip data
		// which is highly available. DO NOT CALL functions which require the
		// cluster to be healthy, such as StatusServer.Nodes().

		if err := p.RequireAdminRole(ctx, "read crdb_internal.gossip_liveness"); err != nil {
			return err
		}

		g, err := p.ExecCfg().Gossip.OptionalErr(47899)
		if err != nil {
			return err
		}

		type nodeInfo struct {
			liveness  kvserverpb.Liveness
			updatedAt int64
		}

		var nodes []nodeInfo
		if err := g.IterateInfos(gossip.KeyNodeLivenessPrefix, func(key string, i gossip.Info) error {
			bytes, err := i.Value.GetBytes()
			if err != nil {
				return errors.NewAssertionErrorWithWrappedErrf(err,
					"failed to extract bytes for key %q", key)
			}

			var l kvserverpb.Liveness
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
				tree.MakeDBool(tree.DBool(l.Decommissioning)),
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
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
	table *sqlbase.TableDescriptor,
	index *sqlbase.IndexDescriptor,
	partitioning *sqlbase.PartitioningDescriptor,
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

	tableID := tree.NewDInt(tree.DInt(table.ID))
	indexID := tree.NewDInt(tree.DInt(index.ID))
	numColumns := tree.NewDInt(tree.DInt(partitioning.NumColumns))

	var buf bytes.Buffer
	for i := uint32(colOffset); i < uint32(colOffset)+partitioning.NumColumns; i++ {
		if i != uint32(colOffset) {
			buf.WriteString(`, `)
		}
		buf.WriteString(index.ColumnNames[i])
	}
	colNames := tree.NewDString(buf.String())

	var datumAlloc sqlbase.DatumAlloc

	// We don't need real prefixes in the DecodePartitionTuple calls because we
	// only use the tree.Datums part of the output.
	fakePrefixDatums := make([]tree.Datum, colOffset)
	for i := range fakePrefixDatums {
		fakePrefixDatums[i] = tree.DNull
	}

	// This produces the list_value column.
	for _, l := range partitioning.List {
		var buf bytes.Buffer
		for j, values := range l.Values {
			if j != 0 {
				buf.WriteString(`, `)
			}
			tuple, _, err := sqlbase.DecodePartitionTuple(
				&datumAlloc, p.ExecCfg().Codec, table, index, partitioning, values, fakePrefixDatums,
			)
			if err != nil {
				return err
			}
			buf.WriteString(tuple.String())
		}

		partitionValue := tree.NewDString(buf.String())
		name := tree.NewDString(l.Name)

		// Figure out which zone and subzone this partition should correspond to.
		zoneID, zone, subzone, err := GetZoneConfigInTxn(
			ctx, p.txn, config.SystemTenantObjectID(table.ID), index, l.Name, false /* getInheritedDefault */)
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
			name,
			numColumns,
			colNames,
			partitionValue,
			tree.DNull, /* null value for partition range */
			tree.NewDInt(tree.DInt(zoneID)),
			tree.NewDInt(tree.DInt(subzoneID)),
		); err != nil {
			return err
		}
		err = addPartitioningRows(ctx, p, database, table, index, &l.Subpartitioning, name,
			colOffset+int(partitioning.NumColumns), addRow)
		if err != nil {
			return err
		}
	}

	// This produces the range_value column.
	for _, r := range partitioning.Range {
		var buf bytes.Buffer
		fromTuple, _, err := sqlbase.DecodePartitionTuple(
			&datumAlloc, p.ExecCfg().Codec, table, index, partitioning, r.FromInclusive, fakePrefixDatums,
		)
		if err != nil {
			return err
		}
		buf.WriteString(fromTuple.String())
		buf.WriteString(" TO ")
		toTuple, _, err := sqlbase.DecodePartitionTuple(
			&datumAlloc, p.ExecCfg().Codec, table, index, partitioning, r.ToExclusive, fakePrefixDatums,
		)
		if err != nil {
			return err
		}
		buf.WriteString(toTuple.String())
		partitionRange := tree.NewDString(buf.String())

		// Figure out which zone and subzone this partition should correspond to.
		zoneID, zone, subzone, err := GetZoneConfigInTxn(
			ctx, p.txn, config.SystemTenantObjectID(table.ID), index, r.Name, false /* getInheritedDefault */)
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
			tree.NewDString(r.Name),
			numColumns,
			colNames,
			tree.DNull, /* null value for partition list */
			partitionRange,
			tree.NewDInt(tree.DInt(zoneID)),
			tree.NewDInt(tree.DInt(subzoneID)),
		); err != nil {
			return err
		}
	}

	return nil
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
	generator: func(ctx context.Context, p *planner, dbContext *DatabaseDescriptor) (virtualTableGenerator, cleanupFunc, error) {
		dbName := ""
		if dbContext != nil {
			dbName = dbContext.Name
		}
		worker := func(pusher rowPusher) error {
			return forEachTableDescAll(ctx, p, dbContext, hideVirtual, /* virtual tables have no partitions*/
				func(db *DatabaseDescriptor, _ string, table *TableDescriptor) error {
					return table.ForeachNonDropIndex(func(index *sqlbase.IndexDescriptor) error {
						return addPartitioningRows(ctx, p, dbName, table, index, &index.Partitioning,
							tree.DNull /* parentName */, 0 /* colOffset */, pusher.pushRow)
					})
				})
		}
		next, cleanup := setupGenerator(ctx, worker)
		return next, cleanup, nil
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.kv_node_status"); err != nil {
			return err
		}
		ss, err := p.extendedEvalCtx.StatusServer.OptionalErr()
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
	populate: func(ctx context.Context, p *planner, _ *DatabaseDescriptor, addRow func(...tree.Datum) error) error {
		if err := p.RequireAdminRole(ctx, "read crdb_internal.kv_store_status"); err != nil {
			return err
		}
		ss, err := p.ExecCfg().StatusServer.OptionalErr()
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
		ctx context.Context, p *planner, dbContext *DatabaseDescriptor, addRow func(...tree.Datum) error,
	) error {
		tableCommentKey := tree.NewDInt(keys.TableCommentType)
		vt := p.getVirtualTabler()
		vEntries := vt.getEntries()
		vSchemaNames := vt.getSchemaNames()

		for _, virtSchemaName := range vSchemaNames {
			e := vEntries[virtSchemaName]

			for _, tName := range e.orderedDefNames {
				vTableEntry := e.defs[tName]
				table := vTableEntry.desc

				if vTableEntry.comment != "" {
					if err := addRow(
						tableCommentKey,
						tree.NewDInt(tree.DInt(table.ID)),
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
