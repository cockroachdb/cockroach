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

package sql

import (
	"bytes"
	"fmt"
	"net"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
	yaml "gopkg.in/yaml.v2"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/config"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/jobs"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const crdbInternalName = "crdb_internal"

var crdbInternal = virtualSchema{
	name: crdbInternalName,
	tables: []virtualSchemaTable{
		crdbInternalBackwardDependenciesTable,
		crdbInternalBuildInfoTable,
		crdbInternalBuiltinFunctionsTable,
		crdbInternalClusterQueriesTable,
		crdbInternalClusterSessionsTable,
		crdbInternalClusterSettingsTable,
		crdbInternalCreateStmtsTable,
		crdbInternalForwardDependenciesTable,
		crdbInternalGossipNodes,
		crdbInternalGossipLiveness,
		crdbInternalIndexColumnsTable,
		crdbInternalJobsTable,
		crdbInternalLeasesTable,
		crdbInternalLocalQueriesTable,
		crdbInternalLocalSessionsTable,
		crdbInternalRangesTable,
		crdbInternalRuntimeInfoTable,
		crdbInternalSchemaChangesTable,
		crdbInternalSessionTraceTable,
		crdbInternalSessionVariablesTable,
		crdbInternalStmtStatsTable,
		crdbInternalTableColumnsTable,
		crdbInternalTableIndexesTable,
		crdbInternalTablesTable,
		crdbInternalZonesTable,
	},
}

var crdbInternalBuildInfoTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.node_build_info (
  node_id INT NOT NULL,
  field   STRING NOT NULL,
  value   STRING NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		execCfg := p.ExecCfg()
		nodeID := tree.NewDInt(tree.DInt(int64(execCfg.NodeID.Get())))

		info := build.GetInfo()
		for k, v := range map[string]string{
			"Name":         "CockroachDB",
			"ClusterID":    execCfg.ClusterID().String(),
			"Organization": execCfg.Organization(),
			"Build":        info.Short(),
			"Version":      info.Tag,
		} {
			if err := addRow(
				nodeID,
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
	schema: `
CREATE TABLE crdb_internal.node_runtime_info (
  node_id   INT NOT NULL,
  component STRING NOT NULL,
  field     STRING NOT NULL,
  value     STRING NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		if err := p.RequireSuperUser("access the node runtime information"); err != nil {
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

var crdbInternalTablesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.tables (
  table_id                 INT NOT NULL,
  parent_id                INT NOT NULL,
  name                     STRING NOT NULL,
  database_name            STRING NOT NULL,
  version                  INT NOT NULL,
  mod_time                 TIMESTAMP NOT NULL,
  mod_time_logical         DECIMAL NOT NULL,
  format_version           STRING NOT NULL,
  state                    STRING NOT NULL,
  sc_lease_node_id         INT,
  sc_lease_expiration_time TIMESTAMP,
  gc_deadline              TIMESTAMP
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		descs, err := GetAllDescriptors(ctx, p.txn)
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
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, ok := desc.(*sqlbase.TableDescriptor)
			if !ok || p.CheckAnyPrivilege(table) != nil {
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
			leaseNodeDatum := tree.DNull
			leaseExpDatum := tree.DNull
			if table.Lease != nil {
				leaseNodeDatum = tree.NewDInt(tree.DInt(int64(table.Lease.NodeID)))
				leaseExpDatum = tree.MakeDTimestamp(
					timeutil.Unix(0, table.Lease.ExpirationTime), time.Nanosecond,
				)
			}
			gcDeadlineDatum := tree.DNull
			if table.GCDeadline != 0 {
				gcDeadlineDatum = tree.MakeDTimestamp(
					timeutil.Unix(0, table.GCDeadline), time.Nanosecond,
				)
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(int64(table.ID))),
				tree.NewDInt(tree.DInt(int64(table.GetParentID()))),
				tree.NewDString(table.Name),
				tree.NewDString(dbName),
				tree.NewDInt(tree.DInt(int64(table.Version))),
				tree.MakeDTimestamp(timeutil.Unix(0, table.ModificationTime.WallTime), time.Microsecond),
				tree.TimestampToDecimal(table.ModificationTime),
				tree.NewDString(table.FormatVersion.String()),
				tree.NewDString(table.State.String()),
				leaseNodeDatum,
				leaseExpDatum,
				gcDeadlineDatum,
			); err != nil {
				return err
			}
		}
		return nil
	},
}

var crdbInternalSchemaChangesTable = virtualSchemaTable{
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
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		descs, err := GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, ok := desc.(*sqlbase.TableDescriptor)
			if !ok || p.CheckAnyPrivilege(table) != nil {
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

var crdbInternalLeasesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.leases (
  node_id     INT NOT NULL,
  table_id    INT NOT NULL,
  name        STRING NOT NULL,
  parent_id   INT NOT NULL,
  expiration  TIMESTAMP NOT NULL,
  deleted     BOOL NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		leaseMgr := p.LeaseMgr()
		nodeID := tree.NewDInt(tree.DInt(int64(leaseMgr.nodeID.Get())))

		leaseMgr.mu.Lock()
		defer leaseMgr.mu.Unlock()

		for tid, ts := range leaseMgr.mu.tables {
			tableID := tree.NewDInt(tree.DInt(int64(tid)))

			adder := func() error {
				ts.mu.Lock()
				defer ts.mu.Unlock()

				dropped := tree.MakeDBool(tree.DBool(ts.mu.dropped))

				for _, state := range ts.mu.active.data {
					if p.CheckAnyPrivilege(&state.TableDescriptor) != nil {
						continue
					}

					if !state.leased {
						continue
					}
					expCopy := state.leaseExpiration()
					if err := addRow(
						nodeID,
						tableID,
						tree.NewDString(state.Name),
						tree.NewDInt(tree.DInt(int64(state.GetParentID()))),
						&expCopy,
						dropped,
					); err != nil {
						return err
					}
				}
				return nil
			}

			if err := adder(); err != nil {
				return err
			}
		}
		return nil
	},
}

var crdbInternalJobsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.jobs (
	id                 INT,
	type               STRING,
	description        STRING,
	username           STRING,
	descriptor_ids     INT[],
	status             STRING,
	created            TIMESTAMP,
	started            TIMESTAMP,
	finished           TIMESTAMP,
	modified           TIMESTAMP,
	fraction_completed FLOAT,
	error              STRING,
	coordinator_id     INT
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		p = makeInternalPlanner("jobs", p.txn, p.evalCtx.User, p.session.memMetrics)
		defer finishInternalPlanner(p)
		rows, err := p.queryRows(ctx, `SELECT id, status, created, payload FROM system.jobs`)
		if err != nil {
			return err
		}

		for _, r := range rows {
			id, status, created, bytes := r[0], r[1], r[2], r[3]
			payload, err := jobs.UnmarshalPayload(bytes)
			if err != nil {
				return err
			}
			tsOrNull := func(micros int64) tree.Datum {
				if micros == 0 {
					return tree.DNull
				}
				ts := timeutil.Unix(0, micros*time.Microsecond.Nanoseconds())
				return tree.MakeDTimestamp(ts, time.Microsecond)
			}
			descriptorIDs := tree.NewDArray(types.Int)
			for _, descID := range payload.DescriptorIDs {
				if err := descriptorIDs.Append(tree.NewDInt(tree.DInt(int(descID)))); err != nil {
					return err
				}
			}
			leaseNode := tree.DNull
			if payload.Lease != nil {
				leaseNode = tree.NewDInt(tree.DInt(payload.Lease.NodeID))
			}
			if err := addRow(
				id,
				tree.NewDString(payload.Type().String()),
				tree.NewDString(payload.Description),
				tree.NewDString(payload.Username),
				descriptorIDs,
				status,
				created,
				tsOrNull(payload.StartedMicros),
				tsOrNull(payload.FinishedMicros),
				tsOrNull(payload.ModifiedMicros),
				tree.NewDFloat(tree.DFloat(payload.FractionCompleted)),
				tree.NewDString(payload.Error),
				leaseNode,
			); err != nil {
				return err
			}
		}

		return nil
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
  overhead_lat_var    FLOAT NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		if err := p.RequireSuperUser("access application statistics"); err != nil {
			return err
		}

		sqlStats := p.session.sqlStats
		if sqlStats == nil {
			return errors.New("cannot access sql statistics from this context")
		}

		leaseMgr := p.LeaseMgr()
		nodeID := tree.NewDInt(tree.DInt(int64(leaseMgr.nodeID.Get())))

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

			// Now retrieve the per-stmt stats proper.
			for _, stmtKey := range stmtKeys {
				anonymized := tree.DNull
				anonStr, ok := scrubStmtStatKey(p.session.virtualSchemas, stmtKey.stmt)
				if ok {
					anonymized = tree.NewDString(anonStr)
				}

				s := appStats.getStatsForStmt(stmtKey)

				s.Lock()
				errString := tree.DNull
				if s.data.LastErr != "" {
					errString = tree.NewDString(s.data.LastErr)
				}
				err := addRow(
					nodeID,
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

// crdbInternalSessionTraceTable exposes the latest trace collected on this
// session (via SET TRACING={ON/OFF})
var crdbInternalSessionTraceTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.session_trace (
  txn_idx     INT NOT NULL,        -- The transaction's 0-based index, among all
                                   -- transactions that have been traced.
                                   -- Only filled for the first log message in a
                                   -- transaction's top-level span.
  span_idx    INT NOT NULL,        -- The span's index.
  message_idx INT NOT NULL,        -- The message's index within its span.
  timestamp   TIMESTAMPTZ NOT NULL,-- The message's timestamp.
  duration    INTERVAL,            -- The span's duration. Set only on the first
                                   -- (dummy) message on a span.
                                   -- NULL if the span was not finished at the time
                                   -- the trace has been collected.
  operation   STRING NULL,         -- The span's operation. Set only on
                                   -- the first (dummy) message in a span.
  loc         STRING NOT NULL,     -- The file name / line number prefix, if any.
  tag         STRING NOT NULL,     -- The logging tag, if any.
  message     STRING NOT NULL      -- The logged message.
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		rows, err := p.session.Tracing.generateSessionTraceVTable()
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
var crdbInternalClusterSettingsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.cluster_settings (
  name          STRING NOT NULL,
  current_value STRING NOT NULL,
  type          STRING NOT NULL,
  description   STRING NOT NULL
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		if err := p.RequireSuperUser("read crdb_internal.cluster_settings"); err != nil {
			return err
		}
		for _, k := range settings.Keys() {
			setting, _ := settings.Lookup(k)
			if err := addRow(
				tree.NewDString(k),
				tree.NewDString(setting.String(&p.session.execCfg.Settings.SV)),
				tree.NewDString(setting.Typ()),
				tree.NewDString(setting.Description()),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalSessionVariablesTable exposes the session variables.
var crdbInternalSessionVariablesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.session_variables (
  variable STRING NOT NULL,
  value    STRING NOT NULL
);
`,
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		for _, vName := range varNames {
			gen := varGen[vName]
			value := gen.Get(p.session)
			if err := addRow(
				tree.NewDString(vName),
				tree.NewDString(value),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

const queriesSchemaPattern = `
CREATE TABLE crdb_internal.%s (
  query_id         STRING,         -- the cluster-unique ID of the query
  node_id          INT NOT NULL,   -- the node on which the query is running
  username         STRING,         -- the user running the query
  start            TIMESTAMP,      -- the start time of the query
  query            STRING,         -- the SQL code of the query
  client_address   STRING,         -- the address of the client that issued the query
  application_name STRING,         -- the name of the application as per SET application_name
  distributed      BOOL,           -- whether the query is running distributed
  phase            STRING          -- the current execution phase
);
`

// crdbInternalLocalQueriesTable exposes the list of running queries
// on the current node. The results are dependent on the current user.
var crdbInternalLocalQueriesTable = virtualSchemaTable{
	schema: fmt.Sprintf(queriesSchemaPattern, "node_queries"),
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		req := serverpb.ListSessionsRequest{Username: p.session.User}
		response, err := p.session.execCfg.StatusServer.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateQueriesTable(ctx, addRow, response)
	},
}

// crdbInternalClusterQueriesTable exposes the list of running queries
// on the entire cluster. The result is dependent on the current user.
var crdbInternalClusterQueriesTable = virtualSchemaTable{
	schema: fmt.Sprintf(queriesSchemaPattern, "cluster_queries"),
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		req := serverpb.ListSessionsRequest{Username: p.session.User}
		response, err := p.session.execCfg.StatusServer.ListSessions(ctx, &req)
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
		for _, query := range session.ActiveQueries {
			isDistributedDatum := tree.DNull
			phase := strings.ToLower(query.Phase.String())
			if phase == "executing" {
				isDistributedDatum = tree.DBoolFalse
				if query.IsDistributed {
					isDistributedDatum = tree.DBoolTrue
				}
			}
			if err := addRow(
				tree.NewDString(query.ID),
				tree.NewDInt(tree.DInt(session.NodeID)),
				tree.NewDString(session.Username),
				tree.MakeDTimestamp(query.Start, time.Microsecond),
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
		log.Warning(ctx, rpcErr.Message)
		if rpcErr.NodeID != 0 {
			// Add a row with this node ID, and nulls for all other columns
			if err := addRow(
				tree.DNull,
				tree.NewDInt(tree.DInt(rpcErr.NodeID)),
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
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
  username           STRING,         -- the user running the query
  client_address     STRING,         -- the address of the client that issued the query
  application_name   STRING,         -- the name of the application as per SET application_name
  active_queries     STRING,         -- the currently running queries as SQL
  last_active_query  STRING,         -- the query that finished last on this session as SQL
  session_start      TIMESTAMP,      -- the time when the session was opened
  oldest_query_start TIMESTAMP,      -- the time when the oldest query in the session was started
  kv_txn             STRING          -- the ID of the current KV transaction
);
`

// crdbInternalLocalSessionsTable exposes the list of running sessions
// on the current node. The results are dependent on the current user.
var crdbInternalLocalSessionsTable = virtualSchemaTable{
	schema: fmt.Sprintf(sessionsSchemaPattern, "node_sessions"),
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		req := serverpb.ListSessionsRequest{Username: p.session.User}
		response, err := p.session.execCfg.StatusServer.ListLocalSessions(ctx, &req)
		if err != nil {
			return err
		}
		return populateSessionsTable(ctx, addRow, response)
	},
}

// crdbInternalClusterSessionsTable exposes the list of running sessions
// on the entire cluster. The result is dependent on the current user.
var crdbInternalClusterSessionsTable = virtualSchemaTable{
	schema: fmt.Sprintf(sessionsSchemaPattern, "cluster_sessions"),
	populate: func(ctx context.Context, p *planner, _ string, addRow func(...tree.Datum) error) error {
		req := serverpb.ListSessionsRequest{Username: p.session.User}
		response, err := p.session.execCfg.StatusServer.ListSessions(ctx, &req)
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

		if oldestStart.IsZero() {
			oldestStartDatum = tree.DNull
		} else {
			oldestStartDatum = tree.MakeDTimestamp(oldestStart, time.Microsecond)
		}

		kvTxnIDDatum := tree.DNull
		if session.KvTxnID != nil {
			kvTxnIDDatum = tree.NewDString(session.KvTxnID.String())
		}

		if err := addRow(
			tree.NewDInt(tree.DInt(session.NodeID)),
			tree.NewDString(session.Username),
			tree.NewDString(session.ClientAddress),
			tree.NewDString(session.ApplicationName),
			tree.NewDString(activeQueries.String()),
			tree.NewDString(session.LastActiveQuery),
			tree.MakeDTimestamp(session.Start, time.Microsecond),
			oldestStartDatum,
			kvTxnIDDatum,
		); err != nil {
			return err
		}
	}

	for _, rpcErr := range response.Errors {
		log.Warning(ctx, rpcErr.Message)
		if rpcErr.NodeID != 0 {
			// Add a row with this node ID, and nulls for all other columns
			if err := addRow(
				tree.NewDInt(tree.DInt(rpcErr.NodeID)),
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
				tree.DNull,
			); err != nil {
				return err
			}
		}
	}

	return nil
}

// crdbInternalBuiltinFunctionsTable exposes the built-in function
// metadata.
var crdbInternalBuiltinFunctionsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.builtin_functions (
  function  STRING NOT NULL,
  signature STRING NOT NULL,
  category  STRING NOT NULL,
  details   STRING NOT NULL
);
`,
	populate: func(ctx context.Context, _ *planner, _ string, addRow func(...tree.Datum) error) error {
		for _, name := range builtins.AllBuiltinNames {
			overloads := builtins.Builtins[name]
			for _, f := range overloads {
				if err := addRow(
					tree.NewDString(name),
					tree.NewDString(f.Signature()),
					tree.NewDString(f.Category),
					tree.NewDString(f.Info),
				); err != nil {
					return err
				}
			}
		}
		return nil
	},
}

// crdbInternalCreateStmtsTable exposes the CREATE TABLE/CREATE VIEW
// statements.
var crdbInternalCreateStmtsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.create_statements (
  database_id      INT,
  database_name    STRING NOT NULL,
  descriptor_id    INT,
  descriptor_type  STRING NOT NULL,
  descriptor_name  STRING NOT NULL,
  create_statement STRING NOT NULL,
  state            STRING NOT NULL
)
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		return forEachTableDescAll(ctx, p, prefix,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				var descType tree.Datum
				var stmt string
				var err error
				var typeView = tree.DString("view")
				var typeTable = tree.DString("table")
				if table.IsView() {
					descType = &typeView
					stmt, err = p.showCreateView(ctx, tree.Name(table.Name), table)
				} else {
					descType = &typeTable
					stmt, err = p.showCreateTable(ctx, tree.Name(table.Name), prefix, table)
				}
				if err != nil {
					return err
				}

				descID := tree.DNull
				if table.ID != keys.VirtualDescriptorID {
					descID = tree.NewDInt(tree.DInt(table.ID))
				}
				dbDescID := tree.DNull
				if db.ID != keys.VirtualDescriptorID {
					dbDescID = tree.NewDInt(tree.DInt(db.ID))
				}
				return addRow(
					dbDescID,
					tree.NewDString(db.Name),
					descID,
					descType,
					tree.NewDString(table.Name),
					tree.NewDString(stmt),
					tree.NewDString(table.State.String()),
				)
			})
	},
}

// crdbInternalTableColumnsTable exposes the column descriptors.
var crdbInternalTableColumnsTable = virtualSchemaTable{
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
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		return forEachTableDescAll(ctx, p, prefix,
			func(_ *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				tableID := tree.DNull
				if table.ID != keys.VirtualDescriptorID {
					tableID = tree.NewDInt(tree.DInt(table.ID))
				}
				tableName := tree.NewDString(table.Name)
				for _, col := range table.Columns {
					defStr := tree.DNull
					if col.DefaultExpr != nil {
						defStr = tree.NewDString(*col.DefaultExpr)
					}
					if err := addRow(
						tableID,
						tableName,
						tree.NewDInt(tree.DInt(col.ID)),
						tree.NewDString(col.Name),
						tree.NewDString(col.Type.String()),
						tree.MakeDBool(tree.DBool(col.Nullable)),
						defStr,
						tree.MakeDBool(tree.DBool(col.Hidden)),
					); err != nil {
						return err
					}
				}
				return nil
			})
	},
}

// crdbInternalTableIndexesTable exposes the index descriptors.
var crdbInternalTableIndexesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.table_indexes (
  descriptor_id    INT,
  descriptor_name  STRING NOT NULL,
  index_id         INT NOT NULL,
  index_name       STRING NOT NULL,
  index_type       STRING NOT NULL,
  is_unique        BOOL NOT NULL
)
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		primary := tree.NewDString("primary")
		secondary := tree.NewDString("secondary")
		return forEachTableDescAll(ctx, p, prefix,
			func(_ *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				tableID := tree.DNull
				if table.ID != keys.VirtualDescriptorID {
					tableID = tree.NewDInt(tree.DInt(table.ID))
				}
				tableName := tree.NewDString(table.Name)
				if err := addRow(
					tableID,
					tableName,
					tree.NewDInt(tree.DInt(table.PrimaryIndex.ID)),
					tree.NewDString(table.PrimaryIndex.Name),
					primary,
					tree.MakeDBool(tree.DBool(table.PrimaryIndex.Unique)),
				); err != nil {
					return err
				}
				for _, idx := range table.Indexes {
					if err := addRow(
						tableID,
						tableName,
						tree.NewDInt(tree.DInt(idx.ID)),
						tree.NewDString(idx.Name),
						secondary,
						tree.MakeDBool(tree.DBool(idx.Unique)),
					); err != nil {
						return err
					}
				}
				return nil
			})
	},
}

// crdbInternalIndexColumnsTable exposes the index columns.
var crdbInternalIndexColumnsTable = virtualSchemaTable{
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
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		key := tree.NewDString("key")
		storing := tree.NewDString("storing")
		extra := tree.NewDString("extra")
		composite := tree.NewDString("composite")
		idxDirMap := map[sqlbase.IndexDescriptor_Direction]tree.Datum{
			sqlbase.IndexDescriptor_ASC:  tree.NewDString(sqlbase.IndexDescriptor_ASC.String()),
			sqlbase.IndexDescriptor_DESC: tree.NewDString(sqlbase.IndexDescriptor_DESC.String()),
		}
		return forEachTableDescAll(ctx, p, prefix,
			func(db *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				tableID := tree.DNull
				if table.ID != keys.VirtualDescriptorID {
					tableID = tree.NewDInt(tree.DInt(table.ID))
				}
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
								table.ID, idx.ID, db.Name, table.Name, idx.Name,
								len(idx.ColumnIDs), len(idx.ColumnNames))
						} else {
							colName = tree.NewDString(idx.ColumnNames[i])
						}
						if i >= len(idx.ColumnDirections) {
							// See comment above.
							log.Errorf(ctx, "index descriptor for [%d@%d] (%s.%s@%s) has more key column IDs (%d) than directions (%d) (corrupted schema?)",
								table.ID, idx.ID, db.Name, table.Name, idx.Name,
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
var crdbInternalBackwardDependenciesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.backward_dependencies (
  descriptor_id      INT,
  descriptor_name    STRING NOT NULL,
  index_id           INT,
  dependson_id       INT NOT NULL,
  dependson_type     STRING NOT NULL,
  dependson_index_id INT,
  dependson_name     STRING,
  dependson_details  STRING
)
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		fkDep := tree.NewDString("fk")
		viewDep := tree.NewDString("view")
		interleaveDep := tree.NewDString("interleave")
		return forEachTableDescAll(ctx, p, prefix,
			func(_ *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				tableID := tree.DNull
				if table.ID != keys.VirtualDescriptorID {
					tableID = tree.NewDInt(tree.DInt(table.ID))
				}
				tableName := tree.NewDString(table.Name)

				reportIdxDeps := func(idx *sqlbase.IndexDescriptor) error {
					idxID := tree.NewDInt(tree.DInt(idx.ID))
					if idx.ForeignKey.Table != 0 {
						fkRef := &idx.ForeignKey
						if err := addRow(
							tableID, tableName,
							idxID,
							tree.NewDInt(tree.DInt(fkRef.Table)),
							fkDep,
							tree.NewDInt(tree.DInt(fkRef.Index)),
							tree.NewDString(fkRef.Name),
							tree.NewDString(fmt.Sprintf("SharedPrefixLen: %d", fkRef.SharedPrefixLen)),
						); err != nil {
							return err
						}
					}

					for _, interleaveParent := range idx.Interleave.Ancestors {
						if err := addRow(
							tableID, tableName,
							idxID,
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
						tree.NewDInt(tree.DInt(tIdx)),
						viewDep,
						tree.DNull,
						tree.DNull,
						tree.DNull,
					); err != nil {
						return err
					}
				}

				return nil
			})
	},
}

// crdbInternalForwardDependenciesTable exposes the forward
// inter-descriptor dependencies.
var crdbInternalForwardDependenciesTable = virtualSchemaTable{
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
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		fkDep := tree.NewDString("fk")
		viewDep := tree.NewDString("view")
		interleaveDep := tree.NewDString("interleave")
		return forEachTableDescAll(ctx, p, prefix,
			func(_ *sqlbase.DatabaseDescriptor, table *sqlbase.TableDescriptor) error {
				tableID := tree.DNull
				if table.ID != keys.VirtualDescriptorID {
					tableID = tree.NewDInt(tree.DInt(table.ID))
				}
				tableName := tree.NewDString(table.Name)

				reportIdxDeps := func(idx *sqlbase.IndexDescriptor) error {
					idxID := tree.NewDInt(tree.DInt(idx.ID))
					for _, fkRef := range idx.ReferencedBy {
						if err := addRow(
							tableID, tableName,
							idxID,
							tree.NewDInt(tree.DInt(fkRef.Table)),
							fkDep,
							tree.NewDInt(tree.DInt(fkRef.Index)),
							tree.NewDString(fkRef.Name),
							tree.NewDString(fmt.Sprintf("SharedPrefixLen: %d", fkRef.SharedPrefixLen)),
						); err != nil {
							return err
						}
					}

					for _, interleaveRef := range idx.InterleavedBy {
						if err := addRow(
							tableID, tableName,
							idxID,
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

				return nil
			})
	},
}

// crdbInternalRangesTable exposes system ranges.
var crdbInternalRangesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.ranges (
  range_id     INT NOT NULL,
  start_key    BYTES NOT NULL,
  start_pretty STRING NOT NULL,
  end_key      BYTES NOT NULL,
  end_pretty   STRING NOT NULL,
  database     STRING NOT NULL,
  "table"      STRING NOT NULL,
  "index"      STRING NOT NULL,
  replicas     INT[] NOT NULL,
  lease_holder INT NOT NULL
)
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		if err := p.RequireSuperUser("read crdb_internal.ranges"); err != nil {
			return err
		}
		descs, err := GetAllDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		dbNames := make(map[uint64]string)
		tableNames := make(map[uint64]string)
		indexNames := make(map[uint64]map[sqlbase.IndexID]string)
		parents := make(map[uint64]uint64)
		for _, desc := range descs {
			id := uint64(desc.GetID())
			switch desc := desc.(type) {
			case *sqlbase.TableDescriptor:
				parents[id] = uint64(desc.ParentID)
				tableNames[id] = desc.GetName()
				indexNames[id] = make(map[sqlbase.IndexID]string)
				for _, idx := range desc.Indexes {
					indexNames[id][idx.ID] = idx.Name
				}
			case *sqlbase.DatabaseDescriptor:
				dbNames[id] = desc.GetName()
			}
		}
		ranges, err := scanMetaKVs(ctx, p.txn, roachpb.Span{
			Key:    keys.MinKey,
			EndKey: keys.MaxKey,
		})
		if err != nil {
			return err
		}
		var desc roachpb.RangeDescriptor
		for _, r := range ranges {
			if err := r.ValueProto(&desc); err != nil {
				return err
			}
			arr := tree.NewDArray(types.Int)
			for _, replica := range desc.Replicas {
				if err := arr.Append(tree.NewDInt(tree.DInt(replica.StoreID))); err != nil {
					return err
				}
			}
			var dbName, tableName, indexName string
			if _, id, err := keys.DecodeTablePrefix(desc.StartKey.AsRawKey()); err == nil {
				parent := parents[id]
				if parent != 0 {
					tableName = tableNames[id]
					dbName = dbNames[parent]
					if _, _, idxID, err := sqlbase.DecodeTableIDIndexID(desc.StartKey.AsRawKey()); err == nil {
						indexName = indexNames[id][idxID]
					}
				} else {
					dbName = dbNames[id]
				}
			}

			// Get the lease holder.
			// TODO(radu): this will be slow if we have a lot of ranges; find a way to
			// make this part optional.
			b := &client.Batch{}
			b.AddRawRequest(&roachpb.LeaseInfoRequest{
				Span: roachpb.Span{
					Key: desc.StartKey.AsRawKey(),
				},
			})
			if err := p.txn.Run(ctx, b); err != nil {
				return errors.Wrap(err, "error getting lease info")
			}
			resp := b.RawResponse().Responses[0].GetInner().(*roachpb.LeaseInfoResponse)

			if err := addRow(
				tree.NewDInt(tree.DInt(desc.RangeID)),
				tree.NewDBytes(tree.DBytes(desc.StartKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, desc.StartKey.AsRawKey())),
				tree.NewDBytes(tree.DBytes(desc.EndKey)),
				tree.NewDString(keys.PrettyPrint(nil /* valDirs */, desc.EndKey.AsRawKey())),
				tree.NewDString(dbName),
				tree.NewDString(tableName),
				tree.NewDString(indexName),
				arr,
				tree.NewDInt(tree.DInt(resp.Lease.Replica.StoreID)),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalZonesTable decodes and exposes the zone configs in the
// system.zones table.
var crdbInternalZonesTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.zones (
  id            INT NOT NULL,
  cli_specifier STRING NOT NULL,
  config_yaml   BYTES NOT NULL,
  config_proto  BYTES NOT NULL
)
`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		namespace, err := p.getAllNames(ctx)
		if err != nil {
			return err
		}
		resolveID := func(id uint32) (parentID uint32, name string, err error) {
			if entry, ok := namespace[sqlbase.ID(id)]; ok {
				return uint32(entry.parentID), entry.name, nil
			}
			return 0, "", fmt.Errorf("object with ID %d does not exist", id)
		}

		p = makeInternalPlanner("zones", p.txn, p.evalCtx.User, p.session.memMetrics)
		defer finishInternalPlanner(p)
		rows, err := p.queryRows(ctx, `SELECT id, config FROM system.zones`)
		if err != nil {
			return err
		}
		for _, r := range rows {
			id := uint32(tree.MustBeDInt(r[0]))
			zs, err := config.ZoneSpecifierFromID(id, resolveID)
			if err != nil {
				return err
			}

			configBytes := []byte(*r[1].(*tree.DBytes))
			var configProto config.ZoneConfig
			if err := protoutil.Unmarshal(configBytes, &configProto); err != nil {
				return err
			}
			subzones := configProto.Subzones

			if !configProto.IsSubzonePlaceholder() {
				// Ensure subzones don't infect the value of the config_proto column.
				configProto.Subzones = nil
				configProto.SubzoneSpans = nil
				configBytes, err = protoutil.Marshal(&configProto)
				if err != nil {
					return err
				}
				configYAML, err := yaml.Marshal(configProto)
				if err != nil {
					return err
				}
				if err := addRow(
					r[0], // id
					tree.NewDString(config.CLIZoneSpecifier(zs)),
					tree.NewDBytes(tree.DBytes(configYAML)),
					tree.NewDBytes(tree.DBytes(configBytes)),
				); err != nil {
					return err
				}
			}

			if len(subzones) > 0 {
				table, err := sqlbase.GetTableDescFromID(ctx, p.txn, sqlbase.ID(id))
				if err != nil {
					return err
				}
				for _, s := range subzones {
					index, err := table.FindIndexByID(sqlbase.IndexID(s.IndexID))
					if err != nil {
						return err
					}
					zs := zs
					zs.TableOrIndex.Index = tree.UnrestrictedName(index.Name)
					zs.Partition = tree.Name(s.PartitionName)
					configYAML, err := yaml.Marshal(s.Config)
					if err != nil {
						return err
					}
					configBytes, err := protoutil.Marshal(&s.Config)
					if err != nil {
						return err
					}
					if err := addRow(
						r[0], // id
						tree.NewDString(config.CLIZoneSpecifier(zs)),
						tree.NewDBytes(tree.DBytes(configYAML)),
						tree.NewDBytes(tree.DBytes(configBytes)),
					); err != nil {
						return err
					}
				}
			}
		}
		return nil
	},
}

// crdbInternalGossipNodes exposes local information about the cluster nodes.
var crdbInternalGossipNodes = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.gossip_nodes (
  node_id  INT NOT NULL,
  network  STRING NOT NULL,
  address  STRING NOT NULL,
  attrs    STRING NOT NULL,
  locality STRING NOT NULL,
  version  STRING NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		if err := p.RequireSuperUser("read crdb_internal.gossip_nodes"); err != nil {
			return err
		}
		g := p.session.execCfg.Gossip
		for key, info := range g.GetInfoStatusByPrefix(gossip.KeyNodeIDPrefix).Infos {
			bytes, err := info.Value.GetBytes()
			if err != nil {
				return errors.Wrapf(err, "failed to extract bytes for key %q", key)
			}
			var d roachpb.NodeDescriptor
			if err := protoutil.Unmarshal(bytes, &d); err != nil {
				return errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(d.NodeID)),
				tree.NewDString(d.Address.NetworkField),
				tree.NewDString(d.Address.AddressField),
				tree.NewDString(d.Attrs.String()),
				tree.NewDString(d.Locality.String()),
				tree.NewDString(d.ServerVersion.String()),
			); err != nil {
				return err
			}
		}
		return nil
	},
}

// crdbInternalGossipLiveness exposes local information about the nodes liveness.
var crdbInternalGossipLiveness = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.gossip_liveness (
  node_id         INT NOT NULL,
  epoch           INT NOT NULL,
  expiration      STRING NOT NULL,
  draining        BOOL NOT NULL,
  decommissioning BOOL NOT NULL
)
	`,
	populate: func(ctx context.Context, p *planner, prefix string, addRow func(...tree.Datum) error) error {
		if err := p.RequireSuperUser("read crdb_internal.gossip_liveness"); err != nil {
			return err
		}
		g := p.session.execCfg.Gossip
		for key, info := range g.GetInfoStatusByPrefix(gossip.KeyNodeLivenessPrefix).Infos {
			bytes, err := info.Value.GetBytes()
			if err != nil {
				return errors.Wrapf(err, "failed to extract bytes for key %q", key)
			}
			var l storage.Liveness
			if err := protoutil.Unmarshal(bytes, &l); err != nil {
				return errors.Wrapf(err, "failed to parse value for key %q", key)
			}
			if err := addRow(
				tree.NewDInt(tree.DInt(l.NodeID)),
				tree.NewDInt(tree.DInt(l.Epoch)),
				tree.NewDString(l.Expiration.String()),
				tree.MakeDBool(tree.DBool(l.Draining)),
				tree.MakeDBool(tree.DBool(l.Decommissioning)),
			); err != nil {
				return err
			}
		}
		return nil
	},
}
