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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package sql

import (
	"reflect"
	"sort"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var crdbInternal = virtualSchema{
	name: "crdb_internal",
	tables: []virtualSchemaTable{
		crdbInternalBuildInfoTable,
		crdbInternalTablesTable,
		crdbInternalLeasesTable,
		crdbInternalSchemaChangesTable,
		crdbInternalStmtStatsTable,
		crdbInternalJobsTable,
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		leaseMgr := p.LeaseMgr()
		nodeID := parser.NewDInt(parser.DInt(int64(leaseMgr.nodeID.Get())))

		if err := addRow(
			nodeID,
			parser.NewDString("Name"),
			parser.NewDString("CockroachDB"),
		); err != nil {
			return err
		}

		info := build.GetInfo()
		s := reflect.ValueOf(&info).Elem()
		t := s.Type()
		for i := 0; i < s.NumField(); i++ {
			f := s.Field(i)
			if sv, ok := f.Interface().(string); ok {
				fname := t.Field(i).Name
				if err := addRow(nodeID, parser.NewDString(fname), parser.NewDString(sv)); err != nil {
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
  create_table             STRING NOT NULL
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		descs, err := getAllDescriptors(ctx, p.txn)
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
			if !ok || !userCanSeeDescriptor(table, p.session.User) {
				continue
			}
			dbName := dbNames[table.ParentID]
			if dbName == "" {
				return errors.Errorf("could not find database %d", table.ParentID)
			}
			leaseNodeDatum := parser.DNull
			leaseExpDatum := parser.DNull
			if table.Lease != nil {
				leaseNodeDatum = parser.NewDInt(parser.DInt(int64(table.Lease.NodeID)))
				leaseExpDatum = parser.MakeDTimestamp(
					time.Unix(0, table.Lease.ExpirationTime), time.Nanosecond,
				)
			}
			create, err := p.showCreateTable(ctx, parser.Name(table.Name), table)
			if err != nil {
				return err
			}
			if err := addRow(
				parser.NewDInt(parser.DInt(int64(table.ID))),
				parser.NewDInt(parser.DInt(int64(table.ParentID))),
				parser.NewDString(table.Name),
				parser.NewDString(dbName),
				parser.NewDInt(parser.DInt(int64(table.Version))),
				parser.MakeDTimestamp(time.Unix(0, table.ModificationTime.WallTime), time.Microsecond),
				parser.TimestampToDecimal(table.ModificationTime),
				parser.NewDString(table.FormatVersion.String()),
				parser.NewDString(table.State.String()),
				leaseNodeDatum,
				leaseExpDatum,
				parser.NewDString(create),
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
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		descs, err := getAllDescriptors(ctx, p.txn)
		if err != nil {
			return err
		}
		// Note: we do not use forEachTableDesc() here because we want to
		// include added and dropped descriptors.
		for _, desc := range descs {
			table, ok := desc.(*sqlbase.TableDescriptor)
			if !ok || !userCanSeeDescriptor(table, p.session.User) {
				continue
			}
			tableID := parser.NewDInt(parser.DInt(int64(table.ID)))
			parentID := parser.NewDInt(parser.DInt(int64(table.ParentID)))
			tableName := parser.NewDString(table.Name)
			for _, mut := range table.Mutations {
				mutType := "UNKNOWN"
				targetID := parser.DNull
				targetName := parser.DNull
				switch d := mut.Descriptor_.(type) {
				case *sqlbase.DescriptorMutation_Column:
					mutType = "COLUMN"
					targetID = parser.NewDInt(parser.DInt(int64(d.Column.ID)))
					targetName = parser.NewDString(d.Column.Name)
				case *sqlbase.DescriptorMutation_Index:
					mutType = "INDEX"
					targetID = parser.NewDInt(parser.DInt(int64(d.Index.ID)))
					targetName = parser.NewDString(d.Index.Name)
				}
				if err := addRow(
					tableID,
					parentID,
					tableName,
					parser.NewDString(mutType),
					targetID,
					targetName,
					parser.NewDString(mut.State.String()),
					parser.NewDString(mut.Direction.String()),
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
  released    BOOL NOT NULL,
  deleted     BOOL NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		leaseMgr := p.LeaseMgr()
		nodeID := parser.NewDInt(parser.DInt(int64(leaseMgr.nodeID.Get())))

		leaseMgr.mu.Lock()
		defer leaseMgr.mu.Unlock()

		for tid, ts := range leaseMgr.mu.tables {
			tableID := parser.NewDInt(parser.DInt(int64(tid)))

			adder := func() error {
				ts.mu.Lock()
				defer ts.mu.Unlock()

				deleted := parser.MakeDBool(parser.DBool(ts.deleted))

				for _, state := range ts.active.data {
					if !userCanSeeDescriptor(&state.TableDescriptor, p.session.User) {
						continue
					}
					expCopy := state.expiration
					if err := addRow(
						nodeID,
						tableID,
						parser.NewDString(state.Name),
						parser.NewDInt(parser.DInt(int64(state.ParentID))),
						&expCopy,
						parser.MakeDBool(parser.DBool(state.released)),
						deleted,
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
	error              STRING
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		rows, err := p.queryRows(ctx, `SELECT id, status, created, payload FROM system.jobs`)
		if err != nil {
			return err
		}

		for _, r := range rows {
			id, status, created, bytes := r[0], r[1], r[2], r[3]
			payload, err := unmarshalJobPayload(bytes)
			if err != nil {
				return err
			}
			tsOrNull := func(micros int64) parser.Datum {
				if micros == 0 {
					return parser.DNull
				}
				ts := time.Unix(0, micros*time.Microsecond.Nanoseconds())
				return parser.MakeDTimestamp(ts, time.Microsecond)
			}
			descriptorIDs := parser.NewDArray(parser.TypeInt)
			for _, descID := range payload.DescriptorIDs {
				if err := descriptorIDs.Append(parser.NewDInt(parser.DInt(int(descID)))); err != nil {
					return err
				}
			}
			if err := addRow(
				id,
				parser.NewDString(payload.typ()),
				parser.NewDString(payload.Description),
				parser.NewDString(payload.Username),
				descriptorIDs,
				status,
				created,
				tsOrNull(payload.StartedMicros),
				tsOrNull(payload.FinishedMicros),
				tsOrNull(payload.ModifiedMicros),
				parser.NewDFloat(parser.DFloat(payload.FractionCompleted)),
				parser.NewDString(payload.Error),
			); err != nil {
				return err
			}
		}

		return nil
	},
}

var crdbInternalStmtStatsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.node_statement_statistics (
  node_id             INT NOT NULL,
  application_name    STRING NOT NULL,
  key                 STRING NOT NULL,
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
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		if p.session.User != security.RootUser {
			return errors.New("only root can access application statistics")
		}

		sqlStats := p.session.sqlStats
		if sqlStats == nil {
			return errors.New("cannot access sql statistics from this context")
		}

		leaseMgr := p.LeaseMgr()
		nodeID := parser.NewDInt(parser.DInt(int64(leaseMgr.nodeID.Get())))

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
			var stmtKeys []string
			appStats.Lock()
			for k := range appStats.stmts {
				stmtKeys = append(stmtKeys, k)
			}
			appStats.Unlock()
			sort.Strings(stmtKeys)

			// Now retrieve the per-stmt stats proper.
			for _, stmtKey := range stmtKeys {
				s := appStats.getStatsForStmt(stmtKey)

				s.Lock()
				errString := parser.DNull
				if s.data.LastErr != "" {
					errString = parser.NewDString(s.data.LastErr)
				}
				err := addRow(
					nodeID,
					parser.NewDString(appName),
					parser.NewDString(stmtKey),
					parser.NewDInt(parser.DInt(s.data.Count)),
					parser.NewDInt(parser.DInt(s.data.FirstAttemptCount)),
					parser.NewDInt(parser.DInt(s.data.MaxRetries)),
					errString,
					parser.NewDFloat(parser.DFloat(s.data.NumRows.Mean)),
					parser.NewDFloat(parser.DFloat(s.data.NumRows.getVariance(s.data.Count))),
					parser.NewDFloat(parser.DFloat(s.data.ParseLat.Mean)),
					parser.NewDFloat(parser.DFloat(s.data.ParseLat.getVariance(s.data.Count))),
					parser.NewDFloat(parser.DFloat(s.data.PlanLat.Mean)),
					parser.NewDFloat(parser.DFloat(s.data.PlanLat.getVariance(s.data.Count))),
					parser.NewDFloat(parser.DFloat(s.data.RunLat.Mean)),
					parser.NewDFloat(parser.DFloat(s.data.RunLat.getVariance(s.data.Count))),
					parser.NewDFloat(parser.DFloat(s.data.ServiceLat.Mean)),
					parser.NewDFloat(parser.DFloat(s.data.ServiceLat.getVariance(s.data.Count))),
					parser.NewDFloat(parser.DFloat(s.data.OverheadLat.Mean)),
					parser.NewDFloat(parser.DFloat(s.data.OverheadLat.getVariance(s.data.Count))),
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
