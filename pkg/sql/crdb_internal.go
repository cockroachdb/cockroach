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

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

var crdbInternal = virtualSchema{
	name: "crdb_internal",
	tables: []virtualSchemaTable{
		crdbInternalBuildInfoTable,
		crdbInternalTablesTable,
		crdbInternalLeasesTable,
		crdbInternalSchemaChangesTable,
		crdbInternalStmtStatsTable,
	},
}

var crdbInternalBuildInfoTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.build_info (
  FIELD STRING NOT NULL,
  VALUE STRING NOT NULL
);
`,
	populate: func(_ context.Context, _ *planner, addRow func(...parser.Datum) error) error {
		if err := addRow(parser.NewDString("Name"), parser.NewDString("CockroachDB")); err != nil {
			return err
		}

		info := build.GetInfo()
		s := reflect.ValueOf(&info).Elem()
		t := s.Type()
		for i := 0; i < s.NumField(); i++ {
			f := s.Field(i)
			if sv, ok := f.Interface().(string); ok {
				fname := t.Field(i).Name
				if err := addRow(parser.NewDString(fname), parser.NewDString(sv)); err != nil {
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
  TABLE_ID                 INT NOT NULL,
  PARENT_ID                INT NOT NULL,
  NAME                     STRING NOT NULL,
  DATABASE_NAME            STRING NOT NULL,
  VERSION                  INT NOT NULL,
  MOD_TIME                 TIMESTAMP NOT NULL,
  MOD_TIME_LOGICAL         DECIMAL NOT NULL,
  FORMAT_VERSION           STRING NOT NULL,
  STATE                    STRING NOT NULL,
  SC_LEASE_NODE_ID         INT,
  SC_LEASE_EXPIRATION_TIME TIMESTAMP,
  CREATE_TABLE             STRING NOT NULL
);
`,
	populate: func(ctx context.Context, p *planner, addRow func(...parser.Datum) error) error {
		descs, err := p.getAllDescriptors()
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
  TABLE_ID      INT NOT NULL,
  PARENT_ID     INT NOT NULL,
  NAME          STRING NOT NULL,
  TYPE          STRING NOT NULL,
  TARGET_ID     INT,
  TARGET_NAME   STRING,
  STATE         STRING NOT NULL,
  DIRECTION     STRING NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		descs, err := p.getAllDescriptors()
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
  NODE_ID     INT NOT NULL,
  TABLE_ID    INT NOT NULL,
  NAME        STRING NOT NULL,
  PARENT_ID   INT NOT NULL,
  EXPIRATION  TIMESTAMP NOT NULL,
  RELEASED    BOOL NOT NULL,
  DELETED     BOOL NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		leaseMgr := p.leaseMgr
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

var crdbInternalStmtStatsTable = virtualSchemaTable{
	schema: `
CREATE TABLE crdb_internal.stmt_stats (
  APPLICATION_NAME    STRING NOT NULL,
  KEY                 STRING NOT NULL,
  COUNT               INT NOT NULL,
  FIRST_ATTEMPT_COUNT INT NOT NULL,
  MAX_RETRIES         INT NOT NULL,
  LAST_ERROR          STRING,
  ROWS_AVG            FLOAT NOT NULL,
  ROWS_VAR            FLOAT NOT NULL,
  PARSE_LAT_AVG       FLOAT NOT NULL,
  PARSE_LAT_VAR       FLOAT NOT NULL,
  PLAN_LAT_AVG        FLOAT NOT NULL,
  PLAN_LAT_VAR        FLOAT NOT NULL,
  RUN_LAT_AVG         FLOAT NOT NULL,
  RUN_LAT_VAR         FLOAT NOT NULL,
  EXEC_LAT_AVG        FLOAT NOT NULL,
  EXEC_LAT_VAR        FLOAT NOT NULL,
  OVERHEAD_LAT_AVG    FLOAT NOT NULL,
  OVERHEAD_LAT_VAR    FLOAT NOT NULL
);
`,
	populate: func(_ context.Context, p *planner, addRow func(...parser.Datum) error) error {
		if p.session.User != security.RootUser {
			return errors.New("only root can access application statistics")
		}

		sqlStats := p.sqlStats
		if sqlStats == nil {
			return errors.New("cannot access sql statistics from this context")
		}

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
				if s.lastErr != nil {
					errString = parser.NewDString(s.lastErr.Error())
				}
				err := addRow(
					parser.NewDString(appName),
					parser.NewDString(stmtKey),
					parser.NewDInt(parser.DInt(int64(s.count))),
					parser.NewDInt(parser.DInt(int64(s.firstAttemptCount))),
					parser.NewDInt(parser.DInt(int64(s.maxRetries))),
					errString,
					parser.NewDFloat(parser.DFloat(s.numRows.getMean(s.count))),
					parser.NewDFloat(parser.DFloat(s.numRows.getVariance(s.count))),
					parser.NewDFloat(parser.DFloat(s.parseLat.getMean(s.count))),
					parser.NewDFloat(parser.DFloat(s.parseLat.getVariance(s.count))),
					parser.NewDFloat(parser.DFloat(s.planLat.getMean(s.count))),
					parser.NewDFloat(parser.DFloat(s.planLat.getVariance(s.count))),
					parser.NewDFloat(parser.DFloat(s.runLat.getMean(s.count))),
					parser.NewDFloat(parser.DFloat(s.runLat.getVariance(s.count))),
					parser.NewDFloat(parser.DFloat(s.execLat.getMean(s.count))),
					parser.NewDFloat(parser.DFloat(s.execLat.getVariance(s.count))),
					parser.NewDFloat(parser.DFloat(s.ovhLat.getMean(s.count))),
					parser.NewDFloat(parser.DFloat(s.ovhLat.getVariance(s.count))),
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
