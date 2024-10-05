// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { assert } from "chai";

import { summarize, computeOrUseStmtSummary } from "./summarize";

describe("summarize", () => {
  describe("summarize", () => {
    it("correctly identifies simple statement keywords", () => {
      const simpleStatements = [
        {
          type: "update",
          stmt: "UPDATE system.jobs SET progress = 'value' WHERE id = '12312'",
        },
        {
          type: "select",
          stmt: "SELECT (SELECT job_id FROM system.jobs) FROM system.apps",
        },
        {
          type: "insert",
          stmt: "INSERT INTO system.table_statistics(\"tableID\", name) SELECT 'cockroach', app_names FROM system.apps",
        },
        {
          type: "upsert",
          stmt: 'UPSERT INTO system.reports_meta(id, "generated") VALUES ($1, $2)',
        },
        {
          type: "delete",
          stmt: 'DELETE FROM system.public.lease WHERE ("descID", version, "nodeID", expiration) = ($1, $2, __more1_10__)',
        },
        {
          type: "create",
          stmt: "CREATE TABLE foo (x INT8)",
        },
        {
          type: "set",
          stmt: "SET CLUSTER SETTING sql.defaults.distsql = 1;",
        },
      ];

      simpleStatements.forEach(s =>
        assert.equal(s.type, summarize(s.stmt).statement),
      );
    });

    it("correctly identifies more complicated statement keywords", () => {
      const simpleStatements = [
        {
          type: "update",
          stmt: "UPDATE system.jobs SET claim_session_id = _ WHERE claim_session_id IN (SELECT claim_session_id WHERE ((claim_session_id != $1) AND (status IN ('_', '_', __more1_10__))) AND (NOT crdb_internal.sql_liveness_is_alive(claim_session_id)) LIMIT $2)",
        },
        {
          type: "insert",
          stmt: 'INSERT INTO system.settings(name, value, "lastUpdated", "valueType") SELECT \'unique_pear\', value, "lastUpdated", "valueType" FROM system.settings JOIN system.internal_tables ON system.settings.id = system.internal_tables.id WHERE name = \'_\' ON CONFLICT (name) DO NOTHING',
        },
        {
          type: "select",
          stmt: "SELECT aggregated_ts, fingerprint_id, app_name, metadata, statistics, agg_interval FROM system.transaction_statistics AS OF SYSTEM TIME follower_read_timestamp() ORDER BY aggregated_ts, app_name, fingerprint_id",
        },
      ];

      simpleStatements.forEach(s =>
        assert.equal(s.type, summarize(s.stmt).statement),
      );
    });
  });

  describe("computeOrUseStmtSummary", () => {
    it("returns the given statement summary for supported formats", () => {
      const simpleStatements = [
        {
          stmt: "UPDATE system.jobs SET progress = 'value' WHERE id = '12312'",
          summary: "UPDATE system.jobs SET progress = '_' WHERE id = '_'",
        },
        {
          stmt: "SELECT (SELECT job_id FROM system.jobs) FROM system.apps",
          summary: "SELECT (SELECT FROM sy...) FROM system.apps",
        },
        {
          stmt: "INSERT INTO system.table_statistics(\"tableID\", name) SELECT 'cockroach', app_names FROM system.apps",
          summary:
            "INSERT INTO system.table_statistics SELECT '_', app_names FROM system.apps",
        },
        {
          stmt: 'UPSERT INTO system.reports_meta(id, "generated") VALUES ($1, $2)',
          summary: 'UPSERT INTO system.reports_meta(id, "generated")',
        },
      ];

      simpleStatements.forEach(s =>
        assert.equal(s.summary, computeOrUseStmtSummary(s.stmt, s.summary)),
      );
    });

    it("returns the regex statement summary for unsupported formats", () => {
      const simpleStatements = [
        {
          stmt: 'DELETE FROM system.public.lease WHERE ("descID", version, "nodeID", expiration) = ($1, $2, __more1_10__)\n',
          summary:
            'DELETE FROM system.public.lease WHERE ("descID", version, "nodeID", expiration) = ($1, $2, __more1_10__)\n',
          expected: "DELETE FROM system.public.lease",
        },
        {
          stmt: "CREATE TABLE crdb_internal.index_usage_statistics (table_id INT8 NOT NULL, index_id INT8 NOT NULL, total_reads INT8 NOT NULL, last_read TIMESTAMPTZ NULL)",
          summary:
            "CREATE TABLE crdb_internal.index_usage_statistics (table_id INT8 NOT NULL, index_id INT8 NOT NULL, total_reads INT8 NOT NULL, last_read TIMESTAMPTZ NULL)",
          expected: "CREATE TABLE crdb_internal.index_usage_statistics",
        },
        {
          stmt: "SET CLUSTER SETTING sql.defaults.distsql = 1",
          summary: "SET CLUSTER SETTING sql.defaults.distsql = 1",
          expected: "SET CLUSTER SETTING sql.defaults.distsql",
        },
      ];

      simpleStatements.forEach(s =>
        assert.equal(s.expected, computeOrUseStmtSummary(s.stmt, s.summary)),
      );
    });
  });
});
