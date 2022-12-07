// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  executeInternalSql,
  LARGE_RESULT_SIZE,
  SqlExecutionErrorMessage,
  SqlExecutionRequest,
  SqlStatement,
  SqlTxnResult,
  txnResultIsEmpty,
} from "./sqlApi";
import { recommendDropUnusedIndex } from "../insights";
import { parseReplicaLocalities } from "./util";

type DatabaseDetailsResponse = {
  grants_resp: DatabaseGrantsResponse;
  tables_resp: DatabaseTablesResponse;
  id_resp: DatabaseIdResponse;
  stats?: DatabaseDetailsStats;
  error?: SqlExecutionErrorMessage;
};

function newDatabaseDetailsResponse(): DatabaseDetailsResponse {
  return {
    grants_resp: { grants: [] },
    tables_resp: { tables: [] },
    id_resp: { id: { database_id: "" } },
    stats: {
      ranges_data: {
        count: 0,
        regions: [],
      },
      index_stats: { num_index_recommendations: 0 },
    },
  };
}

type DatabaseDetailsStats = {
  ranges_data: DatabaseRangesData;
  index_stats: DatabaseIndexUsageStatsResponse;
};

type DatabaseRangesData = {
  // missing_tables (not sure if this is necessary)
  count?: number;
  // approximate_disk_bytes: number (can't get this currently - range_size_mb in SHOW RANGES gives total uncompressed disk space - not the same... this is used for 'Live Data')
  // node_ids?: number (don't think we can get this currently, SHOW RANGES only shows replica ids)
  regions: string[];
  error?: Error;
};

type DatabaseIdResponse = {
  id: DatabaseIdRow;
  error?: Error;
};

type DatabaseIdRow = {
  database_id: string;
};

const getDatabaseId = (dbName: string): DatabaseDetailsQuery<DatabaseIdRow> => {
  const stmt: SqlStatement = {
    sql: `SELECT crdb_internal.get_database_id($1) as database_id`,
    arguments: [dbName],
  };
  const addToDatabaseDetail = (
    txn_result: SqlTxnResult<DatabaseIdRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.id_resp.id.database_id = txn_result.rows[0].database_id;
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  };
  return {
    stmt,
    addToDatabaseDetail,
  };
};

type DatabaseGrantsResponse = {
  grants: DatabaseGrantsRow[];
  error?: Error;
};

type DatabaseGrantsRow = {
  database_name: string;
  grantee: string;
  privilege_type: string;
  is_grantable: boolean;
};

const getDatabaseGrantsQuery = (
  dbName: string,
): DatabaseDetailsQuery<DatabaseGrantsRow> => {
  const stmt: SqlStatement = {
    sql: `SELECT * FROM [SHOW GRANTS ON DATABASE ${dbName}]`,
  };
  const addToDatabaseDetail = (
    txn_result: SqlTxnResult<DatabaseGrantsRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.grants_resp.grants = txn_result.rows;
      if (txn_result.error) {
        resp.grants_resp.error = txn_result.error;
      }
    }
  };
  return {
    stmt,
    addToDatabaseDetail,
  };
};

type DatabaseTablesResponse = {
  tables: DatabaseTablesRow[];
  error?: Error;
};

type DatabaseTablesRow = {
  table_schema: string;
  table_name: string;
};

const getDatabaseTablesQuery = (
  dbName: string,
): DatabaseDetailsQuery<DatabaseTablesRow> => {
  const stmt: SqlStatement = {
    sql: `SELECT table_schema, table_name FROM ${dbName}.information_schema.tables WHERE table_catalog = $1 AND table_type != 'SYSTEM VIEW' ORDER BY table_name`,
    arguments: [dbName],
  };
  const addToDatabaseDetail = (
    txn_result: SqlTxnResult<DatabaseTablesRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.tables_resp.tables = txn_result.rows;
    }
    if (txn_result.error) {
      resp.tables_resp.error = txn_result.error;
    }
  };
  return {
    stmt,
    addToDatabaseDetail,
  };
};

type DatabaseRangesRow = {
  range_id: number;
  table_id: number;
  database_name: string;
  schema_name: string;
  table_name: string;
  replicas: number[];
  replica_localities: string[];
  range_size: number;
};

const getDatabaseRanges = (
  dbName: string,
): DatabaseDetailsQuery<DatabaseRangesRow> => {
  const stmt: SqlStatement = {
    sql: `SELECT
            r.range_id,
            t.table_id,
            t.database_name,
            t.name as table_name,
            t.schema_name,
            r.replicas,
            r.replica_localities,
            (crdb_internal.range_stats(r.start_key) ->>'key_bytes') ::INT +
            (crdb_internal.range_stats(r.start_key)->>'val_bytes')::INT +
            coalesce((crdb_internal.range_stats(r.start_key)->>'range_key_bytes')::INT, 0) +
            coalesce((crdb_internal.range_stats(r.start_key)->>'range_val_bytes')::INT, 0) AS range_size
          FROM crdb_internal.tables as t
          JOIN crdb_internal.ranges_no_leases as r ON r.start_key = (crdb_internal.table_span(t.table_id)[1])
          WHERE t.database_name = $1`,
    arguments: [dbName],
  };
  const addToDatabaseDetail = (
    txn_result: SqlTxnResult<DatabaseRangesRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      txn_result.rows.forEach(row => {
        resp.stats.ranges_data.regions = parseReplicaLocalities(
          row.replica_localities,
        );
      });
      resp.stats.ranges_data.count = txn_result.rows.length;
    }
    if (txn_result.error) {
      resp.stats.ranges_data.error = txn_result.error;
    }
  };
  return {
    stmt,
    addToDatabaseDetail,
  };
};

type DatabaseIndexUsageStatsResponse = {
  num_index_recommendations?: number;
  error?: Error;
};

export type DatabaseIndexUsageStatsRow = {
  database_name: string;
  table_name: string;
  table_id: number;
  index_name: string;
  index_id: number;
  index_type: string;
  total_reads: number;
  last_read: string;
  created_at: string;
  unused_threshold: string;
};

const getDatabaseIndexUsageStats = (
  dbName: string,
): DatabaseDetailsQuery<DatabaseIndexUsageStatsRow> => {
  const stmt: SqlStatement = {
    sql: `SELECT
            t.database_name,
            ti.descriptor_name as table_name,
            ti.descriptor_id as table_id,
            ti.index_name,
            ti.index_id,
            ti.index_type,
            total_reads,
            last_read,
            ti.created_at,
            (SELECT value FROM crdb_internal.cluster_settings WHERE variable = 'sql.index_recommendation.drop_unused_duration') AS unused_threshold
          FROM ${dbName}.crdb_internal.index_usage_statistics AS us
                 JOIN ${dbName}.crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id AND us.table_id = ti.descriptor_id AND index_type = 'secondary')
                 JOIN ${dbName}.crdb_internal.tables AS t ON (ti.descriptor_id = t.table_id AND t.database_name != 'system')`,
  };
  const addToDatabaseDetail = (
    txn_result: SqlTxnResult<DatabaseIndexUsageStatsRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    txn_result.rows?.forEach(row => {
      const rec = recommendDropUnusedIndex(row);
      if (rec.recommend) {
        resp.stats.index_stats.num_index_recommendations += 1;
      }
    });
    if (txn_result.error) {
      resp.stats.index_stats.error = txn_result.error;
    }
  };
  return {
    stmt,
    addToDatabaseDetail,
  };
};

type DatabaseDetailsRow =
  | DatabaseIdRow
  | DatabaseGrantsRow
  | DatabaseTablesRow
  | DatabaseRangesRow
  | DatabaseIndexUsageStatsRow;

type DatabaseDetailsQuery<RowType> = {
  stmt: SqlStatement;
  addToDatabaseDetail: (
    response: SqlTxnResult<RowType>,
    dbDetail: DatabaseDetailsResponse,
  ) => void;
};

export async function getDatabaseDetails(databaseName: string) {
  const databaseDetailQueries: DatabaseDetailsQuery<DatabaseDetailsRow>[] = [
    getDatabaseId(databaseName),
    getDatabaseGrantsQuery(databaseName),
    getDatabaseTablesQuery(databaseName),
    getDatabaseRanges(databaseName),
    getDatabaseIndexUsageStats(databaseName),
  ];

  const req: SqlExecutionRequest = {
    execute: true,
    statements: databaseDetailQueries.map(query => query.stmt),
    max_result_size: LARGE_RESULT_SIZE,
  };

  const resp: DatabaseDetailsResponse = newDatabaseDetailsResponse();

  const res = await executeInternalSql<DatabaseDetailsRow>(req);

  res.execution.txn_results.forEach(txn_result => {
    if (txn_result.rows) {
      const query: DatabaseDetailsQuery<DatabaseDetailsRow> =
        databaseDetailQueries[txn_result.statement - 1];
      query.addToDatabaseDetail(txn_result, resp);
    }
  });

  if (res.error) {
    resp.error = res.error;
  }

  return resp;
}
