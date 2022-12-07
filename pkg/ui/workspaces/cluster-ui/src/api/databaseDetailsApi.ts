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
import { Format, Identifier } from "./safesql";
import moment from "moment";
import { withTimeout } from "./util";

export type DatabaseDetailsResponse = {
  grants_resp: DatabaseGrantsResponse;
  tables_resp: DatabaseTablesResponse;
  id_resp: DatabaseIdResponse;
  stats?: DatabaseDetailsStats;
  error?: SqlExecutionErrorMessage;
};

export function newDatabaseDetailsResponse(): DatabaseDetailsResponse {
  return {
    grants_resp: { grants: [] },
    tables_resp: { tables: [] },
    id_resp: { id: { database_id: "" } },
    stats: {
      ranges_data: {
        count: 0,
        node_ids: [],
        regions: [],
      },
      index_stats: { num_index_recommendations: 0 },
    },
  };
}

type DatabaseDetailsStats = {
  pebble_data?: DatabasePebbleData;
  ranges_data: DatabaseRangesData;
  index_stats: DatabaseIndexUsageStatsResponse;
};

type DatabasePebbleData = {
  approximate_disk_bytes: number; // (can't get this currently via SQL)
};

type DatabaseRangesData = {
  count: number;
  // TODO(thomas): currently we are using replicas to populate node ids
  // which does not map 1 to 1.
  node_ids: number[];
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

const getDatabaseId: DatabaseDetailsQuery<DatabaseIdRow> = {
  createStmt: dbName => {
    return {
      sql: `SELECT crdb_internal.get_database_id($1) as database_id`,
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseIdRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.id_resp.id.database_id = txn_result.rows[0].database_id;
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
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

const getDatabaseGrantsQuery: DatabaseDetailsQuery<DatabaseGrantsRow> = {
  createStmt: dbName => {
    return {
      sql: Format(`SELECT * FROM [SHOW GRANTS ON DATABASE %1]`, [
        new Identifier(dbName),
      ]),
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseGrantsRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.grants_resp.grants = txn_result.rows;
      if (txn_result.error) {
        resp.grants_resp.error = txn_result.error;
      }
    }
  },
};

type DatabaseTablesResponse = {
  tables: DatabaseTablesRow[];
  error?: Error;
};

type DatabaseTablesRow = {
  table_schema: string;
  table_name: string;
};

const getDatabaseTablesQuery: DatabaseDetailsQuery<DatabaseTablesRow> = {
  createStmt: dbName => {
    return {
      sql: Format(
        `SELECT table_schema, table_name FROM %1.information_schema.tables WHERE table_catalog = $1 AND table_type != 'SYSTEM VIEW' ORDER BY table_name`,
        [new Identifier(dbName)],
      ),
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseTablesRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.tables_resp.tables = txn_result.rows;
    }
    if (txn_result.error) {
      resp.tables_resp.error = txn_result.error;
    }
  },
};

type DatabaseRangesRow = {
  range_id: number;
  table_id: number;
  database_name: string;
  schema_name: string;
  table_name: string;
  replicas: number[];
  regions: string[];
  range_size: number;
};

const getDatabaseRanges: DatabaseDetailsQuery<DatabaseRangesRow> = {
  createStmt: dbName => {
    return {
      sql: Format(
        `SELECT
            r.range_id,
            t.table_id,
            t.database_name,
            t.name as table_name,
            t.schema_name,
            r.replicas,
            ARRAY(SELECT DISTINCT split_part(split_part(unnest(replica_localities),',',1),'=',2)) as regions,
            (crdb_internal.range_stats(s.start_key) ->>'key_bytes') ::INT +
            (crdb_internal.range_stats(s.start_key)->>'val_bytes')::INT +
            coalesce((crdb_internal.range_stats(s.start_key)->>'range_key_bytes')::INT, 0) +
            coalesce((crdb_internal.range_stats(s.start_key)->>'range_val_bytes')::INT, 0) AS range_size
          FROM crdb_internal.tables as t
          JOIN %1.crdb_internal.table_spans as s ON s.descriptor_id = t.table_id
          JOIN crdb_internal.ranges_no_leases as r ON s.start_key < r.end_key AND s.end_key > r.start_key
          WHERE t.database_name = $1`,
        [new Identifier(dbName)],
      ),
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseRangesRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    // Build set of unique regions for this database.
    const regions = new Set<string>();
    // Build set of unique replicas for this database.
    const replicas = new Set<number>();
    if (!txnResultIsEmpty(txn_result)) {
      txn_result.rows.forEach(row => {
        row.regions.forEach(regions.add, regions);
        row.replicas.forEach(replicas.add, replicas);
      });
      resp.stats.ranges_data.regions = Array.from(regions.values());
      resp.stats.ranges_data.node_ids = Array.from(replicas.values());
      resp.stats.ranges_data.count = txn_result.rows.length;
    }
    if (txn_result.error) {
      resp.stats.ranges_data.error = txn_result.error;
    }
  },
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

const getDatabaseIndexUsageStats: DatabaseDetailsQuery<DatabaseIndexUsageStatsRow> =
  {
    createStmt: dbName => {
      return {
        sql: Format(
          `SELECT
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
          FROM %1.crdb_internal.index_usage_statistics AS us
                 JOIN %1.crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id AND us.table_id = ti.descriptor_id AND index_type = 'secondary')
                 JOIN %1.crdb_internal.tables AS t ON (ti.descriptor_id = t.table_id AND t.database_name != 'system')`,
          [new Identifier(dbName)],
        ),
      };
    },
    addToDatabaseDetail: (
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
    },
  };

export type DatabaseDetailsRow =
  | DatabaseIdRow
  | DatabaseGrantsRow
  | DatabaseTablesRow
  | DatabaseRangesRow
  | DatabaseIndexUsageStatsRow;

type DatabaseDetailsQuery<RowType> = {
  createStmt: (dbName: string) => SqlStatement;
  addToDatabaseDetail: (
    response: SqlTxnResult<RowType>,
    dbDetail: DatabaseDetailsResponse,
  ) => void;
};

const databaseDetailQueries: DatabaseDetailsQuery<DatabaseDetailsRow>[] = [
  getDatabaseId,
  getDatabaseGrantsQuery,
  getDatabaseTablesQuery,
  getDatabaseRanges,
  getDatabaseIndexUsageStats,
];

export function createDatabaseDetailsReq(dbName: string): SqlExecutionRequest {
  return {
    execute: true,
    statements: databaseDetailQueries.map(query => query.createStmt(dbName)),
    max_result_size: LARGE_RESULT_SIZE,
  };
}

export async function getDatabaseDetails(
  databaseName: string,
  timeout?: moment.Duration,
): Promise<DatabaseDetailsResponse> {
  const req: SqlExecutionRequest = createDatabaseDetailsReq(databaseName);
  const resp: DatabaseDetailsResponse = newDatabaseDetailsResponse();

  return withTimeout(executeInternalSql<DatabaseDetailsRow>(req), timeout).then(
    res => {
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
    },
  );
}
