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
  LONG_TIMEOUT,
  SqlExecutionErrorMessage,
  SqlExecutionRequest,
  SqlStatement,
  SqlTxnResult,
  txnResultIsEmpty,
} from "./sqlApi";
import { IndexUsageStatistic, recommendDropUnusedIndex } from "../insights";
import { Format, Identifier } from "./safesql";
import moment from "moment";
import { withTimeout } from "./util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

const { ZoneConfig } = cockroach.config.zonepb;
const { ZoneConfigurationLevel } = cockroach.server.serverpb;
type ZoneConfigType = cockroach.config.zonepb.ZoneConfig;
type ZoneConfigLevelType = cockroach.server.serverpb.ZoneConfigurationLevel;

export type DatabaseDetailsResponse = {
  id_resp: DatabaseIdResponse;
  grants_resp: DatabaseGrantsResponse;
  tables_resp: DatabaseTablesResponse;
  zone_config_resp: DatabaseZoneConfigResponse;
  stats?: DatabaseDetailsStats;
  error?: SqlExecutionErrorMessage;
};

function newDatabaseDetailsResponse(): DatabaseDetailsResponse {
  return {
    id_resp: { id: { database_id: "" } },
    grants_resp: { grants: [] },
    tables_resp: { tables: [] },
    zone_config_resp: {
      zone_config: new ZoneConfig({
        inherited_constraints: true,
        inherited_lease_preferences: true,
      }),
      zone_config_level: ZoneConfigurationLevel.CLUSTER,
    },
    stats: {
      ranges_data: {
        range_count: 0,
        live_bytes: 0,
        total_bytes: 0,
        // Note: we are currently populating this with replica ids which do not map 1 to 1
        node_ids: [],
        regions: [],
      },
      pebble_data: {
        approximate_disk_bytes: 0,
      },
      index_stats: { num_index_recommendations: 0 },
    },
  };
}

// Database ID
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
    // Check that txn_result.rows[0].database_id is not null
    if (!txnResultIsEmpty(txn_result) && txn_result.rows[0].database_id) {
      resp.id_resp.id.database_id = txn_result.rows[0].database_id;
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Database Grants
type DatabaseGrantsResponse = {
  grants: DatabaseGrant[];
  error?: Error;
};

type DatabaseGrant = {
  user: string;
  privileges: string[];
};

type DatabaseGrantsRow = {
  grantee: string;
  privilege_type: string;
};

const getDatabaseGrantsQuery: DatabaseDetailsQuery<DatabaseGrantsRow> = {
  createStmt: dbName => {
    return {
      sql: `SELECT grantee, privilege_type
            FROM crdb_internal.cluster_database_privileges
            WHERE database_name = $1`,
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseGrantsRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      // Transform DatabaseGrantsRow -> DatabaseGrant.
      resp.grants_resp.grants = txn_result.rows.map(row => {
        return {
          user: row.grantee,
          privileges: row.privilege_type.split(","),
        };
      });
      if (txn_result.error) {
        resp.grants_resp.error = txn_result.error;
      }
    }
  },
};

// Database Tables
type DatabaseTablesResponse = {
  tables: string[];
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
        `SELECT table_schema, table_name
         FROM %1.information_schema.tables
         WHERE table_type != 'SYSTEM VIEW'
         ORDER BY table_name`,
        [new Identifier(dbName)],
      ),
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseTablesRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.tables_resp.tables = txn_result.rows.map(row => {
        return `${row.table_schema}.${row.table_name}`;
      });
    }
    if (txn_result.error) {
      resp.tables_resp.error = txn_result.error;
    }
  },
};

// Database Zone Config
type DatabaseZoneConfigResponse = {
  zone_config: ZoneConfigType;
  zone_config_level: ZoneConfigLevelType;
  error?: Error;
};

type DatabaseZoneConfigRow = {
  zone_config_bytes: Uint8Array;
};

const getDatabaseZoneConfig: DatabaseDetailsQuery<DatabaseZoneConfigRow> = {
  createStmt: dbName => {
    return {
      sql: `SELECT crdb_internal.get_zone_config((SELECT crdb_internal.get_database_id($1))) as zone_config_bytes`,
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseZoneConfigRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (
      !txnResultIsEmpty(txn_result) &&
      // Check that txn_result.rows[0].zone_config_bytes is not null
      txn_result.rows[0].zone_config_bytes &&
      txn_result.rows[0].zone_config_bytes.length !== 0
    ) {
      // Try to decode the zone config bytes response.
      try {
        resp.zone_config_resp.zone_config = ZoneConfig.decode(
          txn_result.rows[0].zone_config_bytes,
        );
        resp.zone_config_resp.zone_config_level =
          ZoneConfigurationLevel.DATABASE;
      } catch (e) {
        // Catch and assign the error if we encounter one decoding.
        resp.zone_config_resp.error = e;
        resp.zone_config_resp.zone_config_level =
          ZoneConfigurationLevel.UNKNOWN;
      }
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Database Stats
type DatabaseDetailsStats = {
  pebble_data: DatabasePebbleData;
  ranges_data: DatabaseRangesData;
  index_stats: DatabaseIndexUsageStatsResponse;
};

type DatabasePebbleData = {
  approximate_disk_bytes: number;
};

type DatabaseRangesData = {
  range_count: number;
  live_bytes: number;
  total_bytes: number;
  // Note: we are currently populating this with replica ids which do not map 1 to 1
  node_ids: number[];
  regions: string[];
  error?: Error;
};

type DatabaseSpanStatsRow = {
  approximate_disk_bytes: number;
  live_bytes: number;
  total_bytes: number;
  range_count: number;
};

const getDatabaseSpanStats: DatabaseDetailsQuery<DatabaseSpanStatsRow> = {
  createStmt: dbName => {
    return {
      sql: `SELECT
            sum(range_count) as range_count,
            sum(approximate_disk_bytes) as approximate_disk_bytes,
            sum(live_bytes) as live_bytes,
            sum(total_bytes) as total_bytes
          FROM crdb_internal.tenant_span_stats((SELECT crdb_internal.get_database_id($1)))`,
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseSpanStatsRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.stats.pebble_data.approximate_disk_bytes =
        txn_result.rows[0].approximate_disk_bytes;
      resp.stats.ranges_data.range_count = txn_result.rows[0].range_count;
      resp.stats.ranges_data.live_bytes = txn_result.rows[0].live_bytes;
      resp.stats.ranges_data.total_bytes = txn_result.rows[0].total_bytes;
    }
    if (txn_result.error) {
      resp.stats.ranges_data.error = txn_result.error;
    }
  },
};

type DatabaseReplicasRegionsRow = {
  replicas: number[];
  regions: string[];
};

const getDatabaseReplicasAndRegions: DatabaseDetailsQuery<DatabaseReplicasRegionsRow> =
  {
    createStmt: dbName => {
      return {
        sql: Format(
          `SELECT
            r.replicas,
            ARRAY(SELECT DISTINCT split_part(split_part(unnest(replica_localities),',',1),'=',2)) as regions
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
      txn_result: SqlTxnResult<DatabaseReplicasRegionsRow>,
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

const getDatabaseIndexUsageStats: DatabaseDetailsQuery<IndexUsageStatistic> = {
  createStmt: dbName => {
    return {
      sql: Format(
        `SELECT
            $1 as database_name,
            last_read,
            ti.created_at,
            (SELECT value FROM crdb_internal.cluster_settings WHERE variable = 'sql.index_recommendation.drop_unused_duration') AS unused_threshold
          FROM %1.crdb_internal.index_usage_statistics AS us
                 JOIN %1.crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id AND us.table_id = ti.descriptor_id AND index_type = 'secondary')`,
        [new Identifier(dbName)],
      ),
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<IndexUsageStatistic>,
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
  | DatabaseZoneConfigRow
  | DatabaseSpanStatsRow
  | DatabaseReplicasRegionsRow
  | IndexUsageStatistic;

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
  getDatabaseReplicasAndRegions,
  getDatabaseIndexUsageStats,
  getDatabaseZoneConfig,
  getDatabaseSpanStats,
];

export function createDatabaseDetailsReq(dbName: string): SqlExecutionRequest {
  return {
    execute: true,
    statements: databaseDetailQueries.map(query => query.createStmt(dbName)),
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
  };
}

export async function getDatabaseDetails(
  databaseName: string,
  timeout?: moment.Duration,
): Promise<DatabaseDetailsResponse> {
  return withTimeout(fetchDatabaseDetails(databaseName), timeout).then(resp => {
    return resp;
  });
}

async function fetchDatabaseDetails(
  databaseName: string,
): Promise<DatabaseDetailsResponse> {
  const detailsResponse: DatabaseDetailsResponse = newDatabaseDetailsResponse();

  // Request database details that require the database name argument.
  const req: SqlExecutionRequest = createDatabaseDetailsReq(databaseName);
  const resp = await executeInternalSql<DatabaseDetailsRow>(req);
  resp.execution.txn_results.forEach(txn_result => {
    if (txn_result.rows) {
      const query: DatabaseDetailsQuery<DatabaseDetailsRow> =
        databaseDetailQueries[txn_result.statement - 1];
      query.addToDatabaseDetail(txn_result, detailsResponse);
    }
  });
  if (resp.error) {
    detailsResponse.error = resp.error;
  }
  return detailsResponse;
}
