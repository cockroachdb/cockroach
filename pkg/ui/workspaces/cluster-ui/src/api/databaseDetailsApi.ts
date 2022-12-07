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
  formatApiResult,
  LARGE_RESULT_SIZE,
  LONG_TIMEOUT,
  SqlApiQueryResponse,
  SqlApiResponse,
  SqlExecutionErrorMessage,
  SqlExecutionRequest,
  SqlStatement,
  SqlTxnResult,
  txnResultIsEmpty,
} from "./sqlApi";
import { IndexUsageStatistic, recommendDropUnusedIndex } from "../insights";
import { Format, Identifier, QualifiedIdentifier } from "./safesql";
import moment from "moment";
import { fromHexString, withTimeout } from "./util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

const { ZoneConfig } = cockroach.config.zonepb;
const { ZoneConfigurationLevel } = cockroach.server.serverpb;
type ZoneConfigType = cockroach.config.zonepb.ZoneConfig;
type ZoneConfigLevelType = cockroach.server.serverpb.ZoneConfigurationLevel;

export type DatabaseDetailsResponse = {
  id_resp: SqlApiQueryResponse<DatabaseIdRow>;
  grants_resp: SqlApiQueryResponse<DatabaseGrantsResponse>;
  tables_resp: SqlApiQueryResponse<DatabaseTablesResponse>;
  zone_config_resp: SqlApiQueryResponse<DatabaseZoneConfigResponse>;
  stats?: DatabaseDetailsStats;
  error?: SqlExecutionErrorMessage;
};

function newDatabaseDetailsResponse(): DatabaseDetailsResponse {
  return {
    id_resp: { database_id: "" },
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
      resp.id_resp.database_id = txn_result.rows[0].database_id;
    }
    if (txn_result.error) {
      resp.id_resp.error = txn_result.error;
    }
  },
};

// Database Grants
type DatabaseGrantsResponse = {
  grants: DatabaseGrantsRow[];
};

type DatabaseGrantsRow = {
  user: string;
  privileges: string[];
};

const getDatabaseGrantsQuery: DatabaseDetailsQuery<DatabaseGrantsRow> = {
  createStmt: dbName => {
    return {
      sql: `SELECT grantee as user, array_agg(privilege_type) as privileges
            FROM crdb_internal.cluster_database_privileges
            WHERE database_name = $1 group by grantee`,
      arguments: [dbName],
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

// Database Tables
type DatabaseTablesResponse = {
  tables: string[];
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
        const escTableName = new QualifiedIdentifier([
          row.table_schema,
          row.table_name,
        ]).SQLString();
        return `${escTableName}`;
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
};

type DatabaseZoneConfigRow = {
  zone_config_hex_string: string;
};

const getDatabaseZoneConfig: DatabaseDetailsQuery<DatabaseZoneConfigRow> = {
  createStmt: dbName => {
    return {
      sql: `SELECT 
        encode(
          crdb_internal.get_zone_config(
            (SELECT crdb_internal.get_database_id($1))
          ),
          'hex'
        ) as zone_config_hex_string`,
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseZoneConfigRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (
      !txnResultIsEmpty(txn_result) &&
      // Check that txn_result.rows[0].zone_config_hex_string is not null
      // and not empty.
      txn_result.rows[0].zone_config_hex_string &&
      txn_result.rows[0].zone_config_hex_string.length !== 0
    ) {
      const zoneConfigHexString = txn_result.rows[0].zone_config_hex_string;
      // Try to decode the zone config bytes response.
      try {
        // Parse the bytes from the hex string.
        const zoneConfigBytes = fromHexString(zoneConfigHexString);
        // Decode the bytes using ZoneConfig protobuf.
        resp.zone_config_resp.zone_config = ZoneConfig.decode(
          new Uint8Array(zoneConfigBytes),
        );
        resp.zone_config_resp.zone_config_level =
          ZoneConfigurationLevel.DATABASE;
      } catch (e) {
        console.error(
          `Database Details API - encountered an error decoding zone config string: ${zoneConfigHexString}`,
        );
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

type DatabaseRangesData = SqlApiQueryResponse<{
  range_count: number;
  live_bytes: number;
  total_bytes: number;
  // Note: we are currently populating this with replica ids which do not map 1 to 1
  node_ids: number[];
  regions: string[];
}>;

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
    if (txn_result.error) {
      resp.stats.ranges_data.error = txn_result.error;
    }
    if (txnResultIsEmpty(txn_result)) {
      return;
    }
    if (txn_result.rows.length === 1) {
      const row = txn_result.rows[0];
      resp.stats.pebble_data.approximate_disk_bytes =
        row.approximate_disk_bytes;
      resp.stats.ranges_data.range_count = row.range_count;
      resp.stats.ranges_data.live_bytes = row.live_bytes;
      resp.stats.ranges_data.total_bytes = row.total_bytes;
    } else {
      resp.stats.ranges_data.error = new Error(
        `DatabaseDetails - Span Stats, expected 1 row, got ${txn_result.rows.length}`,
      );
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
          `WITH
          replicasAndregions as (
              SELECT
                r.replicas,
                ARRAY(SELECT DISTINCT split_part(split_part(unnest(replica_localities),',',1),'=',2)) as regions
              FROM crdb_internal.tables as t
                     JOIN %1.crdb_internal.table_spans as s ON s.descriptor_id = t.table_id
             JOIN crdb_internal.ranges_no_leases as r ON s.start_key < r.end_key AND s.end_key > r.start_key
           WHERE t.database_name = $1
          ),
          unique_replicas AS (SELECT array_agg(distinct(unnest(replicas))) as replicas FROM replicasAndRegions),
          unique_regions AS (SELECT array_agg(distinct(unnest(regions))) as regions FROM replicasAndRegions)
          SELECT replicas, regions FROM unique_replicas CROSS JOIN unique_regions`,
          [new Identifier(dbName)],
        ),
        arguments: [dbName],
      };
    },
    addToDatabaseDetail: (
      txn_result: SqlTxnResult<DatabaseReplicasRegionsRow>,
      resp: DatabaseDetailsResponse,
    ) => {
      if (!txnResultIsEmpty(txn_result)) {
        resp.stats.ranges_data.regions = txn_result.rows[0].regions;
        resp.stats.ranges_data.node_ids = txn_result.rows[0].replicas;
      }
      if (txn_result.error) {
        resp.stats.ranges_data.error = txn_result.error;
      }
    },
  };

type DatabaseIndexUsageStatsResponse = SqlApiQueryResponse<{
  num_index_recommendations?: number;
}>;

const getDatabaseIndexUsageStats: DatabaseDetailsQuery<IndexUsageStatistic> = {
  createStmt: dbName => {
    return {
      sql: Format(
        `WITH cs AS (
          SELECT value 
              FROM crdb_internal.cluster_settings 
          WHERE variable = 'sql.index_recommendation.drop_unused_duration'
          )
          SELECT * FROM (SELECT
                  ti.created_at,
                  us.last_read,
                  us.total_reads,
                  cs.value as unused_threshold,
                  cs.value::interval as interval_threshold,
                  now() - COALESCE(us.last_read AT TIME ZONE 'UTC', COALESCE(ti.created_at, '0001-01-01')) as unused_interval
                  FROM %1.crdb_internal.index_usage_statistics AS us
                  JOIN %1.crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id AND us.table_id = ti.descriptor_id AND ti.index_type = 'secondary')
                  CROSS JOIN cs
                 WHERE $1 != 'system')
               WHERE unused_interval > interval_threshold
               ORDER BY total_reads DESC;`,
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
): Promise<SqlApiResponse<DatabaseDetailsResponse>> {
  return withTimeout(fetchDatabaseDetails(databaseName), timeout);
}

async function fetchDatabaseDetails(
  databaseName: string,
): Promise<SqlApiResponse<DatabaseDetailsResponse>> {
  const detailsResponse: DatabaseDetailsResponse = newDatabaseDetailsResponse();
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
  return formatApiResult<DatabaseDetailsResponse>(
    detailsResponse,
    detailsResponse.error,
    "retrieving database details information",
  );
}
