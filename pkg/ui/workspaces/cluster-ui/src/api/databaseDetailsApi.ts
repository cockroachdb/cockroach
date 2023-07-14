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
  createSqlExecutionRequest,
  executeInternalSql,
  formatApiResult,
  isMaxSizeError,
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
import moment from "moment-timezone";
import { fromHexString, withTimeout } from "./util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

const { ZoneConfig } = cockroach.config.zonepb;
const { ZoneConfigurationLevel } = cockroach.server.serverpb;
type ZoneConfigType = cockroach.config.zonepb.ZoneConfig;
type ZoneConfigLevelType = cockroach.server.serverpb.ZoneConfigurationLevel;

export type DatabaseDetailsResponse = {
  idResp: SqlApiQueryResponse<DatabaseIdRow>;
  grantsResp: SqlApiQueryResponse<DatabaseGrantsResponse>;
  tablesResp: SqlApiQueryResponse<DatabaseTablesResponse>;
  zoneConfigResp: SqlApiQueryResponse<DatabaseZoneConfigResponse>;
  stats?: DatabaseDetailsStats;
  error?: SqlExecutionErrorMessage;
};

function newDatabaseDetailsResponse(): DatabaseDetailsResponse {
  return {
    idResp: { database_id: "" },
    grantsResp: { grants: [] },
    tablesResp: { tables: [] },
    zoneConfigResp: {
      zone_config: new ZoneConfig({
        inherited_constraints: true,
        inherited_lease_preferences: true,
      }),
      zone_config_level: ZoneConfigurationLevel.CLUSTER,
    },
    stats: {
      spanStats: {
        approximate_disk_bytes: 0,
        live_bytes: 0,
        total_bytes: 0,
        range_count: 0,
      },
      replicaData: {
        replicas: [],
        regions: [],
      },
      indexStats: { num_index_recommendations: 0 },
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
      resp.idResp.database_id = txn_result.rows[0].database_id;
    }
    if (txn_result.error) {
      resp.idResp.error = txn_result.error;
    }
  },
  handleMaxSizeError: (
    dbName: string,
    response: SqlTxnResult<DatabaseIdRow>,
    dbDetail: DatabaseDetailsResponse,
  ) => {
    return new Promise<boolean>(() => false);
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
      sql: Format(
        `SELECT grantee as user, array_agg(privilege_type) as privileges
            FROM %1.crdb_internal.cluster_database_privileges
            WHERE database_name = $1 group by grantee`,
        [new Identifier(dbName)],
      ),
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txn_result: SqlTxnResult<DatabaseGrantsRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txn_result)) {
      resp.grantsResp.grants = txn_result.rows;
      if (txn_result.error) {
        resp.grantsResp.error = txn_result.error;
      }
    }
  },
  handleMaxSizeError: (
    dbName: string,
    response: SqlTxnResult<DatabaseGrantsRow>,
    dbDetail: DatabaseDetailsResponse,
  ) => {
    return new Promise<boolean>(() => false);
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
      if (!resp.tablesResp.tables) {
        resp.tablesResp.tables = [];
      }

      txn_result.rows.forEach(row => {
        const escTableName = new QualifiedIdentifier([
          row.table_schema,
          row.table_name,
        ]).SQLString();
        return resp.tablesResp.tables.push(escTableName);
      });
    }
    if (txn_result.error) {
      resp.tablesResp.error = txn_result.error;
    }
  },
  handleMaxSizeError: async (
    dbName: string,
    response: SqlTxnResult<DatabaseTablesRow>,
    dbDetail: DatabaseDetailsResponse,
  ) => {
    // Reset the error. It pulls all the names.
    dbDetail.tablesResp.error = null;
    let isMaxSize = true;
    do {
      const offsetQuery = {
        sql: Format(
          `SELECT table_schema, table_name
         FROM %1.information_schema.tables
         WHERE table_type != 'SYSTEM VIEW'
         ORDER BY table_name offset %2`,
          [new Identifier(dbName), dbDetail.tablesResp.tables.length],
        ),
      };
      const req = createSqlExecutionRequest(dbName, [offsetQuery]);
      const result = await executeInternalSql<DatabaseTablesRow>(req);
      if (result?.execution?.txn_results?.length ?? 0 > 0) {
        response = result.execution.txn_results[0];
        if (result.execution.txn_results[0].rows?.length ?? 0 > 0) {
          getDatabaseTablesQuery.addToDatabaseDetail(
            result.execution.txn_results[0],
            dbDetail,
          );
        }
      }

      isMaxSize = isMaxSizeError(result.error?.message);
      if (!isMaxSize) {
        dbDetail.error = result.error;
      }
    } while (isMaxSize);

    return true;
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
        resp.zoneConfigResp.zone_config = ZoneConfig.decode(
          new Uint8Array(zoneConfigBytes),
        );
        resp.zoneConfigResp.zone_config_level = ZoneConfigurationLevel.DATABASE;
      } catch (e) {
        console.error(
          `Database Details API - encountered an error decoding zone config string: ${zoneConfigHexString}`,
        );
        // Catch and assign the error if we encounter one decoding.
        resp.zoneConfigResp.error = e;
        resp.zoneConfigResp.zone_config_level = ZoneConfigurationLevel.UNKNOWN;
      }
    }
    if (txn_result.error) {
      resp.idResp.error = txn_result.error;
    }
  },
  handleMaxSizeError: (
    dbName: string,
    response: SqlTxnResult<DatabaseZoneConfigRow>,
    dbDetail: DatabaseDetailsResponse,
  ) => {
    return new Promise<boolean>(() => false);
  },
};

// Database Stats
type DatabaseDetailsStats = {
  spanStats: SqlApiQueryResponse<DatabaseSpanStatsRow>;
  replicaData: SqlApiQueryResponse<DatabaseReplicasRegionsRow>;
  indexStats: SqlApiQueryResponse<DatabaseIndexUsageStatsResponse>;
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
    if (txn_result && txn_result.error) {
      resp.stats.spanStats.error = txn_result.error;
    }
    if (txnResultIsEmpty(txn_result)) {
      return;
    }
    if (txn_result.rows.length === 1) {
      const row = txn_result.rows[0];
      resp.stats.spanStats.approximate_disk_bytes = row.approximate_disk_bytes;
      resp.stats.spanStats.range_count = row.range_count;
      resp.stats.spanStats.live_bytes = row.live_bytes;
      resp.stats.spanStats.total_bytes = row.total_bytes;
    } else {
      resp.stats.spanStats.error = new Error(
        `DatabaseDetails - Span Stats, expected 1 row, got ${txn_result.rows.length}`,
      );
    }
  },
  handleMaxSizeError: (
    dbName: string,
    response: SqlTxnResult<DatabaseSpanStatsRow>,
    dbDetail: DatabaseDetailsResponse,
  ) => {
    return new Promise<boolean>(() => false);
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
          replicasAndRegions AS (
              SELECT
                r.replicas,
                ARRAY(SELECT DISTINCT split_part(split_part(unnest(replica_localities),',',1),'=',2)) AS regions
              FROM crdb_internal.tables AS t
              JOIN %1.crdb_internal.table_spans AS s ON s.descriptor_id = t.table_id
              JOIN crdb_internal.ranges_no_leases AS r ON s.start_key < r.end_key AND s.end_key > r.start_key
           WHERE t.database_name = $1
          ),
          unique_replicas AS (SELECT (SELECT array_agg(distinct unnested) FROM unnest(replicas) AS unnested) AS regions FROM replicasAndRegions),
          unique_regions AS (SELECT (SELECT array_agg(distinct unnested) FROM unnest(regions) AS unnested) AS replicas FROM replicasAndRegions)
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
        resp.stats.replicaData.regions = txn_result.rows[0].regions;
        resp.stats.replicaData.replicas = txn_result.rows[0].replicas;
      }
      if (txn_result.error) {
        resp.stats.replicaData.error = txn_result.error;
      }
    },
    handleMaxSizeError: (
      dbName: string,
      response: SqlTxnResult<DatabaseReplicasRegionsRow>,
      dbDetail: DatabaseDetailsResponse,
    ) => {
      return new Promise<boolean>(() => false);
    },
  };

type DatabaseIndexUsageStatsResponse = {
  num_index_recommendations?: number;
};

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
                  JOIN %1.crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id AND us.table_id = ti.descriptor_id)
                  CROSS JOIN cs
                 WHERE $1 != 'system' AND ti.is_unique IS false)
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
        resp.stats.indexStats.num_index_recommendations += 1;
      }
    });
    if (txn_result.error) {
      resp.stats.indexStats.error = txn_result.error;
    }
  },
  handleMaxSizeError: (
    dbName: string,
    response: SqlTxnResult<IndexUsageStatistic>,
    dbDetail: DatabaseDetailsResponse,
  ) => {
    return new Promise<boolean>(() => false);
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
  handleMaxSizeError: (
    dbName: string,
    response: SqlTxnResult<RowType>,
    dbDetail: DatabaseDetailsResponse,
  ) => Promise<boolean>;
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
  return createSqlExecutionRequest(
    dbName,
    databaseDetailQueries.map(query => query.createStmt(dbName)),
  );
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
    if (resp.error.message.includes("max result size exceeded")) {
      return fetchSeparatelyDatabaseDetails(databaseName);
    }
    detailsResponse.error = resp.error;
  }
  return formatApiResult<DatabaseDetailsResponse>(
    detailsResponse,
    detailsResponse.error,
    "retrieving database details information",
  );
}

async function fetchSeparatelyDatabaseDetails(
  databaseName: string,
): Promise<SqlApiResponse<DatabaseDetailsResponse>> {
  const detailsResponse: DatabaseDetailsResponse = newDatabaseDetailsResponse();
  for (const databaseDetailQuery of databaseDetailQueries) {
    const req = createSqlExecutionRequest(databaseName, [
      databaseDetailQuery.createStmt(databaseName),
    ]);
    const resp = await executeInternalSql<DatabaseDetailsRow>(req);
    const txn_result = resp.execution.txn_results[0];
    if (txn_result.rows) {
      databaseDetailQuery.addToDatabaseDetail(txn_result, detailsResponse);
    }

    if (resp.error) {
      const handleFailure = await databaseDetailQuery.handleMaxSizeError(
        databaseName,
        txn_result,
        detailsResponse,
      );
      if (!handleFailure) {
        detailsResponse.error = resp.error;
      }
    }
  }

  return formatApiResult<DatabaseDetailsResponse>(
    detailsResponse,
    detailsResponse.error,
    "retrieving database details information",
  );
}
