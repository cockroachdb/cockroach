// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";

import { IndexUsageStatistic, recommendDropUnusedIndex } from "../insights";
import { getLogger, indexUnusedDuration, maybeError } from "../util";

import { Format, Identifier, QualifiedIdentifier } from "./safesql";
import {
  combineQueryErrors,
  createSqlExecutionRequest,
  executeInternalSql,
  formatApiResult,
  isMaxSizeError,
  SqlApiQueryResponse,
  SqlApiResponse,
  SqlExecutionErrorMessage,
  SqlExecutionRequest,
  sqlResultsAreEmpty,
  SqlExecutionResponse,
  SqlStatement,
  SqlTxnResult,
  txnResultIsEmpty,
} from "./sqlApi";
import { fromHexString, withTimeout } from "./util";

const { ZoneConfig } = cockroach.config.zonepb;
const { ZoneConfigurationLevel } = cockroach.server.serverpb;
type ZoneConfigType = cockroach.config.zonepb.ZoneConfig;
type ZoneConfigLevelType = cockroach.server.serverpb.ZoneConfigurationLevel;

export type DatabaseDetailsReqParams = {
  database: string;
  csIndexUnusedDuration: string;
};

export type DatabaseDetailsSpanStatsReqParams = {
  database: string;
};

export type DatabaseDetailsResponse = {
  idResp: SqlApiQueryResponse<DatabaseIdRow>;
  grantsResp: SqlApiQueryResponse<DatabaseGrantsResponse>;
  tablesResp: SqlApiQueryResponse<DatabaseTablesResponse>;
  zoneConfigResp: SqlApiQueryResponse<DatabaseZoneConfigResponse>;
  stats?: DatabaseDetailsStats;
  error?: SqlExecutionErrorMessage;
};

export type DatabaseDetailsSpanStatsResponse = {
  spanStats: SqlApiQueryResponse<DatabaseSpanStatsRow>;
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
      replicaData: {
        storeIDs: [],
      },
      indexStats: { num_index_recommendations: 0 },
    },
  };
}

function newDatabaseDetailsSpanStatsResponse(): DatabaseDetailsSpanStatsResponse {
  return {
    spanStats: {
      approximate_disk_bytes: 0,
      live_bytes: 0,
      total_bytes: 0,
    },
    error: undefined,
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
    txnResult: SqlTxnResult<DatabaseIdRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    // Check that txn_result.rows[0].database_id is not null
    if (!txnResultIsEmpty(txnResult) && txnResult.rows[0].database_id) {
      resp.idResp.database_id = txnResult.rows[0].database_id;
    }
    if (txnResult.error) {
      resp.idResp.error = txnResult.error;
    }
  },
  handleMaxSizeError: (_dbName, _response, _dbDetail) => {
    return Promise.resolve(false);
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
    txnResult: SqlTxnResult<DatabaseGrantsRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      resp.grantsResp.grants = txnResult.rows;
      if (txnResult.error) {
        resp.grantsResp.error = txnResult.error;
      }
    }
  },
  handleMaxSizeError: (_dbName, _response, _dbDetail) => {
    return Promise.resolve(false);
  },
};

export type TableNameParts = {
  // Raw unquoted, unescaped schema name.
  schema: string;
  // Raw unquoted, unescaped table name.
  table: string;

  // qualifiedNameWithSchemaAndTable is the qualifed
  // table name containing escaped, quoted schema and
  // table name parts.
  qualifiedNameWithSchemaAndTable: string;
};

// Database Tables
export type DatabaseTablesResponse = {
  tables: TableNameParts[];
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
         WHERE table_type NOT IN ('SYSTEM VIEW', 'VIEW')
         ORDER BY table_name`,
        [new Identifier(dbName)],
      ),
    };
  },
  addToDatabaseDetail: (
    txnResult: SqlTxnResult<DatabaseTablesRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      if (!resp.tablesResp.tables) {
        resp.tablesResp.tables = [];
      }

      txnResult.rows.forEach(row => {
        const escTableName = new QualifiedIdentifier([
          row.table_schema,
          row.table_name,
        ]).sqlString();
        resp.tablesResp.tables.push({
          schema: row.table_schema,
          table: row.table_name,
          qualifiedNameWithSchemaAndTable: escTableName,
        });
      });
    }
    if (txnResult.error) {
      resp.tablesResp.error = txnResult.error;
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
         WHERE table_type NOT IN ('SYSTEM VIEW', 'VIEW')
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
    txnResult: SqlTxnResult<DatabaseZoneConfigRow>,
    resp: DatabaseDetailsResponse,
  ) => {
    if (
      !txnResultIsEmpty(txnResult) &&
      // Check that txn_result.rows[0].zone_config_hex_string is not null
      // and not empty.
      txnResult.rows[0].zone_config_hex_string &&
      txnResult.rows[0].zone_config_hex_string.length !== 0
    ) {
      const zoneConfigHexString = txnResult.rows[0].zone_config_hex_string;
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
        getLogger().error(
          `Database Details API - encountered an error decoding zone config string: ${zoneConfigHexString}`,
          /* additional context */ undefined,
          e,
        );
        // Catch and assign the error if we encounter one decoding.
        resp.zoneConfigResp.error = maybeError(e);
        resp.zoneConfigResp.zone_config_level = ZoneConfigurationLevel.UNKNOWN;
      }
    }
    if (txnResult.error) {
      resp.zoneConfigResp.error = txnResult.error;
    }
  },
  handleMaxSizeError: (_dbName, _response, _dbDetail) => {
    return Promise.resolve(false);
  },
};

// Database Stats
type DatabaseDetailsStats = {
  replicaData: {
    storeIDs: number[];
    error?: Error;
  };
  indexStats: SqlApiQueryResponse<DatabaseIndexUsageStatsResponse>;
};

export type DatabaseSpanStatsRow = {
  approximate_disk_bytes: number;
  live_bytes: number;
  total_bytes: number;
};

function formatSpanStatsExecutionResult(
  res: SqlExecutionResponse<DatabaseSpanStatsRow>,
): DatabaseDetailsSpanStatsResponse {
  const out = newDatabaseDetailsSpanStatsResponse();

  if (res.execution.txn_results.length === 0) {
    return out;
  }

  const txnResult = res.execution.txn_results[0];

  if (txnResult && txnResult.error) {
    // Copy the SQLExecutionError and the SqlTransactionResult error.
    out.error = res.error;
    out.spanStats.error = txnResult.error;
  }
  if (txnResultIsEmpty(txnResult)) {
    return out;
  }
  if (txnResult.rows.length === 1) {
    const row = txnResult.rows[0];
    out.spanStats.approximate_disk_bytes = row.approximate_disk_bytes;
    out.spanStats.live_bytes = row.live_bytes;
    out.spanStats.total_bytes = row.total_bytes;
  } else {
    out.spanStats.error = new Error(
      `DatabaseDetails - Span Stats, expected 1 row, got ${txnResult.rows.length}`,
    );
  }
  return out;
}

type DatabaseReplicasRegionsRow = {
  store_ids: number[];
};

const getDatabaseReplicasAndRegions: DatabaseDetailsQuery<DatabaseReplicasRegionsRow> =
  {
    createStmt: dbName => {
      // This query is meant to retrieve the per-database set of store ids.
      return {
        sql: Format(
          `
            SELECT array_agg(DISTINCT unnested_store_ids) AS store_ids
            FROM [SHOW RANGES FROM DATABASE %1], unnest(replicas) AS unnested_store_ids
`,
          [new Identifier(dbName)],
        ),
      };
    },
    addToDatabaseDetail: (
      txnResult: SqlTxnResult<DatabaseReplicasRegionsRow>,
      resp: DatabaseDetailsResponse,
    ) => {
      if (txnResult.error) {
        resp.stats.replicaData.error = txnResult.error;
        // We don't expect to have any rows for this query on error.
        return;
      }
      if (!txnResultIsEmpty(txnResult)) {
        resp.stats.replicaData.storeIDs = txnResult?.rows[0]?.store_ids ?? [];
      }
    },
    handleMaxSizeError: (_dbName, _response, _dbDetail) => {
      return Promise.resolve(false);
    },
  };

type DatabaseIndexUsageStatsResponse = {
  num_index_recommendations?: number;
};

const getDatabaseIndexUsageStats: DatabaseDetailsQuery<IndexUsageStatistic> = {
  createStmt: (dbName: string, csIndexUnusedDuration: string) => {
    csIndexUnusedDuration = csIndexUnusedDuration ?? indexUnusedDuration;
    return {
      sql: Format(
        `SELECT * FROM (SELECT
                  ti.created_at,
                  us.last_read,
                  us.total_reads,
                  '${csIndexUnusedDuration}' as unused_threshold,
                  '${csIndexUnusedDuration}'::interval as interval_threshold,
                  now() - COALESCE(us.last_read AT TIME ZONE 'UTC', COALESCE(ti.created_at, '0001-01-01')) as unused_interval
                  FROM %1.crdb_internal.index_usage_statistics AS us
                  JOIN %1.crdb_internal.table_indexes AS ti ON (us.index_id = ti.index_id AND us.table_id = ti.descriptor_id)
                 WHERE $1 != 'system' AND ti.is_unique IS false)
               WHERE unused_interval > interval_threshold
               ORDER BY total_reads DESC;`,
        [new Identifier(dbName)],
      ),
      arguments: [dbName],
    };
  },
  addToDatabaseDetail: (
    txnResult: SqlTxnResult<IndexUsageStatistic>,
    resp: DatabaseDetailsResponse,
  ) => {
    txnResult.rows?.forEach(row => {
      const rec = recommendDropUnusedIndex(row);
      if (rec.recommend) {
        resp.stats.indexStats.num_index_recommendations += 1;
      }
    });
    if (txnResult.error) {
      resp.stats.indexStats.error = txnResult.error;
    }
  },
  handleMaxSizeError: (_dbName, _response, _dbDetail) => {
    return Promise.resolve(false);
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
  createStmt: (dbName: string, csIndexUnusedDuration: string) => SqlStatement;
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
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getDatabaseId,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getDatabaseGrantsQuery,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getDatabaseTablesQuery,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getDatabaseReplicasAndRegions,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getDatabaseIndexUsageStats,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getDatabaseZoneConfig,
];

export function createDatabaseDetailsReq(
  params: DatabaseDetailsReqParams,
): SqlExecutionRequest {
  return {
    ...createSqlExecutionRequest(
      params.database,
      databaseDetailQueries.map(query =>
        query.createStmt(params.database, params.csIndexUnusedDuration),
      ),
    ),
    separate_txns: true,
  };
}

export function createDatabaseDetailsSpanStatsReq(
  params: DatabaseDetailsSpanStatsReqParams,
): SqlExecutionRequest {
  const statement = {
    sql: `SELECT
            sum(approximate_disk_bytes) as approximate_disk_bytes,
            sum(live_bytes) as live_bytes,
            sum(total_bytes) as total_bytes
          FROM crdb_internal.tenant_span_stats((SELECT crdb_internal.get_database_id($1)))`,
    arguments: [params.database],
  };
  return createSqlExecutionRequest(params.database, [statement]);
}

export async function getDatabaseDetailsSpanStats(
  params: DatabaseDetailsSpanStatsReqParams,
) {
  const req: SqlExecutionRequest = createDatabaseDetailsSpanStatsReq(params);
  const sqlResp = await executeInternalSql<DatabaseSpanStatsRow>(req);
  const res = formatSpanStatsExecutionResult(sqlResp);
  return formatApiResult<DatabaseDetailsSpanStatsResponse>(
    res,
    res.error,
    "retrieving database span stats",
    false,
  );
}

export async function getDatabaseDetails(
  params: DatabaseDetailsReqParams,
  timeout?: moment.Duration,
): Promise<SqlApiResponse<DatabaseDetailsResponse>> {
  return withTimeout(fetchDatabaseDetails(params), timeout);
}

async function fetchDatabaseDetails(
  params: DatabaseDetailsReqParams,
): Promise<SqlApiResponse<DatabaseDetailsResponse>> {
  const detailsResponse: DatabaseDetailsResponse = newDatabaseDetailsResponse();
  const req: SqlExecutionRequest = createDatabaseDetailsReq(params);
  const resp = await executeInternalSql<DatabaseDetailsRow>(req);
  const errs: Error[] = [];
  resp.execution.txn_results.forEach(txnResult => {
    if (txnResult.error) {
      errs.push(txnResult.error);
    }
    const query: DatabaseDetailsQuery<DatabaseDetailsRow> =
      databaseDetailQueries[txnResult.statement - 1];
    query.addToDatabaseDetail(txnResult, detailsResponse);
  });
  if (resp.error) {
    if (isMaxSizeError(resp.error.message)) {
      return fetchSeparatelyDatabaseDetails(params);
    }
    detailsResponse.error = resp.error;
  }

  detailsResponse.error = combineQueryErrors(errs, detailsResponse.error);
  return formatApiResult<DatabaseDetailsResponse>(
    detailsResponse,
    detailsResponse.error,
    `retrieving database details information for database '${params.database}'`,
    false,
  );
}

async function fetchSeparatelyDatabaseDetails(
  params: DatabaseDetailsReqParams,
): Promise<SqlApiResponse<DatabaseDetailsResponse>> {
  const detailsResponse: DatabaseDetailsResponse = newDatabaseDetailsResponse();
  const errs: Error[] = [];
  for (const databaseDetailQuery of databaseDetailQueries) {
    const req = createSqlExecutionRequest(params.database, [
      databaseDetailQuery.createStmt(
        params.database,
        params.csIndexUnusedDuration,
      ),
    ]);
    const resp = await executeInternalSql<DatabaseDetailsRow>(req);
    if (sqlResultsAreEmpty(resp)) {
      continue;
    }
    const txnResult = resp.execution.txn_results[0];
    if (txnResult.error) {
      errs.push(txnResult.error);
    }
    databaseDetailQuery.addToDatabaseDetail(txnResult, detailsResponse);

    if (resp.error) {
      const handleFailure = await databaseDetailQuery.handleMaxSizeError(
        params.database,
        txnResult,
        detailsResponse,
      );
      if (!handleFailure) {
        detailsResponse.error = resp.error;
      }
    }
  }

  detailsResponse.error = combineQueryErrors(errs, detailsResponse.error);
  return formatApiResult<DatabaseDetailsResponse>(
    detailsResponse,
    detailsResponse.error,
    `retrieving database details information for database '${params.database}'`,
    false,
  );
}
