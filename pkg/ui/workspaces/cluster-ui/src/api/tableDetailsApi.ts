// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import moment from "moment-timezone";

import { IndexUsageStatistic, recommendDropUnusedIndex } from "../insights";
import { getLogger, indexUnusedDuration, maybeError } from "../util";

import { Format, Identifier, Join, SQL } from "./safesql";
import {
  combineQueryErrors,
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
import { fromHexString, withTimeout } from "./util";

const { ZoneConfig } = cockroach.config.zonepb;
const { ZoneConfigurationLevel } = cockroach.server.serverpb;
type ZoneConfigType = cockroach.config.zonepb.ZoneConfig;
type ZoneConfigLevelType = cockroach.server.serverpb.ZoneConfigurationLevel;

export function newTableDetailsResponse(): TableDetailsResponse {
  return {
    idResp: { table_id: "" },
    createStmtResp: { create_statement: "" },
    grantsResp: { grants: [] },
    schemaDetails: { columns: [], indexes: [] },
    zoneConfigResp: {
      configure_zone_statement: "",
      zone_config: new ZoneConfig({
        inherited_constraints: true,
        inherited_lease_preferences: true,
      }),
      zone_config_level: ZoneConfigurationLevel.CLUSTER,
    },
    heuristicsDetails: { stats_last_created_at: null },
    stats: {
      spanStats: {
        approximate_disk_bytes: 0,
        live_bytes: 0,
        total_bytes: 0,
        range_count: 0,
        live_percentage: 0,
      },
      replicaData: {
        storeIDs: [],
        replicaCount: 0,
      },
      indexStats: {
        has_index_recommendations: false,
      },
    },
  };
}

export type TableDetailsResponse = {
  idResp: SqlApiQueryResponse<TableIdRow>;
  createStmtResp: SqlApiQueryResponse<TableCreateStatementRow>;
  grantsResp: SqlApiQueryResponse<TableGrantsResponse>;
  schemaDetails: SqlApiQueryResponse<TableSchemaDetailsRow>;
  zoneConfigResp: SqlApiQueryResponse<TableZoneConfigResponse>;
  heuristicsDetails: SqlApiQueryResponse<TableHeuristicDetailsRow>;
  stats: TableDetailsStats;
  error?: SqlExecutionErrorMessage;
};

// Table ID.
type TableIdRow = {
  table_id: string;
};

const getTableId: TableDetailsQuery<TableIdRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: `SELECT $1::regclass::oid as table_id`,
      arguments: [escFullTableName.sqlString()],
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableIdRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      resp.idResp.table_id = txnResult.rows[0].table_id;
    } else {
      txnResult.error = new Error("fetchTableId: unexpected empty results");
    }
    if (txnResult.error) {
      resp.idResp.error = txnResult.error;
    }
  },
};

// Table create statement.
export type TableCreateStatementRow = { create_statement: string };

const getTableCreateStatement: TableDetailsQuery<TableCreateStatementRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(`SELECT create_statement FROM [SHOW CREATE %1]`, [
        escFullTableName,
      ]),
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableCreateStatementRow>,
    resp: TableDetailsResponse,
  ) => {
    if (txnResult.error) {
      resp.createStmtResp.error = txnResult.error;
    }
    if (!txnResultIsEmpty(txnResult)) {
      resp.createStmtResp.create_statement = txnResult.rows[0].create_statement;
    } else if (!txnResult.error) {
      txnResult.error = new Error(
        "getTableCreateStatement: unexpected empty results",
      );
    }
  },
};

// Table grants.
export type TableGrantsResponse = {
  grants: TableGrantsRow[];
};

type TableGrantsRow = {
  user: string;
  privileges: string[];
};

const getTableGrants: TableDetailsQuery<TableGrantsRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(
        `SELECT grantee as user, array_agg(privilege_type) as privileges 
         FROM [SHOW GRANTS ON TABLE %1] group by grantee`,
        [escFullTableName],
      ),
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableGrantsRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      resp.grantsResp.grants = txnResult.rows;
    }
    if (txnResult.error) {
      resp.grantsResp.error = txnResult.error;
    }
  },
};

// Table schema details.
export type TableSchemaDetailsRow = {
  columns: string[];
  indexes: string[];
};

const getTableSchemaDetails: TableDetailsQuery<TableSchemaDetailsRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(
        `WITH 
     columns AS (SELECT array_agg(distinct(column_name)) as unique_columns FROM [SHOW COLUMNS FROM %1]),
     indexes AS (SELECT array_agg(distinct(index_name)) as unique_indexes FROM [SHOW INDEX FROM %1])
        SELECT 
            unique_columns as columns, 
            unique_indexes as indexes 
        FROM columns CROSS JOIN indexes`,
        [escFullTableName],
      ),
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableSchemaDetailsRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      resp.schemaDetails.columns = txnResult.rows[0].columns;
      resp.schemaDetails.indexes = txnResult.rows[0].indexes;
    }
    if (txnResult.error) {
      resp.schemaDetails.error = txnResult.error;
    }
  },
};

// Table zone config.
type TableZoneConfigResponse = {
  configure_zone_statement: string;
  zone_config: ZoneConfigType;
  zone_config_level: ZoneConfigLevelType;
};

type TableZoneConfigStatementRow = { raw_config_sql: string };

const getTableZoneConfigStmt: TableDetailsQuery<TableZoneConfigStatementRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(
        `SELECT raw_config_sql FROM [SHOW ZONE CONFIGURATION FOR TABLE %1]`,
        [escFullTableName],
      ),
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableZoneConfigStatementRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      resp.zoneConfigResp.configure_zone_statement =
        txnResult.rows[0].raw_config_sql;
    }
    if (txnResult.error) {
      resp.zoneConfigResp.error = txnResult.error;
    }
  },
};

type TableZoneConfigRow = {
  database_zone_config_hex_string: string;
  table_zone_config_hex_string: string;
};

const getTableZoneConfig: TableDetailsQuery<TableZoneConfigRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: `SELECT 
      encode(crdb_internal.get_zone_config((SELECT crdb_internal.get_database_id($1))), 'hex') as database_zone_config_hex_string,
      encode(crdb_internal.get_zone_config((SELECT $2::regclass::int)), 'hex') as table_zone_config_hex_string`,
      arguments: [dbName, escFullTableName.sqlString()],
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableZoneConfigRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      let hexString = "";
      let configLevel = ZoneConfigurationLevel.CLUSTER;
      const row = txnResult.rows[0];
      // Check that database_zone_config_bytes is not null
      // and not empty.
      if (
        row.database_zone_config_hex_string &&
        row.database_zone_config_hex_string.length !== 0
      ) {
        hexString = row.database_zone_config_hex_string;
        configLevel = ZoneConfigurationLevel.DATABASE;
      }
      // Fall back to table_zone_config_bytes if we don't have database_zone_config_bytes.
      if (
        hexString === "" &&
        row.table_zone_config_hex_string &&
        row.table_zone_config_hex_string.length !== 0
      ) {
        hexString = row.table_zone_config_hex_string;
        configLevel = ZoneConfigurationLevel.TABLE;
      }

      // No zone configuration, return.
      if (hexString === "") {
        return;
      }

      // Try to decode the zone config bytes response.
      try {
        // Parse the bytes from the hex string.
        const zoneConfigBytes = fromHexString(hexString);
        // Decode the bytes using ZoneConfig protobuf.
        resp.zoneConfigResp.zone_config = ZoneConfig.decode(
          new Uint8Array(zoneConfigBytes),
        );
        resp.zoneConfigResp.zone_config_level = configLevel;
      } catch (e) {
        getLogger().error(
          `Table Details API - encountered an error decoding zone config string: ${hexString}`,
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
};

// Table heuristics details.
export type TableHeuristicDetailsRow = {
  stats_last_created_at: moment.Moment;
};

const getTableHeuristicsDetails: TableDetailsQuery<TableHeuristicDetailsRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(
        `SELECT max(created) AS stats_last_created_at FROM [SHOW STATISTICS FOR TABLE %1]`,
        [escFullTableName],
      ),
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableHeuristicDetailsRow>,
    resp: TableDetailsResponse,
  ) => {
    if (!txnResultIsEmpty(txnResult)) {
      resp.heuristicsDetails.stats_last_created_at =
        txnResult.rows[0].stats_last_created_at;
    }
    if (txnResult.error) {
      resp.heuristicsDetails.error = txnResult.error;
    }
  },
};

// Table details stats.
type TableDetailsStats = {
  spanStats: SqlApiQueryResponse<TableSpanStatsRow>;
  replicaData: SqlApiQueryResponse<TableReplicaData>;
  indexStats: SqlApiQueryResponse<TableIndexUsageStats>;
};

// Table span stats.
export type TableSpanStatsRow = {
  approximate_disk_bytes: number;
  live_bytes: number;
  total_bytes: number;
  range_count: number;
  live_percentage: number;
};

const getTableSpanStats: TableDetailsQuery<TableSpanStatsRow> = {
  createStmt: (dbName, tableName) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: `SELECT
              range_count,
              approximate_disk_bytes,
              live_bytes,
              total_bytes,
              live_percentage
            FROM crdb_internal.tenant_span_stats(
                (SELECT crdb_internal.get_database_id($1)), 
                (SELECT $2::regclass::int))`,
      arguments: [dbName, escFullTableName.sqlString()],
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableSpanStatsRow>,
    resp: TableDetailsResponse,
  ) => {
    if (txnResult && txnResult.error) {
      resp.stats.spanStats.error = txnResult.error;
    }
    if (txnResultIsEmpty(txnResult)) {
      return;
    }
    if (txnResult.rows.length === 1) {
      resp.stats.spanStats = txnResult.rows[0];
    } else {
      resp.stats.spanStats.error = new Error(
        `getTableSpanStats: unexpected number of rows (expected 1, got ${txnResult.rows.length})`,
      );
    }
  },
};

export type TableReplicaData = SqlApiQueryResponse<{
  storeIDs: number[];
  replicaCount: number;
}>;

// Table replicas.
type TableReplicasRow = {
  store_ids: number[];
  replica_count: number;
};

// This query is used to get the store ids for which replicas for a table
// are stored.
const getTableReplicaStoreIDs: TableDetailsQuery<TableReplicasRow> = {
  createStmt: (dbName, tableName) => {
    // (xinhaoz) Note that the table name provided here is in an escaped string of the
    // form <schema>.<table>, and is the result of formatting the retrieved schema and
    // table name using QualifiedIdentifier.
    //
    // Currently we create a QualifiedIdentifier from the table name and schema
    // immediately upon receiving the request, extracting the escaped string to use
    // throughout the pages - this It can be unclear what format the table name is in
    // at any given time and makes formatting less flexible. We should consider either storing
    // the schema and table names separately and only creating the QualifiedIdentifier as
    // needed, or otherwise indicate through better variable naming the table name format.
    const sqlFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    return {
      sql: Format(
        `
          SELECT count(unnested) AS replica_count, array_agg(DISTINCT unnested) AS store_ids
          FROM [SHOW RANGES FROM TABLE %1], unnest(replicas) AS unnested;
`,
        [sqlFullTableName],
      ),
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<TableReplicasRow>,
    resp: TableDetailsResponse,
  ) => {
    if (txnResult.error) {
      resp.stats.replicaData.error = txnResult.error;
      // We don't expect to have any rows for this query on error.
      return;
    }

    // TODO #118957 (xinhaoz) Store IDs and node IDs cannot be used interchangeably.
    if (!txnResultIsEmpty(txnResult)) {
      resp.stats.replicaData.storeIDs = txnResult?.rows[0]?.store_ids ?? [];
      resp.stats.replicaData.replicaCount =
        txnResult?.rows[0]?.replica_count ?? 0;
    }
  },
};

// Table index usage stats.
export type TableIndexUsageStats = {
  has_index_recommendations: boolean;
};

const getTableIndexUsageStats: TableDetailsQuery<IndexUsageStatistic> = {
  createStmt: (dbName, tableName, csIndexUnusedDuration) => {
    const escFullTableName = Join(
      [new Identifier(dbName), new SQL(tableName)],
      new SQL("."),
    );
    csIndexUnusedDuration = csIndexUnusedDuration ?? indexUnusedDuration;
    return {
      sql: Format(
        `WITH tableId AS (SELECT $1::regclass::int as table_id)
          SELECT * FROM (SELECT
                  ti.created_at,
                  us.last_read,
                  us.total_reads,
                  '${csIndexUnusedDuration}' as unused_threshold,
                  '${csIndexUnusedDuration}'::interval as interval_threshold,
                  now() - COALESCE(us.last_read AT TIME ZONE 'UTC', COALESCE(ti.created_at, '0001-01-01')) as unused_interval
                  FROM %1.crdb_internal.index_usage_statistics AS us
                  JOIN tableId ON us.table_id = tableId.table_id
                  JOIN %1.crdb_internal.table_indexes AS ti ON (
                      us.index_id = ti.index_id AND 
                      tableId.table_id = ti.descriptor_id
                  )
                 WHERE $2 != 'system' AND ti.is_unique IS false)
               WHERE unused_interval > interval_threshold
               ORDER BY total_reads DESC`,
        [new Identifier(dbName)],
      ),
      arguments: [escFullTableName.sqlString(), dbName],
    };
  },
  addToTableDetail: (
    txnResult: SqlTxnResult<IndexUsageStatistic>,
    resp: TableDetailsResponse,
  ) => {
    resp.stats.indexStats.has_index_recommendations = txnResult.rows?.some(
      row => recommendDropUnusedIndex(row),
    );
    if (txnResult.error) {
      resp.stats.indexStats.error = txnResult.error;
    }
  },
};

type TableDetailsQuery<RowType> = {
  createStmt: (
    dbName: string,
    tableName: string,
    csIndexUnusedDuration: string,
  ) => SqlStatement;
  addToTableDetail: (
    response: SqlTxnResult<RowType>,
    tableDetail: TableDetailsResponse,
  ) => void;
};

export type TableDetailsRow =
  | TableIdRow
  | TableGrantsRow
  | TableSchemaDetailsRow
  | TableCreateStatementRow
  | TableZoneConfigStatementRow
  | TableHeuristicDetailsRow
  | TableSpanStatsRow
  | IndexUsageStatistic
  | TableZoneConfigRow
  | TableReplicasRow;

const tableDetailQueries: TableDetailsQuery<TableDetailsRow>[] = [
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableId,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableGrants,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableSchemaDetails,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableCreateStatement,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableZoneConfigStmt,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableHeuristicsDetails,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableSpanStats,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableIndexUsageStats,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableZoneConfig,
  // eslint-disable-next-line @typescript-eslint/ban-ts-comment
  // @ts-ignore
  getTableReplicaStoreIDs,
];

export function createTableDetailsReq(
  dbName: string,
  tableName: string,
  csIndexUnusedDuration: string,
): SqlExecutionRequest {
  return {
    execute: true,
    statements: tableDetailQueries.map(query =>
      query.createStmt(dbName, tableName, csIndexUnusedDuration),
    ),
    max_result_size: LARGE_RESULT_SIZE,
    timeout: LONG_TIMEOUT,
    database: dbName,
    separate_txns: true,
  };
}

export type TableDetailsReqParams = {
  database: string;
  // Note: table name is expected in the following format: "schemaName"."tableName"
  table: string;
  csIndexUnusedDuration: string;
};

export async function getTableDetails(
  params: TableDetailsReqParams,
  timeout?: moment.Duration,
): Promise<SqlApiResponse<TableDetailsResponse>> {
  return withTimeout(
    fetchTableDetails(
      params.database,
      params.table,
      params.csIndexUnusedDuration,
    ),
    timeout,
  );
}

async function fetchTableDetails(
  databaseName: string,
  tableName: string,
  csIndexUnusedDuration: string,
): Promise<SqlApiResponse<TableDetailsResponse>> {
  const detailsResponse: TableDetailsResponse = newTableDetailsResponse();
  const req: SqlExecutionRequest = createTableDetailsReq(
    databaseName,
    tableName,
    csIndexUnusedDuration,
  );
  const resp = await executeInternalSql<TableDetailsRow>(req);
  const errs: Error[] = [];
  resp.execution.txn_results.forEach(txnResult => {
    if (txnResult.error) {
      errs.push(txnResult.error);
    }
    const query = tableDetailQueries[txnResult.statement - 1];
    query.addToTableDetail(txnResult, detailsResponse);
  });
  if (resp.error) {
    detailsResponse.error = resp.error;
  }

  detailsResponse.error = combineQueryErrors(errs, detailsResponse.error);
  return formatApiResult<TableDetailsResponse>(
    detailsResponse,
    detailsResponse.error,
    `retrieving table details information for table '${tableName}'`,
    false,
  );
}
