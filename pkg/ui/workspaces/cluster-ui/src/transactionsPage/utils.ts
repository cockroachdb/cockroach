// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import {
  Filters,
  getTimeValueInSeconds,
  calculateActiveFilters,
} from "../queryFilter";
import { AggregateStatistics } from "../statementsTable";
import Long from "long";
import {
  longToInt,
  addStatementStats,
  flattenStatementStats,
  computeOrUseStmtSummary,
  transactionScopedStatementKey,
  addExecStats,
  aggregateNumericStats,
  FixLong,
  unset,
} from "../util";
import { cloneDeep } from "lodash";

type Statement =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type Transaction =
  protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;

export const getTrxAppFilterOptions = (
  transactions: Transaction[],
  prefix: string,
): string[] => {
  const uniqueAppNames = new Set(
    transactions.map(t =>
      t.stats_data.app
        ? t.stats_data.app.startsWith(prefix)
          ? prefix
          : t.stats_data.app
        : unset,
    ),
  );

  return Array.from(uniqueAppNames).sort();
};
export const collectStatementsText = (statements: Statement[]): string =>
  statements.map(s => s.key.key_data.query).join("\n");

export const getStatementsByFingerprintId = (
  statementFingerprintIds: Long[],
  statements: Statement[],
): Statement[] => {
  return (
    statements?.filter(s => statementFingerprintIds.some(id => id.eq(s.id))) ||
    []
  );
};

export const statementFingerprintIdsToText = (
  statementFingerprintIds: Long[],
  statements: Statement[],
): string => {
  return statementFingerprintIds
    .map(s => statements.find(stmt => stmt.id.eq(s))?.key.key_data.query)
    .join("\n");
};

// Combine all statement summaries into a string.
export const statementFingerprintIdsToSummarizedText = (
  statementFingerprintIds: Long[],
  statements: Statement[],
): string => {
  return statementFingerprintIds
    .map(s => {
      const query = statements.find(stmt => stmt.id.eq(s))?.key.key_data.query;
      const querySummary = statements.find(stmt => stmt.id.eq(s))?.key.key_data
        .query_summary;
      return computeOrUseStmtSummary(query, querySummary);
    })
    .join("\n");
};

// Aggregate transaction statements from different nodes.
export const aggregateStatements = (
  statements: Statement[],
): AggregateStatistics[] => {
  const statsKey: { [key: string]: AggregateStatistics } = {};

  flattenStatementStats(statements).forEach(s => {
    const key = transactionScopedStatementKey(s);
    if (!(key in statsKey)) {
      statsKey[key] = {
        aggregatedFingerprintID: s.statement_fingerprint_id?.toString(),
        aggregatedFingerprintHexID: s.statement_fingerprint_id?.toString(16),
        label: s.statement,
        summary: s.statement_summary,
        aggregatedTs: s.aggregated_ts,
        implicitTxn: s.implicit_txn,
        database: s.database,
        applicationName: s.app,
        fullScan: s.full_scan,
        stats: s.stats,
      };
    } else {
      statsKey[key].stats = addStatementStats(statsKey[key].stats, s.stats);
    }
  });

  return Object.values(statsKey);
};
export const searchTransactionsData = (
  search: string,
  transactions: Transaction[],
  statements: Statement[],
): Transaction[] => {
  let searchTerms = search?.split(" ");
  // If search term is wrapped by quotes, do the exact search term.
  if (search?.startsWith('"') && search?.endsWith('"')) {
    searchTerms = [search.substring(1, search.length - 1)];
  }

  if (!search) {
    return transactions;
  }

  return transactions.filter((t: Transaction) =>
    searchTerms.every(val => {
      if (
        collectStatementsText(
          getStatementsByFingerprintId(
            t.stats_data.statement_fingerprint_ids,
            statements,
          ),
        )
          .toLowerCase()
          .includes(val.toLowerCase())
      ) {
        return true;
      }

      return t.stats_data.transaction_fingerprint_id
        ?.toString(16)
        ?.includes(val);
    }),
  );
};

// TODO(todd): Remove unused nodeRegions parameter.
export const filterTransactions = (
  data: Transaction[],
  filters: Filters,
  internalAppNamePrefix: string,
  statements: Statement[],
  nodeRegions: { [key: string]: string },
  isTenant: boolean,
): { transactions: Transaction[]; activeFilters: number } => {
  if (!filters)
    return {
      transactions: data,
      activeFilters: 0,
    };
  const timeValue = getTimeValueInSeconds(filters);
  const regions = filters.regions?.length > 0 ? filters.regions.split(",") : [];
  const nodes = filters.nodes?.length > 0 ? filters.nodes.split(",") : [];

  const activeFilters = calculateActiveFilters(filters);

  // Return transactions filtered by the values selected on the filter. A
  // transaction must match all selected filters.
  // We don't want to show statements that are internal or with unset App names by default.
  // Current filters: app, service latency, nodes and regions.
  const filteredTransactions = data
    .filter((t: Transaction) => {
      const app = t.stats_data.app;
      const isInternal = app.startsWith(internalAppNamePrefix);

      if (filters.app && filters.app != "All") {
        const apps = filters.app.split(",");
        let showInternal = false;
        if (apps.includes(internalAppNamePrefix)) {
          showInternal = true;
        }
        if (apps.includes(unset)) {
          apps.push("");
        }

        return (
          (showInternal && isInternal) ||
          app === filters.app ||
          apps.includes(app)
        );
      } else {
        return true;
      }
    })
    .filter(
      (t: Transaction) =>
        t.stats_data.stats.service_lat.mean >= timeValue ||
        timeValue === "empty",
    )
    .filter((t: Transaction) => {
      // The transaction must contain at least one value of the regions list
      // (if the list is not empty).
      if (regions.length == 0) return true;

      return getStatementsByFingerprintId(
        t.stats_data.statement_fingerprint_ids,
        statements,
      ).some(stmt =>
        stmt.stats.regions?.some(region => regions.includes(region)),
      );
    })
    .filter((t: Transaction) => {
      // The transaction must contain at least one value of the nodes list
      // (if the list is not empty).
      if (nodes.length == 0) return true;

      // If the cluster is a tenant cluster we don't care about nodes.
      if (isTenant) return true;

      return getStatementsByFingerprintId(
        t.stats_data.statement_fingerprint_ids,
        statements,
      ).some(stmt =>
        stmt.stats.nodes?.some(node => nodes.includes("n" + node)),
      );
    });

  return {
    transactions: filteredTransactions,
    activeFilters,
  };
};

/**
 * For each transaction, generate the list of regions all
 * its statements were executed on.
 * E.g. of one element of the list: `gcp-us-east1`
 * @param transaction: list of transactions.
 * @param statements: list of all statements collected.
 */
export const generateRegion = (
  transaction: Transaction,
  statements: Statement[],
): string[] => {
  const regions: Set<string> = new Set<string>();

  getStatementsByFingerprintId(
    transaction.stats_data.statement_fingerprint_ids,
    statements,
  ).forEach(stmt => {
    stmt.stats.regions?.forEach(region => regions.add(region));
  });

  return Array.from(regions).sort();
};

/**
 * For each transaction, generate the list of regions and nodes all
 * its statements were executed on.
 * E.g. of one element of the list: `gcp-us-east1 (n1, n2, n3)`
 * @param transaction: list of transactions.
 * @param statements: list of all statements collected.
 * @param nodeRegions: object with keys being the node id and the value
 * which region it belongs to.
 */
export const generateRegionNode = (
  transaction: Transaction,
  statements: Statement[],
  nodeRegions: { [p: string]: string },
): string[] => {
  const regions: { [region: string]: Set<number> } = {};
  // Get the list of statements that were executed on the transaction. Combine all
  // nodes and regions of all the statements to a single list of `region: nodes`
  // for the transaction.
  // E.g. {"gcp-us-east1" : [1,3,4]}
  getStatementsByFingerprintId(
    transaction.stats_data.statement_fingerprint_ids,
    statements,
  ).forEach(stmt => {
    stmt.stats.nodes &&
      stmt.stats.nodes.forEach(n => {
        const node = n.toString();
        if (Object.keys(regions).includes(nodeRegions[node])) {
          regions[nodeRegions[node]].add(longToInt(n));
        } else {
          regions[nodeRegions[node]] = new Set([longToInt(n)]);
        }
      });
  });

  // Create a list nodes/regions where a transaction was executed on, with
  // format: region (node1,node2)
  const regionNodes: string[] = [];
  Object.keys(regions).forEach(region => {
    regionNodes.push(
      region +
        " (" +
        Array.from(regions[region])
          .sort()
          .map(n => "n" + n)
          .toString() +
        ")",
    );
  });
  return regionNodes;
};

/**
 * getStatementsForTransaction returns the list of stmts with transaction ids
 * matching and app name matching that of the provided txn.
 * @param txnFingerprintIDString Txn fingerprint id for which we will find matching stmts.
 * @param apps List of app names to filter stmts by.
 * @param stmts List of available stmts.
 */
export const getStatementsForTransaction = (
  txnFingerprintIDString: string,
  apps: string[],
  stmts: Statement[] | null,
): Statement[] => {
  if (!txnFingerprintIDString || !stmts?.length) return [];

  return stmts.filter(
    s =>
      (s.key.key_data?.transaction_fingerprint_id?.toString() ===
        txnFingerprintIDString ||
        s.txn_fingerprint_ids
          ?.map(t => t.toString())
          .includes(txnFingerprintIDString)) &&
      (!apps?.length ||
        apps.includes(s.key.key_data.app ? s.key.key_data.app : unset)),
  );
};

/**
 * getTxnQueryString returns the transaction query built from the provided stmt list
 * @param txn Transaction to build query for.
 * @param stmts List of stmts from which to get query strings.
 */
export const getTxnQueryString = (
  txn: Transaction | null,
  stmts: Statement[],
): string => {
  if (!txn || !stmts?.length) return "";

  const statementFingerprintIds = txn?.stats_data?.statement_fingerprint_ids;

  return (
    (statementFingerprintIds &&
      statementFingerprintIdsToText(statementFingerprintIds, stmts)) ??
    ""
  );
};

// addTransactionStats adds together two stat objects into one using their counts to compute a new
// average for the numeric statistics. It's modeled after the similar `addStatementStats` function
function addTransactionStats(
  a: TransactionStats,
  b: TransactionStats,
): Required<TransactionStats> {
  const countA = FixLong(a.count).toInt();
  const countB = FixLong(b.count).toInt();
  return {
    count: a.count.add(b.count),
    max_retries: a.max_retries.greaterThan(b.max_retries)
      ? a.max_retries
      : b.max_retries,
    num_rows: aggregateNumericStats(a.num_rows, b.num_rows, countA, countB),
    service_lat: aggregateNumericStats(
      a.service_lat,
      b.service_lat,
      countA,
      countB,
    ),
    retry_lat: aggregateNumericStats(a.retry_lat, b.retry_lat, countA, countB),
    commit_lat: aggregateNumericStats(
      a.commit_lat,
      b.commit_lat,
      countA,
      countB,
    ),
    idle_lat: aggregateNumericStats(a.idle_lat, b.idle_lat, countA, countB),
    rows_read: aggregateNumericStats(a.rows_read, b.rows_read, countA, countB),
    rows_written: aggregateNumericStats(
      a.rows_written,
      b.rows_written,
      countA,
      countB,
    ),
    bytes_read: aggregateNumericStats(
      a.bytes_read,
      b.bytes_read,
      countA,
      countB,
    ),
    exec_stats: addExecStats(a.exec_stats, b.exec_stats),
  };
}

// mergeTransactionStats takes a list of transactions (assuming they're all for the same fingerprint)
// and returns a copy of the first element with its `stats_data.stats` object replaced with a
// merged stats object that aggregates statistics from every copy of the fingerprint in the list
// provided. This function SHOULD NOT mutate any objects in the provided txns array.
const mergeTransactionStats = function (txns: Transaction[]): Transaction {
  if (txns.length === 0) {
    return null;
  }

  if (txns.length === 1) {
    return txns[0];
  }

  const txn = cloneDeep(txns[0]);

  txn.stats_data.stats = txns
    .map(t => t.stats_data.stats)
    .reduce(addTransactionStats);

  return txn;
};

/*
 * getTxnFromSqlStatsTxns aggregates txn stats from the provided list which match the
 * specified txn fingerprint ID and contains one of the provided app names.
 * Note that the returned txn will have other properties matching the first txn
 * in the list matching the provided criteria, but its stats_data.stats will be
 * aggregated from all matching txns.
 *
 * @param txns List of txns to search through.
 * @param txnFingerprintID Txn fingerprint ID to search for.
 * @param apps list of app names to filter by.
 */
export const getTxnFromSqlStatsTxns = (
  txns: Transaction[] | null,
  txnFingerprintID: string | null,
  apps: string[] | null,
): Transaction | null => {
  if (!txns?.length || !txnFingerprintID) {
    return null;
  }

  return mergeTransactionStats(
    txns.filter(
      txn =>
        txn.stats_data.transaction_fingerprint_id.toString() ===
          txnFingerprintID &&
        (!apps?.length || apps.includes(txn.stats_data.app || unset)),
    ),
  );
};
