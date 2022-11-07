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
import _ from "lodash";
import {
  addExecStats,
  aggregateNumericStats,
  FixLong,
  longToInt,
  TimestampToNumber,
  addStatementStats,
  flattenStatementStats,
  DurationToNumber,
  computeOrUseStmtSummary,
  transactionScopedStatementKey,
  unset,
  HexStringToInt64String,
} from "../util";
import { ExecutionInsightCountEvent } from "../insights";
import { TransactionInfo } from "../transactionsTable";

type Statement =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type TransactionStats = protos.cockroach.sql.ITransactionStatistics;
type Transaction =
  protos.cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

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
        aggregatedFingerprintHexID: s.statement_fingerprint_id.toString(16),
        label: s.statement,
        summary: s.statement_summary,
        aggregatedTs: s.aggregated_ts,
        aggregationInterval: s.aggregation_interval,
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
  const regions = filters.regions.length > 0 ? filters.regions.split(",") : [];
  const nodes = filters.nodes.length > 0 ? filters.nodes.split(",") : [];

  const activeFilters = calculateActiveFilters(filters);

  // Return transactions filtered by the values selected on the filter. A
  // transaction must match all selected filters.
  // We don't want to show statements that are internal or with unset App names by default.
  // Current filters: app, service latency, nodes and regions.
  const filteredTransactions = data
    .filter((t: Transaction) => {
      const isInternal = (t: Transaction) =>
        t.stats_data.app.startsWith(internalAppNamePrefix);

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
          (showInternal && isInternal(t)) ||
          t.stats_data.app === filters.app ||
          apps.includes(t.stats_data.app)
        );
      } else {
        // We don't want to show internal transactions by default.
        return !isInternal(t);
      }
    })
    .filter(
      (t: Transaction) =>
        t.stats_data.stats.service_lat.mean >= timeValue ||
        timeValue === "empty",
    )
    .filter((t: Transaction) => {
      // The transaction must contain at least one value of the nodes
      // and regions list (if the list is not empty).
      if (regions.length == 0 && nodes.length == 0) return true;
      // If the cluster is a tenant cluster we don't care
      // about nodes.
      let foundRegion: boolean = regions.length == 0;
      let foundNode: boolean = isTenant || nodes.length == 0;

      getStatementsByFingerprintId(
        t.stats_data.statement_fingerprint_ids,
        statements,
      ).some(stmt => {
        stmt.stats.nodes &&
          stmt.stats.nodes.some(node => {
            if (foundRegion || regions.includes(nodeRegions[node.toString()])) {
              foundRegion = true;
            }
            if (foundNode || nodes.includes("n" + node)) {
              foundNode = true;
            }
            if (foundNode && foundRegion) return true;
          });
      });

      return foundRegion && foundNode;
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
 * @param nodeRegions: object with keys being the node id and the value
 * which region it belongs to.
 */
export const generateRegion = (
  transaction: Transaction,
  statements: Statement[],
  nodeRegions: { [p: string]: string },
): string[] => {
  const regions: Set<string> = new Set<string>();
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
        regions.add(nodeRegions[n.toString()]);
      });
  });

  return Array.from(regions)
    .filter(r => r) // Remove undefined / unknown regions.
    .sort();
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

type TransactionWithFingerprint = Transaction & { fingerprint: string };

// withFingerprint adds the concatenated statement fingerprints to the Transaction object since it
// only comes with statement_fingerprint_ids
const withFingerprint = function (
  t: Transaction,
  stmts: Statement[],
): TransactionWithFingerprint {
  return {
    ...t,
    fingerprint: statementFingerprintIdsToText(
      t.stats_data.statement_fingerprint_ids,
      stmts,
    ),
  };
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

function combineTransactionStats(
  txnStats: TransactionStats[],
): TransactionStats {
  return _.reduce(txnStats, addTransactionStats);
}

// mergeTransactionStats takes a list of transactions (assuming they're all for the same fingerprint
// and returns a copy of the first element with its `stats_data.stats` object replaced with a
// merged stats object that aggregates statistics from every copy of the fingerprint in the list
// provided
const mergeTransactionStats = function (txns: Transaction[]): Transaction {
  if (txns.length === 0) {
    return null;
  }
  const txn = { ...txns[0] };
  txn.stats_data.stats = combineTransactionStats(
    txns.map(t => t.stats_data.stats),
  );
  return txn;
};

// aggregateAcrossNodeIDs takes a list of transactions and a list of statements that those
// transactions reference and returns a list of transactions that have been grouped by their
// fingerprints and had their statistics aggregated across copies of the transaction. This is used
// to deduplicate identical copies of the transaction that are run on different nodes. CRDB returns
// different objects to represent those transactions.
//
// The function uses the fingerprint and the `app` that ran the transaction as the key to group the
// transactions when deduping.
//
export const aggregateAcrossNodeIDs = function (
  t: Transaction[],
  stmts: Statement[],
): Transaction[] {
  return _.chain(t)
    .map(t => withFingerprint(t, stmts))
    .groupBy(
      t =>
        t.fingerprint +
        t.stats_data.app +
        TimestampToNumber(t.stats_data.aggregated_ts) +
        DurationToNumber(t.stats_data.aggregation_interval),
    )
    .mapValues(mergeTransactionStats)
    .values()
    .value();
};

export const addInsightCounts = function (
  txns: TransactionInfo[],
  insightCounts: ExecutionInsightCountEvent[],
): TransactionInfo[] {
  if (!insightCounts) {
    return txns;
  }
  const res: TransactionInfo[] = [];
  txns.forEach(txn => {
    const count = insightCounts?.find(
      insightCount =>
        HexStringToInt64String(insightCount.fingerprintID) ===
        txn.stats_data.transaction_fingerprint_id.toString(),
    )?.insightCount;
    if (count) {
      txn.insightCount = count;
    } else {
      txn.insightCount = 0;
    }
    res.push(txn);
  });
  return res;
};
