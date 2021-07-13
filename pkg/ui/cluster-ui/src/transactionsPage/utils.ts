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
  SelectOptions,
  getTimeValueInSeconds,
  calculateActiveFilters,
} from "../queryFilter";
import { AggregateStatistics } from "../statementsTable";
import Long from "long";
import {
  addExecStats,
  aggregateNumericStats,
  CollectedStatementStatistics,
  combineStatementStats,
  FixLong,
  ICollectedTransactionStatistics,
  longToInt,
  StatementStatistics,
  Transaction,
} from "../util";
import _ from "lodash";

type Statement = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type ICollectedStatementStatistics = protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type IExtendedStatementStatisticsKey = protos.cockroach.server.serverpb.StatementsResponse.IExtendedStatementStatisticsKey;

export interface StatementsCount {
  query: string;
  count: number;
}

export interface SummaryData {
  key: IExtendedStatementStatisticsKey;
  stats: StatementStatistics[];
}

export const getTrxAppFilterOptions = (
  transactions: Transaction[],
  prefix: string,
): SelectOptions[] => {
  const defaultAppFilters = ["All", prefix];
  const uniqueAppNames = new Set(
    transactions
      .filter(t => !t.stats_data.app.startsWith(prefix))
      .map(t => t.stats_data.app),
  );

  return defaultAppFilters
    .concat(Array.from(uniqueAppNames))
    .map(filterValue => ({
      label: filterValue,
      value: filterValue,
    }));
};

export const getStatementsByFingerprintId = (
  statementFingerprintIds: Long[],
  statements: Statement[],
): Statement[] => {
  return statements.filter(s =>
    statementFingerprintIds.some(id => id.eq(s.id)),
  );
};

export const aggregateStatements = (
  statements: ICollectedStatementStatistics[],
): AggregateStatistics[] =>
  statements.map((s: Statement) => ({
    label: s.key.key_data.query,
    implicitTxn: s.key.key_data.implicit_txn,
    database: s.key.key_data.database,
    fullScan: s.key.key_data.full_scan,
    stats: s.stats,
  }));

export const searchTransactionsData = (
  search: string,
  transactions: Transaction[],
): Transaction[] => {
  return transactions.filter((t: Transaction) =>
    search
      .split(" ")
      .every(val => t.fingerprint.toLowerCase().includes(val.toLowerCase())),
  );
};

export const filterTransactions = (
  data: Transaction[],
  filters: Filters,
  internalAppNamePrefix: string,
  statements: Statement[],
  nodeRegions: { [key: string]: string },
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
  // Current filters: app, service latency, nodes and regions.
  const filteredTransactions = data
    .filter(
      (t: Transaction) =>
        filters.app === "All" ||
        t.stats_data.app === filters.app ||
        (filters.app === internalAppNamePrefix &&
          t.stats_data.app.includes(filters.app)),
    )
    .filter(
      (t: Transaction) =>
        t.stats_data.stats.service_lat.mean >= timeValue ||
        timeValue === "empty",
    )
    .filter((t: Transaction) => {
      // The transaction must contain at least one value of the nodes
      // and regions list (if the list is not empty).
      if (regions.length == 0 && nodes.length == 0) return true;
      let foundRegion: boolean = regions.length == 0;
      let foundNode: boolean = nodes.length == 0;

      getStatementsByFingerprintId(
        t.stats_data.statement_fingerprint_ids,
        statements,
      ).some(stmt => {
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

// Returns a string with the fingerprint of each statement executed
// in order and with the repetitions count (when is more than one)
// joined by a new line `\n`.
// E.g. `SELECT * FROM test\nINSERT INTO test VALUES (_) (x3)\nSELECT * FROM test`
export const collectStatementsTextWithReps = (
  statements: CollectedStatementStatistics[],
): string => {
  const statementsInfo: StatementsCount[] = [];
  let index = 0;

  statements.forEach(s => {
    if (index > 0 && statementsInfo[index - 1].query === s.key.key_data.query) {
      statementsInfo[index - 1].count++;
    } else {
      statementsInfo.push({ query: s.key.key_data.query, count: 1 });
      index++;
    }
  });

  return statementsInfo
    .map(s => {
      if (s.count > 1) return s.query + " (x" + s.count + ")";
      return s.query;
    })
    .join("\n");
};

// Returns a complete list (including repetition) of all statements
// from a transaction in their order of execution.
export const getTransactionStatementsByIdInOrder = (
  statementsIds: Long[],
  statements: CollectedStatementStatistics[],
): CollectedStatementStatistics[] => {
  const allStatements: CollectedStatementStatistics[] = [];
  statementsIds.forEach(id => {
    allStatements.push(statements.filter(s => id.eq(s.id))[0]);
  });
  return allStatements;
};

// addTransactionStats adds together two stat objects into one using their counts to compute a new
// average for the numeric statistics. It's modeled after the similar `addStatementStats` function
function addTransactionStats(
  a: ICollectedTransactionStatistics,
  b: ICollectedTransactionStatistics,
): Required<ICollectedTransactionStatistics> {
  const countA = FixLong(a.stats.count).toInt();
  const countB = FixLong(b.stats.count).toInt();
  return {
    app: a.app,
    statement_fingerprint_ids: a.statement_fingerprint_ids.concat(
      b.statement_fingerprint_ids,
    ),
    stats: {
      bytes_read: aggregateNumericStats(
        a.stats.bytes_read,
        b.stats.bytes_read,
        countA,
        countB,
      ),
      commit_lat: aggregateNumericStats(
        a.stats.commit_lat,
        b.stats.commit_lat,
        countA,
        countB,
      ),
      count: a.stats.count.add(b.stats.count),
      max_retries: a.stats.max_retries.greaterThan(b.stats.max_retries)
        ? a.stats.max_retries
        : b.stats.max_retries,
      num_rows: aggregateNumericStats(
        a.stats.num_rows,
        b.stats.num_rows,
        countA,
        countB,
      ),
      retry_lat: aggregateNumericStats(
        a.stats.retry_lat,
        b.stats.retry_lat,
        countA,
        countB,
      ),
      rows_read: aggregateNumericStats(
        a.stats.rows_read,
        b.stats.rows_read,
        countA,
        countB,
      ),
      service_lat: aggregateNumericStats(
        a.stats.service_lat,
        b.stats.service_lat,
        countA,
        countB,
      ),
      exec_stats: addExecStats(a.stats.exec_stats, b.stats.exec_stats),
    },
  };
}

export function combineTransactionStatsData(
  statsData: ICollectedTransactionStatistics[],
): ICollectedTransactionStatistics {
  return _.reduce(statsData, addTransactionStats);
}

function collectedStatementKey(stmt: CollectedStatementStatistics): string {
  return (
    stmt.key.key_data.query +
    stmt.key.key_data.implicit_txn +
    stmt.key.key_data.database
  );
}

// Returns a list of CollectedStatementStatistics,
// combining the stats for statements with the same fingerprint
// to be used on Transaction aggregation.
export function combineStatements(
  statements: CollectedStatementStatistics[],
): CollectedStatementStatistics[] {
  const statsByStatementKey: {
    [statement: string]: SummaryData;
  } = {};
  statements.forEach((stmt: CollectedStatementStatistics) => {
    const key = collectedStatementKey(stmt);
    if (!(key in statsByStatementKey)) {
      statsByStatementKey[key] = {
        key: stmt.key,
        stats: [],
      };
    }
    statsByStatementKey[key].stats.push(stmt.stats);
  });

  return Object.keys(statsByStatementKey).map(key => {
    const stmt = statsByStatementKey[key];
    return {
      key: stmt.key,
      stats: combineStatementStats(stmt.stats),
    };
  });
}
