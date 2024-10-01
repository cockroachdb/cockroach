// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import * as protos from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";

import {
  Filters,
  getTimeValueInSeconds,
  calculateActiveFilters,
} from "../queryFilter";
import { AggregateStatistics } from "../statementsTable";
import {
  longToInt,
  addStatementStats,
  flattenStatementStats,
  computeOrUseStmtSummary,
  transactionScopedStatementKey,
  unset,
} from "../util";

type Statement =
  protos.cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
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

      if (filters.app && filters.app !== "All") {
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
        timeValue === "empty" ||
        t.stats_data.stats.service_lat.mean >= Number(timeValue),
    )
    .filter((t: Transaction) => {
      // The transaction must contain at least one value of the regions list
      // (if the list is not empty).
      if (regions.length === 0) return true;

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
      if (nodes.length === 0) return true;

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
