// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "@reduxjs/toolkit";

import { SqlStatsResponse } from "src/api/statementsApi";
import { Filters, getTimeValueInSeconds } from "src/queryFilter";
import { AggregateStatistics } from "src/statementsTable";
import {
  CollectedStatementStatistics,
  flattenStatementStats,
} from "src/util/appStats/appStats";
import { containAny } from "src/util/arrays";
import { INTERNAL_APP_NAME_PREFIX, unset } from "src/util/constants";
import { FixFingerprintHexValue } from "src/util/format";

// filterBySearchQuery returns true if a search query matches the statement.
export function filterBySearchQuery(
  statement: AggregateStatistics,
  search: string,
): boolean {
  const matchString = statement.label.toLowerCase();
  const matchFingerPrintId = statement.aggregatedFingerprintHexID;

  // If search term is wrapped by quotes, do the exact search term.
  if (search.startsWith('"') && search.endsWith('"')) {
    search = search.substring(1, search.length - 1);

    return matchString.includes(search) || matchFingerPrintId.includes(search);
  }

  return search
    .toLowerCase()
    .split(" ")
    .every(
      val => matchString.includes(val) || matchFingerPrintId.includes(val),
    );
}

export function filterStatementsData(
  filters: Filters,
  search: string,
  statements: AggregateStatistics[],
  isTenant: boolean,
): AggregateStatistics[] {
  const timeValue = getTimeValueInSeconds(filters);
  const sqlTypes =
    filters.sqlType?.length > 0
      ? filters.sqlType.split(",").map(sqlType => {
          // Adding "Type" to match the value on the Statement
          // Possible values: TypeDDL, TypeDML, TypeDCL and TypeTCL
          return "Type" + sqlType.trim();
        })
      : [];
  const databases =
    filters.database?.length > 0
      ? filters.database.split(",").map(db => db.trim())
      : [];
  if (databases.includes(unset)) {
    databases.push("");
  }
  const regions = filters.regions?.length > 0 ? filters.regions.split(",") : [];
  const nodes = filters.nodes?.length > 0 ? filters.nodes.split(",") : [];
  const appNames = filters.app
    ?.split(",")
    .map(app => app.trim())
    .filter(appName => !!appName);

  const includeInternalApps = !!appNames?.includes(INTERNAL_APP_NAME_PREFIX);

  // Return statements filtered by the values selected on the filter and
  // the search text. A statement must match all selected filters to be
  // displayed on the table.
  // Current filters: search text, database, fullScan, service latency,
  // SQL Type, nodes and regions.
  return statements
    .filter(statement => {
      try {
        // Case where the database is returned as an array in a string form.
        const dbList = JSON.parse(statement.database);
        return (
          databases.length === 0 || databases.some(d => dbList.includes(d))
        );
      } catch (e) {
        // Case where the database is a single value as a string.
        return databases.length === 0 || databases.includes(statement.database);
      }
    })
    .filter(statement => {
      const isInternal = statement.applicationName?.startsWith(
        INTERNAL_APP_NAME_PREFIX,
      );
      return (
        !appNames?.length ||
        (includeInternalApps && isInternal) ||
        appNames?.includes(
          statement.applicationName ? statement.applicationName : unset,
        )
      );
    })
    .filter(statement => (filters.fullScan ? statement.fullScan : true))
    .filter(
      statement =>
        timeValue === "empty" ||
        statement.stats.service_lat.mean >= Number(timeValue),
    )
    .filter(
      statement =>
        sqlTypes.length === 0 || sqlTypes.includes(statement.stats.sql_type),
    )
    .filter(
      // The statement must contain at least one value from the selected regions
      // list if the list is not empty.
      statement =>
        regions.length === 0 ||
        statement.stats.regions?.some(region => regions.includes(region)),
    )
    .filter(
      // The statement must contain at least one value from the selected nodes
      // list if the list is not empty.
      // If the cluster is a tenant cluster we don't care
      // about nodes.
      statement =>
        isTenant ||
        nodes.length === 0 ||
        (statement.stats.nodes &&
          containAny(
            statement.stats.nodes.map(node => "n" + node),
            nodes,
          )),
    )
    .filter(statement =>
      search ? filterBySearchQuery(statement, search) : true,
    );
}
// convertRawStmtsToAggregateStatistics converts statements from the
// server response to AggregatedStatistics[]
export const convertRawStmtsToAggregateStatistics = (
  stmts: CollectedStatementStatistics[],
): AggregateStatistics[] => {
  if (!stmts?.length) return [];

  const statements = flattenStatementStats(stmts);

  return statements.map(stmt => {
    return {
      aggregatedFingerprintID: stmt.statement_fingerprint_id?.toString(),
      aggregatedFingerprintHexID: FixFingerprintHexValue(
        stmt.statement_fingerprint_id?.toString(16),
      ),
      label: stmt.statement,
      summary: stmt.statement_summary,
      aggregatedTs: stmt.aggregated_ts,
      implicitTxn: stmt.implicit_txn,
      fullScan: stmt.full_scan,
      database: stmt.database,
      applicationName: stmt.app,
      stats: stmt.stats,
    };
  });
};

// We separate the memoized version for testing.
export const convertRawStmtsToAggregateStatisticsMemoized = createSelector(
  (stmts: CollectedStatementStatistics[]) => stmts,
  (stmts): AggregateStatistics[] => convertRawStmtsToAggregateStatistics(stmts),
);

// getAppsFromStmtsResponse returns the array of all unique apps within the data.
export const getAppsFromStmtsResponse = (data: SqlStatsResponse): string[] => {
  if (!data?.statements) {
    return [];
  }

  const apps = new Set<string>();
  data.statements?.forEach(statement => {
    const app = statement.key?.key_data?.app;

    if (app == null) return;

    if (
      data.internal_app_name_prefix &&
      app?.startsWith(data.internal_app_name_prefix)
    ) {
      apps.add(data.internal_app_name_prefix);
      return;
    }
    apps.add(app ? app : unset);
  });

  return Array.from(apps).sort();
};

export const getAppsFromStmtsResponseMemoized = createSelector(
  (data: SqlStatsResponse) => data,
  data => getAppsFromStmtsResponse(data),
);
