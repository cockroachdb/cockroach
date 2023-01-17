// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { createSelector } from "reselect";
import {
  aggregateStatementStats,
  appAttr,
  FixFingerprintHexValue,
  combineStatementStats,
  ExecutionStatistics,
  flattenStatementStats,
  formatDate,
  queryByName,
  statementKey,
  StatementStatistics,
  TimestampToMoment,
  unset,
} from "src/util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { RouteComponentProps } from "react-router-dom";

import { AppState } from "src/store";
import { selectDiagnosticsReportsPerStatement } from "../store/statementDiagnostics";
import { AggregateStatistics } from "../statementsTable";
import { sqlStatsSelector } from "../store/sqlStats/sqlStats.selector";
import { SQLStatsState } from "../store/sqlStats";
import { localStorageSelector } from "../store/utils/selectors";

type ICollectedStatementStatistics =
  cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
export interface StatementsSummaryData {
  statementFingerprintID: string;
  statementFingerprintHexID: string;
  statement: string;
  statementSummary: string;
  aggregatedTs: number;
  aggregationInterval: number;
  implicitTxn: boolean;
  fullScan: boolean;
  database: string;
  applicationName: string;
  stats: StatementStatistics[];
}

export const selectStatementsLastUpdated = createSelector(
  sqlStatsSelector,
  sqlStats => sqlStats.lastUpdated,
);

// selectApps returns the array of all apps with statement statistics present
// in the data.
export const selectApps = createSelector(sqlStatsSelector, sqlStatsState => {
  if (!sqlStatsState.data || !sqlStatsState.valid) {
    return [];
  }

  let sawBlank = false;
  let sawInternal = false;
  const apps: { [app: string]: boolean } = {};
  sqlStatsState.data.statements.forEach(
    (statement: ICollectedStatementStatistics) => {
      if (
        sqlStatsState.data.internal_app_name_prefix &&
        statement.key.key_data.app.startsWith(
          sqlStatsState.data.internal_app_name_prefix,
        )
      ) {
        sawInternal = true;
      } else if (statement.key.key_data.app) {
        apps[statement.key.key_data.app] = true;
      } else {
        sawBlank = true;
      }
    },
  );
  return []
    .concat(sawInternal ? [sqlStatsState.data.internal_app_name_prefix] : [])
    .concat(sawBlank ? [unset] : [])
    .concat(Object.keys(apps).sort());
});

// selectDatabases returns the array of all databases with statement statistics present
// in the data.
export const selectDatabases = createSelector(
  sqlStatsSelector,
  sqlStatsState => {
    if (!sqlStatsState.data) {
      return [];
    }

    return Array.from(
      new Set(
        sqlStatsState.data.statements.map(s =>
          s.key.key_data.database ? s.key.key_data.database : unset,
        ),
      ),
    )
      .filter((dbName: string) => dbName !== null && dbName.length > 0)
      .sort();
  },
);

// selectTotalFingerprints returns the count of distinct statement fingerprints
// present in the data.
export const selectTotalFingerprints = createSelector(
  sqlStatsSelector,
  state => {
    if (!state.data) {
      return 0;
    }
    const aggregated = aggregateStatementStats(state.data.statements);
    return aggregated.length;
  },
);

// selectLastReset returns a string displaying the last time the statement
// statistics were reset.
export const selectLastReset = createSelector(sqlStatsSelector, state => {
  if (!state.data) {
    return "";
  }

  return formatDate(TimestampToMoment(state.data.last_reset));
});

export const selectStatementsDataValid = createSelector(
  sqlStatsSelector,
  (state: SQLStatsState): boolean => {
    return state.valid;
  },
);

export const selectStatements = createSelector(
  sqlStatsSelector,
  (_: AppState, props: RouteComponentProps) => props,
  selectDiagnosticsReportsPerStatement,
  (
    state: SQLStatsState,
    props: RouteComponentProps<any>,
    diagnosticsReportsPerStatement,
  ): AggregateStatistics[] => {
    // State is valid if we successfully fetched data, and the data has not yet been invalidated.
    if (!state.data || !state.valid) {
      return null;
    }
    let statements = flattenStatementStats(state.data.statements);
    const app = queryByName(props.location, appAttr);
    const isInternal = (statement: ExecutionStatistics) =>
      statement.app.startsWith(state.data.internal_app_name_prefix);

    if (app && app !== "All") {
      const criteria = decodeURIComponent(app).split(",");
      let showInternal = false;
      if (criteria.includes(state.data.internal_app_name_prefix)) {
        showInternal = true;
      }
      if (criteria.includes(unset)) {
        criteria.push("");
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) =>
          (showInternal && isInternal(statement)) ||
          criteria.includes(statement.app),
      );
    } else {
      // We don't want to show internal statements by default.
      statements = statements.filter(
        (statement: ExecutionStatistics) => !isInternal(statement),
      );
    }

    const statsByStatementKey: {
      [statement: string]: StatementsSummaryData;
    } = {};
    statements.forEach(stmt => {
      const key = statementKey(stmt);
      if (!(key in statsByStatementKey)) {
        statsByStatementKey[key] = {
          statementFingerprintID: stmt.statement_fingerprint_id?.toString(),
          statementFingerprintHexID: FixFingerprintHexValue(
            stmt.statement_fingerprint_id?.toString(16),
          ),
          statement: stmt.statement,
          statementSummary: stmt.statement_summary,
          aggregatedTs: stmt.aggregated_ts,
          aggregationInterval: stmt.aggregation_interval,
          implicitTxn: stmt.implicit_txn,
          fullScan: stmt.full_scan,
          database: stmt.database,
          applicationName: stmt.app,
          stats: [],
        };
      }
      statsByStatementKey[key].stats.push(stmt.stats);
    });

    return Object.keys(statsByStatementKey).map(key => {
      const stmt = statsByStatementKey[key];
      return {
        aggregatedFingerprintID: stmt.statementFingerprintID,
        aggregatedFingerprintHexID: FixFingerprintHexValue(
          stmt.statementFingerprintHexID,
        ),
        label: stmt.statement,
        summary: stmt.statementSummary,
        aggregatedTs: stmt.aggregatedTs,
        aggregationInterval: stmt.aggregationInterval,
        implicitTxn: stmt.implicitTxn,
        fullScan: stmt.fullScan,
        database: stmt.database,
        applicationName: stmt.applicationName,
        stats: combineStatementStats(stmt.stats),
        diagnosticsReports: diagnosticsReportsPerStatement[stmt.statement],
      };
    });
  },
);

export const selectStatementsLastError = createSelector(
  sqlStatsSelector,
  state => state.lastError,
);

export const selectColumns = createSelector(
  localStorageSelector,
  // return array of columns if user have customized it or `null` otherwise
  localStorage =>
    localStorage["showColumns/StatementsPage"]
      ? localStorage["showColumns/StatementsPage"].split(",")
      : null,
);

export const selectTimeScale = createSelector(
  localStorageSelector,
  localStorage => localStorage["timeScale/SQLActivity"],
);

export const selectSortSetting = createSelector(
  localStorageSelector,
  localStorage => localStorage["sortSetting/StatementsPage"],
);

export const selectFilters = createSelector(
  localStorageSelector,
  localStorage => localStorage["filters/StatementsPage"],
);

export const selectSearch = createSelector(
  localStorageSelector,
  localStorage => localStorage["search/StatementsPage"],
);
