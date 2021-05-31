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
  combineStatementStats,
  ExecutionStatistics,
  flattenStatementStats,
  formatDate,
  getMatchParamByName,
  statementKey,
  StatementStatistics,
  TimestampToMoment,
} from "src/util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { RouteComponentProps } from "react-router-dom";

import { AppState } from "src/store";
import { StatementsState } from "../store/statements";
import { selectDiagnosticsReportsPerStatement } from "../store/statementDiagnostics";
import { AggregateStatistics } from "../statementsTable";

type ICollectedStatementStatistics = cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
export interface StatementsSummaryData {
  statement: string;
  implicitTxn: boolean;
  fullScan: boolean;
  database: string;
  stats: StatementStatistics[];
}

export const adminUISelector = createSelector(
  (state: AppState) => state.adminUI,
  adminUiState => adminUiState,
);

export const statementsSelector = createSelector(
  adminUISelector,
  adminUiState => adminUiState.statements,
);

export const localStorageSelector = createSelector(
  adminUISelector,
  adminUiState => adminUiState.localStorage,
);

// selectApps returns the array of all apps with statement statistics present
// in the data.
export const selectApps = createSelector(
  statementsSelector,
  statementsState => {
    if (!statementsState.data) {
      return [];
    }

    let sawBlank = false;
    let sawInternal = false;
    const apps: { [app: string]: boolean } = {};
    statementsState.data.statements.forEach(
      (statement: ICollectedStatementStatistics) => {
        if (
          statementsState.data.internal_app_name_prefix &&
          statement.key.key_data.app.startsWith(
            statementsState.data.internal_app_name_prefix,
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
      .concat(sawInternal ? ["(internal)"] : [])
      .concat(sawBlank ? ["(unset)"] : [])
      .concat(Object.keys(apps));
  },
);

// selectDatabases returns the array of all databases with statement statistics present
// in the data.
export const selectDatabases = createSelector(
  statementsSelector,
  statementsState => {
    if (!statementsState.data) {
      return [];
    }

    return Array.from(
      new Set(
        statementsState.data.statements.map(s => s.key.key_data.database),
      ),
    ).filter((dbName: string) => dbName !== null && dbName.length > 0);
  },
);

// selectTotalFingerprints returns the count of distinct statement fingerprints
// present in the data.
export const selectTotalFingerprints = createSelector(
  statementsSelector,
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
export const selectLastReset = createSelector(statementsSelector, state => {
  if (!state.data) {
    return "";
  }

  return formatDate(TimestampToMoment(state.data.last_reset));
});

export const selectStatements = createSelector(
  statementsSelector,
  (_: AppState, props: RouteComponentProps) => props,
  selectDiagnosticsReportsPerStatement,
  (
    state: StatementsState,
    props: RouteComponentProps<any>,
    diagnosticsReportsPerStatement,
  ): AggregateStatistics[] => {
    if (!state.data) {
      return null;
    }
    let statements = flattenStatementStats(state.data.statements);
    const app = getMatchParamByName(props.match, appAttr);
    const isInternal = (statement: ExecutionStatistics) =>
      statement.app.startsWith(state.data.internal_app_name_prefix);

    if (app && app !== "All") {
      let criteria = decodeURIComponent(app);
      let showInternal = false;
      if (criteria === "(unset)") {
        criteria = "";
      } else if (criteria === "(internal)") {
        showInternal = true;
      }

      statements = statements.filter(
        (statement: ExecutionStatistics) =>
          (showInternal && isInternal(statement)) || statement.app === criteria,
      );
    }

    const statsByStatementKey: {
      [statement: string]: StatementsSummaryData;
    } = {};
    statements.forEach(stmt => {
      const key = statementKey(stmt);
      if (!(key in statsByStatementKey)) {
        statsByStatementKey[key] = {
          statement: stmt.statement,
          implicitTxn: stmt.implicit_txn,
          fullScan: stmt.full_scan,
          database: stmt.database,
          stats: [],
        };
      }
      statsByStatementKey[key].stats.push(stmt.stats);
    });

    return Object.keys(statsByStatementKey).map(key => {
      const stmt = statsByStatementKey[key];
      return {
        label: stmt.statement,
        implicitTxn: stmt.implicitTxn,
        fullScan: stmt.fullScan,
        database: stmt.database,
        stats: combineStatementStats(stmt.stats),
        diagnosticsReports: diagnosticsReportsPerStatement[stmt.statement],
      };
    });
  },
);

export const selectStatementsLastError = createSelector(
  statementsSelector,
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
