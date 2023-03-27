// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { RouteComponentProps } from "react-router";
import {
  getMatchParamByName,
  executionIdAttr,
  idAttr,
  statementAttr,
  txnFingerprintIdAttr,
  unset,
  ExecutionStatistics,
  queryByName,
  appAttr,
  flattenStatementStats,
  FixFingerprintHexValue,
} from "src/util";
import { createSelector } from "@reduxjs/toolkit";
import { SqlStatsResponse } from "../api";
import { AggregateStatistics } from "src/statementsTable";
import { StatementDiagnosticsDictionary } from "src/store/statementDiagnostics";

// The functions in this file are agnostic to the different shape of each
// state in db-console and cluster-ui. This file contains selector functions
// and combiners that can be reused across both packages.
// This is to avoid unnecessary repeated logic in the creation of selectors
// between db-console and cluster-ui.

export const selectExecutionID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => {
  return getMatchParamByName(props.match, executionIdAttr);
};

export const selectID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => {
  return getMatchParamByName(props.match, idAttr);
};

export const selectStatementFingerprintID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => getMatchParamByName(props.match, statementAttr);

export const selectTransactionFingerprintID = (
  _state: unknown,
  props: RouteComponentProps,
): string | null => getMatchParamByName(props.match, txnFingerprintIdAttr);

// selectStmtsAllApps returns the array of all unique apps within the data.
export const selectStmtsAllApps = createSelector(
  (data: SqlStatsResponse) => data,
  data => {
    if (!data) {
      return [];
    }

    const apps = new Set<string>();
    data.statements?.forEach(statement => {
      const app = statement.key.key_data.app;
      if (
        data.internal_app_name_prefix &&
        app.startsWith(data.internal_app_name_prefix)
      ) {
        apps.add(data.internal_app_name_prefix);
        return;
      }
      apps.add(app ? app : unset);
    });

    return Array.from(apps).sort();
  },
);

export const selectStmtsCombiner = (
  statsResp: SqlStatsResponse,
  props: RouteComponentProps<unknown>,
  diagnosticsReportsPerStatement: StatementDiagnosticsDictionary,
): AggregateStatistics[] => {
  // State is valid if we successfully fetched data, and the data has not yet been invalidated.
  if (!statsResp) {
    return null;
  }
  let statements = flattenStatementStats(statsResp.statements);
  const app = queryByName(props.location, appAttr);

  const isInternal = (statement: ExecutionStatistics) =>
    statement.app?.startsWith(statsResp.internal_app_name_prefix);

  if (app && app !== "All") {
    const criteria = decodeURIComponent(app).split(",");
    let showInternal = false;
    if (criteria.includes(statsResp.internal_app_name_prefix)) {
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
    // We don't want to show statements that only come from internal apps.
    statements = statements.filter(
      (statement: ExecutionStatistics) => !isInternal(statement),
    );
  }

  return statements.map(stmt => ({
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
    diagnosticsReports: diagnosticsReportsPerStatement[stmt.statement],
  }));
};
