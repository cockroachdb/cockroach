// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { chain, orderBy } from "lodash";
import { createSelector } from "reselect";
import { AdminUIState } from "src/redux/state";
import { api as clusterUiApi } from "@cockroachlabs/cluster-ui";
import moment from "moment-timezone";

export const selectStatementByFingerprint = createSelector(
  (state: AdminUIState) => state.cachedData.statements.data?.statements,
  (_state: AdminUIState, statementFingerprint: string) => statementFingerprint,
  (statements, statementFingerprint) =>
    (statements || []).find(
      statement => statement.key.key_data.query === statementFingerprint,
    ),
);

export const selectDiagnosticsReportsByStatementFingerprint = createSelector(
  (state: AdminUIState) =>
    state.cachedData.statementDiagnosticsReports.data || [],
  (_state: AdminUIState, statementFingerprint: string) => statementFingerprint,
  (requests, statementFingerprint) =>
    (requests || []).filter(
      request => request.statement_fingerprint === statementFingerprint,
    ),
);

export const selectDiagnosticsReportsCountByStatementFingerprint =
  createSelector(
    selectDiagnosticsReportsByStatementFingerprint,
    requests => requests.length,
  );

export const selectStatementDiagnosticsReports = createSelector(
  (state: AdminUIState) =>
    state.cachedData.statementDiagnosticsReports.data || [],
  diagnosticsReports => diagnosticsReports,
);

export const statementDiagnosticsReportsInFlight = createSelector(
  (state: AdminUIState) =>
    state.cachedData.statementDiagnosticsReports.inFlight,
  inFlight => inFlight,
);

type StatementDiagnosticsDictionary = {
  [statementFingerprint: string]: clusterUiApi.StatementDiagnosticsReport[];
};

export const selectDiagnosticsReportsPerStatement = createSelector(
  selectStatementDiagnosticsReports,
  (
    diagnosticsReports: clusterUiApi.StatementDiagnosticsReport[],
  ): StatementDiagnosticsDictionary =>
    chain(diagnosticsReports)
      .groupBy(diagnosticsReport => diagnosticsReport.statement_fingerprint)
      .mapValues(diagnostics =>
        orderBy(diagnostics, d => moment(d.requested_at).unix(), ["desc"]),
      )
      .value(),
);
