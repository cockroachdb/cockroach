// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { chain, sortBy, last } from "lodash";
import { createSelector } from "reselect";
import { AdminUIState } from "src/redux/state";
import { cockroach } from "src/js/protos";
import IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

export const selectStatementByFingerprint = createSelector(
  (state: AdminUIState) => state.cachedData.statements.data?.statements,
  (_state: AdminUIState, statementFingerprint: string) => statementFingerprint,
  (statements, statementFingerprint) =>
    (statements || []).find(statement => statement.key.key_data.query === statementFingerprint),
);

export const selectDiagnosticsReportsByStatementFingerprint = createSelector(
  (state: AdminUIState) => state.cachedData.statementDiagnosticsReports.data?.reports || [],
  (_state: AdminUIState, statementFingerprint: string) => statementFingerprint,
  (requests, statementFingerprint) =>
    (requests || []).filter(request => request.statement_fingerprint === statementFingerprint),
);

export const selectDiagnosticsReportsCountByStatementFingerprint = createSelector(
  selectDiagnosticsReportsByStatementFingerprint,
  (requests) => requests.length,
);

export const selectStatementDiagnosticsReports = createSelector(
  (state: AdminUIState) => state.cachedData.statementDiagnosticsReports.data?.reports,
  diagnosticsReports => diagnosticsReports,
);

type StatementDiagnosticsDictionary = {
  [statementFingerprint: string]: IStatementDiagnosticsReport;
};

export const selectLastDiagnosticsReportPerStatement = createSelector(
  selectStatementDiagnosticsReports,
  (diagnosticsReports: IStatementDiagnosticsReport[]): StatementDiagnosticsDictionary => chain(diagnosticsReports)
    .groupBy(diagnosticsReport => diagnosticsReport.statement_fingerprint)
    // Perform ASC sorting and take the last item
    .mapValues(diagnostics => last(sortBy(diagnostics, d => d.requested_at.seconds.toNumber())))
    .value(),
);
