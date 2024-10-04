// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { createSelector } from "reselect";
import { chain, orderBy } from "lodash";
import { AppState } from "../reducers";
import { StatementDiagnosticsReport } from "../../api";
import moment from "moment-timezone";

export const statementDiagnostics = createSelector(
  (state: AppState) => state.adminUI,
  state => state?.statementDiagnostics,
);

export const selectStatementDiagnosticsReports = createSelector(
  statementDiagnostics,
  state => state.data,
);

export type StatementDiagnosticsDictionary = {
  [statementFingerprint: string]: StatementDiagnosticsReport[];
};

export const selectDiagnosticsReportsPerStatement = createSelector(
  selectStatementDiagnosticsReports,
  (
    diagnosticsReports: StatementDiagnosticsReport[],
  ): StatementDiagnosticsDictionary =>
    chain(diagnosticsReports)
      .groupBy(diagnosticsReport => diagnosticsReport.statement_fingerprint)
      // Perform DESC sorting to get latest report on top
      .mapValues(diagnostics =>
        orderBy(diagnostics, [d => moment(d.requested_at).unix()], ["desc"]),
      )
      .value(),
);

export const selectDiagnosticsReportsByStatementFingerprint = createSelector(
  selectStatementDiagnosticsReports,
  (_state: AppState, statementFingerprint: string) => statementFingerprint,
  (requests, statementFingerprint) =>
    (requests || []).filter(
      request => request.statement_fingerprint === statementFingerprint,
    ),
);
