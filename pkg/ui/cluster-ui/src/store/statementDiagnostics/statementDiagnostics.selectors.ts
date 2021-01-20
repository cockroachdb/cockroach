import { createSelector } from "reselect";
import { chain, orderBy } from "lodash";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { AppState } from "../reducers";

type IStatementDiagnosticsReport = cockroach.server.serverpb.IStatementDiagnosticsReport;

export const statementDiagnostics = createSelector(
  (state: AppState) => state.adminUI,
  state => state.statementDiagnostics,
);

export const selectStatementDiagnosticsReports = createSelector(
  statementDiagnostics,
  state => state.data?.reports,
);

type StatementDiagnosticsDictionary = {
  [statementFingerprint: string]: IStatementDiagnosticsReport[];
};

export const selectDiagnosticsReportsPerStatement = createSelector(
  selectStatementDiagnosticsReports,
  (
    diagnosticsReports: IStatementDiagnosticsReport[],
  ): StatementDiagnosticsDictionary =>
    chain(diagnosticsReports)
      .groupBy(diagnosticsReport => diagnosticsReport.statement_fingerprint)
      // Perform DESC sorting to get latest report on top
      .mapValues(diagnostics =>
        orderBy(
          diagnostics,
          [d => d.requested_at.seconds.toNumber()],
          ["desc"],
        ),
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
