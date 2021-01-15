import { createSelector } from "reselect";
import { chain, last, sortBy } from "lodash";
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
  [statementFingerprint: string]: IStatementDiagnosticsReport;
};

export const selectLastDiagnosticsReportPerStatement = createSelector(
  selectStatementDiagnosticsReports,
  (
    diagnosticsReports: IStatementDiagnosticsReport[],
  ): StatementDiagnosticsDictionary =>
    chain(diagnosticsReports)
      .groupBy(diagnosticsReport => diagnosticsReport.statement_fingerprint)
      // Perform ASC sorting and take the last item
      .mapValues(diagnostics =>
        last(sortBy(diagnostics, d => d.requested_at.seconds.toNumber())),
      )
      .value(),
);
