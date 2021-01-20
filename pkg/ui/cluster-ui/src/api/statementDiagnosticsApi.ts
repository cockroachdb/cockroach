import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";

const STATEMENT_DIAGNOSTICS_PATH = "/_status/stmtdiagreports";
const CREATE_STATEMENT_DIAGNOSTICS_REPORT_PATH = "/_status/stmtdiagreports";

type CreateStatementDiagnosticsReportResponseMessage = cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse;

export function getStatementDiagnosticsReports(): Promise<
  cockroach.server.serverpb.StatementDiagnosticsReportsResponse
> {
  return fetchData(
    cockroach.server.serverpb.StatementDiagnosticsReportsResponse,
    STATEMENT_DIAGNOSTICS_PATH,
  );
}

export function createStatementDiagnosticsReport(
  statementsFingerprint: string,
): Promise<CreateStatementDiagnosticsReportResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse,
    CREATE_STATEMENT_DIAGNOSTICS_REPORT_PATH,
    cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest,
    {
      statement_fingerprint: statementsFingerprint,
    },
  );
}
