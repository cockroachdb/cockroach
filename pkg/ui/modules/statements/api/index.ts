import * as protos from "src/js/protos";
import moment from "moment";
import {STATUS_PREFIX, timeoutFetch} from "src/util/api";
import {cockroach} from "src/js/protos";
import serverpb = cockroach.server.serverpb;

export type StatementsResponseMessage = protos.cockroach.server.serverpb.StatementsResponse;

export type StatementDiagnosticsReportsRequestMessage = protos.cockroach.server.serverpb.StatementDiagnosticsReportsRequest;
export type StatementDiagnosticsReportsResponseMessage = protos.cockroach.server.serverpb.StatementDiagnosticsReportsResponse;

export type CreateStatementDiagnosticsReportRequestMessage = protos.cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
export type CreateStatementDiagnosticsReportResponseMessage = protos.cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse;

export type StatementDiagnosticsRequestMessage = protos.cockroach.server.serverpb.StatementDiagnosticsRequest;
export type StatementDiagnosticsResponseMessage = protos.cockroach.server.serverpb.StatementDiagnosticsResponse;

// getStatements returns statements the cluster has recently executed, and some stats about them.
export function getStatements(timeout?: moment.Duration): Promise<StatementsResponseMessage> {
  return timeoutFetch(serverpb.StatementsResponse, `${STATUS_PREFIX}/statements`, null, timeout);
}

export function getStatementDiagnosticsReports(timeout?: moment.Duration): Promise<StatementDiagnosticsReportsResponseMessage> {
  return timeoutFetch(serverpb.StatementDiagnosticsReportsResponse, `${STATUS_PREFIX}/stmtdiagreports`, null, timeout);
}

export function createStatementDiagnosticsReport(req: CreateStatementDiagnosticsReportRequestMessage, timeout?: moment.Duration): Promise<CreateStatementDiagnosticsReportResponseMessage> {
  return timeoutFetch(serverpb.CreateStatementDiagnosticsReportResponse, `${STATUS_PREFIX}/stmtdiagreports`, req as any, timeout);
}

export function getStatementDiagnostics(req: StatementDiagnosticsRequestMessage, timeout?: moment.Duration): Promise<StatementDiagnosticsResponseMessage> {
  return timeoutFetch(serverpb.StatementDiagnosticsResponse, `${STATUS_PREFIX}/stmtdiag/${req.statement_diagnostics_id}`, null, timeout);
}
