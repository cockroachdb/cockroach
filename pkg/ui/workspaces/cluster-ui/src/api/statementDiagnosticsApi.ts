// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import moment from "moment-timezone";

import { fetchData } from "src/api";

import { DurationToMomentDuration, NumberToDuration } from "../util";

const STATEMENT_DIAGNOSTICS_PATH = "_status/stmtdiagreports";
const CANCEL_STATEMENT_DIAGNOSTICS_PATH =
  STATEMENT_DIAGNOSTICS_PATH + "/cancel";

export type StatementDiagnosticsReport = {
  id: string;
  statement_fingerprint: string;
  completed: boolean;
  statement_diagnostics_id?: string;
  requested_at: moment.Moment;
  min_execution_latency?: moment.Duration;
  expires_at?: moment.Moment;
};

export type StatementDiagnosticsResponse = StatementDiagnosticsReport[];

export async function getStatementDiagnosticsReports(): Promise<StatementDiagnosticsResponse> {
  const response = await fetchData(
    cockroach.server.serverpb.StatementDiagnosticsReportsResponse,
    STATEMENT_DIAGNOSTICS_PATH,
  );
  return response.reports.map(report => {
    const minExecutionLatency = report.min_execution_latency
      ? DurationToMomentDuration(report.min_execution_latency)
      : null;
    return {
      id: report.id.toString(),
      statement_fingerprint: report.statement_fingerprint,
      completed: report.completed,
      statement_diagnostics_id: report.statement_diagnostics_id.toString(),
      requested_at: moment.unix(report.requested_at?.seconds.toNumber()),
      min_execution_latency: minExecutionLatency,
      expires_at: moment.unix(report.expires_at?.seconds.toNumber()),
    };
  });
}

export type InsertStmtDiagnosticRequest = {
  stmtFingerprint: string;
  samplingProbability?: number;
  minExecutionLatencySeconds?: number;
  expiresAfterSeconds?: number;
  planGist: string;
  redacted: boolean;
};

export type InsertStmtDiagnosticResponse = {
  req_resp: boolean;
};

export async function createStatementDiagnosticsReport(
  req: InsertStmtDiagnosticRequest,
): Promise<InsertStmtDiagnosticResponse> {
  return fetchData(
    cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse,
    STATEMENT_DIAGNOSTICS_PATH,
    cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest,
    new cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest({
      statement_fingerprint: req.stmtFingerprint,
      sampling_probability: req.samplingProbability,
      min_execution_latency: NumberToDuration(req.minExecutionLatencySeconds),
      expires_after: NumberToDuration(req.expiresAfterSeconds),
      plan_gist: req.planGist,
      redacted: req.redacted,
    }),
  ).then(response => {
    return {
      req_resp: response.report !== null,
    };
  });
}

export type CancelStmtDiagnosticRequest = {
  requestId: string;
};

export type CancelStmtDiagnosticResponse = {
  stmt_diag_req_id: string;
};

export async function cancelStatementDiagnosticsReport(
  req: CancelStmtDiagnosticRequest,
): Promise<CancelStmtDiagnosticResponse> {
  return fetchData(
    cockroach.server.serverpb.CancelStatementDiagnosticsReportResponse,
    CANCEL_STATEMENT_DIAGNOSTICS_PATH,
    cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest,
    new cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest({
      request_id: Long.fromString(req.requestId),
    }),
  ).then(response => {
    if (response.error) {
      throw new Error(response.error);
    }
    return {
      stmt_diag_req_id: req.requestId,
    };
  });
}
