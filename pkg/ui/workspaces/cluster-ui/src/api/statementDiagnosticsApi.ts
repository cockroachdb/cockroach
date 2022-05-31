// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";

const STATEMENT_DIAGNOSTICS_PATH = "/_status/stmtdiagreports";
const CREATE_STATEMENT_DIAGNOSTICS_REPORT_PATH = "/_status/stmtdiagreports";
const CANCEL_STATEMENT_DIAGNOSTICS_REPORT_PATH =
  "/_status/stmtdiagreports/cancel";

type CreateStatementDiagnosticsReportRequestMessage =
  cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest;
type CreateStatementDiagnosticsReportResponseMessage =
  cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse;
type CancelStatementDiagnosticsReportRequestMessage =
  cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest;
type CancelStatementDiagnosticsReportResponseMessage =
  cockroach.server.serverpb.CancelStatementDiagnosticsReportResponse;

export function getStatementDiagnosticsReports(): Promise<cockroach.server.serverpb.StatementDiagnosticsReportsResponse> {
  return fetchData(
    cockroach.server.serverpb.StatementDiagnosticsReportsResponse,
    STATEMENT_DIAGNOSTICS_PATH,
  );
}

export function createStatementDiagnosticsReport(
  req: CreateStatementDiagnosticsReportRequestMessage,
): Promise<CreateStatementDiagnosticsReportResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.CreateStatementDiagnosticsReportResponse,
    CREATE_STATEMENT_DIAGNOSTICS_REPORT_PATH,
    cockroach.server.serverpb.CreateStatementDiagnosticsReportRequest,
    req,
  );
}

export function cancelStatementDiagnosticsReport(
  req: CancelStatementDiagnosticsReportRequestMessage,
): Promise<CancelStatementDiagnosticsReportResponseMessage> {
  return fetchData(
    cockroach.server.serverpb.CancelStatementDiagnosticsReportResponse,
    CANCEL_STATEMENT_DIAGNOSTICS_REPORT_PATH,
    cockroach.server.serverpb.CancelStatementDiagnosticsReportRequest,
    req,
  );
}
