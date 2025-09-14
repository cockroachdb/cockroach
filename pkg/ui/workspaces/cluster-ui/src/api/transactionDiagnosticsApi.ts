// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import Long from "long";
import moment from "moment-timezone";

import { fetchData } from "src/api";

import { DurationToMomentDuration, NumberToDuration } from "../util";

const TRANSACTION_DIAGNOSTICS_PATH = "_status/txndiagreports";
const CANCEL_TRANSACTION_DIAGNOSTICS_PATH =
  TRANSACTION_DIAGNOSTICS_PATH + "/cancel";

export type TransactionDiagnosticsReport = {
  id: string;
  transaction_fingerprint: string;
  transaction_fingerprint_id: bigint;
  statement_fingerprint_ids: Uint8Array[];
  completed: boolean;
  transaction_diagnostics_id?: string;
  requested_at: moment.Moment;
  min_execution_latency?: moment.Duration;
  expires_at?: moment.Moment;
  sampling_probability?: number;
  redacted: boolean;
  username: string;
};

export type TransactionDiagnosticsResponse = TransactionDiagnosticsReport[];

export async function getTransactionDiagnosticsReports(): Promise<TransactionDiagnosticsResponse> {
  const response = await fetchData(
    cockroach.server.serverpb.TransactionDiagnosticsReportsResponse,
    TRANSACTION_DIAGNOSTICS_PATH,
  );
  return response.reports.map(report => {
    const minExecutionLatency = report.min_execution_latency
      ? DurationToMomentDuration(report.min_execution_latency)
      : null;
    let txnId = BigInt(0);
    if (
      report.transaction_fingerprint_id &&
      report.transaction_fingerprint_id.length >= 8
    ) {
      // Note: the probuf code constructs the Uint8Array fingeprint
      // using a shared underlying buffer so we need to use its
      // specific offset when decoding the Uint64 within.
      const dv = new DataView(report.transaction_fingerprint_id.buffer);
      txnId = dv.getBigUint64(report.transaction_fingerprint_id.byteOffset);
    }
    return {
      id: report.id.toString(),
      transaction_fingerprint: report.transaction_fingerprint,
      transaction_fingerprint_id: txnId,
      statement_fingerprint_ids: report.statement_fingerprint_ids,
      completed: report.completed,
      transaction_diagnostics_id: report.transaction_diagnostics_id?.toString(),
      requested_at: moment.unix(report.requested_at?.seconds.toNumber()),
      min_execution_latency: minExecutionLatency,
      expires_at: moment.unix(report.expires_at?.seconds.toNumber()),
      sampling_probability: report.sampling_probability,
      redacted: report.redacted,
      username: report.username,
    };
  });
}

export type InsertTxnDiagnosticRequest = {
  transactionFingerprintId: Uint8Array;
  statementFingerprintIds: Uint8Array[];
  samplingProbability?: number;
  minExecutionLatencySeconds?: number;
  expiresAfterSeconds?: number;
  redacted: boolean;
};

export type InsertTxnDiagnosticResponse = {
  req_resp: boolean;
};

export async function createTransactionDiagnosticsReport(
  req: InsertTxnDiagnosticRequest,
): Promise<InsertTxnDiagnosticResponse> {
  return fetchData(
    cockroach.server.serverpb.CreateTransactionDiagnosticsReportResponse,
    TRANSACTION_DIAGNOSTICS_PATH,
    cockroach.server.serverpb.CreateTransactionDiagnosticsReportRequest,
    new cockroach.server.serverpb.CreateTransactionDiagnosticsReportRequest({
      transaction_fingerprint_id: req.transactionFingerprintId,
      statement_fingerprint_ids: req.statementFingerprintIds,
      sampling_probability: req.samplingProbability,
      min_execution_latency: NumberToDuration(req.minExecutionLatencySeconds),
      expires_at: NumberToDuration(req.expiresAfterSeconds),
      redacted: req.redacted,
    }),
  ).then(response => {
    return {
      req_resp: response.report !== null,
    };
  });
}

export type CancelTxnDiagnosticRequest = {
  requestId: string;
};

export type CancelTxnDiagnosticResponse = {
  txn_diag_req_id: string;
};

export async function cancelTransactionDiagnosticsReport(
  req: CancelTxnDiagnosticRequest,
): Promise<CancelTxnDiagnosticResponse> {
  return fetchData(
    cockroach.server.serverpb.CancelTransactionDiagnosticsReportResponse,
    CANCEL_TRANSACTION_DIAGNOSTICS_PATH,
    cockroach.server.serverpb.CancelTransactionDiagnosticsReportRequest,
    new cockroach.server.serverpb.CancelTransactionDiagnosticsReportRequest({
      request_id: Long.fromString(req.requestId),
    }),
  ).then(response => {
    if (response.error) {
      throw new Error(response.error);
    }
    return {
      txn_diag_req_id: req.requestId,
    };
  });
}
