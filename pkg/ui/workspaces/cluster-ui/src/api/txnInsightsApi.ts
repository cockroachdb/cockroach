// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import {
  getInsightsFromProblemsAndCauses,
  getTxnInsightStatus,
  InsightExecEnum,
  TxnInsightEvent,
} from "src/insights";
import moment from "moment-timezone";
import {
  ByteArrayToUuid,
  DurationToMomentDuration,
  FixFingerprintHexValue,
  FixLong,
  HexStringToByteArray,
  makeTimestamp,
  TimestampToMoment,
} from "../util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "./fetchData";
import { StatementExecutionInsight } from "./stmtInsightsApi";

const TXN_EXEC_INSIGHTS_PATH = "_status/insights/transactions";

export type TransactionExecutionInsightsRequest =
  cockroach.server.serverpb.TransactionExecutionInsightsRequest;
export type TransactionExecutionInsightsResponse =
  cockroach.server.serverpb.TransactionExecutionInsightsResponse;
export type TransactionExecutionInsight =
  cockroach.server.serverpb.TransactionExecutionInsightsResponse.ITransaction;
export const TxnInsightStatus = cockroach.sql.insights.Transaction.Status;

// TODO(thomas): populate "withContentionEvents" and "withStatementInsights" where necessary
//  - I think this is only for details
export type TxnInsightsRequest = {
  txnExecutionID?: string;
  txnFingerprintID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

// TODO(thomas): maybe move to txnInsightsUtils.ts
export function createTxnInsightsReq(
  req?: TxnInsightsRequest,
): TransactionExecutionInsightsRequest {
  const fingerprintID = req.txnExecutionID
    ? fromString(req.txnExecutionID)
    : fromNumber(0);
  const execID = req.txnExecutionID
    ? HexStringToByteArray(req.txnExecutionID)
    : null;
  const start = req.start ? makeTimestamp(req.start.unix()) : null;
  const end = req.end ? makeTimestamp(req.end.unix()) : null;

  return {
    txn_fingerprint_id: fingerprintID,
    transaction_id: execID,
    start_time: start,
    end_time: end,
    with_contention_events: false,
    with_statement_insights: false,
  };
}

export async function getTxnInsightsApi(
  req?: TxnInsightsRequest,
): Promise<TxnInsightEvent[]> {
  return fetchTxnInsights(createTxnInsightsReq(req));
}

export async function fetchTxnInsights(
  req: TransactionExecutionInsightsRequest,
): Promise<TxnInsightEvent[]> {
  const response = await fetchData(
    cockroach.server.serverpb.TransactionExecutionInsightsResponse,
    TXN_EXEC_INSIGHTS_PATH,
    cockroach.server.serverpb.TransactionExecutionInsightsRequest,
    req,
    "5M",
  );
  return formatTxnInsightsResponse(response);
}

// TODO(thomas): we can probably move these "formatting" functions to txnInsightsUtils.ts
//  - for stmtInsights as well
//  - we can probably also generalize them, instead of writing one for txn insights and
//  one for stmt insights, we can probably write a single function with a formatter
//  callback. This can be used for everything, not just these functions/use cases.

function formatTxnInsightsResponse(
  response: TransactionExecutionInsightsResponse,
): TxnInsightEvent[] {
  if (!response.transactions) {
    return [];
  }
  return formatTxnInsights(response.transactions);
}

function formatTxnInsights(
  txnInsights: TransactionExecutionInsight[],
): TxnInsightEvent[] {
  if (!txnInsights.length) {
    return [];
  }

  return txnInsights.map((txnInsight: TransactionExecutionInsight) => {
    const startTime = TimestampToMoment(txnInsight.start_time).utc();
    const endTime = TimestampToMoment(txnInsight.end_time).utc();
    const insights = getInsightsFromProblemsAndCauses(
      txnInsight.problems,
      txnInsight.causes,
      InsightExecEnum.TRANSACTION,
    );
    return {
      sessionID: ByteArrayToUuid(txnInsight.session_id, ""),
      transactionExecutionID: ByteArrayToUuid(txnInsight.id),
      transactionFingerprintID: FixFingerprintHexValue(
        txnInsight.fingerprint_id.toString(16),
      ),
      implicitTxn: txnInsight.implicit_txn,
      query: formatTxnStmtQueries(txnInsight),
      startTime,
      endTime,
      elapsedTimeMillis: endTime.diff(startTime, "milliseconds"),
      application: txnInsight.application_name,
      username: txnInsight.user,
      rowsRead: FixLong(txnInsight.rows_read ?? 0).toNumber(),
      rowsWritten: FixLong(txnInsight.rows_written ?? 0).toNumber(),
      priority: txnInsight.user_priority,
      retries: FixLong(txnInsight.retries ?? 0).toNumber(),
      lastRetryReason: txnInsight.auto_retry_reason,
      contentionTime: txnInsight.contention
        ? DurationToMomentDuration(txnInsight.contention)
        : null,
      insights,
      stmtExecutionIDs: txnInsight.stmt_execution_ids.map(stmtExecId =>
        ByteArrayToUuid(stmtExecId, ""),
      ),
      cpuSQLNanos: FixLong(txnInsight.cpu_sql_nanos ?? 0).toNumber(),
      errorCode: txnInsight.last_error_code,
      errorMsg: txnInsight.last_error_msg,
      status: getTxnInsightStatus(txnInsight.status),
    };
  });
}

function formatTxnStmtQueries(txnInsight: TransactionExecutionInsight): string {
  if (!txnInsight?.statements?.length) {
    return "";
  }
  const res = txnInsight.statements
    .filter(({ query }: StatementExecutionInsight) => query)
    .map(({ query }) => query);
  return res.join("\n");
}
