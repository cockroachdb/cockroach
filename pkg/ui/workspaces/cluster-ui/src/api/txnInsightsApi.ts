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
  TimestampToMoment,
} from "../util";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "./fetchData";
import { StatementExecutionInsight } from "./stmtInsightsApi";
import { createTxnInsightsReq } from "./txnInsightsUtils";

const TXN_EXEC_INSIGHTS_PATH = "_status/insights/transactions";

export type TransactionExecutionInsightsRequest =
  cockroach.server.serverpb.TransactionExecutionInsightsRequest;
export type TransactionExecutionInsightsResponse =
  cockroach.server.serverpb.TransactionExecutionInsightsResponse;
export type TransactionExecutionInsight =
  cockroach.server.serverpb.TransactionExecutionInsightsResponse.ITransaction;
export const TxnInsightStatus = cockroach.sql.insights.Transaction.Status;

export type TxnInsightsRequest = {
  txnExecutionID?: string;
  txnFingerprintID?: string;
  start?: moment.Moment;
  end?: moment.Moment;
};

export async function getTxnInsightsApi(
  req?: TxnInsightsRequest,
): Promise<TxnInsightEvent[]> {
  const response = await fetchTxnInsights(createTxnInsightsReq(req));
  return formatTxnInsightsResponse(response);
}

export function fetchTxnInsights(
  req: TransactionExecutionInsightsRequest,
): Promise<TransactionExecutionInsightsResponse> {
  return fetchData(
    cockroach.server.serverpb.TransactionExecutionInsightsResponse,
    TXN_EXEC_INSIGHTS_PATH,
    cockroach.server.serverpb.TransactionExecutionInsightsRequest,
    req,
    "5M",
  );
}

function formatTxnInsightsResponse(
  response: TransactionExecutionInsightsResponse,
): TxnInsightEvent[] {
  if (!response.transactions) {
    return [];
  }
  return formatTxnInsights(response.transactions);
}

export function formatTxnInsights(
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
      // Note: that we're changing the txn exec id here, it no
      //  longer contains hyphens. The reason for this is that manually adding
      //  hyphens add bytes to the string that make it exceed the number of
      //  bytes for a UUID (16).
      transactionExecutionID: ByteArrayToUuid(txnInsight.id, ""),
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
      retries: FixLong(txnInsight.retry_count ?? 0).toNumber(),
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
