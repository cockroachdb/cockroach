// Copyright 2023 The Cockroach Authors.
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
  getStmtInsightStatus,
  InsightExecEnum,
  StmtInsightEvent,
} from "src/insights";
import moment from "moment-timezone";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "./fetchData";
import {
  ByteArrayToUuid,
  DurationToMomentDuration,
  FixFingerprintHexValue,
  FixLong,
  HexStringToByteArray,
  makeTimestamp,
  TimestampToMoment,
} from "../util";
import { fromNumber, fromString } from "long";

const STMT_EXEC_INSIGHTS_PATH = "_status/insights/statements";

export type StatementExecutionInsightsRequest =
  cockroach.server.serverpb.StatementExecutionInsightsRequest;
export type StatementExecutionInsightsResponse =
  cockroach.server.serverpb.StatementExecutionInsightsResponse;
export type StatementExecutionInsight =
  cockroach.server.serverpb.StatementExecutionInsightsResponse.IStatement;
export const InsightStatus = cockroach.sql.insights.Statement.Status;
export const InsightProblem = cockroach.sql.insights.Problem;
export const InsightCause = cockroach.sql.insights.Cause;

export type StmtInsightsReq = {
  start?: moment.Moment;
  end?: moment.Moment;
  stmtExecutionID?: string;
  stmtFingerprintId?: string;
};

function createStmtInsightReq(
  req: StmtInsightsReq,
): StatementExecutionInsightsRequest {
  const fingerprintID = req.stmtFingerprintId
    ? fromString(req.stmtFingerprintId)
    : fromNumber(0);
  const execID = req.stmtExecutionID
    ? HexStringToByteArray(req.stmtExecutionID)
    : null;
  const start = req.start ? makeTimestamp(req.start.unix()) : null;
  const end = req.end ? makeTimestamp(req.end.unix()) : null;

  return {
    stmt_fingerprint_id: fingerprintID,
    statement_id: execID,
    start_time: start,
    end_time: end,
  };
}

export const getStmtInsightsApi = (
  req: StmtInsightsReq,
): Promise<StmtInsightEvent[]> => {
  return fetchStmtInsights(createStmtInsightReq(req));
};

const fetchStmtInsights = async (
  req: StatementExecutionInsightsRequest,
): Promise<StmtInsightEvent[]> => {
  const response = await fetchData(
    cockroach.server.serverpb.StatementExecutionInsightsResponse,
    STMT_EXEC_INSIGHTS_PATH,
    cockroach.server.serverpb.StatementExecutionInsightsRequest,
    req,
    "5M",
  );
  return formatStmtInsightsResponse(response);
};

function formatStmtInsightsResponse(
  response: StatementExecutionInsightsResponse,
): StmtInsightEvent[] {
  if (!response?.statements) {
    return [];
  }
  return formatStmtInsights(response.statements);
}

function formatStmtInsights(
  stmtInsights: StatementExecutionInsight[],
): StmtInsightEvent[] {
  if (!stmtInsights.length) {
    return [];
  }

  return stmtInsights.map((stmtInsight: StatementExecutionInsight) => {
    const start = TimestampToMoment(stmtInsight.start_time).utc();
    const end = TimestampToMoment(stmtInsight.end_time).utc();

    return {
      transactionExecutionID: ByteArrayToUuid(stmtInsight.transaction_id),
      transactionFingerprintID: FixFingerprintHexValue(
        stmtInsight.txn_fingerprint_id.toString(16),
      ),
      implicitTxn: stmtInsight.implicit_txn,
      databaseName: stmtInsight.database,
      application: stmtInsight.application_name,
      username: stmtInsight.user,
      sessionID: ByteArrayToUuid(stmtInsight.session_id, ""),
      priority: stmtInsight.user_priority,
      retries: FixLong(stmtInsight.retries ?? 0).toNumber(),
      lastRetryReason: stmtInsight.auto_retry_reason,
      query: stmtInsight.query,
      startTime: start,
      endTime: end,
      elapsedTimeMillis: end.diff(start, "milliseconds"),
      statementExecutionID: ByteArrayToUuid(stmtInsight.id, ""),
      statementFingerprintID: FixFingerprintHexValue(
        stmtInsight.fingerprint_id.toString(16),
      ),
      isFullScan: stmtInsight.full_scan,
      rowsRead: FixLong(stmtInsight.rows_read ?? 0).toNumber(),
      rowsWritten: FixLong(stmtInsight.rows_written ?? 0).toNumber(),
      // This is the total stmt contention.
      contentionTime: stmtInsight.contention
        ? DurationToMomentDuration(stmtInsight.contention)
        : null,
      indexRecommendations: stmtInsight.index_recommendations,
      insights: getInsightsFromProblemsAndCauses(
        [stmtInsight.problem],
        stmtInsight.causes,
        InsightExecEnum.STATEMENT,
      ),
      planGist: stmtInsight.plan_gist,
      cpuSQLNanos: FixLong(stmtInsight.cpu_sql_nanos ?? 0).toNumber(),
      errorCode: stmtInsight.error_code,
      errorMsg: stmtInsight.last_error_msg,
      status: getStmtInsightStatus(stmtInsight.status),
    };
  });
}
