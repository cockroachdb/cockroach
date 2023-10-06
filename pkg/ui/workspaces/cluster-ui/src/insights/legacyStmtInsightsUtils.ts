import {
  ContentionDetails,
  failedExecutionInsight,
  highContentionInsight,
  highRetryCountInsight,
  Insight,
  InsightExecEnum,
  InsightNameEnum,
  planRegressionInsight,
  slowExecutionInsight,
  StatementStatus,
  StmtInsightEvent,
  suboptimalPlanInsight,
} from "./index";
import { SqlTxnResult } from "../api";
import moment from "moment-timezone";
import { FixFingerprintHexValue } from "../util";

/*
  NOTE: utils in this file are expected to removed when migrating to the new txn insights endpoint
  as they will no longer be needed
 */

export type StmtInsightsResponseRow = {
  session_id: string;
  txn_id: string;
  txn_fingerprint_id: string; // hex string
  implicit_txn: boolean;
  stmt_id: string;
  stmt_fingerprint_id: string; // hex string
  query: string;
  start_time: string; // Timestamp
  end_time: string; // Timestamp
  full_scan: boolean;
  user_name: string;
  app_name: string;
  database_name: string;
  rows_read: number;
  rows_written: number;
  priority: string;
  retries: number;
  exec_node_ids: number[];
  contention: string; // interval
  contention_events: ContentionDetails[];
  last_retry_reason?: string;
  causes: string[];
  problem: string;
  index_recommendations: string[];
  plan_gist: string;
  cpu_sql_nanos: number;
  error_code: string;
  last_error_redactable: string;
  status: StatementStatus;
};

const stmtColumns = `
session_id,
txn_id,
txn_fingerprint_id,
implicit_txn,
stmt_id,
stmt_fingerprint_id,
query,
start_time,
end_time,
full_scan,
user_name,
app_name,
database_name,
rows_read,
rows_written,
priority,
retries,
exec_node_ids,
contention,
last_retry_reason,
causes,
problem,
index_recommendations,
plan_gist,
cpu_sql_nanos,
error_code,
last_error_redactable,
status
`;

export const legacyStmtInsightsByTxnExecutionQuery = (id: string): string => `
 SELECT ${stmtColumns}
 FROM crdb_internal.cluster_execution_insights
 WHERE txn_id = '${id}'
`;

export function legacyFormatStmtInsights(
  response: SqlTxnResult<StmtInsightsResponseRow>,
): StmtInsightEvent[] {
  if (!response?.rows?.length) {
    return [];
  }

  return response.rows.map((row: StmtInsightsResponseRow) => {
    const start = moment.utc(row.start_time);
    const end = moment.utc(row.end_time);

    return {
      transactionExecutionID: row.txn_id,
      transactionFingerprintID: FixFingerprintHexValue(row.txn_fingerprint_id),
      implicitTxn: row.implicit_txn,
      databaseName: row.database_name,
      application: row.app_name,
      username: row.user_name,
      sessionID: row.session_id,
      priority: row.priority,
      retries: row.retries,
      lastRetryReason: row.last_retry_reason,
      query: row.query,
      startTime: start,
      endTime: end,
      elapsedTimeMillis: end.diff(start, "milliseconds"),
      statementExecutionID: row.stmt_id,
      statementFingerprintID: FixFingerprintHexValue(row.stmt_fingerprint_id),
      isFullScan: row.full_scan,
      rowsRead: row.rows_read,
      rowsWritten: row.rows_written,
      // This is the total stmt contention.
      contentionTime: row.contention ? moment.duration(row.contention) : null,
      indexRecommendations: row.index_recommendations,
      insights: legacyGetInsightsFromProblemsAndCauses(
        [row.problem],
        row.causes,
        InsightExecEnum.STATEMENT,
      ),
      planGist: row.plan_gist,
      cpuSQLNanos: row.cpu_sql_nanos,
      errorCode: row.error_code,
      errorMsg: row.last_error_redactable,
      status: row.status,
    } as StmtInsightEvent;
  });
}

/**
 * legacyGetInsightsFromProblemsAndCauses returns a list of insight objects with
 * labels and descriptions based on the problem, causes for the problem, and
 * the execution type. To be replaced by getInsightsFromProblemAndCauses.
 * @param problems the array of problems with the query, should be a InsightNameEnum[]
 * @param causes an array of strings detailing the causes for the problem, if known
 * @param execType execution type
 * @returns list of insight objects
 */
export function legacyGetInsightsFromProblemsAndCauses(
  problems: string[],
  causes: string[] | null,
  execType: InsightExecEnum,
): Insight[] {
  // TODO(ericharmeling,todd): Replace these strings when using the insights protos.
  const insights: Insight[] = [];

  problems.forEach(problem => {
    switch (problem) {
      case "SlowExecution":
        causes?.forEach(cause =>
          insights.push(getInsightFromCause(cause, execType)),
        );

        if (insights.length === 0) {
          insights.push(
            getInsightFromCause(InsightNameEnum.slowExecution, execType),
          );
        }
        break;

      case "FailedExecution":
        insights.push(
          getInsightFromCause(InsightNameEnum.failedExecution, execType),
        );
        break;

      default:
    }
  });

  return insights;
}

const getInsightFromCause = (
  cause: string,
  execOption: InsightExecEnum,
  latencyThreshold?: number,
  contentionDuration?: number,
): Insight => {
  switch (cause) {
    case InsightNameEnum.highContention:
      return highContentionInsight(
        execOption,
        latencyThreshold,
        contentionDuration,
      );
    case InsightNameEnum.failedExecution:
      return failedExecutionInsight(execOption);
    case InsightNameEnum.planRegression:
      return planRegressionInsight(execOption);
    case InsightNameEnum.suboptimalPlan:
      return suboptimalPlanInsight(execOption);
    case InsightNameEnum.highRetryCount:
      return highRetryCountInsight(execOption);
    default:
      return slowExecutionInsight(execOption, latencyThreshold);
  }
};
