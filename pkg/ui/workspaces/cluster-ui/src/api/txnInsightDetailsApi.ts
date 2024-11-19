// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";

import {
  InsightNameEnum,
  StmtFailureCodesStr,
  TxnInsightDetails,
} from "../insights";
import { maybeError } from "../util";

import { getTxnInsightsContentionDetailsApi } from "./contentionApi";
import {
  executeInternalSql,
  isMaxSizeError,
  sqlApiErrorMessage,
  SqlApiResponse,
} from "./sqlApi";
import {
  formatStmtInsights,
  stmtInsightsByTxnExecutionQuery,
  StmtInsightsResponseRow,
} from "./stmtInsightsApi";
import {
  formatTxnInsightsRow,
  createTxnInsightsQuery,
  TxnInsightsResponseRow,
} from "./txnInsightsApi";
import { makeInsightsSqlRequest } from "./txnInsightsUtils";

export type TxnInsightDetailsRequest = {
  txnExecutionID: string;
  excludeStmts?: boolean;
  excludeTxn?: boolean;
  excludeContention?: boolean;
  mergeResultWith?: TxnInsightDetails;
  start?: moment.Moment;
  end?: moment.Moment;
};

export type TxnInsightDetailsReqErrs = {
  txnDetailsErr: Error | null;
  contentionErr: Error | null;
  statementsErr: Error | null;
};

export type TxnInsightDetailsResponse = {
  txnExecutionID: string;
  result: TxnInsightDetails;
  errors: TxnInsightDetailsReqErrs;
};

export async function getTxnInsightDetailsApi(
  req: TxnInsightDetailsRequest,
): Promise<SqlApiResponse<TxnInsightDetailsResponse>> {
  // All queries in this request read from virtual tables, which is an
  // expensive operation. To reduce the number of RPC fanouts, we have the
  // caller specify which parts of the txn details we should return, since
  // some parts may be available in the cache or are unnecessary to fetch
  // (e.g. when there is no high contention to report).
  //
  // Note the way we construct the object below is important. We spread the
  // existing object fields into a new object in order to ensure a new
  // reference is returned so that components will be notified that there
  // was a change. However, we want the internal objects (e.g. txnDetails)
  // should only change when they are re-fetched so that components don't update
  // unnecessarily.
  const txnInsightDetails: TxnInsightDetails = { ...req.mergeResultWith };
  const errors: TxnInsightDetailsReqErrs = {
    txnDetailsErr: null,
    contentionErr: null,
    statementsErr: null,
  };

  let maxSizeReached = false;
  if (!req.excludeTxn) {
    const request = makeInsightsSqlRequest([
      createTxnInsightsQuery({
        execID: req?.txnExecutionID,
        start: req?.start,
        end: req?.end,
      }),
    ]);

    try {
      const result = await executeInternalSql<TxnInsightsResponseRow>(request);
      maxSizeReached = isMaxSizeError(result.error?.message);

      if (result.error && !maxSizeReached) {
        throw new Error(
          `Error while retrieving insights information: ${sqlApiErrorMessage(
            result.error.message,
          )}`,
        );
      }

      const txnDetailsRes = result.execution.txn_results[0];
      if (txnDetailsRes.rows?.length) {
        txnInsightDetails.txnDetails = formatTxnInsightsRow(
          txnDetailsRes.rows[0],
        );
      }
    } catch (e) {
      errors.txnDetailsErr = maybeError(e);
    }
  }

  if (!req.excludeStmts) {
    try {
      const request = makeInsightsSqlRequest([
        stmtInsightsByTxnExecutionQuery(req.txnExecutionID),
      ]);

      const result = await executeInternalSql<StmtInsightsResponseRow>(request);
      const maxSizeStmtReached = isMaxSizeError(result.error?.message);

      if (result.error && !maxSizeStmtReached) {
        throw new Error(
          `Error while retrieving insights information: ${sqlApiErrorMessage(
            result.error.message,
          )}`,
        );
      }
      maxSizeReached = maxSizeReached || maxSizeStmtReached;

      const stmts = result.execution.txn_results[0];
      if (stmts.rows?.length) {
        txnInsightDetails.statements = formatStmtInsights(stmts);
      }
    } catch (e) {
      errors.statementsErr = maybeError(e);
    }
  }

  const highContention = txnInsightDetails.txnDetails?.insights?.some(
    insight => insight.name === InsightNameEnum.HIGH_CONTENTION,
  );

  const isRetrySerializableFailure =
    txnInsightDetails.txnDetails?.errorCode ===
    StmtFailureCodesStr.RETRY_SERIALIZABLE;

  try {
    if (
      !req.excludeContention &&
      (highContention || isRetrySerializableFailure)
    ) {
      const contentionInfo = await getTxnInsightsContentionDetailsApi(req);
      txnInsightDetails.blockingContentionDetails =
        contentionInfo?.blockingContentionDetails;
    }
  } catch (e) {
    errors.contentionErr = maybeError(e);
  }

  return {
    maxSizeReached: maxSizeReached,
    results: {
      txnExecutionID: req.txnExecutionID,
      result: txnInsightDetails,
      errors,
    },
  };
}
