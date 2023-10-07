// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { ContentionDetails, TxnInsightDetails } from "../insights";
import { formatStmtInsights } from "./stmtInsightsApi";
import {
  TransactionExecutionInsightsRequest,
  TxnInsightsRequest,
  fetchTxnInsights,
  formatTxnInsights,
  TransactionExecutionInsightsResponse,
} from "./txnInsightsApi";
import {
  ByteArrayToUuid,
  DurationToMomentDuration,
  FixFingerprintHexValue,
  TimestampToMoment,
} from "../util";
import { createTxnInsightsReq } from "./txnInsightsUtils";
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

type ContentionEvent = cockroach.server.serverpb.IContentionEvent;

// To reduce the number of RPC fanouts, we have the caller
// specify which parts of the txn details we should return, since
// some parts may be available in the cache or are unnecessary to
// fetch (e.g. when there is no high contention to report).
export type TxnInsightDetailsRequest = TxnInsightsRequest & {
  withStatementInsights?: boolean;
  withContentionEvents?: boolean;
  mergeResultWith?: TxnInsightDetails;
};

export type TxnInsightDetailsResponse = {
  txnExecutionID: string;
  result: TxnInsightDetails;
};

function createTxnInsightDetailsReq(
  req?: TxnInsightDetailsRequest,
): TransactionExecutionInsightsRequest {
  const baseReq = createTxnInsightsReq(req);
  return {
    ...baseReq,
    with_contention_events: req.withStatementInsights,
    with_statement_insights: req.withContentionEvents,
  };
}

export async function getTxnInsightDetailsApi(
  req: TxnInsightDetailsRequest,
): Promise<TxnInsightDetailsResponse> {
  // Note the way we construct the object below is important. We spread the
  // existing object fields into a new object in order to ensure a new
  // reference is returned so that components will be notified that there
  // was a change. However, we want the internal objects (e.g. txnDetails)
  // should only change when they are re-fetched so that components don't update
  // unnecessarily.
  const txnInsightDetails: TxnInsightDetails = { ...req.mergeResultWith };
  const response = await fetchTxnInsights(createTxnInsightDetailsReq(req));

  if (response?.transactions?.length > 1) {
    throw new Error(
      `Expected 1 result from txn insight details request, got ${response?.transactions?.length}.`,
    );
  }

  return formatTxnInsightDetailsResponse(
    req.txnExecutionID,
    response,
    txnInsightDetails,
  );
}

function formatTxnInsightDetailsResponse(
  txnExecID: string,
  response: TransactionExecutionInsightsResponse,
  txnInsightDetails: TxnInsightDetails,
): TxnInsightDetailsResponse {
  const txnDetails = response?.transactions[0];
  txnInsightDetails.txnDetails = formatTxnInsights([txnDetails])[0];
  txnInsightDetails.statements = formatStmtInsights(txnDetails?.statements);
  txnInsightDetails.blockingContentionDetails = formatContentionEvents(
    txnDetails?.contention_events,
  );
  return {
    txnExecutionID: txnExecID,
    result: txnInsightDetails,
  };
}

function formatContentionEvents(
  events: ContentionEvent[],
): ContentionDetails[] {
  if (!events.length) {
    return [];
  }

  // TODO(thomas): check for undefined vals? proto definition doesn't think so
  return events.map(event => {
    return {
      blockingExecutionID: ByteArrayToUuid(event.id),
      blockingTxnFingerprintID: FixFingerprintHexValue(
        event.blocking_txn_fingerprint_id.toString(16),
      ),
      blockingTxnQuery: null,
      waitingTxnID: ByteArrayToUuid(event.waiting_txn_id),
      waitingTxnFingerprintID: FixFingerprintHexValue(
        event.waiting_txn_fingerprint_id.toString(16),
      ),
      waitingStmtID: ByteArrayToUuid(event.waiting_stmt_id),
      waitingStmtFingerprintID: FixFingerprintHexValue(
        event.waiting_stmt_fingerprint_id.toString(16),
      ),
      collectionTimeStamp: TimestampToMoment(event.collection_ts).utc(),
      contendedKey: event.pretty_key,
      contentionTimeMs: DurationToMomentDuration(event.duration).milliseconds(),
      databaseName: event.database_name,
      schemaName: event.schema_name,
      tableName: event.table_name,
      indexName:
        event.index_name && event.index_name !== ""
          ? event.index_name
          : "index not found",
    };
  });
}
