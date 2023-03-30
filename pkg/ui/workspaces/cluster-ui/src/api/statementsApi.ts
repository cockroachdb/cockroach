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
import { fetchData } from "src/api/fetchData";
import { propsToQueryString } from "src/util";
import Long from "long";
import moment from "moment";

const STATEMENTS_PATH = "/_status/combinedstmts";
const STATEMENT_DETAILS_PATH = "/_status/stmtdetails";

export type StatementsRequest = cockroach.server.serverpb.CombinedStatementsStatsRequest;
export type StatementDetailsRequest = cockroach.server.serverpb.StatementDetailsRequest;
export type StatementDetailsResponse = cockroach.server.serverpb.StatementDetailsResponse;

export type StatementDetailsResponseWithKey = {
  stmtResponse: StatementDetailsResponse;
  key: string;
};

export type SqlStatsResponse = cockroach.server.serverpb.StatementsResponse;
export const SqlStatsSortOptions = cockroach.server.serverpb.StatsSortOptions;
export type SqlStatsSortType = cockroach.server.serverpb.StatsSortOptions;

type Stmt = cockroach.server.serverpb.StatementsResponse.ICollectedStatementStatistics;
type Txn = cockroach.server.serverpb.StatementsResponse.IExtendedCollectedTransactionStatistics;

const FetchStatsMode =
  cockroach.server.serverpb.CombinedStatementsStatsRequest.StatsType;

export type ErrorWithKey = {
  err: Error;
  key: string;
};

export const DEFAULT_STATS_REQ_OPTIONS = {
  limit: 100,
  sort: SqlStatsSortOptions.SERVICE_LAT,
};

// The required fields to create a stmts request.
type StmtReqFields = {
  limit: number;
  sort: SqlStatsSortType;
  start: moment.Moment | null;
  end: moment.Moment | null;
};

export function createCombinedStmtsRequest({
  limit,
  sort,
  start,
  end,
}: StmtReqFields): StatementsRequest {
  return new cockroach.server.serverpb.CombinedStatementsStatsRequest({
    start: start != null ? Long.fromNumber(start.unix()) : null,
    end: end != null ? Long.fromNumber(end.unix()) : null,
    limit: Long.fromNumber(limit ?? DEFAULT_STATS_REQ_OPTIONS.limit),
    fetch_mode: new cockroach.server.serverpb.CombinedStatementsStatsRequest.FetchMode(
      {
        sort: sort,
      },
    ),
  });
}

// Mutates the sqlstats response to conform to the provided sort and limit params.
export function sortAndTruncateStmtsResponse(
  res: SqlStatsResponse,
  sort: SqlStatsSortType,
  limit: number,
): void {
  // Discard txn half of the response. This is a little wasteful but the
  // cleanest and least complex way of handling this scenario.
  res.transactions = [];

  switch (sort) {
    case SqlStatsSortOptions.SERVICE_LAT:
      res.statements?.sort((stmtA: Stmt, stmtB: Stmt): number => {
        return stmtB.stats.service_lat.mean - stmtA.stats.service_lat.mean;
      });
      break;
    case SqlStatsSortOptions.CONTENTION_TIME:
      res.statements?.sort((stmtA: Stmt, stmtB: Stmt): number => {
        return (
          stmtB.stats.exec_stats.contention_time.mean -
          stmtA.stats.exec_stats.contention_time.mean
        );
      });
      break;
    case SqlStatsSortOptions.EXECUTION_COUNT:
      res.statements?.sort((stmtA: Stmt, stmtB: Stmt): number => {
        return stmtB.stats.count.toInt() - stmtA.stats.count.toInt();
      });
      break;
    case SqlStatsSortOptions.PCT_RUNTIME:
    default:
      res.statements?.sort((stmtA: Stmt, stmtB: Stmt): number => {
        return (
          stmtB.stats.service_lat.mean * stmtB.stats.count.toInt() -
          stmtA.stats.service_lat.mean * stmtA.stats.count.toInt()
        );
      });
  }

  // Finally, truncate the response not fitting into limit.
  res.statements.splice(limit);
}

export const getCombinedStatements = (
  req: StatementsRequest,
): Promise<SqlStatsResponse> => {
  const limit = req.limit?.toInt() ?? DEFAULT_STATS_REQ_OPTIONS.limit;

  const queryStr = propsToQueryString({
    start: req.start.toInt(),
    end: req.end.toInt(),
    "fetch_mode.stats_type": FetchStatsMode.StmtStatsOnly,
    "fetch_mode.sort": req.fetch_mode?.sort,
    limit,
  });

  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${STATEMENTS_PATH}?${queryStr}`,
    null,
    null,
    "10M",
  ).then(res => {
    // We may fall into the scenario of a newer UI version talking to an older server
    // version that does not support the fetch_mode and limit request params. In that
    // case We will have to manually sort and truncate the data to align the UI with
    // the data returned.

    const isOldServer =
      res?.transactions?.length || res?.statements?.length > limit;

    if (isOldServer) {
      sortAndTruncateStmtsResponse(res, req?.fetch_mode?.sort, limit);
    }

    return res;
  });
};

// Mutates the sqlstats txns response to conform to the provided sort and limit params.
function sortAndTruncateTxnsResponse(
  res: SqlStatsResponse,
  sort: SqlStatsSortType,
  limit: number,
): void {
  switch (sort) {
    case SqlStatsSortOptions.SERVICE_LAT:
      res.transactions?.sort((txnA: Txn, txnB: Txn): number => {
        return (
          txnB.stats_data.stats.service_lat.mean -
          txnA.stats_data.stats.service_lat.mean
        );
      });
      break;
    case SqlStatsSortOptions.CONTENTION_TIME:
      res.transactions?.sort((txnA: Txn, txnB: Txn): number => {
        return (
          txnB.stats_data.stats.exec_stats.contention_time.mean -
          txnA.stats_data.stats.exec_stats.contention_time.mean
        );
      });
      break;
    case SqlStatsSortOptions.EXECUTION_COUNT:
      res.transactions?.sort((txnA: Txn, txnB: Txn): number => {
        return (
          txnB.stats_data.stats.count.toInt() -
          txnA.stats_data.stats.count.toInt()
        );
      });
      break;
    case SqlStatsSortOptions.PCT_RUNTIME:
    default:
      res.transactions?.sort((txnA: Txn, txnB: Txn): number => {
        return (
          txnB.stats_data.stats.service_lat.mean *
            txnB.stats_data.stats.count.toInt() -
          txnA.stats_data.stats.service_lat.mean *
            txnA.stats_data.stats.count.toInt()
        );
      });
  }

  // Finally, truncate the response not fitting into limit.
  res.transactions.splice(limit);

  const txnFingerprintsIDs = new Set(
    res.transactions.map(txn =>
      txn.stats_data.transaction_fingerprint_id?.toInt(),
    ),
  );

  // Filter out stmts not belonging to txns response.
  res.statements = res.statements.filter(stmt =>
    txnFingerprintsIDs.has(
      stmt.key.key_data.transaction_fingerprint_id?.toInt(),
    ),
  );
}

export const getFlushedTxnStatsApi = (
  req: StatementsRequest,
): Promise<SqlStatsResponse> => {
  const limit = req.limit?.toInt() ?? DEFAULT_STATS_REQ_OPTIONS.limit;

  const queryStr = propsToQueryString({
    start: req.start?.toInt(),
    end: req.end?.toInt(),
    "fetch_mode.stats_type": FetchStatsMode.TxnStatsOnly,
    "fetch_mode.sort": req.fetch_mode?.sort,
    limit,
  });

  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${STATEMENTS_PATH}?${queryStr}`,
    null,
    null,
    "10M",
  ).then(res => {
    // We may fall into the scenario of a newer UI version talking to an older server
    // version that does not support the fetch_mode and limit request params. In that
    // case We will have to manually sort and truncate the data to align the UI with
    // the data returned.

    const isOldServer = res?.transactions?.length > limit;

    if (isOldServer) {
      sortAndTruncateTxnsResponse(res, req.fetch_mode?.sort, limit);
    }

    return res;
  });
};

export const getStatementDetails = (
  req: StatementDetailsRequest,
): Promise<cockroach.server.serverpb.StatementDetailsResponse> => {
  let queryStr = propsToQueryString({
    start: req.start.toInt(),
    end: req.end.toInt(),
  });
  for (const app of req.app_names) {
    queryStr += `&appNames=${app}`;
  }
  return fetchData(
    cockroach.server.serverpb.StatementDetailsResponse,
    `${STATEMENT_DETAILS_PATH}/${req.fingerprint_id}?${queryStr}`,
    null,
    null,
    "30M",
  );
};
