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

export type StatementsRequest =
  cockroach.server.serverpb.CombinedStatementsStatsRequest;
export type StatementDetailsRequest =
  cockroach.server.serverpb.StatementDetailsRequest;
export type StatementDetailsResponse =
  cockroach.server.serverpb.StatementDetailsResponse;
export type StatementDetailsResponseWithKey = {
  stmtResponse: StatementDetailsResponse;
  key: string;
};

export type SqlStatsResponse = cockroach.server.serverpb.StatementsResponse;

const FetchStatsMode =
  cockroach.server.serverpb.CombinedStatementsStatsRequest.StatsType;

export type ErrorWithKey = {
  err: Error;
  key: string;
};

export const SqlStatsSortOptions = cockroach.server.serverpb.StatsSortOptions;
export type SqlStatsSortType = cockroach.server.serverpb.StatsSortOptions;

export const DEFAULT_STATS_REQ_OPTIONS = {
  limit: 100,
  sort: SqlStatsSortOptions.SERVICE_LAT,
};

// The required fields to create a stmts request.
type StmtReqFields = {
  limit: number;
  sort: SqlStatsSortType;
  start: moment.Moment;
  end: moment.Moment;
};

export function createCombinedStmtsRequest({
  limit,
  sort,
  start,
  end,
}: StmtReqFields): StatementsRequest {
  return new cockroach.server.serverpb.CombinedStatementsStatsRequest({
    start: Long.fromNumber(start.unix()),
    end: Long.fromNumber(end.unix()),
    limit: Long.fromNumber(limit ?? DEFAULT_STATS_REQ_OPTIONS.limit),
    fetch_mode:
      new cockroach.server.serverpb.CombinedStatementsStatsRequest.FetchMode({
        sort: sort,
      }),
  });
}

export const getCombinedStatements = (
  req: StatementsRequest,
): Promise<SqlStatsResponse> => {
  const queryStr = propsToQueryString({
    start: req.start.toInt(),
    end: req.end.toInt(),
    "fetch_mode.stats_type": FetchStatsMode.StmtStatsOnly,
    "fetch_mode.sort": req.fetch_mode.sort,
    limit: req.limit.toInt(),
  });
  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${STATEMENTS_PATH}?${queryStr}`,
    null,
    null,
    "10M",
  );
};

export const getFlushedTxnStatsApi = (
  req: StatementsRequest,
): Promise<SqlStatsResponse> => {
  const queryStr = propsToQueryString({
    start: req.start.toInt(),
    end: req.end.toInt(),
    "fetch_mode.stats_type": FetchStatsMode.TxnStatsOnly,
    "fetch_mode.sort": req.fetch_mode?.sort,
    limit: req.limit.toInt() ?? DEFAULT_STATS_REQ_OPTIONS.limit,
  });
  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${STATEMENTS_PATH}?${queryStr}`,
    null,
    null,
    "10M",
  );
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
