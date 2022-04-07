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
import { propsToQueryString } from "src/util";

const STATEMENTS_PATH = "/_status/statements";
const STATEMENT_DETAILS_PATH = "/_status/stmtdetails";

export type StatementsRequest = cockroach.server.serverpb.StatementsRequest;
export type StatementDetailsRequest = cockroach.server.serverpb.StatementDetailsRequest;
export type StatementDetailsResponse = cockroach.server.serverpb.StatementDetailsResponse;
export type StatementDetailsResponseWithKey = {
  stmtResponse: StatementDetailsResponse;
  key: string;
};
export type ErrorWithKey = {
  err: Error;
  key: string;
};

export const getCombinedStatements = (
  req: StatementsRequest,
): Promise<cockroach.server.serverpb.StatementsResponse> => {
  const queryStr = propsToQueryString({
    start: req.start.toInt(),
    end: req.end.toInt(),
    combined: req.combined,
  });
  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${STATEMENTS_PATH}?${queryStr}`,
    null,
    null,
    "30M",
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
