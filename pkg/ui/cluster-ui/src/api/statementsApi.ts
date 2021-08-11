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
import { fetchData, propsToQueryString } from "src/api";

const STATEMENTS_PATH = "/_status/statements";

const COMBINED_STATEMENTS_PATH = "/_status/combinedstmts";

export const getStatements = (): Promise<cockroach.server.serverpb.StatementsResponse> => {
  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    STATEMENTS_PATH,
  );
};

export interface CombinedStatementsRequest {
  start?: number;
  end?: number;
}

export const getCombinedStatements = (
  req: CombinedStatementsRequest,
): Promise<cockroach.server.serverpb.StatementsResponse> => {
  const queryStr = propsToQueryString(req);
  return fetchData(
    cockroach.server.serverpb.StatementsResponse,
    `${COMBINED_STATEMENTS_PATH}?${queryStr}`,
  );
};
