// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

import { fetchData } from "src/api";

const RESET_SQL_STATS_PATH = "_status/resetsqlstats";

export const resetSQLStats =
  (): Promise<cockroach.server.serverpb.ResetSQLStatsResponse> => {
    return fetchData(
      cockroach.server.serverpb.ResetSQLStatsResponse,
      RESET_SQL_STATS_PATH,
      cockroach.server.serverpb.ResetSQLStatsRequest,
      new cockroach.server.serverpb.ResetSQLStatsRequest({
        // reset_persisted_stats is set to true in order to clear both
        // in-memory stats as well as persisted stats.
        reset_persisted_stats: true,
      }),
    );
  };
