// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { useCallback } from "react";
import { useSWRConfig } from "swr";

import { fetchData } from "src/api";

import {
  STATEMENTS_SWR_KEY,
  STATEMENT_DETAILS_SWR_KEY,
  TRANSACTIONS_SWR_KEY,
} from "./statementsApi";

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

// SWR key prefixes for cache invalidation after reset.
// Resetting SQL stats clears both statement and transaction data, so all
// related SWR caches must be revalidated.
const INVALIDATED_KEY_NAMES = [
  STATEMENTS_SWR_KEY,
  STATEMENT_DETAILS_SWR_KEY,
  TRANSACTIONS_SWR_KEY,
];

export function useResetSQLStats() {
  const { mutate } = useSWRConfig();

  const reset = useCallback(async () => {
    await resetSQLStats();
    await mutate(
      (key: unknown) => {
        if (key && typeof key === "object" && "name" in key) {
          return INVALIDATED_KEY_NAMES.includes((key as { name: string }).name);
        }
        return false;
      },
      undefined,
      { revalidate: true },
    );
  }, [mutate]);

  return { reset };
}
