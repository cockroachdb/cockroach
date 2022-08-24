// Copyright 2022 The Cockroach Authors.
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

export type TableIndexStatsRequest =
  cockroach.server.serverpb.TableIndexStatsRequest;
export type TableIndexStatsResponse =
  cockroach.server.serverpb.TableIndexStatsResponse;
export type TableIndexStatsResponseWithKey = {
  indexStatsResponse: TableIndexStatsResponse;
  key: string;
};

type ResetIndexUsageStatsRequest =
  cockroach.server.serverpb.ResetIndexUsageStatsRequest;
type ResetIndexUsageStatsResponse =
  cockroach.server.serverpb.ResetIndexUsageStatsResponse;

// getIndexStats gets detailed stats about the current table's index usage statistics.
export const getIndexStats = (
  req: TableIndexStatsRequest,
): Promise<TableIndexStatsResponse> => {
  return fetchData(
    cockroach.server.serverpb.TableIndexStatsResponse,
    `/_status/databases/${req.database}/tables/${req.table}/indexstats`,
    null,
    null,
    "30M",
  );
};

// resetIndexStats refreshes all index usage stats for all tables.
export const resetIndexStats = (
  req: ResetIndexUsageStatsRequest,
): Promise<ResetIndexUsageStatsResponse> => {
  return fetchData(
    cockroach.server.serverpb.ResetIndexUsageStatsResponse,
    "/_status/resetindexusagestats",
    null,
    req,
    "30M",
  );
};
