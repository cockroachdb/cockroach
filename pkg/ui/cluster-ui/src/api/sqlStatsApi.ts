import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";

const RESET_SQL_STATS_PATH = "/_status/resetsqlstats";

export const resetSQLStats = (): Promise<cockroach.server.serverpb.ResetSQLStatsResponse> => {
  return fetchData(
    cockroach.server.serverpb.ResetSQLStatsResponse,
    RESET_SQL_STATS_PATH,
    cockroach.server.serverpb.ResetSQLStatsRequest,
  );
};
