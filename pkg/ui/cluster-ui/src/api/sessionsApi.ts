import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";

const SESSIONS_PATH = "/_status/sessions";

export type SessionsRequestMessage = cockroach.server.serverpb.ListSessionsRequest;
export type SessionsResponseMessage = cockroach.server.serverpb.ListSessionsResponse;

// getSessions gets all cluster sessions.
export const getSessions = (): Promise<SessionsResponseMessage> => {
  return fetchData(
    cockroach.server.serverpb.ListSessionsResponse,
    SESSIONS_PATH,
  );
};
