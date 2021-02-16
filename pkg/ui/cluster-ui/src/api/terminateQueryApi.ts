import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";

const STATUS_PREFIX = "/_status";

export type CancelSessionRequestMessage = cockroach.server.serverpb.CancelSessionRequest;
export type CancelSessionResponseMessage = cockroach.server.serverpb.CancelSessionResponse;
export type CancelQueryRequestMessage = cockroach.server.serverpb.CancelQueryRequest;
export type CancelQueryResponseMessage = cockroach.server.serverpb.CancelQueryResponse;

export const terminateSession = (
  req: CancelSessionRequestMessage,
): Promise<CancelSessionResponseMessage> => {
  return fetchData(
    cockroach.server.serverpb.CancelSessionResponse,
    `${STATUS_PREFIX}/cancel_session/${req.node_id}`,
    cockroach.server.serverpb.CancelSessionRequest,
    req,
  );
};

export const terminateQuery = (
  req: CancelQueryRequestMessage,
): Promise<CancelQueryResponseMessage> => {
  return fetchData(
    cockroach.server.serverpb.CancelQueryResponse,
    `${STATUS_PREFIX}/cancel_query/${req.node_id}`,
    cockroach.server.serverpb.CancelQueryRequest,
    req,
  );
};
