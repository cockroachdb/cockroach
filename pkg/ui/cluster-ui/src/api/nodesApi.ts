import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { fetchData } from "src/api";

const NODES_PATH = "/_status/nodes";

export const getNodes = (): Promise<cockroach.server.serverpb.NodesResponse> => {
  return fetchData(cockroach.server.serverpb.NodesResponse, NODES_PATH);
};
