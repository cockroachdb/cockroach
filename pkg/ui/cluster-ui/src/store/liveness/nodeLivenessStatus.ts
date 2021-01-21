import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

export const NodeLivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
