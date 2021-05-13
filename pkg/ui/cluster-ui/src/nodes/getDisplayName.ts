import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { NoConnection } from "./noConnection";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;

const LivenessStatus =
  cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;

function isNoConnection(
  node: INodeStatus | NoConnection,
): node is NoConnection {
  return (
    (node as NoConnection).to !== undefined &&
    (node as NoConnection).from !== undefined
  );
}

export function getDisplayName(
  node: INodeStatus | NoConnection,
  livenessStatus = LivenessStatus.NODE_STATUS_LIVE,
) {
  const decommissionedString =
    livenessStatus === LivenessStatus.NODE_STATUS_DECOMMISSIONED
      ? "[decommissioned] "
      : "";

  if (isNoConnection(node)) {
    return `${decommissionedString}(n${node.from.nodeID})`;
  }
  // as the only other type possible right now is INodeStatus we don't have a type guard for that
  return `${decommissionedString}(n${node.desc.node_id}) ${node.desc.address.address_field}`;
}
