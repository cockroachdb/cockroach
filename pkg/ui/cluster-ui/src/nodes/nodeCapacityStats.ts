import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import { MetricConstants } from "../util/proto";

type INodeStatus = cockroach.server.status.statuspb.INodeStatus;

export interface CapacityStats {
  used: number;
  usable: number;
  available: number;
}

export function nodeCapacityStats(n: INodeStatus): CapacityStats {
  const used = n.metrics[MetricConstants.usedCapacity];
  const available = n.metrics[MetricConstants.availableCapacity];
  return {
    used,
    available,
    usable: used + available,
  };
}
