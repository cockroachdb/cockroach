import _ from "lodash";

import { cockroach } from "@cockroachlabs/crdb-protobuf-client";

export type INodeStatus = cockroach.server.status.statuspb.INodeStatus;
const nodeStatus: INodeStatus = null;
export type StatusMetrics = typeof nodeStatus.metrics;

/**
 * AccumulateMetrics is a convenience function which accumulates the values
 * in multiple metrics collections. Values from all provided StatusMetrics
 * collections are accumulated into the first StatusMetrics collection
 * passed.
 */
export function AccumulateMetrics(
  dest: StatusMetrics,
  ...srcs: StatusMetrics[]
): void {
  srcs.forEach((s: StatusMetrics) => {
    _.forEach(s, (val: number, key: string) => {
      if (_.has(dest, key)) {
        dest[key] = dest[key] + val;
      } else {
        dest[key] = val;
      }
    });
  });
}

/**
 * RollupStoreMetrics accumulates all store-level metrics into the top level
 * metrics collection of the supplied NodeStatus object. This is convenient
 * for all current usages of NodeStatus in the UI.
 */
export function RollupStoreMetrics(ns: INodeStatus): void {
  AccumulateMetrics(ns.metrics, ..._.map(ns.store_statuses, ss => ss.metrics));
}

/**
 * MetricConstants contains the name of several stats provided by
 * CockroachDB.
 */
export namespace MetricConstants {
  // Store level metrics.
  export const replicas = "replicas";
  export const raftLeaders = "replicas.leaders";
  export const leaseHolders = "replicas.leaseholders";
  export const ranges = "ranges";
  export const unavailableRanges = "ranges.unavailable";
  export const underReplicatedRanges = "ranges.underreplicated";
  export const liveBytes = "livebytes";
  export const keyBytes = "keybytes";
  export const valBytes = "valbytes";
  export const totalBytes = "totalbytes";
  export const intentBytes = "intentbytes";
  export const liveCount = "livecount";
  export const keyCount = "keycount";
  export const valCount = "valcount";
  export const intentCount = "intentcount";
  export const intentAge = "intentage";
  export const gcBytesAge = "gcbytesage";
  export const capacity = "capacity";
  export const availableCapacity = "capacity.available";
  export const usedCapacity = "capacity.used";
  export const sysBytes = "sysbytes";
  export const sysCount = "syscount";

  // Node level metrics.
  export const userCPUPercent = "sys.cpu.user.percent";
  export const sysCPUPercent = "sys.cpu.sys.percent";
  export const allocBytes = "sys.go.allocbytes";
  export const sqlConns = "sql.conns";
  export const rss = "sys.rss";
}

/**
 * TotalCPU computes the total CPU usage accounted for in a NodeStatus.
 */
export function TotalCpu(status: INodeStatus): number {
  const metrics = status.metrics;
  return (
    metrics[MetricConstants.sysCPUPercent] +
    metrics[MetricConstants.userCPUPercent]
  );
}

/**
 * BytesUsed computes the total byte usage accounted for in a NodeStatus.
 */
const aggregateByteKeys = [
  MetricConstants.liveBytes,
  MetricConstants.intentBytes,
  MetricConstants.sysBytes,
];

export function BytesUsed(s: INodeStatus): number {
  const usedCapacity = s.metrics[MetricConstants.usedCapacity];
  if (usedCapacity !== 0) {
    return usedCapacity;
  }
  return _.sumBy(aggregateByteKeys, (key: string) => {
    return s.metrics[key];
  });
}
