import _ from "lodash";

type NodeStatus = Proto2TypeScript.cockroach.server.status.NodeStatus;
type StatusMetrics = Proto2TypeScript.ProtoBufMap<string, number>;

/**
 * AccumulateMetrics is a convenience function which accumulates the values
 * in multiple metrics collections. Values from all provided StatusMetrics
 * collections are accumulated into the first StatusMetrics collection
 * passed.
 */
export function AccumulateMetrics(dest: StatusMetrics, ...srcs: StatusMetrics[]): void {
  srcs.forEach((s: StatusMetrics) => {
    s.forEach((val: number, key: string) => {
      if (dest.has(key)) {
        dest.set(key, dest.get(key) + val);
      } else {
        dest.set(key, val);
      }
    });
  });
}

/**
 * RollupStoreMetrics accumulates all store-level metrics into the top level
 * metrics collection of the supplied NodeStatus object. This is convenient
 * for all current usages of NodeStatus in the UI.
 */
export function RollupStoreMetrics(ns: NodeStatus): void {
  "use strict";
  AccumulateMetrics(ns.metrics, ..._.map(ns.store_statuses, (ss) => ss.metrics));
}

/**
 * MetricConstants contains the name of several stats provided by
 * CockroachDB.
 */
export namespace MetricConstants {
  // Store level metrics.
  export const replicas: string = "replicas";
  export const raftLeaders: string = "replicas.leaders";
  export const leaseHolders: string = "replicas.leaseholders";
  export const availableRanges: string = "ranges.available";
  export const replicatedRanges: string  = "ranges.allocator.noop";
  export const liveBytes: string = "livebytes";
  export const keyBytes: string = "keybytes";
  export const valBytes: string = "valbytes";
  export const intentBytes: string = "intentbytes";
  export const liveCount: string = "livecount";
  export const keyCount: string = "keycount";
  export const valCount: string = "valcount";
  export const intentCount: string = "intentcount";
  export const intentAge: string = "intentage";
  export const gcBytesAge: string = "gcbytesage";
  export const lastUpdateNano: string = "lastupdatenanos";
  export const capacity: string = "capacity";
  export const availableCapacity: string = "capacity.available";
  export const sysBytes: string = "sysbytes";
  export const sysCount: string = "syscount";

  // Node level metrics.
  export const userCPUPercent: string = "sys.cpu.user.percent";
  export const sysCPUPercent: string = "sys.cpu.sys.percent";
  export const allocBytes: string = "sys.go.allocbytes";
  export const sqlConns: string = "sql.conns";
  export const rss: string = "sys.rss";
}

/**
 * TotalCPU computes the total CPU usage accounted for in a NodeStatus.
 */
export function TotalCpu(status: NodeStatus): number {
  let metrics = status.metrics;
  return metrics.get(MetricConstants.sysCPUPercent) + metrics.get(MetricConstants.userCPUPercent);
}

/**
 * BytesUsed computes the total byte usage accounted for in a NodeStatus.
 */
let aggregateByteKeys = [
  MetricConstants.liveBytes,
  MetricConstants.intentBytes,
  MetricConstants.sysBytes,
];

export function BytesUsed(s: NodeStatus): number {
  return _.sumBy(aggregateByteKeys, (key: string) => {
    return s.metrics.get(key);
  });
};

export { NodeStatus };
