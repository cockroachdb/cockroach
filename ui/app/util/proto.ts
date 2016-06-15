import _ = require("lodash");

type NodeStatus = cockroach.server.status.NodeStatus;
type StatusMetrics = cockroach.ProtoBufMap<string, number>;

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
  export var replicas: string = "replicas";
  export var leaderRanges: string = "ranges.leader";
  export var replicatedRanges: string = "ranges.replicated";
  export var availableRanges: string = "ranges.available";
  export var liveBytes: string = "livebytes";
  export var keyBytes: string = "keybytes";
  export var valBytes: string = "valbytes";
  export var intentBytes: string = "intentbytes";
  export var liveCount: string = "livecount";
  export var keyCount: string = "keycount";
  export var valCount: string = "valcount";
  export var intentCount: string = "intentcount";
  export var intentAge: string = "intentage";
  export var gcBytesAge: string = "gcbytesage";
  export var lastUpdateNano: string = "lastupdatenanos";
  export var capacity: string = "capacity";
  export var availableCapacity: string = "capacity.available";
  export var sysBytes: string = "sysbytes";
  export var sysCount: string = "syscount";

  // Node level metrics.
  export var userCPUPercent: string = "sys.cpu.user.percent";
  export var sysCPUPercent: string = "sys.cpu.sys.percent";
  export var allocBytes: string = "sys.go.allocbytes";
  export var sqlConns: string = "sql.conns";
  export var rss: string = "sys.rss";
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
