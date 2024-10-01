// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import forEach from "lodash/forEach";
import has from "lodash/has";
import map from "lodash/map";
import sumBy from "lodash/sumBy";

import * as protos from "src/js/protos";
import { cockroach } from "src/js/protos";

import INodeResponse = cockroach.server.serverpb.INodeResponse;

export type INodeStatus = protos.cockroach.server.status.statuspb.INodeStatus;
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
    forEach(s, (val: number, key: string) => {
      if (has(dest, key)) {
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
export function RollupStoreMetrics(ns: INodeResponse): void {
  AccumulateMetrics(ns.metrics, ...map(ns.store_statuses, ss => ss.metrics));
}

/**
 * MetricConstants contains the name of several stats provided by
 * CockroachDB.
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
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
  export const rangeKeyBytes = "rangekeybytes";
  export const rangeValBytes = "rangevalbytes";
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
  return sumBy(aggregateByteKeys, (key: string) => {
    return s.metrics[key];
  });
}
