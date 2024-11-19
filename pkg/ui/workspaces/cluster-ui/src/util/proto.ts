// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.
import { cockroach } from "@cockroachlabs/crdb-protobuf-client";
import has from "lodash/has";
import sumBy from "lodash/sumBy";

export type INodeStatus = cockroach.server.status.statuspb.INodeStatus;
const nodeStatus: INodeStatus = null;
export type StatusMetrics = typeof nodeStatus.metrics;

/**
 * rollupStoreMetrics extends and aggregates INodeStatus.metrics object
 * with metrics from `store_statuses.metrics` object.
 */
export function rollupStoreMetrics(ns: INodeStatus): StatusMetrics {
  if (!ns.store_statuses) {
    return ns.metrics || {};
  }
  return ns.store_statuses
    .map(ss => ss.metrics)
    .reduce((acc, i) => {
      for (const k in i) {
        acc[k] = has(acc, k) ? acc[k] + i[k] : i[k];
      }
      return acc;
    }, ns.metrics);
}

export function accumulateMetrics(nodeStatuses: INodeStatus[]): INodeStatus[] {
  return (nodeStatuses || []).map(ns => ({
    ...ns,
    metrics: rollupStoreMetrics(ns),
  }));
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
  return sumBy(aggregateByteKeys, (key: string) => {
    return s.metrics[key];
  });
}
