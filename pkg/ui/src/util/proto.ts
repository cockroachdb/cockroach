// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import _ from "lodash";

import * as protos from "src/js/protos";

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
  AccumulateMetrics(
    ns.metrics,
    ..._.map(ns.store_statuses, (ss) => ss.metrics),
  );
}

/**
 * MetricConstants contains the name of several stats provided by
 * CockroachDB.
 */
// eslint-disable-next-line @typescript-eslint/no-namespace
export namespace MetricConstants {
  // Store level metrics.
  export const replicas: string = "replicas";
  export const raftLeaders: string = "replicas.leaders";
  export const leaseHolders: string = "replicas.leaseholders";
  export const ranges: string = "ranges";
  export const unavailableRanges: string = "ranges.unavailable";
  export const underReplicatedRanges: string = "ranges.underreplicated";
  export const liveBytes: string = "livebytes";
  export const keyBytes: string = "keybytes";
  export const valBytes: string = "valbytes";
  export const totalBytes: string = "totalbytes";
  export const intentBytes: string = "intentbytes";
  export const liveCount: string = "livecount";
  export const keyCount: string = "keycount";
  export const valCount: string = "valcount";
  export const intentCount: string = "intentcount";
  export const intentAge: string = "intentage";
  export const gcBytesAge: string = "gcbytesage";
  export const capacity: string = "capacity";
  export const availableCapacity: string = "capacity.available";
  export const usedCapacity: string = "capacity.used";
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
