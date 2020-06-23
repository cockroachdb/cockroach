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

import * as protos from "../js/protos";

export type INodeStatus = protos.cockroach.server.status.statuspb.INodeStatus;
const nodeStatus: INodeStatus = null;
export type StatusMetrics = typeof nodeStatus.metrics;

/**
 * AccumulateMetrics is a convenience function which accumulates the values
 * in multiple metrics collections. Values from all provided StatusMetrics
 * collections are accumulated into the first StatusMetrics collection
 * passed.
 */
export function AccumulateMetrics(dest: StatusMetrics, ...srcs: StatusMetrics[]): void {
  srcs.forEach((s: StatusMetrics) => {
    _.forEach(s, (val: number, key: string) => {
      if (_.has(dest, key)) {
        dest[key] =  dest[key] + val;
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
  AccumulateMetrics(ns.metrics, ..._.map(ns.store_statuses, (ss) => ss.metrics));
}

/**
 * MetricConstants contains the name of several stats provided by
 * CockroachDB.
 */
export const MetricConstants = {
  // Store level metrics.
  replicas: "replicas",
  raftLeaders: "replicas.leaders",
  leaseHolders: "replicas.leaseholders",
  ranges: "ranges",
  unavailableRanges: "ranges.unavailable",
  underReplicatedRanges: "ranges.underreplicated",
  liveBytes: "livebytes",
  keyBytes: "keybytes",
  valBytes: "valbytes",
  totalBytes: "totalbytes",
  intentBytes: "intentbytes",
  liveCount: "livecount",
  keyCount: "keycount",
  valCount: "valcount",
  intentCount: "intentcount",
  intentAge: "intentage",
  gcBytesAge: "gcbytesage",
  lastUpdateNano: "lastupdatenanos",
  capacity: "capacity",
  availableCapacity: "capacity.available",
  usedCapacity: "capacity.used",
  sysBytes: "sysbytes",
  sysCount: "syscount",

  // Node level metrics.
  userCPUPercent: "sys.cpu.user.percent",
  sysCPUPercent: "sys.cpu.sys.percent",
  allocBytes: "sys.go.allocbytes",
  sqlConns: "sql.conns",
  rss: "sys.rss",
}

/**
 * TotalCPU computes the total CPU usage accounted for in a NodeStatus.
 */
export function TotalCpu(status: INodeStatus): number {
  const metrics = status.metrics;
  return metrics[MetricConstants.sysCPUPercent] + metrics[MetricConstants.userCPUPercent];
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
