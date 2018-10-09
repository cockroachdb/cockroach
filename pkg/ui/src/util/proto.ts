// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

import _ from "lodash";

import * as protos from "src/js/protos";

export type INodeStatus = protos.cockroach.server.status.INodeStatus;
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
export namespace MetricConstants {
  // Store level metrics.
  export const replicas: string = "replicas";
  export const raftLeaders: string = "replicas.leaders";
  export const leaseHolders: string = "replicas.leaseholders";
  export const ranges: string = "ranges";
  export const unavailableRanges: string = "ranges.unavailable";
  export const underReplicatedRanges: string  = "ranges.underreplicated";
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
  export const lastUpdateNano: string = "lastupdatenanos";
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
