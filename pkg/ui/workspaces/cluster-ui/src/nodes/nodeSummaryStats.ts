// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

export type NodeSummaryStats = {
  nodeCounts: {
    total: number;
    healthy: number;
    suspect: number;
    dead: number;
    decommissioned: number;
  };
  capacityUsed: number;
  capacityAvailable: number;
  capacityTotal: number;
  capacityUsable: number;
  usedBytes: number;
  usedMem: number;
  totalRanges: number;
  underReplicatedRanges: number;
  unavailableRanges: number;
  replicas: number;
};
