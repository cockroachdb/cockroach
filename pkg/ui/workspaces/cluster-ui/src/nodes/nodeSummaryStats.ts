// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
