// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { Moment } from "moment-timezone";

import { NodeID } from "src/types/clusterTypes";

export type TableRow = {
  qualifiedNameWithSchema: string;
  name: string;
  dbName: string;
  tableID: number;
  dbID: number;
  replicationSizeBytes: number;
  rangeCount: number;
  columnCount: number;
  nodesByRegion: Record<string, NodeID[]>;
  liveDataPercentage: number;
  liveDataBytes: number;
  totalDataBytes: number;
  statsLastUpdated: Moment;
  autoStatsEnabled: boolean;
  key: string;
};
