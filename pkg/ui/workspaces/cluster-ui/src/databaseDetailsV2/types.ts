// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import { Moment } from "moment-timezone";

import { NodeID } from "src/types/clusterTypes";

export type TableRow = {
  qualifiedNameWithSchema: string;
  name: string;
  dbName: string;
  dbID: number;
  replicationSizeBytes: number;
  rangeCount: number;
  columnCount: number;
  nodesByRegion: Record<string, NodeID[]>;
  liveDataPercentage: number;
  liveDataBytes: number;
  totalDataBytes: number;
  statsLastUpdated: Moment;
  key: string;
};
