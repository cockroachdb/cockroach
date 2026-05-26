// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { NodeID } from "src/types/clusterTypes";

export type DatabaseRow = {
  name: string;
  id: number;
  approximateDiskSizeBytes: number;
  tableCount: number;
  rangeCount: number;
  nodesByRegion: {
    isLoading: boolean;
    data: Record<string, NodeID[]>;
  };
  schemaInsightsCount: number;
  key: string;
};
