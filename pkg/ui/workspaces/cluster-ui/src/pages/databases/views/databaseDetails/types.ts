// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { TableMetadata } from "src/api/databases/getTableMetadataApi";
import { NodeID } from "src/types/clusterTypes";

export type TableRow = TableMetadata & {
  qualifiedNameWithSchema: string;
  nodesByRegion: {
    isLoading: boolean;
    data: Record<string, NodeID[]>;
  };
  key: string;
};
