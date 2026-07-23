// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

export interface GroupedBarLayer {
  label: string;
  value: number;
  color: string;
}

export interface GroupedBarGroup {
  label: string;
  layers: GroupedBarLayer[];
}

export interface GroupedBarDatum {
  timestamp: number; // millis since epoch
  groups: GroupedBarGroup[];
}

export type GroupedBarData = GroupedBarDatum[];
