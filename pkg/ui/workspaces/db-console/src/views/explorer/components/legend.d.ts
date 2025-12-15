// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { ScaleSequential } from "d3-scale";

interface LegendOptions {
  title?: string;
  tickSize?: number;
  width?: number;
  height?: number;
  marginTop?: number;
  marginRight?: number;
  marginBottom?: number;
  marginLeft?: number;
  ticks?: number;
  tickFormat?: string | ((d: any, i?: number) => string);
  tickValues?: any[];
}

export function Legend(
  color: ScaleSequential<string> | any,
  options?: LegendOptions,
): SVGSVGElement;
