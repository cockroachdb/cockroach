// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React, { useEffect, useRef } from "react";
import uPlot, { AlignedData, Options } from "uplot";

import { AxisUnits, calculateYAxisDomain } from "../utils/domain";
import { Visualization } from "../visualization";

import styles from "./bargraph.module.scss";
import { categoricalBarTooltipPlugin, BarMetadata } from "./plugins";

const cx = classNames.bind(styles);

export type { BarMetadata };

export type BarGraphDataPoint = {
  label: string;
  value: number;
  databases?: string[];
  tables?: string[];
  indexes?: string[];
};

export type BarGraphProps = {
  data: Array<BarGraphDataPoint>;
  colourPalette?: string[];
  preCalcGraphSize?: boolean;
  title: string;
  tooltip?: React.ReactNode;
  yAxisUnits: AxisUnits;
};

// Simple bar graph component for categorical data
export const BarGraph: React.FC<BarGraphProps> = ({
  data,
  colourPalette = ["#2196F3"],
  preCalcGraphSize = true,
  title,
  tooltip,
  yAxisUnits,
}) => {
  const graphRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!data || data.length === 0) return;

    const xValues = data.map((_, i) => i);
    const yValues = data.map(d => d.value);
    const plotData: AlignedData = [xValues, yValues];

    const yAxisDomain = calculateYAxisDomain(yAxisUnits, yValues);

    // Extract labels and metadata for tooltip
    const labels = data.map(d => d.label);
    const metadata: BarMetadata[] = data.map(d => ({
      databases: d.databases,
      tables: d.tables,
      indexes: d.indexes,
    }));

    // Standard uPlot bar chart configuration
    const opts: Options = {
      id: "chart",
      class: cx("bargraph"),
      width: 800,
      height: 450,
      legend: {
        show: false,
      },
      cursor: {
        points: {
          show: false,
        },
      },
      series: [
        {},
        {
          label: "Value",
          fill: colourPalette[0],
          stroke: colourPalette[0],
          paths: uPlot.paths.bars({ size: [0.9, 80] }),
          points: { show: false },
        },
      ],
      axes: [
        {
          // X-axis: categorical labels
          values: (_u, splits) => splits.map(i => labels[Math.round(i)] || ""),
        },
        {
          // Y-axis: numeric values with units
          label: yAxisDomain.label,
          values: (_u, vals) =>
            vals.map(v => yAxisDomain.tickFormat(v as number)),
          splits: () => [
            yAxisDomain.extent[0],
            ...yAxisDomain.ticks,
            yAxisDomain.extent[1],
          ],
        },
      ],
      scales: {
        x: {
          range: () => [-0.5, data.length - 0.5],
        },
        y: {
          range: () => [yAxisDomain.extent[0], yAxisDomain.extent[1]],
        },
      },
      plugins: [categoricalBarTooltipPlugin(yAxisUnits, labels, metadata)],
    };

    const plot = new uPlot(opts, plotData, graphRef.current);

    return () => {
      plot?.destroy();
    };
  }, [data, yAxisUnits, colourPalette]);

  return (
    <Visualization
      title={title}
      loading={!data || data.length === 0}
      preCalcGraphSize={preCalcGraphSize}
      tooltip={tooltip}
    >
      <div className={cx("bargraph")}>
        <div ref={graphRef} />
      </div>
    </Visualization>
  );
};
