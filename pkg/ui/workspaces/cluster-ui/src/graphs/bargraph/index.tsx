// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import React, { useContext, useEffect, useRef } from "react";
import uPlot, { AlignedData, Options } from "uplot";

import { TimezoneContext } from "../../contexts";
import {
  AxisUnits,
  calculateXAxisDomainBarChart,
  calculateYAxisDomain,
} from "../utils/domain";
import { Visualization } from "../visualization";

import styles from "./bargraph.module.scss";
import { getStackedBarOpts, getGroupedStackedBarOpts, stack } from "./bars";
import { categoricalBarTooltipPlugin, BarMetadata } from "./plugins";

const cx = classNames.bind(styles);

export type { BarMetadata };

export type BarGraphTimeSeriesProps = {
  alignedData?: AlignedData;
  colourPalette?: string[]; // Series colour palette.
  preCalcGraphSize?: boolean;
  title: string;
  tooltip?: React.ReactNode;
  uPlotOptions: Partial<Options>;
  yAxisUnits: AxisUnits;
  xScale?: XScale;
};

export type XScale = {
  graphTsStartMillis: number;
  graphTsEndMillis: number;
};

// Currently this component only supports stacked multi-series bars.
// The value of xScale will take precedent over the start and end of the data provided.
export const BarGraphTimeSeries: React.FC<BarGraphTimeSeriesProps> = ({
  alignedData,
  colourPalette,
  preCalcGraphSize = true,
  title,
  tooltip,
  uPlotOptions,
  yAxisUnits,
  xScale,
}) => {
  const graphRef = useRef<HTMLDivElement>(null);
  const samplingIntervalMillis =
    alignedData[0].length > 1 ? alignedData[0][1] - alignedData[0][0] : 1e3;
  const timezone = useContext(TimezoneContext);

  useEffect(() => {
    if (!alignedData) return;

    const start = xScale.graphTsStartMillis
      ? xScale.graphTsStartMillis
      : alignedData[0][0];
    const end = xScale.graphTsEndMillis
      ? xScale.graphTsEndMillis
      : alignedData[0][alignedData[0].length - 1];
    const xAxisDomain = calculateXAxisDomainBarChart(
      start, // startMillis
      end, // endMillis
      samplingIntervalMillis,
      timezone,
    );

    const stackedData = stack(alignedData, () => false);

    const allYDomainPoints: number[] = [];
    stackedData.slice(1).forEach(points => allYDomainPoints.push(...points));
    const yAxisDomain = calculateYAxisDomain(yAxisUnits, allYDomainPoints);

    const opts = getStackedBarOpts(
      alignedData,
      uPlotOptions,
      xAxisDomain,
      yAxisDomain,
      yAxisUnits,
      colourPalette,
      timezone,
    );

    const plot = new uPlot(opts, stackedData, graphRef.current);

    return () => {
      plot?.destroy();
    };
  }, [
    alignedData,
    colourPalette,
    uPlotOptions,
    yAxisUnits,
    samplingIntervalMillis,
    timezone,
    xScale,
  ]);

  return (
    <Visualization
      title={title}
      loading={!alignedData}
      preCalcGraphSize={preCalcGraphSize}
      tooltip={tooltip}
    >
      <div className={cx("bargraph")}>
        <div ref={graphRef} />
      </div>
    </Visualization>
  );
};

// GroupedStackedBarGraphTimeSeries renders two groups of stacked bars
// side by side per timestamp. Data layout:
//   [timestamps, ...group1_series(N), ...group2_series(N)]
// groupSize specifies N (layers per group). Defaults to half the data series.
export type GroupedStackedBarGraphProps = {
  alignedData?: AlignedData;
  colourPalette?: string[];
  groupSize?: number;
  // Text labels drawn above each bar stack (e.g. ["Canary","Stable"]).
  // When set, the built-in uPlot legend is hidden.
  groupLabels?: [string, string];
  // Distinct border colours for group 1 / group 2 bars
  // (e.g. ["#c62828", "#1565c0"] for canary vs stable).
  groupStrokeColors?: [string, string];
  // Layer labels (e.g. plan gist IDs). When provided together with
  // groupLabels, the tooltip shows only the hovered sub-bar layer.
  gistLabels?: string[];
  preCalcGraphSize?: boolean;
  title: string;
  tooltip?: React.ReactNode;
  uPlotOptions: Partial<Options>;
  yAxisUnits: AxisUnits;
  xScale?: XScale;
};

export const GroupedStackedBarGraphTimeSeries: React.FC<
  GroupedStackedBarGraphProps
> = ({
  alignedData,
  colourPalette,
  gistLabels,
  groupLabels,
  groupSize,
  groupStrokeColors,
  preCalcGraphSize = true,
  title,
  tooltip,
  uPlotOptions,
  yAxisUnits,
  xScale,
}) => {
  const graphRef = useRef<HTMLDivElement>(null);
  const samplingIntervalMillis =
    alignedData[0].length > 1 ? alignedData[0][1] - alignedData[0][0] : 1e3;
  const timezone = useContext(TimezoneContext);

  useEffect(() => {
    if (!alignedData) return;

    const start = xScale.graphTsStartMillis
      ? xScale.graphTsStartMillis
      : alignedData[0][0];
    const end = xScale.graphTsEndMillis
      ? xScale.graphTsEndMillis
      : alignedData[0][alignedData[0].length - 1];
    const xAxisDomain = calculateXAxisDomainBarChart(
      start,
      end,
      samplingIntervalMillis,
      timezone,
    );

    // Compute y-axis domain from stacked tops of both groups.
    const n = groupSize ?? Math.floor((alignedData.length - 1) / 2);
    const group1Top = alignedData[1].map((_, j) => {
      let sum = 0;
      for (let i = 0; i < n; i++) sum += alignedData[1 + i][j];
      return sum;
    });
    const group2Top = alignedData[1].map((_, j) => {
      let sum = 0;
      for (let i = 0; i < n; i++) sum += alignedData[1 + n + i][j];
      return sum;
    });
    const yAxisDomain = calculateYAxisDomain(yAxisUnits, [
      ...group1Top,
      ...group2Top,
    ]);

    const { opts, stackedData } = getGroupedStackedBarOpts(
      alignedData,
      uPlotOptions,
      xAxisDomain,
      yAxisDomain,
      yAxisUnits,
      colourPalette,
      timezone,
      groupSize,
      groupStrokeColors,
      groupLabels,
      gistLabels,
    );

    // When groupLabels is set but the caller overrides the legend to be
    // visible (e.g. for the Statement Times or Plan Distribution chart),
    // hide group 2's duplicate legend entries so only one set of labels
    // appears. Visibility syncing between groups is handled by the
    // setSeries hook in getGroupedStackedBarOpts.
    if (groupLabels && opts.legend?.show) {
      const nn = groupSize ?? Math.floor((alignedData.length - 1) / 2);
      opts.plugins.push({
        hooks: {
          init: (u: uPlot) => {
            const rows = u.root.querySelectorAll(".u-legend .u-series");
            // Legend rows: 0=x-axis, 1..nn=group1, nn+1..2*nn=group2.
            // Hide the x-axis row and all group 2 (duplicate) rows so
            // only the layer entries from group 1 are shown.
            if (rows[0]) {
              (rows[0] as HTMLElement).style.display = "none";
            }
            for (let i = nn + 1; i <= 2 * nn; i++) {
              if (rows[i]) {
                (rows[i] as HTMLElement).style.display = "none";
              }
            }
            // Remove the group-stroke border from legend markers so
            // they show only the fill colour.
            for (let i = 1; i <= nn; i++) {
              const marker = rows[i]?.querySelector(
                ".u-marker",
              ) as HTMLElement | null;
              if (marker) {
                marker.style.borderColor = "transparent";
              }
            }
          },
        },
      });
    }

    const plot = new uPlot(opts, stackedData, graphRef.current);

    return () => {
      plot?.destroy();
    };
  }, [
    alignedData,
    colourPalette,
    gistLabels,
    groupLabels,
    groupSize,
    groupStrokeColors,
    uPlotOptions,
    yAxisUnits,
    samplingIntervalMillis,
    timezone,
    xScale,
  ]);

  return (
    <Visualization
      title={title}
      loading={!alignedData}
      preCalcGraphSize={preCalcGraphSize}
      tooltip={tooltip}
    >
      <div className={cx("bargraph")}>
        <div ref={graphRef} />
      </div>
    </Visualization>
  );
};

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
