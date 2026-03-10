// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import merge from "lodash/merge";
import uPlot, { Options, Band, AlignedData } from "uplot";

import { AxisUnits, AxisDomain } from "../utils/domain";

import { barTooltipPlugin } from "./plugins";

const seriesPalette = [
  "#003EBD",
  "#2AAF44",
  "#F16969",
  "#4E9FD1",
  "#49D990",
  "#D77FBF",
  "#87326D",
  "#A3415B",
  "#B59153",
  "#C9DB6D",
  "#475872",
  "#748BF2",
  "#91C8F2",
  "#FF9696",
  "#EF843C",
  "#DCCD4B",
];

// Aggregate the series.
export function stack(
  data: AlignedData,
  omit: (i: number) => boolean,
): AlignedData {
  const stackedData = [];
  const xAxisLength = data[0].length;
  const accum = Array(xAxisLength);
  accum.fill(0);

  for (let i = 1; i < data.length; i++)
    stackedData.push(
      omit(i) ? data[i] : data[i].map((v, i) => (accum[i] += v)),
    );

  return [data[0]].concat(stackedData) as AlignedData;
}

function getStackedBands(
  unstackedData: AlignedData,
  omit: (i: number) => boolean,
): Band[] {
  const bands = [];

  for (let i = 1; i < unstackedData.length; i++)
    !omit(i) &&
      bands.push({
        series: [
          unstackedData.findIndex(
            (_series, seriesIdx) => seriesIdx > i && !omit(seriesIdx),
          ),
          i,
        ] as Band.Bounds,
      });

  return bands.filter(b => b.series[1] > -1);
}

const { bars } = uPlot.paths;

export const getBarsBuilder = (
  barWidthFactor: number, // percentage of space allocated to bar in the range [0, 1]
  maxWidth: number,
  minWidth = 10,
  align: 0 | 1 | -1 = 1, // -1 = left aligned, 0 = center, 1 = right aligned
): uPlot.Series.PathBuilder => {
  return bars({ size: [barWidthFactor, maxWidth, minWidth], align });
};

export const getStackedBarOpts = (
  unstackedData: AlignedData,
  userOptions: Partial<Options>,
  xAxisDomain: AxisDomain,
  yAxisDomain: AxisDomain,
  yyAxisUnits: AxisUnits,
  colourPalette = seriesPalette,
  timezone: string,
): Options => {
  const options = getBarChartOpts(
    userOptions,
    xAxisDomain,
    yAxisDomain,
    yyAxisUnits,
    colourPalette,
    timezone,
  );

  options.bands = getStackedBands(unstackedData, () => false);

  options.series.forEach(s => {
    s.value = (_u, _v, si, i) => unstackedData[si][i];

    s.points = s.points || { show: false };

    // Scan raw unstacked data to return only real points.
    s.points.filter = (_u, seriesIdx, show) => {
      if (show) {
        const pts: number[] = [];
        unstackedData[seriesIdx].forEach((v, i) => {
          v && pts.push(i);
        });
        return pts;
      }
    };
  });

  options.cursor = options.cursor || {};
  options.cursor.dataIdx = (_u, seriesIdx, closestIdx, _xValue) => {
    return unstackedData[seriesIdx][closestIdx] == null ? null : closestIdx;
  };

  options.hooks = options.hooks || {};
  options.hooks.setSeries = options.hooks.setSeries || [];
  options.hooks.setSeries.push(u => {
    // Restack on toggle.
    const bands = getStackedBands(unstackedData, i => !u.series[i].show);
    const data = stack(unstackedData, i => !u.series[i].show);
    u.delBand(null); // Clear bands.
    bands.forEach(b => u.addBand(b));
    u.setData(data);
  });

  return options;
};

// getGroupedStackedBarOpts creates options for a chart with two groups of
// stacked bars side by side. The data series are expected in order:
// [timestamps, group1_bottom, group1_top, group2_bottom, group2_top].
// group1 bars are left-aligned, group2 bars are right-aligned.
export const getGroupedStackedBarOpts = (
  unstackedData: AlignedData,
  userOptions: Partial<Options>,
  xAxisDomain: AxisDomain,
  yAxisDomain: AxisDomain,
  yAxisUnits: AxisUnits,
  colourPalette = seriesPalette,
  timezone: string,
): { opts: Options; stackedData: AlignedData } => {
  const { series, ...providedOpts } = userOptions;
  const leftBars = getBarsBuilder(0.4, 40, 4, -1);
  const rightBars = getBarsBuilder(0.4, 40, 4, 1);

  const opts: Options = {
    width: 947,
    height: 300,
    ms: 1,
    legend: {
      isolate: true,
      live: false,
    },
    scales: {
      x: {
        range: () => [xAxisDomain.extent[0], xAxisDomain.extent[1]],
      },
      yAxis: {
        range: () => [yAxisDomain.extent[0], yAxisDomain.extent[1]],
      },
    },
    axes: [
      {
        values: (_u, vals) => vals.map(xAxisDomain.tickFormat),
        splits: () => xAxisDomain.ticks,
      },
      {
        values: (_u, vals) => vals.map(yAxisDomain.tickFormat),
        splits: () => [
          yAxisDomain.extent[0],
          ...yAxisDomain.ticks,
          yAxisDomain.extent[1],
        ],
        scale: "yAxis",
      },
    ],
    series: [
      {
        value: (_u, millis) => xAxisDomain.guideFormat(millis),
      },
      // Group 1 (canary): left-aligned bars
      ...series.slice(1, 3).map((s, i) => ({
        fill: colourPalette[i % colourPalette.length],
        stroke: colourPalette[i % colourPalette.length],
        width: 2,
        paths: leftBars,
        points: { show: false },
        scale: "yAxis",
        ...s,
      })),
      // Group 2 (stable): right-aligned bars
      ...series.slice(3, 5).map((s, i) => ({
        fill: colourPalette[(i + 2) % colourPalette.length],
        stroke: colourPalette[(i + 2) % colourPalette.length],
        width: 2,
        paths: rightBars,
        points: { show: false },
        scale: "yAxis",
        ...s,
      })),
    ],
    plugins: [barTooltipPlugin(yAxisUnits, timezone)],
  };

  const combinedOpts = merge(opts, providedOpts);
  combinedOpts.axes[1].label += ` ${yAxisDomain.label}`;

  // Pre-stack each group independently: series 2 += series 1, series 4 += series 3.
  const stackedData: AlignedData = [
    unstackedData[0],
    unstackedData[1],
    unstackedData[1].map((v, i) => v + unstackedData[2][i]),
    unstackedData[3],
    unstackedData[3].map((v, i) => v + unstackedData[4][i]),
  ];

  // Bands fill between the top and bottom of each group.
  combinedOpts.bands = [
    { series: [2, 1] as Band.Bounds }, // canary: execution on top of planning
    { series: [4, 3] as Band.Bounds }, // stable: execution on top of planning
  ];

  // Show unstacked values in tooltip/legend.
  combinedOpts.series.forEach((s, si) => {
    s.value = (_u, _v, _si, i) => unstackedData[si]?.[i];
    s.points = s.points || { show: false };
    s.points.filter = (_u, seriesIdx, show) => {
      if (show) {
        const pts: number[] = [];
        unstackedData[seriesIdx]?.forEach((v, i) => {
          v && pts.push(i);
        });
        return pts;
      }
    };
  });

  return { opts: combinedOpts, stackedData };
};

export const getBarChartOpts = (
  userOptions: Partial<Options>,
  xAxisDomain: AxisDomain,
  yAxisDomain: AxisDomain,
  yAxisUnits: AxisUnits,
  colourPalette = seriesPalette,
  timezone: string,
): Options => {
  const { series, ...providedOpts } = userOptions;
  const defaultBars = getBarsBuilder(0.9, 80);

  const opts: Options = {
    // Default width and height.
    width: 947,
    height: 300,
    ms: 1, // Interpret timestamps in milliseconds.
    legend: {
      isolate: true, // Isolate series on click.
      live: false,
    },
    scales: {
      x: {
        range: () => [xAxisDomain.extent[0], xAxisDomain.extent[1]],
      },
      yAxis: {
        range: () => [yAxisDomain.extent[0], yAxisDomain.extent[1]],
      },
    },
    axes: [
      {
        values: (_u, vals) => vals.map(xAxisDomain.tickFormat),
        splits: () => xAxisDomain.ticks,
      },
      {
        values: (_u, vals) => vals.map(yAxisDomain.tickFormat),
        splits: () => [
          yAxisDomain.extent[0],
          ...yAxisDomain.ticks,
          yAxisDomain.extent[1],
        ],
        scale: "yAxis",
      },
    ],
    series: [
      {
        value: (_u, millis) => xAxisDomain.guideFormat(millis),
      },
      ...series.slice(1).map((s, i) => ({
        fill: colourPalette[i % colourPalette.length],
        stroke: colourPalette[i % colourPalette.length],
        width: 2,
        paths: defaultBars,
        points: { show: false },
        scale: "yAxis",
        ...s,
      })),
    ],
    plugins: [barTooltipPlugin(yAxisUnits, timezone)],
  };

  const combinedOpts = merge(opts, providedOpts);

  // Set y-axis label with units.
  combinedOpts.axes[1].label += ` ${yAxisDomain.label}`;

  return combinedOpts;
};
