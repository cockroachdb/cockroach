// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import merge from "lodash/merge";
import uPlot, { Options, Band, AlignedData } from "uplot";

import { AxisUnits, AxisDomain } from "../utils/domain";

import {
  barTooltipPlugin,
  groupedStackedBarTooltipPlugin,
} from "./plugins";

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

// Horizontally translate a Path2D by the given offset.
function offsetPath2D(
  path: Path2D | null | undefined,
  transform: DOMMatrix2DInit,
): Path2D | undefined {
  if (!path) return undefined;
  const p = new Path2D();
  p.addPath(path, transform);
  return p;
}

// Wraps a standard bars builder and shifts the resulting paths by
// offsetCssPx CSS pixels horizontally, creating a visible gap
// between grouped bar pairs.
const getOffsetBarsBuilder = (
  barWidthFactor: number,
  maxWidth: number,
  minWidth: number,
  align: 0 | 1 | -1,
  offsetCssPx: number,
): uPlot.Series.PathBuilder => {
  const baseBars = getBarsBuilder(
    barWidthFactor,
    maxWidth,
    minWidth,
    align,
  );
  if (offsetCssPx === 0) return baseBars;

  return (u, seriesIdx, idx0, idx1) => {
    const result = baseBars(u, seriesIdx, idx0, idx1);
    if (!result) return result;

    // Paths are in device-pixel space.
    const offset =
      offsetCssPx * (window.devicePixelRatio || 1);
    const tx: DOMMatrix2DInit = {
      a: 1, b: 0, c: 0, d: 1, e: offset, f: 0,
    };

    return {
      flags: result.flags,
      fill:
        result.fill instanceof Path2D
          ? offsetPath2D(result.fill, tx)
          : result.fill,
      stroke:
        result.stroke instanceof Path2D
          ? offsetPath2D(result.stroke, tx)
          : result.stroke,
      clip:
        result.clip instanceof Path2D
          ? offsetPath2D(result.clip, tx)
          : result.clip,
      band:
        result.band instanceof Path2D
          ? offsetPath2D(result.band, tx)
          : result.band,
    };
  };
};

// pairGroupingPlugin draws alternating light background bands behind
// every other timestamp bin so that each {group1 + group2} pair is
// visually grouped together.
function pairGroupingPlugin(): uPlot.Plugin {
  return {
    hooks: {
      drawClear: (u: uPlot) => {
        const ctx = u.ctx;
        const timestamps = u.data[0] as number[];
        if (!timestamps || timestamps.length < 2) return;

        const { top, height } = u.bbox;

        // Bin width in canvas-pixel space.
        const x0 = u.valToPos(timestamps[0], "x", true);
        const x1 = u.valToPos(timestamps[1], "x", true);
        const binWidth = Math.abs(x1 - x0);
        const halfBin = binWidth / 2;

        ctx.save();
        ctx.fillStyle = "rgba(0, 0, 0, 0.04)";

        for (let i = 1; i < timestamps.length; i += 2) {
          const cx = u.valToPos(timestamps[i], "x", true);
          ctx.fillRect(cx - halfBin, top, binWidth, height);
        }

        ctx.restore();
      },
    },
  };
}

// groupDividerPlugin draws light grey vertical dashed lines between
// consecutive timestamp bins so that each {canary + stable} pair is
// visually separated from its neighbours.
function groupDividerPlugin(): uPlot.Plugin {
  return {
    hooks: {
      draw: (u: uPlot) => {
        const ctx = u.ctx;
        const timestamps = u.data[0] as number[];
        if (!timestamps || timestamps.length < 2) return;

        const { top, height } = u.bbox;

        ctx.save();
        ctx.strokeStyle = "#c0c0c0";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);

        for (let i = 0; i < timestamps.length - 1; i++) {
          const x0 = u.valToPos(timestamps[i], "x", true);
          const x1 = u.valToPos(timestamps[i + 1], "x", true);
          const midX = Math.round((x0 + x1) / 2);
          ctx.beginPath();
          ctx.moveTo(midX, top);
          ctx.lineTo(midX, top + height);
          ctx.stroke();
        }

        ctx.restore();
      },
    },
  };
}

// groupLabelsPlugin draws text labels (e.g. "Canary", "Stable")
// above each bar stack so the user can tell the two groups apart
// without relying on the legend.
function groupLabelsPlugin(
  labels: [string, string],
  labelColors: [string, string],
  n: number,
  unstackedData: AlignedData,
  barWidthFactor: number,
  gapCssPx: number,
): uPlot.Plugin {
  return {
    hooks: {
      draw: (u: uPlot) => {
        const ctx = u.ctx;
        const timestamps = u.data[0] as number[];
        if (!timestamps || timestamps.length < 1) return;

        const pxr = window.devicePixelRatio || 1;

        // Approximate bar width in device pixels.
        const colWid =
          timestamps.length > 1
            ? Math.abs(
                u.valToPos(timestamps[1], "x", true) -
                  u.valToPos(timestamps[0], "x", true),
              )
            : u.bbox.width;
        const barWid = Math.min(
          Math.max(colWid * barWidthFactor, 4 * pxr),
          40 * pxr,
        );
        const gap = gapCssPx * pxr;

        ctx.save();
        ctx.font = `bold ${9 * pxr}px sans-serif`;
        ctx.textAlign = "center";

        for (let t = 0; t < timestamps.length; t++) {
          const cx = u.valToPos(timestamps[t], "x", true);

          // Sum unstacked values for each group at this ts.
          let g1Total = 0;
          for (let i = 0; i < n; i++) {
            g1Total += unstackedData[1 + i]?.[t] || 0;
          }
          let g2Total = 0;
          for (let i = 0; i < n; i++) {
            g2Total += unstackedData[1 + n + i]?.[t] || 0;
          }

          // Bar centres: align=-1 shifts by -barWid/2,
          // then the gap offset shifts further out.
          const leftCX = cx - barWid / 2 - gap;
          const rightCX = cx + barWid / 2 + gap;

          if (g1Total > 0) {
            const topY = u.valToPos(g1Total, "yAxis", true);
            ctx.fillStyle = labelColors[0];
            ctx.fillText(labels[0], leftCX, topY - 4 * pxr);
          }
          if (g2Total > 0) {
            const topY = u.valToPos(g2Total, "yAxis", true);
            ctx.fillStyle = labelColors[1];
            ctx.fillText(labels[1], rightCX, topY - 4 * pxr);
          }
        }

        ctx.restore();
      },
    },
  };
}

// getGroupedStackedBarOpts creates options for a chart with two groups of
// stacked bars side by side. Data layout:
//   [timestamps, ...group1_series(N), ...group2_series(N)]
// where groupSize = N is the number of stacked layers per group.
// group1 bars are left-aligned, group2 bars are right-aligned.
//
// groupStrokeColors optionally provides distinct border colors for each
// group (e.g. ["#c62828", "#1565c0"]) so the user can tell group1 from
// group2 at a glance. Fill colors still come from colourPalette.
//
// groupLabels optionally provides text labels (e.g. ["Canary","Stable"])
// drawn above each bar stack. When set, the built-in uPlot legend is
// hidden (the caller should render its own gist-only legend).
export const getGroupedStackedBarOpts = (
  unstackedData: AlignedData,
  userOptions: Partial<Options>,
  xAxisDomain: AxisDomain,
  yAxisDomain: AxisDomain,
  yAxisUnits: AxisUnits,
  colourPalette = seriesPalette,
  timezone: string,
  groupSize?: number,
  groupStrokeColors?: [string, string],
  groupLabels?: [string, string],
  gistLabels?: string[],
): { opts: Options; stackedData: AlignedData } => {
  const { series, ...providedOpts } = userOptions;

  // Default: half the data series belong to each group.
  const n = groupSize ?? Math.floor((unstackedData.length - 1) / 2);

  const barWidthFactor = 0.3;
  const gapCssPx = 2;

  // Offset bars so each group is shifted away from the centre,
  // creating a visible gap between the two bars in every pair.
  const leftBars = getOffsetBarsBuilder(
    barWidthFactor, 40, 4, -1, -gapCssPx,
  );
  const rightBars = getOffsetBarsBuilder(
    barWidthFactor, 40, 4, 1, gapCssPx,
  );

  const opts: Options = {
    width: 947,
    height: 300,
    ms: 1,
    legend: {
      show: !groupLabels,
      isolate: false,
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
      // Group 1: left-aligned bars
      ...series.slice(1, 1 + n).map((s, i) => ({
        fill: colourPalette[i % colourPalette.length],
        stroke: groupStrokeColors ? groupStrokeColors[0] : "#fff",
        width: 1,
        paths: leftBars,
        points: { show: false },
        scale: "yAxis",
        ...s,
      })),
      // Group 2: right-aligned bars
      ...series.slice(1 + n, 1 + 2 * n).map((s, i) => ({
        fill: colourPalette[i % colourPalette.length],
        stroke: groupStrokeColors ? groupStrokeColors[1] : "#fff",
        width: 1,
        paths: rightBars,
        points: { show: false },
        scale: "yAxis",
        ...s,
      })),
    ],
    plugins: [], // populated below after stackedData is computed
  };

  const combinedOpts = merge(opts, providedOpts);
  combinedOpts.axes[1].label += ` ${yAxisDomain.label}`;

  // Pre-stack each group independently by cumulatively summing layers.
  const stackedData: AlignedData = [unstackedData[0]];
  // Group 1 (series indices 1..n)
  for (let i = 0; i < n; i++) {
    if (i === 0) {
      stackedData.push([...unstackedData[1]]);
    } else {
      stackedData.push(
        unstackedData[1 + i].map(
          (v, j) => v + (stackedData[i] as number[])[j],
        ),
      );
    }
  }
  // Group 2 (series indices n+1..2n)
  for (let i = 0; i < n; i++) {
    if (i === 0) {
      stackedData.push([...unstackedData[1 + n]]);
    } else {
      stackedData.push(
        unstackedData[1 + n + i].map(
          (v, j) => v + (stackedData[n + i] as number[])[j],
        ),
      );
    }
  }

  // Build plugins now that stackedData is available.
  const plugins: uPlot.Plugin[] = [];
  if (gistLabels && groupLabels) {
    plugins.push(
      groupedStackedBarTooltipPlugin(
        yAxisUnits,
        timezone,
        unstackedData,
        stackedData,
        n,
        gistLabels,
        groupLabels,
        colourPalette,
      ),
    );
  } else {
    plugins.push(barTooltipPlugin(yAxisUnits, timezone));
  }
  plugins.push(pairGroupingPlugin());
  plugins.push(groupDividerPlugin());
  if (groupLabels && groupStrokeColors) {
    plugins.push(
      groupLabelsPlugin(
        groupLabels,
        groupStrokeColors,
        n,
        unstackedData,
        barWidthFactor,
        gapCssPx,
      ),
    );
  }
  combinedOpts.plugins = plugins;

  // Bands fill between adjacent stacked layers within each group.
  combinedOpts.bands = [];
  for (let i = 1; i < n; i++) {
    // Group 1: series i+1 on top of series i (1-indexed in uPlot)
    combinedOpts.bands.push({ series: [i + 1, i] as Band.Bounds });
  }
  for (let i = 1; i < n; i++) {
    // Group 2: series n+i+1 on top of series n+i
    combinedOpts.bands.push({
      series: [n + i + 1, n + i] as Band.Bounds,
    });
  }

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

  // restackGroups recomputes the cumulative stacked data for both
  // groups after a series visibility toggle. This is necessary because
  // uPlot has no built-in support for grouped stacked bars — we
  // manually maintain the stacking invariant by:
  //   1. Syncing group 2 visibility to match group 1 (the two groups
  //      always show the same set of layers).
  //   2. Rebuilding cumulative sums for each group independently,
  //      skipping hidden layers so visible bars sit flush on top of
  //      each other.
  //   3. Rebuilding bands (the filled regions between consecutive
  //      visible layers) so uPlot's fill rendering stays correct.
  function restackGroups(u: uPlot): void {
    // Step 1: sync group 2 show flags to match group 1.
    // setSeries is NOT used here to avoid re-triggering hooks.
    for (let i = 0; i < n; i++) {
      u.series[1 + n + i].show = u.series[1 + i].show;
    }

    // Step 2: rebuild stacked data arrays.
    const newData: AlignedData = [unstackedData[0]];
    const accum1 = new Array(unstackedData[0].length).fill(0);
    for (let i = 0; i < n; i++) {
      if (u.series[1 + i].show) {
        newData.push(
          unstackedData[1 + i].map((v, j) => (accum1[j] += v)),
        );
      } else {
        // Hidden series keep their raw values (they won't render
        // because the series is hidden, but the slot is needed to
        // preserve the data array layout).
        newData.push(unstackedData[1 + i]);
      }
    }
    const accum2 = new Array(unstackedData[0].length).fill(0);
    for (let i = 0; i < n; i++) {
      if (u.series[1 + i].show) {
        newData.push(
          unstackedData[1 + n + i].map((v, j) => (accum2[j] += v)),
        );
      } else {
        newData.push(unstackedData[1 + n + i]);
      }
    }

    // Step 3: rebuild bands between consecutive visible series.
    // Each band connects a layer to the nearest visible layer below
    // it, creating the filled stacked region.
    u.delBand(null);
    for (let i = 1; i < n; i++) {
      if (!u.series[1 + i].show) continue;
      for (let j = i - 1; j >= 0; j--) {
        if (u.series[1 + j].show) {
          u.addBand({ series: [1 + i, 1 + j] as Band.Bounds });
          break;
        }
      }
    }
    for (let i = 1; i < n; i++) {
      if (!u.series[1 + i].show) continue;
      for (let j = i - 1; j >= 0; j--) {
        if (u.series[1 + j].show) {
          u.addBand({
            series: [1 + n + i, 1 + n + j] as Band.Bounds,
          });
          break;
        }
      }
    }

    u.setData(newData as AlignedData);
  }

  // Restack when series visibility changes from external sources.
  combinedOpts.hooks = combinedOpts.hooks || {};
  combinedOpts.hooks.setSeries = combinedOpts.hooks.setSeries || [];
  let restacking = false;
  combinedOpts.hooks.setSeries.push((u: uPlot) => {
    if (restacking) return;
    restacking = true;
    restackGroups(u);
    restacking = false;
  });

  // Custom click-to-isolate that handles both groups atomically.
  // uPlot's built-in isolate fires setSeries one series at a time,
  // which conflicts with our group syncing. Instead we intercept
  // legend clicks, set all show flags at once, then restack.
  plugins.push({
    hooks: {
      init: (u: uPlot) => {
        const legendEl = u.root.querySelector(".u-legend");
        if (!legendEl) return;

        legendEl.addEventListener(
          "click",
          (e: Event) => {
            const target = (e.target as Element).closest(
              ".u-series",
            );
            if (!target) return;

            const rows = u.root.querySelectorAll(
              ".u-legend .u-series",
            );
            let idx = -1;
            rows.forEach((row, i) => {
              if (row === target) idx = i;
            });

            // Only handle group 1 entries (series 1..n).
            if (idx < 1 || idx > n) return;

            // Prevent uPlot's default toggle from firing.
            e.stopPropagation();

            // Check if the clicked series is already the only
            // visible one (i.e. currently isolated).
            let isIsolated = u.series[idx].show;
            if (isIsolated) {
              for (let j = 1; j <= n; j++) {
                if (j !== idx && u.series[j].show) {
                  isIsolated = false;
                  break;
                }
              }
            }

            // Set show flags for both groups atomically.
            restacking = true;
            for (let j = 0; j < n; j++) {
              const show = isIsolated || 1 + j === idx;
              u.series[1 + j].show = show;
              u.series[1 + n + j].show = show;

              // Update legend CSS for group 1 entries.
              const row = rows[1 + j] as HTMLElement;
              if (row) {
                if (show) {
                  row.classList.remove("u-off");
                } else {
                  row.classList.add("u-off");
                }
              }
            }
            restackGroups(u);
            restacking = false;
          },
          true,
        ); // capture phase
      },
    },
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
