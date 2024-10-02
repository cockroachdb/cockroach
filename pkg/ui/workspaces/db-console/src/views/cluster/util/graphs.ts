// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { AxisDomain } from "@cockroachlabs/cluster-ui";
import each from "lodash/each";
import isEmpty from "lodash/isEmpty";
import without from "lodash/without";
import React from "react";
import uPlot from "uplot";

import * as protos from "src/js/protos";
import {
  AxisProps,
  MetricProps,
} from "src/views/shared/components/metricQuery";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;
type TSQueryResult = protos.cockroach.ts.tspb.TimeSeriesQueryResponse.IResult;

// Global set of colors for graph series.
const seriesPalette = [
  "#475872",
  "#FFCD02",
  "#F16969",
  "#4E9FD1",
  "#49D990",
  "#D77FBF",
  "#87326D",
  "#A3415B",
  "#B59153",
  "#C9DB6D",
  "#203D9B",
  "#748BF2",
  "#91C8F2",
  "#FF9696",
  "#EF843C",
  "#DCCD4B",
];

export type formattedSeries = {
  values: protos.cockroach.ts.tspb.ITimeSeriesDatapoint[];
  key: string;
  area: boolean;
  fillOpacity: number;
  color?: string;
};

// logic to decide when to show a metric based on the query's result
export function canShowMetric(result: TSQueryResult) {
  return !isEmpty(result.datapoints);
}

export function formatMetricData(
  metrics: React.ReactElement<MetricProps>[],
  data: TSResponse,
): formattedSeries[] {
  const formattedData: formattedSeries[] = [];

  each(metrics, (s, idx) => {
    const result = data.results[idx];
    if (result && canShowMetric(result)) {
      const transform = s.props.transform ?? (d => d);
      const scale = s.props.scale ?? 1;
      const scaledAndTransformedValues = transform(result.datapoints).map(
        v => ({
          ...v,
          // if defined scale/transform it, otherwise remain undefined
          value: v.value && scale * v.value,
        }),
      );

      formattedData.push({
        values: scaledAndTransformedValues,
        key: s.props.title || s.props.name,
        area: true,
        fillOpacity: 0.1,
        color: s.props.color,
      });
    }
  });

  return formattedData;
}

function hoverTooltipPlugin(
  xFormatter: (v: number) => string,
  yFormatter: (v: number) => string,
) {
  const shiftX = 10;
  const shiftY = 10;
  let tooltipLeftOffset = 0;
  let tooltipTopOffset = 0;

  const tooltip = document.createElement("div");
  tooltip.className = "u-tooltip";

  const timeNode = document.createElement("div");
  timeNode.className = "u-time";
  tooltip.appendChild(timeNode);

  const seriesNode = document.createElement("div");
  seriesNode.className = "u-series";

  const markerNode = document.createElement("div");
  markerNode.className = "u-marker";

  const labelNode = document.createElement("div");
  labelNode.className = "u-label";

  const dataNode = document.createTextNode(`--`);

  seriesNode.appendChild(markerNode);
  seriesNode.appendChild(labelNode);
  seriesNode.appendChild(dataNode);
  tooltip.appendChild(seriesNode);

  let seriesIdx: number = null;
  let dataIdx: number = null;
  let over: HTMLDivElement;
  let tooltipVisible = false;

  function showTooltip() {
    if (!tooltipVisible) {
      tooltip.style.display = "block";
      over.style.cursor = "pointer";
      tooltipVisible = true;
    }
  }

  function hideTooltip() {
    if (tooltipVisible) {
      tooltip.style.display = "none";
      over.style.cursor = null;
      tooltipVisible = false;
    }
  }

  function setTooltip(u: uPlot) {
    showTooltip();

    // `yAxis` is used instead of `y` here because that's below in the
    // `uPlot` config as the custom scale that we define.n
    const top = u.valToPos(u.data[seriesIdx][dataIdx], "yAxis");
    const lft = u.valToPos(u.data[0][dataIdx], "x");

    tooltip.style.top = tooltipTopOffset + top + shiftX + "px";
    tooltip.style.left = tooltipLeftOffset + lft + shiftY + "px";

    timeNode.textContent = `Time: ${xFormatter(u.data[0][dataIdx])}`;
    labelNode.textContent = `${u.series[seriesIdx].label}:`;
    dataNode.textContent = ` ${yFormatter(u.data[seriesIdx][dataIdx])}`;

    const stroke = u.series[seriesIdx].stroke;
    if (typeof stroke === "function" && stroke.length === 0) {
      markerNode.style.background = stroke(u, seriesIdx) as string;
    } else if (typeof stroke === "string") {
      markerNode.style.borderColor = stroke;
    }
  }

  return {
    hooks: {
      ready: [
        (u: uPlot) => {
          over = u.over;
          tooltipLeftOffset = parseFloat(over.style.left);
          tooltipTopOffset = parseFloat(over.style.top);
          u.root.querySelector(".u-wrap").appendChild(tooltip);
        },
      ],
      setCursor: [
        (u: uPlot) => {
          const c = u.cursor;

          if (dataIdx !== c.idx) {
            dataIdx = c.idx;

            if (seriesIdx != null) setTooltip(u);
          }
        },
      ],
      setSeries: [
        (u: uPlot, sidx: number) => {
          if (seriesIdx !== sidx) {
            seriesIdx = sidx;

            if (sidx == null) hideTooltip();
            else if (dataIdx !== null) setTooltip(u);
          }
        },
      ],
    },
  };
}

// configureUPlotLineChart constructs the uplot Options object based on
// information about the metrics, axis, and data that we'd like to plot.
// Most of the settings are defined as functions instead of static values
// in order to take advantage of auto-updating behavior built into uPlot
// when we send new data to the uPlot object. This will ensure that all
// axis labeling and extent settings get updated properly.
export function configureUPlotLineChart(
  metrics: React.ReactElement<MetricProps>[],
  axis: React.ReactElement<AxisProps>,
  data: TSResponse,
  setMetricsFixedWindow: (startMillis: number, endMillis: number) => void,
  getLatestXAxisDomain: () => AxisDomain,
  getLatestYAxisDomain: () => AxisDomain,
): uPlot.Options {
  const formattedRaw = formatMetricData(metrics, data);
  // Copy palette over since we mutate it in the `series` function
  // below to cycle through the colors. This ensures that we always
  // start from the same color for each graph so a single-series
  // graph will always have the first color, etc.
  const strokeColors = without(
    seriesPalette,
    // Exclude custom colors provided in metrics from default list of colors.
    ...formattedRaw.filter(r => !!r.color).map(r => r.color),
  );

  // Please see https://github.com/leeoniya/uPlot/tree/master/docs for
  // information on how to construct this object.
  return {
    width: 947,
    height: 300,
    cursor: {
      lock: true,
      focus: {
        prox: 5,
      },
    },
    legend: {
      show: true,

      // This setting sets the default legend behavior to isolate
      // a series when it's clicked in the legend.
      isolate: true,
      markers: {
        stroke: () => {
          return null;
        },
        fill: (u: uPlot, i: number) => {
          const stroke = u.series[i].stroke;
          if (typeof stroke === "function" && stroke.length === 0) {
            return stroke(u, i) as string;
          } else if (typeof stroke === "string") {
            return stroke;
          }
        },
      },
    },
    // By default, uPlot expects unix seconds in the x axis.
    // This setting defaults it to milliseconds which our
    // internal functions already supported.
    ms: 1,
    // These settings govern how the individual series are
    // drawn and how their values are displayed in the legend.
    series: [
      {
        value: (_u, rawValue) => getLatestXAxisDomain().guideFormat(rawValue),
      },
      // Generate a series object for reach of our results
      // picking colors from our palette.
      ...formattedRaw.map(result => {
        let color: string;
        // Assign custom provided color, otherwise assign from
        // the list of default colors.
        if (result.color) {
          color = result.color;
        } else {
          color = strokeColors.shift();
          strokeColors.push(color);
        }

        return {
          show: true,
          scale: "yAxis",
          width: 1,
          label: result.key,
          stroke: color,
          points: {
            show: false,
          },
          // value determines how these values show up in the legend
          value: (_u: uPlot, rawValue: number) =>
            getLatestYAxisDomain().guideFormat(rawValue),
          spanGaps: false,
        };
      }),
    ],
    axes: [
      {
        values: (_u, vals) => vals.map(getLatestXAxisDomain().tickFormat),
        splits: () => getLatestXAxisDomain().ticks,
      },
      {
        // This label will get overridden in the linegraph's update
        // callback when we change the y Axis domain.
        label:
          axis.props.label +
          (getLatestYAxisDomain().label
            ? ` (${getLatestYAxisDomain().label})`
            : ""),
        values: (_u, vals) => vals.map(getLatestYAxisDomain().tickFormat),
        splits: () => {
          const domain = getLatestYAxisDomain();
          return [domain.extent[0], ...domain.ticks, domain.extent[1]];
        },
        scale: "yAxis",
        labelGap: 5,
      },
    ],
    scales: {
      x: {
        range: () => getLatestXAxisDomain().extent,
      },
      yAxis: {
        range: () => getLatestYAxisDomain().extent,
      },
    },
    plugins: [
      hoverTooltipPlugin(
        getLatestXAxisDomain().guideFormat,
        getLatestYAxisDomain().guideFormat,
      ),
    ],
    hooks: {
      // setSelect is a hook that fires when a selection is made on the graph
      // by dragging a range to zoom.
      setSelect: [
        self => {
          // From what I understand, `self.select` contains the pixel edges
          // of the user's selection. Then I use the `posToIdx` to tell me
          // what the xAxis range is of the pixels.
          setMetricsFixedWindow(
            self.data[0][self.posToIdx(self.select.left)],
            self.data[0][self.posToIdx(self.select.left + self.select.width)],
          );
        },
      ],
    },
  };
}
