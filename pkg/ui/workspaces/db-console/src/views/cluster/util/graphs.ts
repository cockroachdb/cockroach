// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import React from "react";
import _ from "lodash";
import * as protos from "src/js/protos";
import { AxisDomain } from "@cockroachlabs/cluster-ui";

import {
  AxisProps,
  MetricProps,
} from "src/views/shared/components/metricQuery";
import uPlot from "uplot";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

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
};

export function formatMetricData(
  metrics: React.ReactElement<MetricProps>[],
  data: TSResponse,
): formattedSeries[] {
  const formattedData: formattedSeries[] = [];

  _.each(metrics, (s, idx) => {
    const result = data.results[idx];
    if (result && !_.isEmpty(result.datapoints)) {
      formattedData.push({
        values: result.datapoints,
        key: s.props.title || s.props.name,
        area: true,
        fillOpacity: 0.1,
      });
    }
  });

  return formattedData;
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
  const strokeColors = [...seriesPalette];

  const tooltipPlugin = () => {
    return {
      hooks: {
        init: (self: uPlot) => {
          const over: HTMLElement = self.root.querySelector(".u-over");
          const legend: HTMLElement = self.root.querySelector(".u-legend");

          // apply class to stick a legend to the bottom of a chart if it has more than 10 series
          if (self.series.length > 10) {
            legend.classList.add("u-legend--place-bottom");
          }

          // Show/hide legend when we enter/exit the bounds of the graph
          over.onmouseenter = () => {
            legend.style.display = "block";
          };

          // persistLegend determines if legend should continue showing even when mouse
          // hovers away.
          let persistLegend = false;

          over.addEventListener("click", () => {
            persistLegend = !persistLegend;
          });

          over.onmouseleave = () => {
            if (!persistLegend) {
              legend.style.display = "none";
            }
          };
        },
        setCursor: (self: uPlot) => {
          // Place legend to the right of the mouse pointer
          const legend: HTMLElement = self.root.querySelector(".u-legend");
          if (self.cursor.left > 0 && self.cursor.top > 0) {
            // TODO(davidh): This placement is not aware of the viewport edges
            legend.style.left = self.cursor.left + 100 + "px";
            legend.style.top = self.cursor.top - 10 + "px";
          }
        },
      },
    };
  };

  // Please see https://github.com/leeoniya/uPlot/tree/master/docs for
  // information on how to construct this object.
  return {
    width: 947,
    height: 300,
    // TODO(davidh): Enable sync-ed guidelines once legend is redesigned
    // currently, if you enable this with a hovering legend, the FPS
    // gets quite choppy on large clusters.
    // cursor: {
    //   sync: {
    //     // graphs with matching keys will get their guidelines
    //     // sync-ed so we just use the same key for the page
    //     key: "sync-everything",
    //   },
    // },
    cursor: {
      lock: true,
    },
    legend: {
      show: true,

      // This setting sets the default legend behavior to isolate
      // a series when it's clicked in the legend.
      isolate: true,
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
        const color = strokeColors.shift();
        strokeColors.push(color);
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
    plugins: [tooltipPlugin()],
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
