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
import * as nvd3 from "nvd3";
import * as d3 from "d3";
import moment from "moment";

import * as protos from "src/js/protos";
import { NanoToMilli } from "src/util/convert";
import {  DurationFitScale, BytesFitScale, ComputeByteScale, ComputeDurationScale} from "src/util/format";

import {
  MetricProps, AxisProps, AxisUnits, QueryTimeInfo,
} from "src/views/shared/components/metricQuery";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

// Global set of colors for graph series.
const seriesPalette = [
  "#475872", "#FFCD02", "#F16969", "#4E9FD1", "#49D990", "#D77FBF", "#87326D", "#A3415B",
  "#B59153", "#C9DB6D", "#203D9B", "#748BF2", "#91C8F2", "#FF9696", "#EF843C", "#DCCD4B",
];

// Chart margins to match design.
export const CHART_MARGINS: nvd3.Margin = {top: 30, right: 20, bottom: 20, left: 55};

// Maximum number of series we will show in the legend. If there are more we hide the legend.
const MAX_LEGEND_SERIES: number = 4;

// The number of ticks to display on a Y axis.
const Y_AXIS_TICK_COUNT: number = 3;

// The number of ticks to display on an X axis.
const X_AXIS_TICK_COUNT: number = 10;

// A tuple of numbers for the minimum and maximum values of an axis.
type Extent = [number, number];

/**
 * AxisDomain is a class that describes the domain of a graph axis; this
 * includes the minimum/maximum extend, tick values, and formatting information
 * for axis values as displayed in various contexts.
 */
class AxisDomain {
  // the values at the ends of the axis.
  extent: Extent;
  // numbers at which an intermediate tick should be displayed on the axis.
  ticks: number[] = [0, 1];
  // label returns the label for the axis.
  label: string = "";
  // tickFormat returns a function used to format the tick values for display.
  tickFormat: (n: number) => string = _.identity;
  // guideFormat returns a function used to format the axis values in the
  // chart's interactive guideline.
  guideFormat: (n: number) => string = _.identity;

  // constructs a new AxisDomain with the given minimum and maximum value, with
  // ticks placed at intervals of the given increment in between the min and
  // max. Ticks are always "aligned" to values that are even multiples of
  // increment. Min and max are also aligned by default - the aligned min will
  // be <= the provided min, while the aligned max will be >= the provided max.
  constructor(extent: Extent, increment: number, alignMinMax: boolean = true) {
    const min = extent[0];
    const max = extent[1];
    if (alignMinMax) {
      const alignedMin = min - min % increment;
      let alignedMax = max;
      if (max % increment !== 0) {
        alignedMax = max - max % increment + increment;
      }
      this.extent = [alignedMin, alignedMax];
    } else {
      this.extent = extent;
    }

    this.ticks = [];
    for (let nextTick = min - min % increment + increment;
         nextTick < this.extent[1];
         nextTick += increment) {
      this.ticks.push(nextTick);
    }
  }
}

const countIncrementTable = [0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0];

// computeNormalizedIncrement computes a human-friendly increment between tick
// values on an axis with a range of the given size. The provided size is taken
// to be the minimum range needed to display all values present on the axis.
// The increment is computed by dividing this minimum range into the correct
// number of segments for the supplied tick count, and then increasing this
// increment to the nearest human-friendly increment.
//
// "Human-friendly" increments are taken from the supplied countIncrementTable,
// which should include decimal values between 0 and 1.
function computeNormalizedIncrement(
  range: number, incrementTbl: number[] = countIncrementTable,
) {
  if (range === 0) {
    throw new Error("cannot compute tick increment with zero range");
  }

  let rawIncrement = range / (Y_AXIS_TICK_COUNT + 1);
  // Compute X such that 0 <= rawIncrement/10^x <= 1
  let x = 0;
  while (rawIncrement > 1) {
    x++;
    rawIncrement = rawIncrement / 10;
  }
  const normalizedIncrementIdx = _.sortedIndex(incrementTbl, rawIncrement);
  return incrementTbl[normalizedIncrementIdx] * Math.pow(10, x);
}

function computeAxisDomain(extent: Extent, factor: number = 1): AxisDomain {
  const range = extent[1] - extent[0];

  // Compute increment on min/max after conversion to the appropriate prefix unit.
  const increment = computeNormalizedIncrement(range / factor);

  // Create axis domain by multiplying computed increment by prefix factor.
  const axisDomain = new AxisDomain(extent, increment * factor);

  // If the tick increment is fractional (e.g. 0.2), we display a decimal
  // point. For non-fractional increments, we display with no decimal points
  // but with a metric prefix for large numbers (i.e. 1000 will display as "1k")
  let unitFormat: (v: number) => string;
  if (Math.floor(increment) !== increment) {
    unitFormat = d3.format(".1f");
  } else {
    unitFormat = d3.format("s");
  }
  axisDomain.tickFormat = (v: number) => unitFormat(v / factor);

  return axisDomain;
}

function ComputeCountAxisDomain(extent: Extent): AxisDomain {
  const axisDomain = computeAxisDomain(extent);

  // For numbers larger than 1, the tooltip displays fractional values with
  // metric multiplicative prefixes (e.g. kilo, mega, giga). For numbers smaller
  // than 1, we simply display the fractional value without converting to a
  // fractional metric prefix; this is because the use of fractional metric
  // prefixes (i.e. milli, micro, nano) have proved confusing to users.
  const metricFormat = d3.format(".4s");
  const decimalFormat = d3.format(".4f");
  axisDomain.guideFormat = (n: number) => {
    if (n < 1) {
      return decimalFormat(n);
    }
    return metricFormat(n);
  };

  return axisDomain;
}

function ComputeByteAxisDomain(extent: Extent): AxisDomain {
  // Compute an appropriate unit for the maximum value to be displayed.
  const scale = ComputeByteScale(extent[1]);
  const prefixFactor = scale.value;

  const axisDomain = computeAxisDomain(extent, prefixFactor);

  axisDomain.label = scale.units;

  axisDomain.guideFormat = BytesFitScale(scale.units);
  return axisDomain;
}

function ComputeDurationAxisDomain(extent: Extent): AxisDomain {
  const scale = ComputeDurationScale(extent[1]);
  const prefixFactor = scale.value;

  const axisDomain = computeAxisDomain(extent, prefixFactor);

  axisDomain.label = scale.units;

  axisDomain.guideFormat = DurationFitScale(scale.units);
  return axisDomain;
}

const percentIncrementTable = [0.25, 0.5, 0.75, 1.0];

function ComputePercentageAxisDomain(
  min: number, max: number,
) {
  const range = max - min;
  const increment = computeNormalizedIncrement(range, percentIncrementTable);
  const axisDomain = new AxisDomain([min, max], increment);
  axisDomain.label = "percentage";
  axisDomain.tickFormat = d3.format(".0%");
  axisDomain.guideFormat = d3.format(".2%");
  return axisDomain;
}

const timeIncrementDurations = [
  moment.duration(1, "m"),
  moment.duration(5, "m"),
  moment.duration(10, "m"),
  moment.duration(15, "m"),
  moment.duration(30, "m"),
  moment.duration(1, "h"),
  moment.duration(2, "h"),
  moment.duration(3, "h"),
  moment.duration(6, "h"),
  moment.duration(12, "h"),
  moment.duration(24, "h"),
  moment.duration(1, "week"),
];
const timeIncrements = _.map(timeIncrementDurations, (inc) => inc.asMilliseconds());

function ComputeTimeAxisDomain(extent: Extent): AxisDomain {
  // Compute increment; for time scales, this is taken from a table of allowed
  // values.
  let increment = 0;
  {
    const rawIncrement = (extent[1] - extent[0]) / (X_AXIS_TICK_COUNT + 1);
    // Compute X such that 0 <= rawIncrement/10^x <= 1
    const tbl = timeIncrements;
    let normalizedIncrementIdx = _.sortedIndex(tbl, rawIncrement);
    if (normalizedIncrementIdx === tbl.length) {
      normalizedIncrementIdx--;
    }
    increment = tbl[normalizedIncrementIdx];
  }

  // Do not normalize min/max for time axis.
  const axisDomain = new AxisDomain(extent, increment, false);

  axisDomain.label = "time";

  let tickDateFormatter: (d: Date) => string;
  if (increment < moment.duration(24, "hours").asMilliseconds()) {
    tickDateFormatter = d3.time.format.utc("%H:%M");
  } else {
    tickDateFormatter = d3.time.format.utc("%m/%d %H:%M");
  }
  axisDomain.tickFormat = (n: number) => {
    return tickDateFormatter(new Date(n));
  };

  axisDomain.guideFormat = (num) => {
    return moment(num).utc().format("HH:mm:ss [<span class=\"legend-subtext\">on</span>] MMM Do, YYYY");
  };
  return axisDomain;
}

function calculateYAxisDomain(axisUnits: AxisUnits, data: TSResponse): AxisDomain {
  const resultDatapoints = _.flatMap(data.results, (result) => _.map(result.datapoints, (dp) => dp.value));
  // TODO(couchand): Remove these random datapoints when NVD3 is gone.
  const allDatapoints = resultDatapoints.concat([0, 1]);
  const yExtent = d3.extent(allDatapoints);

  switch (axisUnits) {
    case AxisUnits.Bytes:
      return ComputeByteAxisDomain(yExtent);
    case AxisUnits.Duration:
      return ComputeDurationAxisDomain(yExtent);
    case AxisUnits.Percentage:
      return ComputePercentageAxisDomain(yExtent[0], yExtent[1]);
    default:
      return ComputeCountAxisDomain(yExtent);
  }
}

function calculateXAxisDomain(timeInfo: QueryTimeInfo): AxisDomain {
  const xExtent: Extent = [NanoToMilli(timeInfo.start.toNumber()), NanoToMilli(timeInfo.end.toNumber())];
  return ComputeTimeAxisDomain(xExtent);
}

type formattedSeries = {
  values: protos.cockroach.ts.tspb.ITimeSeriesDatapoint[],
  key: string,
  area: boolean,
  fillOpacity: number,
};

function formatMetricData(
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

function filterInvalidDatapoints(
  formattedData: formattedSeries[],
  timeInfo: QueryTimeInfo,
): formattedSeries[] {
  return _.map(formattedData, (datum) => {
    // Drop any returned points at the beginning that have a lower timestamp
    // than the explicitly queried domain. This works around a bug in NVD3
    // which causes the interactive guideline to highlight the wrong points.
    // https://github.com/novus/nvd3/issues/1913
    const filteredValues = _.dropWhile(datum.values, (dp) => {
      return dp.timestamp_nanos.toNumber() < timeInfo.start.toNumber();
    });

    return {
      ...datum,
      values: filteredValues,
    };
  });
}

export function InitLineChart(chart: nvd3.LineChart) {
    chart
      .x((d: protos.cockroach.ts.tspb.TimeSeriesDatapoint) => new Date(NanoToMilli(d && d.timestamp_nanos.toNumber())))
      .y((d: protos.cockroach.ts.tspb.TimeSeriesDatapoint) => d && d.value)
      .useInteractiveGuideline(true)
      .showLegend(true)
      .showYAxis(true)
      .color(seriesPalette)
      .margin(CHART_MARGINS);
    chart.xAxis
      .showMaxMin(false);
    chart.yAxis
      .showMaxMin(true)
      .axisLabelDistance(-10);
}

/**
 * ConfigureLineChart renders the given NVD3 chart with the updated data.
 */
export function ConfigureLineChart(
  chart: nvd3.LineChart,
  svgEl: SVGElement,
  metrics: React.ReactElement<MetricProps>[],
  axis: React.ReactElement<AxisProps>,
  data: TSResponse,
  timeInfo: QueryTimeInfo,
) {
  chart.showLegend(metrics.length > 1 && metrics.length <= MAX_LEGEND_SERIES);
  let formattedData: formattedSeries[];
  let xAxisDomain, yAxisDomain: AxisDomain;

  if (data) {
    const formattedRaw = formatMetricData(metrics, data);
    formattedData = filterInvalidDatapoints(formattedRaw, timeInfo);

    xAxisDomain = calculateXAxisDomain(timeInfo);
    yAxisDomain = calculateYAxisDomain(axis.props.units, data);

    chart.yDomain(yAxisDomain.extent);
    if (axis.props.label && yAxisDomain.label) {
      chart.yAxis.axisLabel(`${axis.props.label} (${yAxisDomain.label})`);
    } else if (axis.props.label) {
      chart.yAxis.axisLabel(axis.props.label);
    } else {
      chart.yAxis.axisLabel(yAxisDomain.label);
    }
    chart.xDomain(xAxisDomain.extent);

    chart.yAxis.tickFormat(yAxisDomain.tickFormat);
    chart.interactiveLayer.tooltip.valueFormatter(yAxisDomain.guideFormat);
    chart.xAxis.tickFormat(xAxisDomain.tickFormat);
    chart.interactiveLayer.tooltip.headerFormatter(xAxisDomain.guideFormat);

    // always set the tick values to the lowest axis value, the highest axis
    // value, and one value in between
    chart.yAxis.tickValues(yAxisDomain.ticks);
    chart.xAxis.tickValues(xAxisDomain.ticks);
  }
  try {
    d3.select(svgEl)
      .datum(formattedData)
      .transition().duration(500)
      .call(chart);

    // Reduce radius of circles in the legend, if present. This is done through
    // d3 because it is not exposed as an option by NVD3.
    d3.select(svgEl).selectAll("circle").attr("r", 3);
  } catch (e) {
    console.log("Error rendering graph: ", e);
  }
}

/**
 * ConfigureLinkedGuide renders the linked guideline for a chart.
 */
export function ConfigureLinkedGuideline(
  chart: nvd3.LineChart,
  svgEl: SVGElement,
  axis: React.ReactElement<AxisProps>,
  data: TSResponse,
  hoverTime: moment.Moment,
) {
  if (data) {
    const xScale = chart.xAxis.scale();
    const yScale = chart.yAxis.scale();
    const yAxisDomain = calculateYAxisDomain(axis.props.units, data);
    const yExtent: Extent = data ? [yScale(yAxisDomain.extent[0]), yScale(yAxisDomain.extent[1])] : [0, 1];
    updateLinkedGuideline(svgEl, xScale, yExtent, hoverTime);
  }
}

// updateLinkedGuideline is responsible for maintaining "linked" guidelines on
// all other graphs on the page; a "linked" guideline highlights the same X-axis
// coordinate on different graphs currently visible on the same page. This
// allows the user to visually correlate a single X-axis coordinate across
// multiple visible graphs.
function updateLinkedGuideline(svgEl: SVGElement, x: d3.scale.Linear<number, number>, yExtent: Extent, hoverTime?: moment.Moment) {
  // Construct a data array for use by d3; this allows us to use d3's
  // "enter()/exit()" functions to cleanly add and remove the guideline.
  const data = !_.isNil(hoverTime) ? [x(hoverTime.valueOf())] : [];

  // Linked guideline will be inserted inside of the "nv-wrap" element of the
  // nvd3 graph. This element has several translations applied to it by nvd3
  // which allow us to easily display the linked guideline at the correct
  // position.
  const wrapper = d3.select(svgEl).select(".nv-wrap");
  if (wrapper.empty()) {
    // In cases where no data is available for a chart, it will not have
    // an "nv-wrap" element and thus should not get a linked guideline.
    return;
  }

  const container = wrapper.selectAll("g.linked-guideline__container")
    .data(data);

  // If there is no guideline on the currently hovered graph, data is empty
  // and this exit statement will remove the linked guideline from this graph
  // if it is already present. This occurs, for example, when the user moves
  // the mouse off of a graph.
  container.exit().remove();

  // If there is a guideline on the currently hovered graph, this enter
  // statement will add a linked guideline element to the current graph (if it
  // does not already exist).
  container.enter()
    .append("g")
      .attr("class", "linked-guideline__container")
      .append("line")
        .attr("class", "linked-guideline__line");

  // Update linked guideline (if present) to match the necessary attributes of
  // the current guideline.
  container.select(".linked-guideline__line")
    .attr("x1", (d) => d)
    .attr("x2", (d) => d)
    .attr("y1", () => yExtent[0])
    .attr("y2", () => yExtent[1]);
}
