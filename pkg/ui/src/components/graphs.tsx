import * as React from "react";
import _ from "lodash";
import * as nvd3 from "nvd3";
import * as d3 from "d3";
import * as protos from "../js/protos";
import Long from "long";
import moment from "moment";

import { NanoToMilli } from "../util/convert";
import { Bytes, ComputePrefixExponent, Duration, kibi } from "../util/format";
import { QueryTimeInfo } from "../containers/metricsDataProvider";

import { MetricProps } from "./metric";

type TSResponse = protos.cockroach.ts.tspb.TimeSeriesQueryResponse;

// Global set of colors for graph series.
const seriesPalette = [
  "#5F6C87", "#F2BE2C", "#F16969", "#4E9FD1", "#49D990", "#D77FBF", "#87326D", "#A3415B",
  "#B59153", "#C9DB6D", "#203D9B", "#748BF2", "#91C8F2", "#FF9696", "#EF843C", "#DCCD4B",
];

// Chart margins to match design.
const CHART_MARGINS: nvd3.Margin = {top: 30, right: 20, bottom: 20, left: 55};

// Maximum number of series we will show in the legend. If there are more we hide the legend.
const MAX_LEGEND_SERIES: number = 4;

/**
 * AxisUnits is an enumeration used to specify the units being displayed on an
 * Axis.
 * TODO(mrtracy): See if this can be done with different classes
 * (e.g. 'BytesAxis') instead of a property.
 */
export enum AxisUnits {
  Count,
  Bytes,
  Duration,
  Percentage,
}

/**
 * AxisProps represents the properties of a renderable graph axis.
 */
export interface AxisProps {
  label?: string;
  format?: (n: number) => string;
  range?: number[];
  yLow?: number;
  yHigh?: number;
  units?: AxisUnits;
}

/**
 * Axis is a React component which describes a renderable axis on a graph. This
 * exists as a component for convenient syntax, and should not be rendered
 * directly; rather, a renderable component will contain axes, but use them only
 * informationally without rendering them.
 */
export class Axis extends React.Component<AxisProps, {}> {
  static defaultProps: AxisProps = {
    yLow: 0,
    yHigh: 1,
    units: AxisUnits.Count,
  };

  render(): React.ReactElement<any> {
    throw new Error("Component <Axis /> should never render.");
  }
}

/**
 * AxisRange implements functionality to compute the range of points being
 * displayed on an axis.
 */
class AxisRange {
  public min: number = Infinity;
  public max: number = -Infinity;
  stackedSum: { [key: number]: number } = {};

  // addPoints adds values. It will extend the max/min of the domain if any
  // values are lower/higher than the current max/min respectively.
  addPoints(values: number[]) {
    // Discard infinity values created by #12349
    // TODO(mrtracy): remove when this issue is fixed.
    _.pull(values, Infinity);
    this.min = Math.min(this.min, ...values);
    this.max = Math.max(this.max, ...values);
  }

  // addStacked adds keyed values. It sums the values by key and then extends
  // the max/min of the domain if any sum is higher/lower than the current
  // max/min respectively.
  addStackedPoints(keyedValues: {key: number, value: number}[]) {
    _.each(keyedValues, (v) => {
      // Discard infinity values created by #12349
      // TODO(mrtracy): remove when this issue is fixed.
      if (v.value === Infinity) {
        return;
      }
      this.stackedSum[v.key] = (this.stackedSum[v.key] || 0) + v.value;
    });

    this.min = _.min(_.values<number>(this.stackedSum));
    this.max = _.max(_.values<number>(this.stackedSum));
  }
}

/**
 * AxisDomain is a class that describes the domain of a graph axis; this
 * includes the minimum/maximum extend, tick values, and formatting information
 * for axis values as displayed in various contexts.
 */
class AxisDomain {
  // the minimum value representing the bottom of the axis.
  min: number = 0;
  // the maximum value representing the top of the axis.
  max: number = 1;
  // numbers at which an intermediate tick should be displayed on the axis.
  ticks: number[] = [0, 1];
  // label returns the label for the axis.
  label: string = "";
  // tickFormat returns a function used to format the tick values for display.
  tickFormat: (n: number) => string = _.identity;
  // guideFormat returns a function used to format the axis values in the
  // chart's interactive guideline.
  guideFormat: (n: number) => string = _.identity;

  // constructs a new AxisRange with the given minimum and maximum value, with
  // ticks placed at intervals of the given increment in between the min and
  // max. Ticks are always "aligned" to values that are even multiples of
  // increment. Min and max are also aligned by default - the aligned min will
  // be <= the provided min, while the aligned max will be >= the provided max.
  constructor(min: number, max: number, increment: number, alignMinMax: boolean = true) {
    if (alignMinMax) {
      this.min = min - min % increment;
      this.max = max - max % increment + increment;
    } else {
      this.min = min;
      this.max = max;
    }

    this.ticks = [];
    for (let nextTick = min - min % increment + increment;
         nextTick < this.max;
         nextTick += increment) {
      this.ticks.push(nextTick);
    }
  }

  domain() {
    return [this.min, this.max];
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
  range: number, tickCount: number, incrementTbl: number[] = countIncrementTable,
) {
  if (range === 0) {
    throw new Error("cannot compute tick increment with zero range");
  }

  let rawIncrement = range / (tickCount + 1);
  // Compute X such that 0 <= rawIncrement/10^x <= 1
  let x = 0;
  while (rawIncrement > 1) {
    x++;
    rawIncrement = rawIncrement / 10;
  }
  let normalizedIncrementIdx = _.sortedIndex(incrementTbl, rawIncrement);
  return incrementTbl[normalizedIncrementIdx] * Math.pow(10, x);
}

function ComputeCountAxisDomain(
  min: number, max: number, tickCount: number,
): AxisDomain {
  let range = max - min;
  let increment = computeNormalizedIncrement(range, tickCount);
  let axisDomain = new AxisDomain(min, max, increment);

  // If the tick increment is fractional (e.g. 0.2), we display a decimal
  // point. For non-fractional increments, we display with no decimal points
  // but with a metric prefix for large numbers (i.e. 1000 will display as "1k")
  if (Math.floor(increment) !== increment) {
      axisDomain.tickFormat = d3.format(".1f");
  } else {
      axisDomain.tickFormat = d3.format("s");
  }
  axisDomain.guideFormat = d3.format(".4s");
  axisDomain.label = "count";
  return axisDomain;
}

const byteLabels = ["bytes", "kilobytes", "megabytes", "gigabytes", "terabytes", "petabytes", "exabytes", "zettabytes", "yottabytes"];

function ComputeByteAxisDomain(
  min: number, max: number, tickCount: number,
): AxisDomain {
  // Compute an appropriate unit for the maximum value to be displayed.
  let prefixExponent = ComputePrefixExponent(max, kibi, byteLabels);
  let prefixFactor = Math.pow(kibi, prefixExponent);

  // Compute increment on min/max after conversion to the appropriate prefix unit.
  let increment = computeNormalizedIncrement(max / prefixFactor - min / prefixFactor, tickCount);

  // Create axis domain by multiplying computed increment by prefix factor.
  let axisDomain = new AxisDomain(min, max, increment * prefixFactor);

  // Apply the correct label to the axis.
  axisDomain.label = byteLabels[prefixExponent];

  // Format ticks to display as the correct prefix unit.
  let unitFormat: (v: number) => string;
  if (Math.floor(increment) !== increment) {
      unitFormat = d3.format(".1f");
  } else {
      unitFormat = d3.format("s");
  }
  axisDomain.tickFormat = (v: number) => {
    return unitFormat(v / prefixFactor);
  };

  axisDomain.guideFormat = Bytes;
  return axisDomain;
}

const durationLabels = ["nanoseconds", "microseconds", "milliseconds", "seconds"];

function ComputeDurationAxisDomain(
  min: number, max: number, tickCount: number,
): AxisDomain {
  let prefixExponent = ComputePrefixExponent(max, 1000, durationLabels);
  let prefixFactor = Math.pow(1000, prefixExponent);

  // Compute increment on min/max after conversion to the appropriate prefix unit.
  let increment = computeNormalizedIncrement(max / prefixFactor - min / prefixFactor, tickCount);

  // Create axis domain by multiplying computed increment by prefix factor.
  let axisDomain = new AxisDomain(min, max, increment * prefixFactor);

  // Apply the correct label to the axis.
  axisDomain.label = durationLabels[prefixExponent];

  // Format ticks to display as the correct prefix unit.
  let unitFormat: (v: number) => string;
  if (Math.floor(increment) !== increment) {
      unitFormat = d3.format(".1f");
  } else {
      unitFormat = d3.format("s");
  }
  axisDomain.tickFormat = (v: number) => {
    return unitFormat(v / prefixFactor);
  };

  axisDomain.guideFormat = Duration;
  return axisDomain;
}

const percentIncrementTable = [0.25, 0.5, 0.75, 1.0];

function ComputePercentageAxisDomain(
  min: number, max: number, tickCount: number,
) {
  let range = max - min;
  let increment = computeNormalizedIncrement(range, tickCount, percentIncrementTable);
  let axisDomain = new AxisDomain(min, max, increment);
  axisDomain.label = "percentage";
  axisDomain.tickFormat = d3.format(".0%");
  axisDomain.guideFormat = d3.format(".2%");
  return axisDomain;
}

let timeIncrementDurations = [
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
let timeIncrements = _.map(timeIncrementDurations, (inc) => inc.asMilliseconds());

function ComputeTimeAxisDomain(
  min: number, max: number, tickCount: number,
): AxisDomain {
  // Compute increment; for time scales, this is taken from a table of allowed
  // values.
  let increment = 0;
  {
    let rawIncrement = (max - min) / (tickCount + 1);
    // Compute X such that 0 <= rawIncrement/10^x <= 1
    let tbl = timeIncrements;
    let normalizedIncrementIdx = _.sortedIndex(tbl, rawIncrement);
    if (normalizedIncrementIdx === tbl.length) {
      normalizedIncrementIdx--;
    }
    increment = tbl[normalizedIncrementIdx];
  }

  // Do not normalize min/max for time axis.
  let axisDomain = new AxisDomain(min, max, increment, false);

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

// SeenTimestamps is used to track which timestamps have been included in the
// current dataset and which are missing
interface SeenTimestamps {
  [key: number]: boolean;
}

/**
 * getTimestamps is a helper function that takes graph data from the server and
 * returns a SeenTimestamps object with all the values set to false. This object
 * is used to track missing timestamps for each individual dataset.
 */
function getTimestamps(metrics: React.ReactElement<MetricProps>[], data: TSResponse): SeenTimestamps {
  return _(metrics)
     // Get all the datapoints from all series in a single array.
    .flatMap((_s, idx) => data.results[idx].datapoints)
    // Create a map keyed by the datapoint timestamps.
    .keyBy((d) => d.timestamp_nanos.toNumber())
    // Set all values to false, since we only want the keys.
    .mapValues(() => false)
    // Unwrap the lodash object.
    .value();
}

type formattedDatum = {
  values: protos.cockroach.ts.tspb.TimeSeriesDatapoint$Properties,
  key: string,
  area: boolean,
  fillOpacity: number,
};

/**
 * ProcessDataPoints is a helper function to process graph data from the server
 * into a format appropriate for display on an NVD3 graph. This includes the
 * computation of domains and ticks for all axes.
 */
function ProcessDataPoints(
  metrics: React.ReactElement<MetricProps>[],
  axis: React.ReactElement<AxisProps>,
  data: TSResponse,
  timeInfo: QueryTimeInfo,
  stacked = false,
) {
  let yAxisRange = new AxisRange();
  let xAxisRange = new AxisRange();

  let formattedData: formattedDatum[] = [];

  // timestamps has a key for all the timestamps present across all datasets
  let timestamps = getTimestamps(metrics, data);

  _.each(metrics, (s, idx) => {
    let result = data.results[idx];
    if (result) {
      if (!stacked) {
        yAxisRange.addPoints(_.map(result.datapoints, (dp) => dp.value));
      } else {
        yAxisRange.addStackedPoints(_.map(result.datapoints, (dp) => { return { key: dp.timestamp_nanos.toNumber(), value: dp.value }; }));
      }
      xAxisRange.addPoints(_.map([timeInfo.start.toNumber(), timeInfo.end.toNumber()], NanoToMilli));

      // Drop any returned points at the beginning that have a lower timestamp
      // than the explicitly queried domain. This works around a bug in NVD3
      // which causes the interactive guideline to highlight the wrong points.
      // https://github.com/novus/nvd3/issues/1913
      let datapoints = _.dropWhile(result.datapoints, (dp) => {
        return NanoToMilli(dp.timestamp_nanos.toNumber()) < xAxisRange.min;
      });

      // Fill in null values for series where a value is missing. This
      // correction is needed for stackedAreaCharts to to display correctly.
      let seenTimestamps: SeenTimestamps = _.clone(timestamps);
      _.each(datapoints, (d) => seenTimestamps[d.timestamp_nanos.toNumber()] = true);
      _.each(seenTimestamps, (seen, ts) => {
        if (!seen) {
          datapoints.push({
            timestamp_nanos: Long.fromString(ts),
            value: null,
          });
        }
      });

      formattedData.push({
        values: datapoints,
        key: s.props.title || s.props.name,
        area: true,
        fillOpacity: .1,
      });
    }
  });

  if (_.isNumber(axis.props.yLow)) {
    yAxisRange.addPoints([axis.props.yLow]);
  }
  if (_.isNumber(axis.props.yHigh)) {
    yAxisRange.addPoints([axis.props.yHigh]);
  }

  let yAxisDomain: AxisDomain;
  switch (axis.props.units) {
    case AxisUnits.Bytes:
      yAxisDomain = ComputeByteAxisDomain(yAxisRange.min, yAxisRange.max, 3);
      break;
    case AxisUnits.Duration:
      yAxisDomain = ComputeDurationAxisDomain(yAxisRange.min, yAxisRange.max, 3);
      break;
    case AxisUnits.Percentage:
      yAxisDomain = ComputePercentageAxisDomain(yAxisRange.min, yAxisRange.max, 3);
      break;
    default:
      yAxisDomain = ComputeCountAxisDomain(yAxisRange.min, yAxisRange.max, 3);
  }
  let xAxisDomain = ComputeTimeAxisDomain(xAxisRange.min, xAxisRange.max, 10);

  return {
    formattedData,
    yAxisDomain,
    xAxisDomain,
  };
}

export function InitLineChart(chart: nvd3.LineChart | nvd3.StackedAreaChart) {
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
      .showMaxMin(true);
}

/**
 * ProcessDataPoints is a helper function to process graph data from the server
 * into a format appropriate for display on an NVD3 graph. This includes the
 * computation of domains and ticks for all axes.
 */
export function ConfigureLineChart(
  chart: nvd3.LineChart | nvd3.StackedAreaChart,
  svgEl: SVGElement,
  metrics: React.ReactElement<MetricProps>[],
  axis: React.ReactElement<AxisProps>,
  data: TSResponse,
  timeInfo: QueryTimeInfo,
  stacked = false,
) {
  chart.showLegend(metrics.length > 1 && metrics.length <= MAX_LEGEND_SERIES);
  let formattedData: formattedDatum[];

  if (data) {
    let processed = ProcessDataPoints(metrics, axis, data, timeInfo, stacked);
    formattedData = processed.formattedData;
    let {yAxisDomain, xAxisDomain } = processed;

    chart.yDomain(yAxisDomain.domain());
    if (!axis.props.label) {
      chart.yAxis.axisLabel(yAxisDomain.label);
    }
    chart.xDomain(xAxisDomain.domain());

    // This is ridiculous, but this NVD3 setting appears to be a relative
    // adjustment to a constant pixel distance.
    chart.yAxis.axisLabelDistance(-10);

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

// MetricsDataComponentProps is an interface that should be implemented by any
// components directly contained by a MetricsDataProvider. It is used by a
// MetricsDataProvider to pass query data to its contained component.
export interface MetricsDataComponentProps {
  data?: TSResponse;
  timeInfo?: QueryTimeInfo;
  // Allow graphs to declare a single source for all metrics. This is a
  // convenient syntax for a common use case where all metrics on a graph are
  // are from the same source set.
  sources?: string[];
}

// TextGraph is a proof-of-concept component used to demonstrate that
// MetricsDataProvider is working correctly. Used in tests.
export class TextGraph extends React.Component<MetricsDataComponentProps, {}> {
  render() {
    return <div>{
      (this.props.data && this.props.data.results) ? this.props.data.results.join(":") : ""
    }</div>;
  }
}

// updateLinkedGuidelines should be invoked whenever a user hovers over an NVD3
// graph. When this occurs, NVD3 displays an "interactive guideline", which is a
// vertical line that highlights the X-axis coordinate the mouse is currently
// over.
//
// This function is responsible for maintaining "linked" guidelines on all other
// graphs on the page; a "linked" guideline highlights the same X-axis
// coordinate on different graphs currently visible on the same page. This
// allows the user to visually correlate a single X-axis coordinate across
// multiple visible graphs.
export function updateLinkedGuidelines(hoverGraph: SVGElement) {
  // Select the interactive guideline being displayed by NVD3 on the currently
  // hovered graph, if it exists. Construct a data array for use by d3; this
  // allows us to use d3's "enter()/exit()" functions to cleanly add and remove
  // the guideline from other graphs.
  const sourceGuideline = d3.select(hoverGraph).select("line.nv-guideline");
  const data = !sourceGuideline.empty() ? [sourceGuideline] : [];

  // Select all other graphs on the page. A linked guideline will be applied
  // to these.
  const otherGraphs = d3.selectAll(".linked-guideline").filter(function () {
    return this !== hoverGraph;
  });

  otherGraphs.each(function () {
    // Linked guideline will be inserted inside of the "nv-wrap" element of the
    // nvd3 graph. This element has several translations applied to it by nvd3
    // which allow us to easily display the linked guideline at the correct
    // position.
    const wrapper = d3.select(this).select(".nv-wrap");
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
      .attr("x1", (d) => d.attr("x1"))
      .attr("x2", (d) => d.attr("x2"))
      .attr("y1", (d) => d.attr("y1"))
      .attr("y2", (d) => d.attr("y2"));
  });
}
