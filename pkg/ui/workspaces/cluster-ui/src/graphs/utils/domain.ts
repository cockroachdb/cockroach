// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import max from "lodash/max";
import min from "lodash/min";
import sortedIndex from "lodash/sortedIndex";
import moment from "moment-timezone";

import {
  BytesFitScale,
  ComputeByteScale,
  ComputeDurationScale,
  DATE_WITH_SECONDS_FORMAT_24_TZ,
  FormatWithTimezone,
} from "src/util/format";

/**
 * AxisUnits is an enumeration used to specify the type of units being displayed
 * on an Axis.
 */
export enum AxisUnits {
  /**
   * Units are a simple count.
   */
  Count,
  /**
   * Units are a count of bytes.
   */
  Bytes,
  /**
   * Units are durations expressed in nanoseconds.
   */
  Duration,
  /**
   * Units are percentages expressed as fractional values of 1 (1.0 = 100%).
   */
  Percentage,
}

// The number of ticks to display on a Y axis.
const Y_AXIS_TICK_COUNT = 3;

// The number of ticks to display on an X axis.
const X_AXIS_TICK_COUNT = 10;

// A tuple of numbers for the minimum and maximum values of an axis.
export type Extent = [number, number];

/**
 * AxisDomain is a class that describes the domain of a graph axis; this
 * includes the minimum/maximum extend, tick values, and formatting information
 * for axis values as displayed in various contexts.
 */
export class AxisDomain {
  // the values at the ends of the axis.
  extent: Extent;
  // numbers at which an intermediate tick should be displayed on the axis.
  ticks: number[] = [0, 1];
  // label returns the label for the axis.
  label = "";
  // tickFormat returns a function used to format the tick values for display.
  tickFormat: (n: number) => string = n => n.toString();
  // guideFormat returns a function used to format the axis values in the
  // chart's interactive guideline.
  guideFormat: (n: number) => string = n => n.toString();

  // constructs a new AxisDomain with the given minimum and maximum value, with
  // ticks placed at intervals of the given increment in between the min and
  // max. Ticks are always "aligned" to values that are even multiples of
  // increment. Min and max are also aligned by default - the aligned min will
  // be <= the provided min, while the aligned max will be >= the provided max.
  constructor(extent: Extent, increment: number, alignMinMax = true) {
    let min = extent[0];
    let max = extent[1];
    if (alignMinMax) {
      min = min - (min % increment);
      if (max % increment !== 0) {
        max = max - (max % increment) + increment;
      }
    }

    this.extent = [min, max];

    this.ticks = [];
    for (
      let nextTick = min - (min % increment) + increment;
      nextTick < this.extent[1];
      nextTick += increment
    ) {
      this.ticks.push(nextTick);
    }
  }
}

const countIncrementTable = [
  0.1, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.7, 0.75, 0.8, 0.9, 1.0,
];

// converts a number from raw format to abbreviation k,m,b,t
// e.g. 1500000 -> 1.5m
// @params: num = number to abbreviate, fixed = max number of decimals
const abbreviateNumber = (num: number, fixedDecimals: number) => {
  if (num === 0) {
    return "0";
  }

  // number of decimal places to show
  const fixed = fixedDecimals < 0 ? 0 : fixedDecimals;

  const parts = num.toPrecision(2).split("e");

  // get power: floor at decimals, ceiling at trillions
  const powerIdx =
    parts.length === 1
      ? 0
      : Math.floor(Math.min(Number(parts[1].slice(1)), 14) / 3);

  // then divide by power
  let numAbbrev = Number(
    powerIdx < 1
      ? num.toFixed(fixed)
      : (num / Math.pow(10, powerIdx * 3)).toFixed(fixed),
  );

  numAbbrev = numAbbrev < 0 ? numAbbrev : Math.abs(numAbbrev); // enforce -0 is 0
  // append power
  return numAbbrev + ["", "k", "m", "b", "t"][powerIdx];
};

const formatPercentage = (n: number, fractionDigits: number) => {
  return n?.toFixed ? `${(n * 100).toFixed(fractionDigits)}%` : "0%";
};

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
  range: number,
  incrementTbl: number[] = countIncrementTable,
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
  const normalizedIncrementIdx = sortedIndex(incrementTbl, rawIncrement);
  return incrementTbl[normalizedIncrementIdx] * Math.pow(10, x);
}

function computeAxisDomain(extent: Extent, factor = 1): AxisDomain {
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
    unitFormat = (n: number) => n?.toFixed(1) || "0";
  } else {
    unitFormat = (n: number) => abbreviateNumber(n, 4);
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
  const metricFormat = (n: number) => abbreviateNumber(n, 4);
  const decimalFormat = (n: number) => n?.toFixed(4) || "0";
  axisDomain.guideFormat = (n: number) => {
    if (n < 1) {
      return decimalFormat(n);
    }
    return metricFormat(n);
  };

  return axisDomain;
}

export function ComputeByteAxisDomain(extent: Extent): AxisDomain {
  // Compute an appropriate unit for the maximum value to be displayed.
  const scale = ComputeByteScale(extent[1]);
  const prefixFactor = scale.value;

  const axisDomain = computeAxisDomain(extent, prefixFactor);

  axisDomain.label = scale.units;

  axisDomain.guideFormat = BytesFitScale(scale.units);
  return axisDomain;
}

export function ComputeDurationAxisDomain(extent: Extent): AxisDomain {
  const extentScales = extent.map(e => ComputeDurationScale(e));

  const axisDomain = computeAxisDomain(extent, extentScales[1].value);
  axisDomain.label = extentScales[1].units;

  axisDomain.guideFormat = (nanoseconds: number) => {
    if (!nanoseconds) {
      return `0.00 ${extentScales[0].units}`;
    }
    const scale = ComputeDurationScale(nanoseconds);
    return `${(nanoseconds / scale.value).toFixed(2)} ${scale.units}`;
  };

  return axisDomain;
}

const percentIncrementTable = [0.25, 0.5, 0.75, 1.0];

function ComputePercentageAxisDomain(min: number, max: number) {
  const range = max - min;
  const increment = computeNormalizedIncrement(range, percentIncrementTable);
  const axisDomain = new AxisDomain([min, max], increment);
  axisDomain.label = "percentage";
  axisDomain.tickFormat = (n: number) => formatPercentage(n, 1);
  axisDomain.guideFormat = (n: number) => formatPercentage(n, 2);
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
const timeIncrements: number[] = timeIncrementDurations.map(inc =>
  inc.asMilliseconds(),
);

function ComputeTimeAxisDomain(extent: Extent, timezone: string): AxisDomain {
  // Compute increment; for time scales, this is taken from a table of allowed
  // values.
  let increment = 0;
  {
    const rawIncrement = (extent[1] - extent[0]) / (X_AXIS_TICK_COUNT + 1);
    // Compute X such that 0 <= rawIncrement/10^x <= 1
    const tbl = timeIncrements;
    let normalizedIncrementIdx = sortedIndex(tbl, rawIncrement);
    if (normalizedIncrementIdx === tbl.length) {
      normalizedIncrementIdx--;
    }
    increment = tbl[normalizedIncrementIdx];
  }

  // Do not normalize min/max for time axis.
  const axisDomain = new AxisDomain(extent, increment, false);

  axisDomain.label = "time";

  const tickDateFormatter = (d: Date, format: string) =>
    moment(d).tz(timezone).format(format);

  const format =
    increment < moment.duration(24, "hours").asMilliseconds()
      ? "H:mm"
      : "MM/DD H:mm";

  axisDomain.tickFormat = (n: number) => {
    return tickDateFormatter(new Date(n), format);
  };

  axisDomain.guideFormat = millis => {
    return FormatWithTimezone(
      moment(millis),
      DATE_WITH_SECONDS_FORMAT_24_TZ,
      timezone,
    );
  };
  return axisDomain;
}

export function calculateYAxisDomain(
  axisUnits: AxisUnits,
  data: number[],
): AxisDomain {
  const allDatapoints = data.concat([0, 1]);
  const yExtent = [min(allDatapoints), max(allDatapoints)] as Extent;

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

export function calculateXAxisDomain(
  startMillis: number,
  endMillis: number,
  timezone = "Etc/UTC",
): AxisDomain {
  return ComputeTimeAxisDomain([startMillis, endMillis] as Extent, timezone);
}

export function calculateXAxisDomainBarChart(
  startMillis: number,
  endMillis: number,
  samplingIntervalMillis: number,
  timezone = "Etc/UTC",
): AxisDomain {
  // For bar charts, we want to render past endMillis to fully render the
  // last bar. We should extend the x axis to the next sampling interval.
  return ComputeTimeAxisDomain(
    [startMillis, endMillis + samplingIntervalMillis] as Extent,
    timezone,
  );
}
