const kibi = 1024;
const byteUnits = [
  "B",
  "KiB",
  "MiB",
  "GiB",
  "TiB",
  "PiB",
  "EiB",
  "ZiB",
  "YiB",
];

const BytesFitScale =
  (scale) =>
  (bytes) => {
    if (!bytes) {
      return `0.00 ${scale}`;
    }
    const n = byteUnits.indexOf(scale);
    return `${(bytes / Math.pow(kibi, n)).toFixed(2)} ${scale}`;
  };

// computePrefixExponent is used to compute an appropriate display unit for a value
// for which the units have metric-style prefixes available. For example, the
// value may be expressed in bytes, but we may want to display it on the graph
// as a larger prefix unit (such as "kilobytes" or "gigabytes") in order to make
// the numbers more readable.
function ComputePrefixExponent(
  value,
  prefixMultiple,
  prefixList,
) {
  // Compute the metric prefix that will be used to label the axis.
  let maxUnits = Math.abs(value);
  let prefixScale;
  for (
    prefixScale = 0;
    maxUnits >= prefixMultiple && prefixScale < prefixList.length - 1;
    prefixScale++
  ) {
    maxUnits /= prefixMultiple;
  }
  return prefixScale;
}

/**
 * ComputeByteScale calculates the appropriate scale factor and unit to use to
 * display a given byte value, without actually converting the value.
 *
 * This is used to prescale byte values before passing them to a d3-axis.
 */
function ComputeByteScale(bytes) {
  const scale = ComputePrefixExponent(bytes, kibi, byteUnits);
  return {
    value: Math.pow(kibi, scale),
    units: byteUnits[scale],
  };
}

const durationUnitsDescending = [
  { units: "hr", value: 60 * 60 * 1_000_000_000 },
  { units: "min", value: 60 * 1_000_000_000 },
  { units: "s", value: 1_000_000_000 },
  { units: "ms", value: 1_000_000 },
  { units: "µs", value: 1_000 },
  { units: "ns", value: 1 },
];

/**
 * ComputeDurationScale calculates an appropriate scale factor and unit to use
 * to display a given duration value, without actually converting the value.
 */
function ComputeDurationScale(ns) {
  return durationUnitsDescending.find(
    ({ value }) => ns / value >= 1 || value === 1,
  );
}

const DATE_WITH_SECONDS_FORMAT_24_TZ = {
  year: 'numeric',
  month: 'short',
  day: '2-digit',
  hour: 'numeric',
  minute: '2-digit',
  second: '2-digit',
  hour12: false,
  timeZoneName: 'short'
}

function FormatWithTimezone(milliseconds, format, timezone) {
  const date = new Date(milliseconds);
  const formatter = new Intl.DateTimeFormat('en-US', {timezone: timezone, ...format});
  return formatter.format(date)
}

const AxisUnits = {
  Count: "COUNT",
  Bytes: "BYTES",
  Duration: "DURATION",
  Percentage: "PERCENTAGE",
  DurationMillis: "DURATION_MILLIS",
}

// The number of ticks to display on a Y axis.
const Y_AXIS_TICK_COUNT = 3;

// The number of ticks to display on an X axis.
const X_AXIS_TICK_COUNT = 10;

/**
 * AxisDomain is a class that describes the domain of a graph axis; this
 * includes the minimum/maximum extend, tick values, and formatting information
 * for axis values as displayed in various contexts.
 */
class AxisDomain {
  // the values at the ends of the axis.
  extent;
  // numbers at which an intermediate tick should be displayed on the axis.
  ticks = [0, 1];
  // label returns the label for the axis.
  label = "";
  // tickFormat returns a function used to format the tick values for display.
  tickFormat = n => n.toString();
  // guideFormat returns a function used to format the axis values in the
  // chart's interactive guideline.
  guideFormat = n => n.toString();

  // constructs a new AxisDomain with the given minimum and maximum value, with
  // ticks placed at intervals of the given increment in between the min and
  // max. Ticks are always "aligned" to values that are even multiples of
  // increment. Min and max are also aligned by default - the aligned min will
  // be <= the provided min, while the aligned max will be >= the provided max.
  constructor(extent, increment, alignMinMax = true) {
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
const abbreviateNumber = (num, fixedDecimals) => {
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

const formatPercentage = (n, fractionDigits) => {
  return n?.toFixed ? `${(n * 100).toFixed(fractionDigits)}%` : "0%";
};

function sortedIndex(array, value) {
  for (let i = 0; i < array.length; i++) {
    if (array[i] >= value) {
      return i;
    }
  }
  return array.length;
}

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
  range,
  incrementTbl = countIncrementTable,
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

function computeAxisDomain(extent, factor = 1) {
  const range = extent[1] - extent[0];

  // Compute increment on min/max after conversion to the appropriate prefix unit.
  const increment = computeNormalizedIncrement(range / factor);

  // Create axis domain by multiplying computed increment by prefix factor.
  const axisDomain = new AxisDomain(extent, increment * factor);

  // If the tick increment is fractional (e.g. 0.2), we display a decimal
  // point. For non-fractional increments, we display with no decimal points
  // but with a metric prefix for large numbers (i.e. 1000 will display as "1k")
  let unitFormat;
  if (Math.floor(increment) !== increment) {
    unitFormat = (n) => n?.toFixed(1) || "0";
  } else {
    unitFormat = (n) => abbreviateNumber(n, 4);
  }
  axisDomain.tickFormat = (v) => unitFormat(v / factor);

  return axisDomain;
}

function ComputeCountAxisDomain(extent) {
  const axisDomain = computeAxisDomain(extent);

  // For numbers larger than 1, the tooltip displays fractional values with
  // metric multiplicative prefixes (e.g. kilo, mega, giga). For numbers smaller
  // than 1, we simply display the fractional value without converting to a
  // fractional metric prefix; this is because the use of fractional metric
  // prefixes (i.e. milli, micro, nano) have proved confusing to users.
  const metricFormat = (n) => abbreviateNumber(n, 4);
  const decimalFormat = (n) => n?.toFixed(4) || "0";
  axisDomain.guideFormat = (n) => {
    if (n < 1) {
      return decimalFormat(n);
    }
    return metricFormat(n);
  };

  return axisDomain;
}

function ComputeByteAxisDomain(extent) {
  // Compute an appropriate unit for the maximum value to be displayed.
  const scale = ComputeByteScale(extent[1]);
  const prefixFactor = scale.value;

  const axisDomain = computeAxisDomain(extent, prefixFactor);

  axisDomain.label = scale.units;

  axisDomain.guideFormat = BytesFitScale(scale.units);
  return axisDomain;
}

function ComputeDurationAxisDomain(extent) {
  const extentScales = extent.map(e => ComputeDurationScale(e));

  const axisDomain = computeAxisDomain(extent, extentScales[1].value);
  axisDomain.label = extentScales[1].units;

  axisDomain.guideFormat = (nanoseconds) => {
    if (!nanoseconds) {
      return `0.00 ${extentScales[0].units}`;
    }
    const scale = ComputeDurationScale(nanoseconds);
    return `${(nanoseconds / scale.value).toFixed(2)} ${scale.units}`;
  };

  return axisDomain;
}

const percentIncrementTable = [0.25, 0.5, 0.75, 1.0];

function ComputePercentageAxisDomain(min, max) {
  const range = max - min;
  const increment = computeNormalizedIncrement(range, percentIncrementTable);
  const axisDomain = new AxisDomain([min, max], increment);
  axisDomain.label = "percentage";
  axisDomain.tickFormat = (n) => formatPercentage(n, 1);
  axisDomain.guideFormat = (n) => formatPercentage(n, 2);
  return axisDomain;
}

const timeIncrements = [
  1 * 60 * 1000,        // 1 minute
  5 * 60 * 1000,        // 5 minutes
  10 * 60 * 1000,       // 10 minutes
  15 * 60 * 1000,       // 15 minutes
  30 * 60 * 1000,       // 30 minutes
  1 * 60 * 60 * 1000,   // 1 hour
  2 * 60 * 60 * 1000,   // 2 hours
  3 * 60 * 60 * 1000,   // 3 hours
  6 * 60 * 60 * 1000,   // 6 hours
  12 * 60 * 60 * 1000,  // 12 hours
  24 * 60 * 60 * 1000,  // 24 hours
  7 * 24 * 60 * 60 * 1000, // 1 week
];

function ComputeTimeAxisDomain(extent, timezone) {
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

  const tickDateFormatter = (n, format) =>
    FormatWithTimezone(n, format, timezone)

  const format =
    increment < 24 * 60 * 60 * 1000
      ? {
    hour: 'numeric',
    minute: '2-digit',
    hour12: false,
  }
      : {
    month: 'numeric',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    hour12: false,
  };

  axisDomain.tickFormat = (n) => {
    return tickDateFormatter(n, format);
  };

  axisDomain.guideFormat = millis => {
    return FormatWithTimezone(
      millis,
      DATE_WITH_SECONDS_FORMAT_24_TZ,
      timezone,
    );
  };
  return axisDomain;
}

function calculateYAxisDomain(
  axisUnits,
  data,
) {
  const allDatapoints = data.concat([0, 1]);
  const yExtent = [Math.min(...allDatapoints), Math.max(...allDatapoints)];

  switch (axisUnits) {
    case AxisUnits.Bytes:
      return ComputeByteAxisDomain(yExtent);
    case AxisUnits.Duration:
      return ComputeDurationAxisDomain(yExtent);
    case AxisUnits.Percentage:
      return ComputePercentageAxisDomain(yExtent[0], yExtent[1]);
    case AxisUnits.DurationMillis:
      return ComputeDurationAxisDomain([
        Math.min(...allDatapoints),
        Math.max(Math.max(...allDatapoints), 1000000),
      ]);
    default:
      return ComputeCountAxisDomain(yExtent);
  }
}

function calculateXAxisDomain(
  startMillis,
  endMillis,
  timezone = "Etc/UTC",
) {
  return ComputeTimeAxisDomain([startMillis, endMillis], timezone);
}

function calculateXAxisDomainBarChart(
  startMillis,
  endMillis,
  samplingIntervalMillis,
  timezone = "Etc/UTC",
) {
  // For bar charts, we want to render past endMillis to fully render the
  // last bar. We should extend the x axis to the next sampling interval.
  return ComputeTimeAxisDomain(
    [startMillis, endMillis + samplingIntervalMillis],
    timezone,
  );
}
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

// export type formattedSeries = {
//   values: protos.cockroach.ts.tspb.ITimeSeriesDatapoint[];
//   key: string;
//   area: boolean;
//   fillOpacity: number;
//   color?: string;
// };

// logic to decide when to show a metric based on the query's result
function canShowMetric(result) {
  return result.datapoints.length && result.datapoints.length > 0;
}

function formatMetricData(
  metrics,
  data,
) {
  const formattedData = [];

  metrics.forEach((s, idx) => {
    const result = data.results[idx];
    if (result && canShowMetric(result)) {
      const transform = s.transform ?? (d => d);
      const scale = s.scale ?? 1;
      const scaledAndTransformedValues = transform(result.datapoints).map(
        v => ({
          ...v,
          // if defined scale/transform it, otherwise remain undefined
          value: v.value && scale * v.value,
        }),
      );

      formattedData.push({
        values: scaledAndTransformedValues,
        key: s.title || s.name,
        area: true,
        fillOpacity: 0.1,
        color: s.color,
      });
    }
  });

  return formattedData;
}

function hoverTooltipPlugin(
  xFormatter,
  yFormatter,
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

  let seriesIdx = null;
  let dataIdx = null;
  let over;
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

 function setTooltip(u) {
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
      markerNode.style.background = stroke(u, seriesIdx);
    } else if (typeof stroke === "string") {
      markerNode.style.borderColor = stroke;
    }
  }

  return {
    hooks: {
      ready: [
        (u) => {
          over = u.over;
          tooltipLeftOffset = parseFloat(over.style.left);
          tooltipTopOffset = parseFloat(over.style.top);
          u.root.querySelector(".u-wrap").appendChild(tooltip);
        },
      ],
      setCursor: [
        (u) => {
          const c = u.cursor;

          if (dataIdx !== c.idx) {
            dataIdx = c.idx;

            if (seriesIdx != null) setTooltip(u);
          }
        },
      ],
      setSeries: [
        (u, sidx) => {
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

function without(array, ...values) {
  if (!Array.isArray(array)) {
    return [];
  }
  
  return array.filter(item => !values.includes(item));
}


// configureUPlotLineChart constructs the uplot Options object based on
// information about the metrics, axis, and data that we'd like to plot.
// Most of the settings are defined asfunctions instead of static values
// in order to take advantage of auto-updating behavior built into uPlot
// when we send new data to the uPlot object. This will ensure that all
// axis labeling and extent settings get updated properly.
function configureUPlotLineChart(
  metrics,
  axis, //: React.ReactElement<AxisProps>,
  data, //: TSResponse,
  setMetricsFixedWindow, //: (startMillis: number, endMillis: number) => void,
  getLatestXAxisDomain, //: () => AxisDomain,
  getLatestYAxisDomain, //: () => AxisDomain,
  el, // container element for calculating width
) {
  const formattedRaw = formatMetricData(metrics, data);
  // Copy palette over since we mutate it in the `series`function
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
    width: el.clientWidth || 947, // Use container width, fallback to 947
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
        fill: (u, i) => {
          const stroke = u.series[i].stroke;
          if (typeof stroke === "function" && stroke.length === 0) {
            return stroke(u, i);
          } else if (typeof stroke === "string") {
            return stroke;
          }
        },
      },
    },
    // By default, uPlot expects unix seconds in the x axis.
    // This setting defaults it to milliseconds which our
    // internalfunctions already supported.
    // ms: 1, // Note(davidh): I'm converting nanos from the server into seconds
    // These settings govern how the individual series are
    // drawn and how their values are displayed in the legend.
    series: [
      {
        value: (u, rawValue, seriesIdx) => {
          const v = (rawValue === null) ?  u.data[seriesIdx].at(-1) : rawValue
          return getLatestXAxisDomain().guideFormat(v)
        },
      },
      // Generate a series object for reach of our results
      // picking colors from our palette.
      ...formattedRaw.map(result => {
        let color;
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
          value: (u, rawValue, seriesIdx) => {
            const v = (rawValue === null) ? u.data[seriesIdx].at(-1) : rawValue
            return getLatestYAxisDomain().guideFormat(v)
          },
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
          axis.label +
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

// touPlot formats our timeseries data into the format
// uPlot expects which is a 2-dimensional array where the
// first array contains the x-values (time).
function touPlot(
  data, //: formattedSeries[],
  sampleDuration, //?: Long,
) {
  // Here's an example of what this code is attempting to control for.
  // We produce `result` series that contain their own x-values. uPlot
  // expects *one* x-series that all y-values match up to. So first we
  // need to take the union of all timestamps across all series, and then
  // produce y-values that match up to those. Any missing values will
  // be set to null and uPlot expects that.
  //
  // our data: [
  //   [(11:00, 1),             (11:05, 2),  (11:06, 3), (11:10, 4),           ],
  //   [(11:00, 1), (11:03, 20),             (11:06, 7),            (11:11, 40)],
  // ]
  //
  // for uPlot: [
  //   [11:00, 11:03, 11:05, 11:06, 11:10, 11:11]
  //   [1, null, 2, 3, 4, null],
  //   [1, 20, null, 7, null, 40],
  // ]
  if (!data || data.length === 0) {
    return [[]];
  }

  const xValuesComplete = [
    ...new Set(
      data.flatMap(series =>
        series.values.map(d => Number(BigInt(d.timestampNanos) / 1_000_000n)), // TODO(davidh): This needs to support standard numbers I think instead of longs
      ),
    ),
  ].sort((a, b) => a - b);

  const xValuesWithGaps = fillGaps(xValuesComplete, sampleDuration);

  const yValuesComplete = data.map(series => {
    return xValuesWithGaps.map(ts => {
      const found = series.values.find(
        dp =>  Number(BigInt(dp.timestampNanos) / 1_000_000n) === ts,
      );
      return found ? found.value : null;
    });
  });

  return [xValuesWithGaps, ...yValuesComplete];
}

// TODO (koorosh): the same logic can be achieved with uPlot's series.gaps API starting from 1.6.15 version.
function fillGaps(
  data, //: uPlot.AlignedData[0],
  sampleDuration, //?: Long,
) {
  if (data.length === 0 || !sampleDuration) {
    return data;
  }
  const sampleDurationMillis = sampleDuration;
  const dataPointsNumber = data.length;
  const expectedPointsNumber =
    (data[data.length - 1] - data[0]) / sampleDurationMillis + 1;
  if (dataPointsNumber === expectedPointsNumber) {
    return data;
  }
  const yDataWithGaps = [];
  // validate time intervals for y axis data
  data.forEach((d, idx, arr) => {
    // case for the last item
    if (idx === arr.length - 1) {
      yDataWithGaps.push(d);
    }
    const nextItem = data[idx + 1];
    if (nextItem - d <= sampleDurationMillis) {
      yDataWithGaps.push(d);
      return;
    }
    for (
      let i = d;
      nextItem - i >= sampleDurationMillis;
      i = i + sampleDurationMillis
    ) {
      yDataWithGaps.push(i);
    }
  });
  return yDataWithGaps;
}

function NanoToMilli(nano) {
  return nano / 1.0e6;
}

const sampleDuration = 10000; // nanos in a 10 seconds
function setNewTimeRange(startMillis, endMillis) {
    if (startMillis === endMillis) return;
    const start = util.MilliToSeconds(startMillis);
    const end = util.MilliToSeconds(endMillis);
    const newTimeWindow = {
      start: moment.unix(start),
      end: moment.unix(end),
    };
    const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
    let newTimeScale = {
      ...findClosestTimeScale(defaultTimeScaleOptions, seconds),
      key: "Custom",
      windowSize: moment.duration(moment.unix(end).diff(moment.unix(start))),
      fixedWindowEnd: moment.unix(end),
    };
    if (this.props.adjustTimeScaleOnChange) {
      newTimeScale = this.props.adjustTimeScaleOnChange(
        newTimeScale,
        newTimeWindow,
      );
    }
    this.props.setMetricsFixedWindow(newTimeWindow);
    this.props.setTimeScale(newTimeScale);
  }

// ====================================================
// ====================================================
// ====================================================
// ====================================================
// ====================================================
// ====================================================
// ====================================================
// ====================================================


function makeuPlotChart(metrics, axis, timeInfo, timezone, data, el) {
  const fData = formatMetricData(metrics, data);
  const uPlotData = touPlot(fData, sampleDuration);

  // The values of `this.yAxisDomain` and `this.xAxisDomain`
  // are captured in arguments to `configureUPlotLineChart`
  // and are called when recomputing certain axis and
  // series options. This lets us use updated domains
  // when redrawing the uPlot chart on data change.
  const resultDatapoints = fData.flatMap(result =>
    result.values.map(dp => dp.value),
  );
  const yAxisDomain = calculateYAxisDomain(axis.units, resultDatapoints);
  const xAxisDomain = calculateXAxisDomain(
    timeInfo.start,
    timeInfo.end,
    timezone,
  );
  const options = configureUPlotLineChart(
    metrics,
    axis,
    data,
    setNewTimeRange,
    () => xAxisDomain,
    () => yAxisDomain,
    el,
  );

  return uPlot(options, uPlotData, el);
}

// TODO(davidh): unclear how yAxisDomain is recomputed here. Need to look at original react component
function updateuPlotChart(u, axis, newData) {
  // The axis label option on uPlot doesn't accept
  // afunction that recomputes the label, so we need
  // to manually update it in cases where we change
  // the scale (this happens on byte/time-based Y
  // axes where can change from MiB or KiB scales,
  // for instance).
  this.u.axes[1].label =
    axis.label +
    (this.yAxisDomain.label ? ` (${this.yAxisDomain.label})` : "");

  // Updates existing plot with new points
  u.setData(uPlotData);
}

const timezone = 'America/New_York'


// const myPlot = makeuPlotChart(metrics, axis, timeInfo, timezone, jsonOutput, document.getElementById("graph1"))

// TODO(davidh): this is for propagating data updates later
// const prevKeys =
//   prevProps.data && prevProps.data.results
//     ? formatMetricData(metrics, prevProps.data).map(s => s.key)
//     : [];
// const keys = fData.map(s => s.key);
// const sameKeys =
//   keys.every(k => prevKeys.includes(k)) &&
//   prevKeys.every(k => keys.includes(k));

// let u

// if (
//   u // && // we already created a uPlot instance
//   // prevProps.data && // prior update had data as well
//   // sameKeys // prior update had same set of series identified by key
// ) {
//   // The axis label option on uPlot doesn't accept
//   // afunction that recomputes the label, so we need
//   // to manually update it in cases where we change
//   // the scale (this happens on byte/time-based Y
//   // axes where can change from MiB or KiB scales,
//   // for instance).
//   this.u.axes[1].label =
//     axis.label +
//     (this.yAxisDomain.label ? ` (${this.yAxisDomain.label})` : "");

//   // Updates existing plot with new points
//   u.setData(uPlotData);
// } else {
//   const options = configureUPlotLineChart(
//     metrics,
//     axis,
//     data,
//     setNewTimeRange,
//     () => xAxisDomain,
//     () => yAxisDomain,
//   );

//   if (u) {
//     u.destroy();
//   }
//   u = uPlot(options, uPlotData, document.getElementById("graph1"));
// }




// LineGraph wraps the uPlot library into a React component
// when the component is first initialized, we wait until
// data is available and then construct the uPlot object
// and store its ref in a global variable.
// Once we receive updates to props, we push new data to the
// uPlot object.
// InternalLinegraph is  for testing.
// class InternalLineGraph extends React.Component {
//   constructor(props) {
//     super(props);

//     this.setNewTimeRange = this.setNewTimeRange.bind(this);
//   }

//   static defaultProps = {
//     // Marking a graph as not being KV-related is opt-in.
//     isKvGraph: true,
//   };

//   // TODO(davidh): We will be passing the axis + metrics properties directly from Javascript
//   // axis is copied from the nvd3 LineGraph component above
//   // axis = createSelector(
//   //   (props) => props.children,
//   //   children => {
//   //     const axes = findChildrenOfType(
//   //       children,
//   //       Axis,
//   //     );
//   //     if (axes.length === 0) {
//   //       // eslint-disable-next-line no-console
//   //       console.warn(
//   //         "LineGraph requires the specification of at least one axis.",
//   //       );
//   //       return null;
//   //     }
//   //     if (axes.length > 1) {
//   //       // eslint-disable-next-line no-console
//   //       console.warn(
//   //         "LineGraph currently only supports a single axis; ignoring additional axes.",
//   //       );
//   //     }
//   //     return axes[0];
//   //   },
//   // );

//   // // metrics is copied from the nvd3 LineGraph component above
//   // metrics = createSelector(
//   //   (props) => props.children,
//   //   children => {
//   //     return findChildrenOfType(
//   //       children,
//   //       Metric,
//   //     );
//   //   },
//   // );

//   // setNewTimeRange forces a
//   // reload of the rest of the dashboard at new ranges via the props
//   // `setMetricsFixedWindow` and `setTimeScale`.
//   // TODO(davidh): centralize management of query params for time range
//   // TODO(davidh): figure out why the timescale doesn't get more granular
//   // automatically when a narrower time frame is selected.
//   setNewTimeRange(startMillis, endMillis) {
//     if (startMillis === endMillis) return;
//     const start = util.MilliToSeconds(startMillis);
//     const end = util.MilliToSeconds(endMillis);
//     const newTimeWindow = {
//       start: moment.unix(start),
//       end: moment.unix(end),
//     };
//     const seconds = moment.duration(moment.utc(end).diff(start)).asSeconds();
//     let newTimeScale = {
//       ...findClosestTimeScale(defaultTimeScaleOptions, seconds),
//       key: "Custom",
//       windowSize: moment.duration(moment.unix(end).diff(moment.unix(start))),
//       fixedWindowEnd: moment.unix(end),
//     };
//     if (this.props.adjustTimeScaleOnChange) {
//       newTimeScale = this.props.adjustTimeScaleOnChange(
//         newTimeScale,
//         newTimeWindow,
//       );
//     }
//     this.props.setMetricsFixedWindow(newTimeWindow);
//     this.props.setTimeScale(newTimeScale);
//   }

//   el = React.createRef();

//   // yAxisDomain holds our computed AxisDomain object
//   // for the y Axis. Thefunction to compute this was
//   // created to support the prior iteration
//   // of our line graphs. We recompute it manually
//   // when data changes, and uPlot options have access
//   // to a closure that holds a reference to this value.
//   yAxisDomain;

//   // xAxisDomain holds our computed AxisDomain object
//   // for the x Axis. Thefunction to compute this was
//   // created to support the prior iteration
//   // of our line graphs. We recompute it manually
//   // when data changes, and uPlot options have access
//   // to a closure that holds a reference to this value.
//   xAxisDomain;

//   newTimeInfo(
//     newTimeInfo, //: QueryTimeInfo,
//     prevTimeInfo, //: QueryTimeInfo,
//   ) {
//     if (newTimeInfo.start.compare(prevTimeInfo.start) !== 0) {
//       return true;
//     }
//     if (newTimeInfo.end.compare(prevTimeInfo.end) !== 0) {
//       return true;
//     }
//     if (newTimeInfo.sampleDuration.compare(prevTimeInfo.sampleDuration) !== 0) {
//       return true;
//     }

//     return false;
//   }

//   hasDataPoints = (data) => {
//     let hasData = false;
//     data?.results?.map(result => {
//       if (result?.datapoints?.length > 0) {
//         hasData = true;
//       }
//     });
//     return hasData;
//   };

//   componentDidUpdate(prevProps) {
//     if (
//       !this.props.data?.results ||
//       (prevProps.data === this.props.data &&
//         this.u !== undefined &&
//         !this.newTimeInfo(this.props.timeInfo, prevProps.timeInfo))
//     ) {
//       return;
//     }

//     const data = this.props.data;
//     const metrics = this.metrics(this.props);
//     const axis = this.axis(this.props);

//     const fData = formatMetricData(metrics, data);
//     const uPlotData = touPlot(fData, this.props.timeInfo?.sampleDuration);

//     // The values of `this.yAxisDomain` and `this.xAxisDomain`
//     // are captured in arguments to `configureUPlotLineChart`
//     // and are called when recomputing certain axis and
//     // series options. This lets us use updated domains
//     // when redrawing the uPlot chart on data change.
//     const resultDatapoints = flatMap(fData, result =>
//       result.values.map(dp => dp.value),
//     );
//     this.yAxisDomain = calculateYAxisDomain(axis.props.units, resultDatapoints);
//     this.xAxisDomain = calculateXAxisDomain(
//       util.NanoToMilli(this.props.timeInfo.start.toNumber()),
//       util.NanoToMilli(this.props.timeInfo.end.toNumber()),
//       this.props.timezone,
//     );

//     const prevKeys =
//       prevProps.data && prevProps.data.results
//         ? formatMetricData(metrics, prevProps.data).map(s => s.key)
//         : [];
//     const keys = fData.map(s => s.key);
//     const sameKeys =
//       keys.every(k => prevKeys.includes(k)) &&
//       prevKeys.every(k => keys.includes(k));

//     if (
//       this.u && // we already created a uPlot instance
//       prevProps.data && // prior update had data as well
//       sameKeys // prior update had same set of series identified by key
//     ) {
//       // The axis label option on uPlot doesn't accept
//       // afunction that recomputes the label, so we need
//       // to manually update it in cases where we change
//       // the scale (this happens on byte/time-based Y
//       // axes where can change from MiB or KiB scales,
//       // for instance).
//       this.u.axes[1].label =
//         axis.props.label +
//         (this.yAxisDomain.label ? ` (${this.yAxisDomain.label})` : "");

//       // Updates existing plot with new points
//       this.u.setData(uPlotData);
//     } else {
//       const options = configureUPlotLineChart(
//         metrics,
//         axis,
//         data,
//         this.setNewTimeRange,
//         () => this.xAxisDomain,
//         () => this.yAxisDomain,
//       );

//       if (this.u) {
//         this.u.destroy();
//       }
//       this.u = new uPlot(options, uPlotData, this.el.current);
//     }
//   }

//   componentWillUnmount() {
//     if (this.u) {
//       this.u.destroy();
//       this.u = null;
//     }
//   }

//   render() {
//     const {
//       title,
//       subtitle,
//       tooltip,
//       data,
//       tenantSource,
//       preCalcGraphSize,
//       showMetricsInTooltip,
//     } = this.props;
//     let tt = tooltip;
//     const addLines = tooltip ? (
//       <>
//         <br />
//         <br />
//       </>
//     ) : null;
//     // Extend tooltip to include metrics names
//     if (showMetricsInTooltip) {
//       const metrics = filter(data?.results, canShowMetric);
//       if (metrics.length === 1) {
//         tt = (
//           <>
//             {tt}
//             {addLines}
//             Metric: {metrics[0].query.name}
//           </>
//         );
//       } else if (metrics.length > 1) {
//         const metricNames = unique(metrics.map(m => m.query.name));
//         tt = (
//           <>
//             {tt}
//             {addLines}
//             Metrics:
//             <ul>
//               {metricNames.map(m => (
//                 <li key={m}>{m}</li>
//               ))}
//             </ul>
//           </>
//         );
//       }
//     }

//     if (!this.hasDataPoints(data) && isSecondaryTenant(tenantSource)) {
//       return (
//         <div className="linegraph-empty">
//           <div className="header-empty">
//             <Tooltip placement="bottom" title={tooltip}>
//               <span className="title-empty">{title}</span>
//             </Tooltip>
//           </div>
//           <div className="body-empty">
//             <MonitoringIcon />
//             <span className="body-text-empty">
//               {"Metric is not currently available for this tenant."}
//             </span>
//           </div>
//         </div>
//       );
//     }
//     return (
//       <Visualization
//         title={title}
//         subtitle={subtitle}
//         tooltip={tt}
//         loading={!data}
//         preCalcGraphSize={preCalcGraphSize}
//       >
//         <div className="linegraph">
//           <div ref={this.el} />
//         </div>
//       </Visualization>
//     );
//   }
// }

function metricGraph() {
  return {
    uPlotInstance: null,
    statusMessage: 'Loading...',
    statusClass: 'loading',
    resizeObserver: null,
    resizeTimeout: null,

    init() {
      // Parse metrics and axis from data attributes
      this.metrics = JSON.parse(this.$el.dataset.metrics || '[]');
      this.axis = JSON.parse(this.$el.dataset.axis || '{}');

      // Initial render
      this.fetchAndRender();

      // Watch for time range updates using Alpine reactivity
      this.$watch(() => [this.$store.timeRange.startTimeFull, this.$store.timeRange.endTimeFull], () => {
        this.fetchAndRender();
      });

      // Set up ResizeObserver for responsive chart sizing
      const graphEl = this.$el.querySelector('.graph-container');
      this.resizeObserver = new ResizeObserver(() => {
        this.handleResize();
      });
      this.resizeObserver.observe(graphEl);
    },

    destroy() {
      if (this.resizeObserver) {
        this.resizeObserver.disconnect();
      }
      if (this.resizeTimeout) {
        clearTimeout(this.resizeTimeout);
      }
      if (this.uPlotInstance) {
        this.uPlotInstance.destroy();
      }
    },

    handleResize() {
      // Debounce resize to avoid excessive redraws
      if (this.resizeTimeout) {
        clearTimeout(this.resizeTimeout);
      }

      this.resizeTimeout = setTimeout(() => {
        if (this.uPlotInstance) {
          const graphEl = this.$el.querySelector('.graph-container');
          const newWidth = graphEl.clientWidth;
          const newHeight = 300; // Keep height constant for now
          this.uPlotInstance.setSize({ width: newWidth, height: newHeight });
        }
      }, 150); // 150ms debounce
    },

    async fetchAndRender() {
      try {
        this.statusMessage = 'Loading metric data...';
        this.statusClass = 'loading';

        const timeRange = Alpine.store('timeRange');
        const startNanos = timeRange.getStartNanos();
        const endNanos = timeRange.getEndNanos();

        const TimeSeriesQueryDerivative = {
        	"NONE":                    0,
        	"DERIVATIVE":              1,
        	"NON_NEGATIVE_DERIVATIVE": 2
        }
        
        const TimeSeriesQueryAggregator = {
        	"AVG":      1,
        	"SUM":      2,
        	"MAX":      3,
        	"MIN":      4,
        	"FIRST":    5,
        	"LAST":     6,
        	"VARIANCE": 7
        }
                    
        // Build queries from metrics
        const queries = this.metrics.map((m) => {
          let q = {
            name: `cr.node.${m.name}`,
            sources: m.sources.map(s => `${s}`)
          }
          if (m.downsampler) {
            q.downsampler = TimeSeriesQueryAggregator[m.downsampler]
          }
          if (m.source_aggregator) {
            q.source_aggregator = TimeSeriesQueryAggregator[m.source_aggregator]
          }
          if (m.derivative) {
            q.derivative = TimeSeriesQueryDerivative[m.derivative]
          }
          if (m.tenant_id) {
            q.tenant_id = m.tenant_id
          }
          return q
        });

        const requestBody = {
          start_nanos: startNanos,
          end_nanos: endNanos,
          queries: queries
        };

        const response = await fetch('/future/ts/query', {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
          },
          body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
          throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();

        // Destroy existing chart if it exists
        if (this.uPlotInstance) {
          this.uPlotInstance.destroy();
          this.uPlotInstance = null;
        }

        // Clear the graph container
        const graphEl = this.$el.querySelector('.graph-container');
        graphEl.innerHTML = '';

        // Create time info from the dropdown state extents
        const timeInfo = {
          start: Number(BigInt(startNanos) / 1_000_000n),
          end: Number(BigInt(endNanos) / 1_000_000n),
        };

        const timezone = Alpine.store('timeRange').timezone;

        this.uPlotInstance = makeuPlotChart(
          this.metrics,
          this.axis,
          timeInfo,
          timezone,
          data,
          graphEl
        );

        // Ensure chart is correctly sized to container after creation
        const containerWidth = graphEl.clientWidth;
        if (containerWidth > 0) {
          this.uPlotInstance.setSize({ width: containerWidth, height: 300 });
        }

        this.statusMessage = 'Metrics loaded successfully';
        this.statusClass = 'success';
      } catch (error) {
        console.error('Error fetching metrics:', error);
        this.statusMessage = `Error: ${error.message}`;
        this.statusClass = 'error';
      }
    }
  }
}
