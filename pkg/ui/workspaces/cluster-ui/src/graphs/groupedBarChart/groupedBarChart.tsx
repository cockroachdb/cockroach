// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import classNames from "classnames/bind";
import { scaleLinear } from "d3-scale";
import moment from "moment-timezone";
import React, {
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState,
} from "react";

import { TimezoneContext } from "../../contexts";
import {
  Bytes,
  Count,
  Duration,
  FormatWithTimezone,
  DATE_WITH_SECONDS_FORMAT_24_TZ,
  Percentage,
} from "../../util";
import {
  AxisUnits,
  calculateXAxisDomainBarChart,
  calculateYAxisDomain,
} from "../utils/domain";
import { Visualization } from "../visualization";

import styles from "./groupedBarChart.module.scss";
import { GroupedBarData } from "./types";

const cx = classNames.bind(styles);

const MARGIN = { top: 20, right: 20, bottom: 40, left: 70 };

function formatValue(value: number, units: AxisUnits): string {
  switch (units) {
    case AxisUnits.Bytes:
      return Bytes(value);
    case AxisUnits.Duration:
    case AxisUnits.DurationMillis:
      return Duration(value);
    case AxisUnits.Percentage:
      return Percentage(value, 1);
    default:
      return Count(value);
  }
}

interface HoverState {
  datumIdx: number;
  groupIdx: number;
  layerIdx: number;
  mouseX: number;
  mouseY: number;
}

export interface XScale {
  graphTsStartMillis: number;
  graphTsEndMillis: number;
}

export interface GroupedBarChartProps {
  data: GroupedBarData;
  maxWidth?: number;
  height?: number;
  yAxisUnits: AxisUnits;
  title: string;
  tooltip?: React.ReactNode;
  xScale?: XScale;
}

export const GroupedBarChart: React.FC<GroupedBarChartProps> = ({
  data,
  maxWidth = 947,
  height = 340,
  yAxisUnits,
  title,
  tooltip,
  xScale: xScaleProp,
}) => {
  const timezone = useContext(TimezoneContext);
  const wrapperRef = useRef<HTMLDivElement>(null);
  const chartAreaRef = useRef<HTMLDivElement>(null);
  const [hover, setHover] = useState<HoverState | null>(null);
  // Tracks which layer labels are hidden via legend click-to-isolate.
  const [hiddenLayers, setHiddenLayers] = useState<Set<string>>(new Set());

  // Measure container width so the chart fills its grid column,
  // clamped to maxWidth if provided.
  const [measuredWidth, setMeasuredWidth] = useState(0);
  useEffect(() => {
    const el = wrapperRef.current;
    if (!el) return;
    const ro = new ResizeObserver(entries => {
      const w = entries[0]?.contentRect.width ?? 0;
      if (w > 0) setMeasuredWidth(w);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, []);
  const width = maxWidth ? Math.min(measuredWidth, maxWidth) : measuredWidth;

  const isLayerVisible = useCallback(
    (layerLabel: string) => !hiddenLayers.has(layerLabel),
    [hiddenLayers],
  );

  const innerWidth = width - MARGIN.left - MARGIN.right;
  const innerHeight = height - MARGIN.top - MARGIN.bottom;

  // Compute the aggregation bucket width from the data timestamps.
  // Use the minimum gap between any two adjacent timestamps — this
  // gives the true aggregation interval even when some buckets are
  // missing (e.g., canary was inactive for a period).
  // Fallback to 1s when there's only one data point.
  const samplingIntervalMillis = useMemo(() => {
    if (data.length < 2) return 1e3;
    let minInterval = Infinity;
    for (let i = 1; i < data.length; i++) {
      const gap = data[i].timestamp - data[i - 1].timestamp;
      if (gap > 0 && gap < minInterval) minInterval = gap;
    }
    return minInterval === Infinity ? 1e3 : minInterval;
  }, [data]);

  // Time extent: use xScale prop if provided, else derive from data.
  const timeExtent = useMemo(() => {
    const start = xScaleProp?.graphTsStartMillis ?? data[0]?.timestamp ?? 0;
    const end =
      xScaleProp?.graphTsEndMillis ??
      (data.length > 0 ? data[data.length - 1].timestamp : 0);
    return { start, end };
  }, [data, xScaleProp]);

  // X-axis domain using the same calculation as existing bar charts.
  const xAxisDomain = useMemo(
    () =>
      calculateXAxisDomainBarChart(
        timeExtent.start,
        timeExtent.end,
        samplingIntervalMillis,
        timezone,
      ),
    [timeExtent, samplingIntervalMillis, timezone],
  );

  // Collect all stacked totals for y-axis domain (only visible layers).
  const allTotals = useMemo(
    () =>
      data.flatMap(d =>
        d.groups.map(g =>
          g.layers
            .filter(l => isLayerVisible(l.label))
            .reduce((sum, l) => sum + l.value, 0),
        ),
      ),
    [data, isLayerVisible],
  );

  const yAxisDomain = useMemo(
    () => calculateYAxisDomain(yAxisUnits, allTotals),
    [yAxisUnits, allTotals],
  );

  // Continuous time scale for x-axis.
  const xTimeScale = useMemo(
    () =>
      scaleLinear()
        .domain([xAxisDomain.extent[0], xAxisDomain.extent[1]])
        .range([0, innerWidth]),
    [xAxisDomain, innerWidth],
  );

  const yScale = useMemo(
    () =>
      scaleLinear()
        .domain([yAxisDomain.extent[0], yAxisDomain.extent[1]])
        .range([innerHeight, 0]),
    [yAxisDomain, innerHeight],
  );

  // Bar width per group. The existing uPlot charts render one bar per
  // timestamp with width = clamp(intervalPx * 0.9, 10, 80). We render
  // numGroups bars side-by-side in that same space, so each bar is
  // roughly 1/numGroups of the uPlot bar width.
  const numGroups = data.length > 0 ? data[0].groups.length : 2;
  const groupGapPx = 2;

  const barWidth = useMemo(() => {
    const intervalPx = Math.abs(
      xTimeScale(samplingIntervalMillis) - xTimeScale(0),
    );
    const fullBarWidth = Math.min(Math.max(intervalPx * 0.9, 10), 80);
    return Math.max(
      (fullBarWidth - groupGapPx * (numGroups - 1)) / numGroups,
      1,
    );
  }, [xTimeScale, samplingIntervalMillis, numGroups]);

  // Y-axis ticks from domain.
  const yTicks = useMemo(
    () => [yAxisDomain.extent[0], ...yAxisDomain.ticks, yAxisDomain.extent[1]],
    [yAxisDomain],
  );

  // All unique layer labels (used for isolate logic).
  const allLayerLabels = useMemo(() => {
    if (data.length === 0) return [];
    const labels: string[] = [];
    for (const group of data[0].groups) {
      for (const layer of group.layers) {
        if (!labels.includes(layer.label)) {
          labels.push(layer.label);
        }
      }
    }
    return labels;
  }, [data]);

  // Legend items derived from data. Deduplicated by (label, color)
  // pair: when groups share the same colors per layer (plan
  // distribution), entries collapse to one per layer. When groups use
  // distinct colors (statement times), both appear with group prefix.
  const legendItems = useMemo(() => {
    if (data.length === 0) return [];
    const items: { label: string; color: string; layerLabel: string }[] = [];
    const seen = new Set<string>();

    // Check if all groups use the same color for each layer index.
    const groups = data[0].groups;
    const sharedColors =
      groups.length >= 2 &&
      groups[0].layers.length === groups[1].layers.length &&
      groups[0].layers.every((l, i) => groups[1].layers[i]?.color === l.color);

    for (const group of groups) {
      for (const layer of group.layers) {
        const key = `${layer.label}:${layer.color}`;
        if (!seen.has(key)) {
          seen.add(key);
          items.push({
            label: sharedColors ? layer.label : `${group.label} ${layer.label}`,
            color: layer.color,
            layerLabel: layer.label,
          });
        }
      }
    }
    return items;
  }, [data]);

  // Click-to-isolate: clicking a legend item isolates that layer
  // (hides all others). Clicking again restores all layers.
  const handleLegendClick = useCallback(
    (layerLabel: string) => {
      setHiddenLayers(prev => {
        const isCurrentlyIsolated =
          prev.size === allLayerLabels.length - 1 && !prev.has(layerLabel);
        if (isCurrentlyIsolated) {
          // Already isolated — restore all.
          return new Set();
        }
        // Isolate: hide everything except this layer.
        const next = new Set(allLayerLabels);
        next.delete(layerLabel);
        return next;
      });
    },
    [allLayerLabels],
  );

  const handleMouseMove = (
    e: React.MouseEvent,
    datumIdx: number,
    groupIdx: number,
    layerIdx: number,
  ) => {
    if (!chartAreaRef.current) return;
    const rect = chartAreaRef.current.getBoundingClientRect();
    setHover({
      datumIdx,
      groupIdx,
      layerIdx,
      mouseX: e.clientX - rect.left,
      mouseY: e.clientY - rect.top,
    });
  };

  const handleMouseLeave = () => setHover(null);

  // Tooltip positioning: clamp to stay within chart bounds.
  const tooltipStyle = useMemo(() => {
    if (!hover) return {};
    const tooltipWidth = 220;
    let left = hover.mouseX + 15;
    if (left + tooltipWidth > width) {
      left = hover.mouseX - tooltipWidth - 15;
    }
    return { left, top: Math.max(10, hover.mouseY - 10) };
  }, [hover, width]);

  const hoveredDatum = hover ? data[hover.datumIdx] : null;
  const hoveredGroup = hoveredDatum
    ? hoveredDatum.groups[hover.groupIdx]
    : null;
  const hoveredLayer = hoveredGroup?.layers[hover?.layerIdx];

  const tooltipContent =
    hoveredDatum && hoveredGroup && hoveredLayer ? (
      <>
        <div className={cx("tooltip-timestamp")}>
          {FormatWithTimezone(
            moment(hoveredDatum.timestamp),
            DATE_WITH_SECONDS_FORMAT_24_TZ,
            timezone,
          )}
        </div>
        {hoveredGroup.label && (
          <div className={cx("tooltip-group-label")}>{hoveredGroup.label}</div>
        )}
        <div className={cx("tooltip-layer")}>
          <span
            className={cx("tooltip-swatch")}
            style={{ background: hoveredLayer.color }}
          />
          <span>{hoveredLayer.label}</span>
          <span className={cx("tooltip-value")}>
            {formatValue(hoveredLayer.value, yAxisUnits)}
          </span>
        </div>
      </>
    ) : null;

  return (
    <div className={cx("chart-wrapper")} ref={wrapperRef}>
      <Visualization
        title={title}
        tooltip={tooltip}
        loading={!data || data.length === 0 || width === 0}
        preCalcGraphSize
      >
        <div className={cx("chart-container")} ref={chartAreaRef}>
          <svg width={width} height={height}>
            <g transform={`translate(${MARGIN.left},${MARGIN.top})`}>
              {/* Y-axis */}
              <line x1={0} y1={0} x2={0} y2={innerHeight} stroke="#e0e0e0" />
              {yTicks.map(tick => (
                <g key={tick} transform={`translate(0,${yScale(tick)})`}>
                  <line x1={-6} x2={0} stroke="#999" />
                  <line
                    x1={0}
                    x2={innerWidth}
                    stroke="#f0f0f0"
                    strokeDasharray="3,3"
                  />
                  <text
                    x={-10}
                    textAnchor="end"
                    dominantBaseline="middle"
                    fontSize={10}
                    fill="#666"
                  >
                    {yAxisDomain.tickFormat(tick)}
                  </text>
                </g>
              ))}
              {/* Y-axis label */}
              <text
                transform={`translate(-55,${innerHeight / 2}) rotate(-90)`}
                textAnchor="middle"
                fontSize={11}
                fill="#666"
              >
                {yAxisDomain.label}
              </text>

              {/* X-axis */}
              <line
                x1={0}
                y1={innerHeight}
                x2={innerWidth}
                y2={innerHeight}
                stroke="#e0e0e0"
              />
              {xAxisDomain.ticks.map(tick => {
                const x = xTimeScale(tick);
                return (
                  <g key={tick}>
                    <line
                      x1={x}
                      y1={innerHeight}
                      x2={x}
                      y2={innerHeight + 6}
                      stroke="#999"
                    />
                    <text
                      x={x}
                      y={innerHeight + 20}
                      textAnchor="middle"
                      fontSize={10}
                      fill="#666"
                    >
                      {xAxisDomain.tickFormat(tick)}
                    </text>
                  </g>
                );
              })}

              {/* Bars */}
              {data.map((datum, di) => {
                // Right-aligned (align=1, matching uPlot's default):
                // bars extend to the right of the timestamp tick mark.
                // The group block's left edge starts at the timestamp.
                const groupStartX = xTimeScale(datum.timestamp);

                return (
                  <g key={datum.timestamp}>
                    {datum.groups.map((group, gi) => {
                      const gx = groupStartX + gi * (barWidth + groupGapPx);
                      let cumValue = 0;
                      const totalVisible = group.layers
                        .filter(l => isLayerVisible(l.label))
                        .reduce((sum, l) => sum + l.value, 0);
                      return (
                        <g key={gi}>
                          {group.layers.map((layer, li) => {
                            if (!isLayerVisible(layer.label)) return null;
                            const barY = yScale(cumValue + layer.value);
                            const naturalHeight = yScale(cumValue) - barY;
                            // Show a 1px sliver for zero values so the
                            // data point is visible on the chart.
                            const barHeight = Math.max(1, naturalHeight);
                            cumValue += layer.value;
                            return (
                              <rect
                                key={li}
                                className={cx("bar-rect")}
                                x={gx}
                                y={barY + naturalHeight - barHeight}
                                width={barWidth}
                                height={barHeight}
                                fill={layer.color}
                                onMouseMove={e =>
                                  handleMouseMove(e, di, gi, li)
                                }
                                onMouseLeave={handleMouseLeave}
                              />
                            );
                          })}
                          {group.label && totalVisible > 0 && (
                            <text
                              x={gx + barWidth / 2}
                              y={yScale(totalVisible) - 4}
                              textAnchor="middle"
                              fontSize={9}
                              fontWeight="bold"
                              fill="#475872"
                            >
                              {group.label}
                            </text>
                          )}
                        </g>
                      );
                    })}
                  </g>
                );
              })}
            </g>
          </svg>

          {/* Tooltip */}
          <div
            className={cx("tooltip", hover && "tooltip--visible")}
            style={tooltipStyle}
          >
            {tooltipContent}
          </div>

          {/* Legend */}
          <div className={cx("legend")} style={{ maxWidth: width }}>
            {legendItems.map(item => (
              <span
                key={`${item.label}:${item.color}`}
                className={cx(
                  "legend-item",
                  hiddenLayers.has(item.layerLabel) && "legend-item--hidden",
                )}
                onClick={() => handleLegendClick(item.layerLabel)}
              >
                <span
                  className={cx("legend-swatch")}
                  style={{ background: item.color }}
                />
                {item.label}
              </span>
            ))}
          </div>
        </div>
      </Visualization>
    </div>
  );
};
