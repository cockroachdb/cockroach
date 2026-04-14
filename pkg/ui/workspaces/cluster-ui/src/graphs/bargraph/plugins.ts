// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import moment from "moment-timezone";
import uPlot, { Plugin } from "uplot";

import {
  Bytes,
  Duration,
  Percentage,
  Count,
  FormatWithTimezone,
  DATE_WITH_SECONDS_FORMAT_24_TZ,
} from "../../util";
import { AxisUnits } from "../utils/domain";

// Fallback color for series stroke if one is not defined.
const DEFAULT_STROKE = "#7e89a9";

// Generate a series legend within the provided div showing the data points
// relative to the cursor position.
const generateSeriesLegend = (
  uPlot: uPlot,
  seriesLegend: HTMLDivElement,
  yAxisUnits: AxisUnits,
) => {
  // idx is the closest data index to the cursor position.
  const { idx } = uPlot.cursor;

  if (idx === undefined || idx === null) {
    return;
  }

  // remove all previous child nodes
  seriesLegend.innerHTML = "";

  // Generate new child nodes.
  uPlot.series.forEach((series: uPlot.Series, index: number) => {
    if (index === 0 || series.show === false) {
      // Skip the series for x axis or if series is hidden.
      return;
    }

    // series.stroke can be either a function that returns a canvas stroke
    // value, or a function returning a stroke value.
    const strokeColor =
      typeof series.stroke === "function"
        ? series.stroke(uPlot, idx)
        : series.stroke;

    const container = document.createElement("div");
    container.style.display = "flex";
    container.style.alignItems = "center";

    const colorBox = document.createElement("span");
    colorBox.style.height = "12px";
    colorBox.style.width = "12px";
    colorBox.style.background = String(strokeColor || DEFAULT_STROKE);
    colorBox.style.display = "inline-block";
    colorBox.style.marginRight = "12px";

    const label = document.createElement("span");
    label.textContent = series.label || "";

    const dataValue = uPlot.data[index][idx];
    const value = document.createElement("div");
    value.style.textAlign = "right";
    value.style.flex = "1";
    value.style.fontFamily = "'Source Sans Pro', sans-serif";
    value.textContent =
      series.value instanceof Function && dataValue
        ? getFormattedValue(
            Number(series.value(uPlot, dataValue, index, idx)),
            yAxisUnits,
          )
        : getFormattedValue(dataValue, yAxisUnits);

    container.appendChild(colorBox);
    container.appendChild(label);
    container.appendChild(value);

    seriesLegend.appendChild(container);
  });
};

// Formats the value according to its unit.
function getFormattedValue(value: number, yAxisUnits: AxisUnits): string {
  switch (yAxisUnits) {
    case AxisUnits.Bytes:
      return Bytes(value);
    case AxisUnits.Duration:
      return Duration(value);
    case AxisUnits.DurationMillis:
      return Duration(value);
    case AxisUnits.Percentage:
      return Percentage(value, 1);
    default:
      return Count(value);
  }
}

export interface BarMetadata {
  databases?: string[];
  tables?: string[];
  indexes?: string[];
}

// Tooltip plugin for categorical bar charts (non-time-series)
export function categoricalBarTooltipPlugin(
  yAxis: AxisUnits,
  labels: string[],
  metadata?: BarMetadata[],
): Plugin {
  const cursorToolTip = {
    tooltip: document.createElement("div"),
  };

  function escapeHtml(str: string): string {
    const div = document.createElement("div");
    div.textContent = str;
    return div.innerHTML;
  }

  function setCursor(u: uPlot) {
    const { tooltip } = cursorToolTip;
    const { left = 0, idx } = u.cursor;

    if (idx === null || idx === undefined || idx < 0 || idx >= labels.length) {
      tooltip.style.display = "none";
      return;
    }

    const label = labels[idx];
    const value = u.data[1][idx];
    const meta = metadata?.[idx];

    // Build tooltip content
    let content = `<div style="font-weight: bold; margin-bottom: 8px;">${escapeHtml(label)}</div>`;
    content += `<div style="margin-bottom: 8px;">Value: ${getFormattedValue(value, yAxis)}</div>`;

    // Add metadata if available
    if (meta) {
      if (meta.databases && meta.databases.length > 0) {
        content += `<div style="margin-top: 8px;"><strong>Database${meta.databases.length > 1 ? "s" : ""}:</strong></div>`;
        content += meta.databases
          .map(db => `<div style="margin-left: 8px;">• ${escapeHtml(db)}</div>`)
          .join("");
      } else if (label.toLowerCase().includes("range")) {
        content += `<div style="margin-top: 8px;"><strong>Database:</strong> System range</div>`;
      }

      if (meta.tables && meta.tables.length > 0) {
        content += `<div style="margin-top: 8px;"><strong>Table${meta.tables.length > 1 ? "s" : ""}:</strong></div>`;
        content += meta.tables
          .map(
            table =>
              `<div style="margin-left: 8px;">• ${escapeHtml(table)}</div>`,
          )
          .join("");
      }

      if (meta.indexes && meta.indexes.length > 0) {
        content += `<div style="margin-top: 8px;"><strong>Index${meta.indexes.length > 1 ? "es" : ""}:</strong></div>`;
        content += meta.indexes
          .map(
            index =>
              `<div style="margin-left: 8px;">• ${escapeHtml(index)}</div>`,
          )
          .join("");
      }
    }

    tooltip.innerHTML = content;

    // Position tooltip
    const tooltipWidth = tooltip.offsetWidth;
    const plotWidth = u.over.offsetWidth;
    let tooltipLeft = left + 20;

    // Adjust if tooltip goes off the right edge
    if (tooltipLeft + tooltipWidth > plotWidth) {
      tooltipLeft = left - tooltipWidth - 20;
    }

    tooltip.style.left = `${tooltipLeft}px`;
    tooltip.style.top = `20px`;
    tooltip.style.display = "";
  }

  function ready(u: uPlot) {
    const plot = u.root.querySelector(".u-over");
    const { tooltip } = cursorToolTip;

    plot?.addEventListener("mouseleave", () => {
      tooltip.style.display = "none";
    });
  }

  function init(u: uPlot) {
    const plot = u.root.querySelector(".u-over");
    const { tooltip } = cursorToolTip;

    tooltip.style.display = "none";
    tooltip.style.pointerEvents = "none";
    tooltip.style.position = "absolute";
    tooltip.style.padding = "12px";
    tooltip.style.minWidth = "150px";
    tooltip.style.maxWidth = "350px";
    tooltip.style.background = "#fff";
    tooltip.style.borderRadius = "4px";
    tooltip.style.boxShadow = "0 2px 4px rgba(0,0,0,0.1)";
    tooltip.style.border = "1px solid #999";
    tooltip.style.zIndex = "1000";
    tooltip.style.fontSize = "12px";
    tooltip.style.lineHeight = "1.4";

    plot?.appendChild(tooltip);
  }

  return {
    hooks: {
      init,
      ready,
      setCursor,
    },
  };
}

// Tooltip legend plugin for bar charts.
export function barTooltipPlugin(yAxis: AxisUnits, timezone: string): Plugin {
  const cursorToolTip = {
    tooltip: document.createElement("div"),
    timeStamp: document.createElement("div"),
    seriesLegend: document.createElement("div"),
  };

  function setCursor(u: uPlot) {
    const { tooltip, timeStamp, seriesLegend } = cursorToolTip;
    const { left = 0, top = 0 } = u.cursor;

    // get the current timestamp from the x axis and formatting as
    // the Tooltip header.
    const closestDataPointTimeMillis = u.data[0][u.posToIdx(left)];
    timeStamp.textContent = FormatWithTimezone(
      moment(closestDataPointTimeMillis),
      DATE_WITH_SECONDS_FORMAT_24_TZ,
      timezone,
    );

    // Generating the series legend based on current state of µPlot
    generateSeriesLegend(u, seriesLegend, yAxis);

    // set the position of the Tooltip. Adjusting the tooltip away from the
    // cursor for readability.
    tooltip.style.left = `${left + 20}px`;
    tooltip.style.top = `${top - 10}px`;

    if (tooltip.style.display === "none") {
      tooltip.style.display = "";
    }
  }

  function ready(u: uPlot) {
    const plot = u.root.querySelector(".u-over");
    const { tooltip } = cursorToolTip;

    plot?.addEventListener("mouseleave", () => {
      tooltip.style.display = "none";
    });
  }

  function init(u: uPlot) {
    const plot = u.root.querySelector(".u-over");
    const { tooltip, timeStamp, seriesLegend } = cursorToolTip;
    tooltip.style.display = "none";
    tooltip.style.pointerEvents = "none";
    tooltip.style.position = "absolute";
    tooltip.style.padding = "0 16px 16px";
    tooltip.style.minWidth = "230px";
    tooltip.style.background = "#fff";
    tooltip.style.borderRadius = "5px";
    tooltip.style.boxShadow = "0px 7px 13px rgba(71, 88, 114, 0.3)";
    tooltip.style.zIndex = "100";
    tooltip.style.whiteSpace = "nowrap";

    // Set timeStamp.
    timeStamp.textContent = "time";
    timeStamp.style.paddingTop = "12px";
    timeStamp.style.marginBottom = "16px";
    tooltip.appendChild(timeStamp);

    // appending seriesLegend empty. Content will be generated on mousemove.
    tooltip.appendChild(seriesLegend);

    plot?.appendChild(tooltip);
  }

  return {
    hooks: {
      init,
      ready,
      setCursor,
    },
  };
}
