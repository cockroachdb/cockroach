// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { range, quantile } from "d3-array";
import { axisBottom } from "d3-axis";
import { interpolateRound } from "d3-interpolate";
import { ScaleSequential, scaleSequential } from "d3-scale";
import { select, create } from "d3-selection";
import { zoom, zoomIdentity, ZoomBehavior } from "d3-zoom";
import React, { useEffect, useRef } from "react";

interface NodeData {
  x: number;
  y: number;
  value: number;
  nodeId: number;
}

interface NodeHeatmapProps {
  data: NodeData[];
  width: number;
  height: number;
  metricType?: "cpu" | "writeBytes";
  onHexagonClick?: (nodeId: number) => void;
}

// Helper function to generate hexagon path
const hexagonPath = (radius: number): string => {
  const angles = Array.from({ length: 6 }, (_, i) => (i * Math.PI) / 3);
  const points = angles.map(angle => [
    Math.cos(angle) * radius,
    Math.sin(angle) * radius,
  ]);
  return `M${points.map(p => p.join(",")).join("L")}Z`;
};

// Creates a color legend for d3 sequential scales
function createLegend(
  color: any,
  options: {
    title?: string;
    tickSize?: number;
    width?: number;
    height?: number;
    marginTop?: number;
    marginRight?: number;
    marginBottom?: number;
    marginLeft?: number;
    ticks?: number;
    tickFormat?: (d: any) => string;
    tickValues?: any[];
  } = {},
): SVGSVGElement {
  const {
    title,
    tickSize = 6,
    width = 320,
    height = 44 + tickSize,
    marginTop = 18,
    marginRight = 0,
    marginBottom = 16 + tickSize,
    marginLeft = 0,
    ticks = width / 64,
    tickFormat,
    tickValues,
  } = options;

  function ramp(color: any, n = 256) {
    const canvas = document.createElement("canvas");
    canvas.width = n;
    canvas.height = 1;
    const context = canvas.getContext("2d");
    if (!context) return canvas;
    for (let i = 0; i < n; ++i) {
      context.fillStyle = color(i / (n - 1));
      context.fillRect(i, 0, 1, 1);
    }
    return canvas;
  }

  const svg = create("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", [0, 0, width, height])
    .style("overflow", "visible")
    .style("display", "block");

  let x: any;

  // For sequential scales with interpolator
  if (color.interpolator) {
    x = Object.assign(
      color
        .copy()
        .interpolator(interpolateRound(marginLeft, width - marginRight)),
      {
        range() {
          return [marginLeft, width - marginRight];
        },
      },
    );

    svg
      .append("image")
      .attr("x", marginLeft)
      .attr("y", marginTop)
      .attr("width", width - marginLeft - marginRight)
      .attr("height", height - marginTop - marginBottom)
      .attr("preserveAspectRatio", "none")
      .attr("xlink:href", ramp(color.interpolator()).toDataURL());

    // Build tick values if not provided
    let finalTickValues = tickValues;
    if (!finalTickValues) {
      if (!x.ticks) {
        const n = Math.round(ticks + 1);
        finalTickValues = range(n).map((i: number) =>
          quantile(color.domain(), i / (n - 1)),
        );
      }
    }

    // Add axis
    const tickAdjust = (g: any) =>
      g.selectAll(".tick line").attr("y1", marginTop + marginBottom - height);

    svg
      .append("g")
      .attr("transform", `translate(0,${height - marginBottom})`)
      .call(
        axisBottom(x)
          .ticks(ticks, typeof tickFormat === "string" ? tickFormat : undefined)
          .tickFormat(typeof tickFormat === "function" ? tickFormat : undefined)
          .tickSize(tickSize)
          .tickValues(finalTickValues),
      )
      .call(tickAdjust)
      .call((g: any) => g.select(".domain").remove())
      .call((g: any) =>
        g
          .append("text")
          .attr("x", marginLeft)
          .attr("y", marginTop + marginBottom - height - 6)
          .attr("fill", "currentColor")
          .attr("text-anchor", "start")
          .attr("font-weight", "bold")
          .attr("class", "title")
          .text(title),
      );
  }

  return svg.node() as SVGSVGElement;
}

// Usage of the colorbrewer spectral colorscheme.
const interpolateSpectral = (t: number): string => {
  const colors = [
    "#5e4fa2",
    "#3288bd",
    "#66c2a5",
    "#abdda4",
    "#e6f598",
    "#ffffbf",
    "#fee08b",
    "#fdae61",
    "#f46d43",
    "#d53e4f",
    "#9e0142",
  ];

  const index = Math.floor(t * (colors.length - 1));

  if (index >= colors.length - 1) return colors[colors.length - 1];
  if (index < 0) return colors[0];

  return colors[index];
};

export const HexagonHeatmap: React.FC<NodeHeatmapProps> = ({
  data,
  width,
  height,
  metricType = "cpu",
  onHexagonClick,
}) => {
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    if (!svgRef.current || !data?.length || !tooltipRef.current) return;

    const svg = select(svgRef.current);
    svg.selectAll("*").remove(); // Clear previous content

    const margin = { top: 80, right: 25, bottom: 30, left: 40 };

    // Calculate hex radius based on available space
    const hexRadius = 35;
    // Find the bounds of the hexagon grid
    const xValues = data.map(d => d.x);
    const minX = Math.min(...xValues, 0);
    const maxX = Math.max(...xValues, 0);

    const gridWidth = maxX - minX + hexRadius * 2 * Math.sqrt(3);

    const xOffset = (width - gridWidth) / 2 + hexRadius * Math.sqrt(3);
    const yOffset = margin.top;

    // Create main group
    const g = svg
      .attr("width", width)
      .attr("height", height)
      .append("g")
      .attr("transform", `translate(${xOffset},${yOffset})`);

    // Create a container group for zoom
    const zoomGroup = g.append("g").attr("class", "zoom-group");

    // Set up zoom behavior
    const zoomBehavior: ZoomBehavior<SVGSVGElement, unknown> = zoom<
      SVGSVGElement,
      unknown
    >()
      .scaleExtent([0.5, 4]) // Allow zoom from 50% to 400%
      .on("zoom", event => {
        zoomGroup.attr("transform", event.transform);
      });

    // Apply zoom to SVG
    svg.call(zoomBehavior as any);

    // Calculate initial zoom based on node count
    let initialScale = 1;
    if (data.length > 50) {
      initialScale = 0.8;
    }
    if (data.length > 100) {
      initialScale = 0.6;
    }

    // Calculate translation to center the grid in the viewport
    const translateX = ((minX + maxX) * initialScale) / 4;

    // Set initial zoom with centering
    svg.call(
      zoomBehavior.transform as any,
      zoomIdentity.translate(translateX, 0).scale(initialScale),
    );

    // Create color scale - use fixed domain for CPU, dynamic for writeBytes
    let colorScale: ScaleSequential<string>;
    if (metricType === "cpu") {
      // Fixed scale for CPU percentages (0-75+)
      // Values above 75 will be clamped to the max color
      colorScale = scaleSequential(interpolateSpectral)
        .domain([0, 75])
        .clamp(true);
    } else {
      // Dynamic scale for writeBytes based on actual data
      const values = data.map(d => d.value).filter(v => v > 0);
      const minValue = Math.min(...values, 0);
      const maxValue = Math.max(...values, 1); // Ensure at least 1 to avoid zero domain
      colorScale = scaleSequential(interpolateSpectral).domain([
        minValue,
        maxValue,
      ]);
    }

    const tooltip = select(tooltipRef.current)
      .style("opacity", 0)
      .style("position", "absolute")
      .style("background", "white")
      .style("border", "1px solid #999")
      .style("padding", "5px")
      .style("pointer-events", "none")
      .style("border-radius", "4px")
      .style("box-shadow", "0 2px 4px rgba(0,0,0,0.1)")
      .style("z-index", "1000")
      .style("font-size", "12px")
      .style("min-width", "80px");

    // Mouse event handlers
    const handleMouseOver = function (
      this: SVGElement,
      _event: MouseEvent,
      _d: NodeData,
    ) {
      tooltip.style("opacity", 1);
      select(this).style("stroke", "black").style("opacity", 1);
    };

    const handleMouseMove = function (
      this: SVGElement,
      event: MouseEvent,
      d: NodeData,
    ) {
      // Get the bounding box of the SVG element to calculate relative position
      const svgRect = svgRef.current?.getBoundingClientRect();
      if (!svgRect) return;

      // Calculate position relative to the SVG container
      const relativeX = event.clientX - svgRect.left;
      const relativeY = event.clientY - svgRect.top;

      tooltip
        .html(`The value of<br>this node is: ${d.value}`)
        .style("left", relativeX + 10 + "px")
        .style("top", relativeY - 28 + "px");
    };

    const handleMouseLeave = function (
      this: SVGElement,
      _event: MouseEvent,
      _d: NodeData,
    ) {
      tooltip.style("opacity", 0);
      select(this).style("stroke", "none").style("opacity", 0.8);
    };

    const handleClick = function (
      this: SVGElement,
      _event: MouseEvent,
      d: NodeData,
    ) {
      if (onHexagonClick) {
        onHexagonClick(d.nodeId);
      }
    };

    // Draw hexagons
    zoomGroup
      .selectAll(".hexagon")
      .data(data)
      .enter()
      .append("path")
      .attr("class", "hexagon")
      .attr("d", hexagonPath(hexRadius))
      .attr("transform", (d: NodeData) => `translate(${d.x},${d.y})`)
      .attr("stroke", "none")
      .attr("stroke-width", "1px")
      .style("fill", (d: NodeData) => colorScale(d.value))
      .style("opacity", 0.8)
      .style("cursor", "pointer")
      .on("mouseover", handleMouseOver)
      .on("mousemove", handleMouseMove)
      .on("mouseleave", handleMouseLeave)
      .on("click", handleClick);

    // Add node ID labels
    zoomGroup
      .selectAll(".hex-label")
      .data(data)
      .enter()
      .append("text")
      .attr("class", "hex-label")
      .attr("x", (d: NodeData) => d.x)
      .attr("y", (d: NodeData) => d.y)
      .attr("text-anchor", "middle")
      .attr("dominant-baseline", "central")
      .style("font-size", "14px")
      .style("font-weight", "bold")
      .style("fill", "white")
      .style("stroke", "black")
      .style("stroke-width", "0.5px")
      .style("paint-order", "stroke")
      .style("pointer-events", "none")
      .text((d: NodeData) => d.nodeId);

    // Add legend
    const legendWidth = 320;
    const legendNode = createLegend(colorScale, {
      title: metricType === "cpu" ? "CPU (%)" : "Write Bytes",
      tickSize: 6,
      width: legendWidth,
      marginTop: 10,
      marginLeft: 0,
      marginBottom: 30,
      tickValues: metricType === "cpu" ? [0, 25, 50, 75] : undefined,
      tickFormat:
        metricType === "cpu"
          ? (d: any) => (d >= 75 ? "75+" : d.toString())
          : undefined,
      ticks: metricType === "cpu" ? 4 : 5,
    });

    const legendX = (width - legendWidth) / 2;
    svg
      .append(() => legendNode)
      .attr(
        "transform",
        `translate(${legendX}, ${height - margin.bottom - 50})`,
      );
  }, [data, width, height, metricType, onHexagonClick]);

  return (
    <div style={{ position: "relative" }}>
      <svg ref={svgRef} />
      <div ref={tooltipRef} />
    </div>
  );
};
