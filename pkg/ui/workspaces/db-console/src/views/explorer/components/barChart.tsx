// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { axisBottom, axisLeft } from "d3-axis";
import { scaleBand, scaleLinear } from "d3-scale";
import { select } from "d3-selection";
import React, { useCallback, useEffect, useRef } from "react";

interface RangeData {
  value: number;
  rangeId: number;
  databases?: string[];
  tables?: string[];
  indexes?: string[];
}

interface RangeChartProps {
  data: RangeData[];
  width: number;
  height: number;
  yAxisLabel?: string;
}

interface RangeChartRef {
  update: (newData: RangeData[]) => void;
}

export const BarChart = React.forwardRef<RangeChartRef, RangeChartProps>(
  ({ data, width, height, yAxisLabel = "ms/s" }, ref) => {
    const svgRef = useRef<SVGSVGElement>(null);
    const tooltipRef = useRef<HTMLDivElement>(null);

    const renderChart = useCallback(
      (chartData: RangeData[], isInitial = false) => {
        if (!svgRef.current || !tooltipRef.current || !chartData?.length) {
          return;
        }

        const svg = select(svgRef.current);
        const margin = { top: 30, right: 30, bottom: 70, left: 60 };
        const innerWidth = width - margin.left - margin.right;
        const innerHeight = height - margin.top - margin.bottom;

        // Initialize SVG structure on first render
        if (isInitial) {
          svg.selectAll("*").remove();
          svg
            .attr("width", width)
            .attr("height", height)
            .append("g")
            .attr("transform", `translate(${margin.left},${margin.top})`);
        }

        const g = svg.select("g");

        // Create scales
        const xScale = scaleBand()
          .range([0, innerWidth])
          .domain(chartData.map(d => `Range ${d.rangeId}`))
          .padding(0.2);

        let yMax = 1000;
        const maxValue = Math.max(...chartData.map(d => d.value), 0);
        if (maxValue !== 0) {
          yMax = maxValue * 1.2;
        }
        const yScale = scaleLinear().domain([0, yMax]).range([innerHeight, 0]);

        // Configure tooltip
        const tooltip = select(tooltipRef.current)
          .style("opacity", 0)
          .style("position", "absolute")
          .style("background", "white")
          .style("border", "1px solid #999")
          .style("padding", "8px")
          .style("pointer-events", "none")
          .style("border-radius", "4px")
          .style("box-shadow", "0 2px 4px rgba(0,0,0,0.1)")
          .style("z-index", "1000")
          .style("font-size", "12px")
          .style("min-width", "120px")
          .style("max-width", "300px")
          .style("line-height", "1.4");

        // Mouse event handlers
        const handleMouseOver = function (
          this: SVGRectElement,
          _event: MouseEvent,
          _d: RangeData,
        ) {
          tooltip.style("opacity", 1);
          select(this).style("stroke", "black").style("opacity", 1);
        };

        const handleMouseMove = function (
          this: SVGRectElement,
          event: MouseEvent,
          d: RangeData,
        ) {
          if (!svgRef.current || !tooltipRef.current) return;

          const svgRect = svgRef.current.getBoundingClientRect();

          // Escape HTML to prevent XSS
          const escapeHtml = (str: string) =>
            str.replace(
              /[&<>"']/g,
              m =>
                ({
                  "&": "&amp;",
                  "<": "&lt;",
                  ">": "&gt;",
                  '"': "&quot;",
                  "'": "&#39;",
                })[m],
            );

          // Build tooltip content with database and table information
          let tooltipContent = `<strong>Range ${d.rangeId}</strong><br>`;
          tooltipContent += `Value: ${d.value.toFixed(2)} ${yAxisLabel}<br>`;

          // Add database information if available
          if (d.databases && d.databases.length > 0) {
            tooltipContent += `<br><strong>Database${d.databases.length > 1 ? "s" : ""}:</strong><br>`;
            tooltipContent += d.databases
              .map(db => `• ${escapeHtml(db)}`)
              .join("<br>");
          } else {
            tooltipContent += `<br><strong>Database:</strong> System range`;
          }

          // Add table information if available
          if (d.tables && d.tables.length > 0) {
            tooltipContent += `<br><br><strong>Table${d.tables.length > 1 ? "s" : ""}:</strong><br>`;
            tooltipContent += d.tables
              .map(table => `• ${escapeHtml(table)}`)
              .join("<br>");
          }

          // Add index information if available
          if (d.indexes && d.indexes.length > 0) {
            tooltipContent += `<br><br><strong>Index${d.indexes.length > 1 ? "es" : ""}:</strong><br>`;
            tooltipContent += d.indexes
              .map(index => `• ${escapeHtml(index)}`)
              .join("<br>");
          }

          // Set content first to calculate dimensions
          tooltip.html(tooltipContent);

          // Calculate tooltip position with bounds checking
          let x = event.clientX - svgRect.left + 10;
          let y = event.clientY - svgRect.top - 10;

          // Get tooltip dimensions after content is set
          const tooltipRect = tooltipRef.current.getBoundingClientRect();
          const tooltipWidth = tooltipRect.width;
          const tooltipHeight = tooltipRect.height;

          // Adjust position if tooltip would go off-screen
          if (x + tooltipWidth > width) {
            x = event.clientX - svgRect.left - tooltipWidth - 10;
          }
          if (y + tooltipHeight > height) {
            y = event.clientY - svgRect.top - tooltipHeight - 10;
          }

          tooltip.style("left", x + "px").style("top", y + "px");
        };

        const handleMouseLeave = function (
          this: SVGRectElement,
          _event: MouseEvent,
          _d: RangeData,
        ) {
          tooltip.style("opacity", 0);
          select(this).style("stroke", "none").style("opacity", 0.8);
        };

        // Add/update axes only on initial render
        if (isInitial) {
          // Add X axis
          g.append("g")
            .attr("class", "x-axis")
            .attr("transform", `translate(0,${innerHeight})`)
            .call(axisBottom(xScale));

          // Add X axis label
          g.append("text")
            .attr("class", "x-label")
            .attr(
              "transform",
              `translate(${innerWidth / 2}, ${innerHeight + margin.bottom - 10})`,
            )
            .style("text-anchor", "middle")
            .style("font-size", "14px")
            .text("Ranges");

          // Add Y axis
          g.append("g").attr("class", "y-axis").call(axisLeft(yScale));

          // Add Y axis label
          g.append("text")
            .attr("class", "y-label")
            .attr("transform", "rotate(-90)")
            .attr("y", 0 - margin.left)
            .attr("x", 0 - innerHeight / 2)
            .attr("dy", "1em")
            .style("text-anchor", "middle")
            .style("font-size", "14px")
            .text(yAxisLabel);
        } else {
          // Update axes for new data
          (g.select(".x-axis") as any).call(axisBottom(xScale));
          (g.select(".y-axis") as any).call(axisLeft(yScale));
        }

        // Update bars
        const bars = g.selectAll("rect").data(chartData);

        bars
          .enter()
          .append("rect")
          .attr("x", d => xScale(`Range ${d.rangeId}`) || 0)
          .attr("y", innerHeight)
          .attr("width", xScale.bandwidth())
          .attr("height", 0)
          .attr("fill", "#2196F3")
          .style("cursor", "pointer")
          .on("mouseover", handleMouseOver)
          .on("mousemove", handleMouseMove)
          .on("mouseleave", handleMouseLeave)
          .merge(bars as any)
          .transition()
          .duration(1000)
          .attr("x", d => xScale(`Range ${d.rangeId}`) || 0)
          .attr("y", d => yScale(d.value))
          .attr("width", xScale.bandwidth())
          .attr("height", d => innerHeight - yScale(d.value));

        bars.exit().remove();
      },
      [width, height, yAxisLabel],
    );

    useEffect(() => {
      renderChart(data, true);
    }, [data, renderChart]);

    // Expose update function via ref
    React.useImperativeHandle(
      ref,
      () => ({
        update: (newData: RangeData[]) => renderChart(newData, false),
      }),
      [renderChart],
    );

    return (
      <div style={{ position: "relative" }}>
        <svg ref={svgRef} />
        <div ref={tooltipRef} />
      </div>
    );
  },
);
