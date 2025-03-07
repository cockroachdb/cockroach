// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { util } from "@cockroachlabs/cluster-ui";
import { Axis, axisBottom } from "d3-axis";
import { NumberValue, ScaleLinear, scaleLinear } from "d3-scale";
import { Selection } from "d3-selection";

const LOW_DISK_SPACE_RATIO = 0.15;

interface CapacityChartProps {
  used: number;
  usable: number;
}

/**
 * capacityChart is the small bar chart showing capacity usage displayed on the
 * cluster summary panel of the cluster overview page.
 */
function capacityChart() {
  const size = {
    width: 250,
    height: 20,
  };

  const margin = {
    top: 12,
    right: 20,
    bottom: 25,
    left: 20,
  };

  const TICK_SIZE = 6;
  const AXIS_MARGIN = 4;

  const scale: ScaleLinear<number, number> = scaleLinear().range([
    0,
    size.width,
  ]);

  const axis: Axis<NumberValue> = axisBottom(scale)
    .tickSize(TICK_SIZE)
    .ticks(5);

  function recomputeScale(capacity: CapacityChartProps) {
    // Compute the appropriate scale factor for a value slightly smaller than the
    // usable capacity, so that if the usable capacity is exactly 1 {MiB,GiB,etc}
    // we show the scale in the next-smaller unit.
    const byteScale = util.ComputeByteScale(capacity.usable - 1);

    const scaled = {
      used: capacity.used / byteScale.value,
      usable: capacity.usable / byteScale.value,
    };

    axis.tickFormat(function (d) {
      return d + " " + byteScale.units;
    });
    scale.domain([0, scaled.usable]);
    return scaled;
  }

  return function chart(
    svg: Selection<SVGElement, CapacityChartProps, null, undefined>,
  ) {
    const rect = (svg.node().parentNode as HTMLElement).getBoundingClientRect();
    size.width = rect.width;

    scale.range([0, size.width]);

    svg
      .attr("width", size.width + margin.left + margin.right)
      .attr("height", size.height + margin.top + margin.bottom);

    const mainGroup = svg
      .selectAll(".main")
      .data((d: CapacityChartProps) => [recomputeScale(d)]);

    const mainGroupEnter = mainGroup
      .enter()
      .append("g")
      .attr("class", "main")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    // Merge the enter and update selections
    const main = mainGroupEnter.merge(mainGroup as any);

    // AXIS GROUP
    const axisGroup = main.selectAll("g.axis").data(() => [0]);

    const axisGroupEnter = axisGroup
      .enter()
      .append("g")
      .attr("class", "axis")
      .attr("transform", `translate(0,${size.height + AXIS_MARGIN})`);

    axisGroupEnter
      .merge(axisGroup as any)
      .call(axis)
      .selectAll("text")
      .attr("y", AXIS_MARGIN + TICK_SIZE);

    const lowDiskSpaceWidth = size.width * LOW_DISK_SPACE_RATIO;
    const lowDiskSpacePosition = size.width - lowDiskSpaceWidth;

    // Background Normal
    const bgNormal = main
      .selectAll(".bg-normal")
      .data((d: CapacityChartProps) => [d]);

    const bgNormalEnter = bgNormal
      .enter()
      .append("rect")
      .attr("class", "bg-normal");

    bgNormalEnter
      .merge(bgNormal as any)
      .attr("width", lowDiskSpacePosition)
      .attr("height", size.height);

    // Background Low Disk Space
    const bgLowDiskSpace = main
      .selectAll(".bg-low-disk-space")
      .data((d: CapacityChartProps) => [d]);

    const bgLowDiskSpaceEnter = bgLowDiskSpace
      .enter()
      .append("rect")
      .attr("class", "bg-low-disk-space");

    bgLowDiskSpaceEnter
      .merge(bgLowDiskSpace as any)
      .attr("x", lowDiskSpacePosition)
      .attr("width", lowDiskSpaceWidth)
      .attr("height", size.height);

    // BAR
    const bar = main.selectAll(".bar").data((d: CapacityChartProps) => [d]);

    const barEnter = bar
      .enter()
      .append("rect")
      .attr("class", "bar")
      .attr("height", 10)
      .attr("y", 5);

    barEnter
      .merge(bar as any)
      .attr("width", (d: CapacityChartProps) => scale(d.used));
  };
}

export default capacityChart;
