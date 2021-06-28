// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import d3 from "d3";

import { ComputeByteScale } from "src/util/format";

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

  const scale = d3.scale.linear().range([0, size.width]);

  const axis = d3.svg.axis().scale(scale).tickSize(TICK_SIZE).ticks(5);

  function recomputeScale(capacity: CapacityChartProps) {
    // Compute the appropriate scale factor for a value slightly smaller than the
    // usable capacity, so that if the usable capacity is exactly 1 {MiB,GiB,etc}
    // we show the scale in the next-smaller unit.
    const byteScale = ComputeByteScale(capacity.usable - 1);

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

  return function chart(svg: d3.Selection<CapacityChartProps>) {
    const rect = (svg.node().parentNode as HTMLElement).getBoundingClientRect();
    size.width = rect.width;

    scale.range([0, size.width]);

    svg
      .attr("width", size.width + margin.left + margin.right)
      .attr("height", size.height + margin.top + margin.bottom);

    const el = svg
      .selectAll(".main")
      .data((d: CapacityChartProps) => [recomputeScale(d)]);

    el.enter()
      .append("g")
      .attr("class", "main")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    const axisGroup = el.selectAll("g.axis").data(() => [0]);

    axisGroup
      .enter()
      .append("g")
      .attr("class", "axis")
      .attr("transform", `translate(0,${size.height + AXIS_MARGIN})`);

    axisGroup.call(axis);
    axisGroup.selectAll("text").attr("y", AXIS_MARGIN + TICK_SIZE);

    const lowDiskSpaceWidth = size.width * LOW_DISK_SPACE_RATIO;
    const lowDiskSpacePosition = size.width - lowDiskSpaceWidth;

    const bgNormal = el
      .selectAll(".bg-normal")
      .data((d: CapacityChartProps) => [d]);

    bgNormal.enter().append("rect").attr("class", "bg-normal");

    bgNormal.attr("width", lowDiskSpacePosition).attr("height", size.height);

    const bgLowDiskSpace = el
      .selectAll(".bg-low-disk-space")
      .data((d: CapacityChartProps) => [d]);

    bgLowDiskSpace.enter().append("rect").attr("class", "bg-low-disk-space");

    bgLowDiskSpace
      .attr("x", lowDiskSpacePosition)
      .attr("width", lowDiskSpaceWidth)
      .attr("height", size.height);

    const bar = el.selectAll(".bar").data((d: CapacityChartProps) => [d]);

    bar
      .enter()
      .append("rect")
      .attr("class", "bar")
      .attr("height", 10)
      .attr("y", 5);

    bar.attr("width", (d: CapacityChartProps) => scale(d.used));
  };
}

export default capacityChart;
