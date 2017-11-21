import d3 from "d3";

import { ComputeByteScale } from "src/util/format";

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
    right: 35,
    bottom: 25,
    left: 20,
  };

  const TICK_SIZE = 6;
  const AXIS_MARGIN = 4;

  const scale = d3.scale.linear()
    .range([0, size.width]);

  const axis = d3.svg.axis()
    .scale(scale)
    .tickSize(TICK_SIZE)
    .ticks(5);

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
    svg
      .attr("width", size.width + margin.left + margin.right)
      .attr("height", size.height + margin.top + margin.bottom);

    const el = svg.selectAll(".main")
      .data((d: CapacityChartProps) => [recomputeScale(d)]);

    el.enter()
      .append("g")
      .attr("class", "main")
      .attr("transform", `translate(${margin.left},${margin.top})`);

    const axisGroup = el.selectAll("g.axis")
      .data(() => [0]);

    axisGroup.enter()
      .append("g")
      .attr("class", "axis")
      .attr("transform", `translate(0,${size.height + AXIS_MARGIN})`);

    axisGroup.call(axis);
    axisGroup.selectAll("text").attr("y", AXIS_MARGIN + TICK_SIZE);

    const bg = el.selectAll(".bg")
      .data((d: CapacityChartProps) => [d]);

    bg.enter()
      .append("rect")
      .attr("class", "bg");

    bg
      .attr("width", size.width)
      .attr("height", size.height);

    const bar = el.selectAll(".bar")
      .data((d: CapacityChartProps) => [d]);

    bar.enter()
      .append("rect")
      .attr("class", "bar")
      .attr("height", 10)
      .attr("y", 5);

    bar
      .attr("width", (d: CapacityChartProps) => scale(d.used));

  };
}

export default capacityChart;
