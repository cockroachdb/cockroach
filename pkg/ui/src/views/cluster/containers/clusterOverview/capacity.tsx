import d3 from "d3";
import React from "react";

import { ComputeByteScale } from "src/util/format";

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

interface CapacityChartProps {
  used: number;
  usable: number;
}

const scale = d3.scale.linear()
  .range([0, size.width]);

const axis = d3.svg.axis()
  .scale(scale)
  .tickSize(TICK_SIZE)
  .ticks(5);

function recomputeScale(el: d3.Selection<CapacityChartProps>) {
  var capacity = el.datum();

  // Compute the appropriate scale factor for a value slightly smaller than the
  // usable capacity, so that if the usable capacity is exactly 1 {MiB,GiB,etc}
  // we show the scale in the next-smaller unit.
  var byteScale = ComputeByteScale(capacity.usable - 1);

  var scaled = {
    used: capacity.used / byteScale.value,
    usable: capacity.usable / byteScale.value,
  };
  el.datum(scaled)

  axis.tickFormat(function (d) {
    return d + " " + byteScale.units;
  });
  scale.domain([0, scaled.usable]);
}

function chart(el: d3.Selection<CapacityChartProps>) {
  el.style("shape-rendering", "crispEdges");

  recomputeScale(el);

  const axisJoin = el.selectAll("g.axis")
    .data(() => [null]);

  // const axisEnter =
  axisJoin.enter()
    .append("g")
    .attr("class", "axis")
    .attr("transform", `translate(0,${size.height + AXIS_MARGIN})`);

  // const axisGroup = axisEnter.merge(axisJoin);
  const axisGroup = axisJoin;

  axisGroup.call(axis);
  axisGroup.selectAll("text").attr("y", AXIS_MARGIN + TICK_SIZE);

  const bgJoin = el.selectAll(".bg")
    .data((d: CapacityChartProps) => [d]);

  // const bgEnter =
  bgJoin.enter()
    .append("rect")
    .attr("class", "bg");

//  const bg = bgEnter.merge(bgJoin);
  bgJoin
//  bg
    .attr("width", size.width)
    .attr("height", size.height);

  const barJoin = el.selectAll(".bar")
    .data((d: CapacityChartProps) => [d]);

  // const barEnter =
  barJoin.enter()
    .append("rect")
    .attr("class", "bar")
    .attr("height", 10)
    .attr("y", 5);

  // const bar = barEnter.merge(barJoin);
  barJoin
  // bar
    .attr("width", (d: CapacityChartProps) => scale(d.used));

}

export class CapacityChart extends React.Component<CapacityChartProps, {}> {
  svgEl: SVGElement;

  componentDidMount() {
    d3.select(this.svgEl)
      .datum(this.props)
      .call(chart);
  }

  shouldComponentUpdate(props: CapacityChartProps) {
    d3.select(this.svgEl)
      .datum(props)
      .call(chart);
  }

  render() {
    return (
      <svg width={size.width + margin.left + margin.right} height={size.height + margin.top + margin.bottom}>
        <g ref={(el) => this.svgEl = el} transform={`translate(${margin.left},${margin.top})`}>
        </g>
      </svg>
    );
  }
}
