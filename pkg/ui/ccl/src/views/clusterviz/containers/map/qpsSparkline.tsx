import d3 from "d3";
import React from "react";

import { NanoToMilli } from "src/util/convert";
import { Metric, MetricsDataComponentProps } from "src/views/shared/components/metricQuery";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";
import createChartComponent from "src/views/shared/util/d3-react";

interface Datapoint {
  timestamp: number;
  value: number;
}

function renderSparkline(sel: d3.Selection<{ results: Datapoint[] }>) {
  const { results } = sel.datum();
  console.log("render line here", results);

  const xScale = d3.scale.linear()
    .domain(d3.extent(results, (d: Datapoint) => d.timestamp))
    .range([1, 92]);
  const yScale = d3.scale.linear()
    .domain(d3.extent(results, (d: Datapoint) => d.value))
    .range([19, 1]);

  const drawPath = d3.svg.line<Datapoint>()
    .x((d: Datapoint) => xScale(d.timestamp))
    .y((d: Datapoint) => yScale(d.value));

  const bg = sel.selectAll("rect")
    .data([1]);

  bg
    .enter()
    .append("rect")
    .attr("width", 93)
    .attr("height", 20)
    .attr("fill", "lightgrey")
    .attr("fill-opacity", 1)
    .attr("stroke", "none");

  const line = sel.selectAll("path")
    .data([results]);

  line
    .enter()
    .append("path")
    .attr("fill", "none")
    .attr("stroke", "steelblue");

  line
    .attr("d", drawPath);
}

// tslint:disable-next-line:variable-name
const Sparkline = createChartComponent("g", renderSparkline);

class UnwrapResults extends React.Component<MetricsDataComponentProps> {
  render() {
    const { data } = this.props;
    if (!data || !data.results || !data.results.length) {
      return null;
    }

    const { datapoints } = data.results[0];
    const results: Datapoint[] = datapoints.map(({ timestamp_nanos, value }) => ({
      timestamp: NanoToMilli(timestamp_nanos.toNumber()),
      value,
    }));
    return <Sparkline results={results} />;
  }
}

interface QpsSparklineProps {
  nodes: string[];
}

export function QpsSparkline(props: QpsSparklineProps) {
  const key = "qps-sparkline.nodes." + props.nodes.join(",");

  return (
    <MetricsDataProvider id={key}>
      <UnwrapResults>
        <Metric name="cr.node.sql.select.count" sources={props.nodes} nonNegativeRate />
      </UnwrapResults>
    </MetricsDataProvider>
  );
}
