import d3 from "d3";
import React from "react";

import { SparklineMetricsDataComponent } from "src/views/clusterviz/containers/map/sparkline";
import { Metric } from "src/views/shared/components/metricQuery";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

interface CpuSparklineProps {
  nodes: string[];
}

export function CpuSparkline(props: CpuSparklineProps) {
  const key = "sparkline.cpu.nodes." + props.nodes.join("-");

  return (
    <MetricsDataProvider id={key}>
      <SparklineMetricsDataComponent formatCurrentValue={d3.format(".1%")}>
        <Metric name="cr.node.sys.cpu.sys.percent" sources={props.nodes} />
        <Metric name="cr.node.sys.cpu.user.percent" sources={props.nodes} />
      </SparklineMetricsDataComponent>
    </MetricsDataProvider>
  );
}
