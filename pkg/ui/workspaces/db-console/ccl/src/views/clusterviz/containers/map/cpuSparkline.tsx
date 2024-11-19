// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
        <Metric
          name="cr.node.sys.cpu.combined.percent-normalized"
          sources={props.nodes}
        />
      </SparklineMetricsDataComponent>
    </MetricsDataProvider>
  );
}
