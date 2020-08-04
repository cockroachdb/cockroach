// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import d3 from "d3";
import React from "react";

import { SparklineMetricsDataComponent } from "src/views/clusterviz/containers/map/sparkline";
import { Metric } from "src/views/shared/components/metricQuery";
import { MetricsDataProvider } from "src/views/shared/containers/metricDataProvider";

interface QpsSparklineProps {
  nodes: string[];
}

export function QpsSparkline(props: QpsSparklineProps) {
  const key = "sparkline.qps.nodes." + props.nodes.join("-");

  return (
    <MetricsDataProvider id={key}>
      <SparklineMetricsDataComponent formatCurrentValue={d3.format(".1f")}>
        <Metric
          name="cr.node.sql.select.count"
          sources={props.nodes}
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.insert.count"
          sources={props.nodes}
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.update.count"
          sources={props.nodes}
          nonNegativeRate
        />
        <Metric
          name="cr.node.sql.delete.count"
          sources={props.nodes}
          nonNegativeRate
        />
      </SparklineMetricsDataComponent>
    </MetricsDataProvider>
  );
}
