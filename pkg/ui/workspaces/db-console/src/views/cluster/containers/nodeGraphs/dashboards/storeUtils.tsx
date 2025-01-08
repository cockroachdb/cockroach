// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import React from "react";

import { storeIDsForNode } from "src/views/cluster/containers/nodeGraphs/dashboards/dashboardUtils";
import { Metric, MetricProps } from "src/views/shared/components/metricQuery";

/**
 * Dynamically shows either the aggregated node-level metric when viewing the
 * cluster-level dashboard, or store-level metrics when viewing a single node.
 */
export const storeMetrics = (
  props: MetricProps,
  nodeIDs: string[],
  storeIDsByNodeID: { [key: string]: string[] },
  prefix?: string,
) =>
  nodeIDs.flatMap(nid => {
    const storeIDs = storeIDsForNode(storeIDsByNodeID, nid);

    let aggregateType = "total";
    if (props.aggregateAvg) {
      aggregateType = "average";
    } else if (props.aggregateMax) {
      aggregateType = "max";
    } else if (props.aggregateMin) {
      aggregateType = "min";
    }

    if (!prefix) {
      // if prefix is not set, set it to empty string
      prefix = "";
    }

    const nodeMetric = (
      <Metric
        key={nid}
        title={`${prefix} (n${nid},${aggregateType})`}
        sources={storeIDs}
        {...props}
      />
    );

    // show only the aggregated node-level metric when viewing multiple nodes
    if (nodeIDs.length > 1) {
      return nodeMetric;
    }

    // otherwise, show the aggregated metric and a per-store breakdown
    return [
      nodeMetric,
      ...storeIDs.map(sid => (
        <Metric
          key={`${nid}-${sid}`}
          title={`${prefix} (n${nid},s${sid})`}
          sources={[sid]}
          {...props}
        />
      )),
    ];
  });
