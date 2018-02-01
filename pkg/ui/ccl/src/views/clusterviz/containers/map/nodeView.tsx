// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { MetricConstants } from "src/util/proto";

import { SimulatedNodeStatus } from "./nodeSimulator";
import { StatsView } from "ccl/src/views/clusterviz/containers/map/statsView";

interface NodeViewProps {
  nodeHistory: SimulatedNodeStatus;
  maxClientActivityRate: number;
}

export class NodeView extends React.Component<NodeViewProps, any> {
  render() {
    const { metrics } = this.props.nodeHistory.latest();
    const usedCapacity = metrics[MetricConstants.usedCapacity];
    const capacity = metrics[MetricConstants.capacity];

    return (
      <StatsView
        capacity={capacity}
        usedCapacity={usedCapacity}
        label={this.props.nodeHistory.latest().desc.address.address_field}
      />
    );
  }
}
