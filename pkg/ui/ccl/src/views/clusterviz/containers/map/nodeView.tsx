// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { Dictionary } from "lodash";

import { NodeStatus$Properties } from "src/util/proto";
import { StatsView } from "ccl/src/views/clusterviz/containers/map/statsView";
import { sumNodeStats } from "src/redux/nodes";
import { cockroach } from "src/js/protos";

type NodeLivenessStatus = cockroach.storage.NodeLivenessStatus;

interface NodeViewProps {
  node: NodeStatus$Properties;
  liveness: Dictionary<NodeLivenessStatus>;
}

export class NodeView extends React.Component<NodeViewProps, any> {
  render() {
    const { node, liveness } = this.props;
    const { capacityUsable, capacityUsed } = sumNodeStats([node], liveness);

    return (
      <StatsView
        usableCapacity={capacityUsable}
        usedCapacity={capacityUsed}
        label={this.props.node.desc.address.address_field}
      />
    );
  }
}
