// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { NodeStatus$Properties } from "src/util/proto";
import { sumNodeStats } from "src/redux/nodes";
import { cockroach } from "src/js/protos";
import { NodeHistory } from "ccl/src/views/clusterviz/containers/map/nodeHistory";
import { trustIcon } from "src/util/trust";
import liveIcon from "!!raw-loader!assets/livenessIcons/live.svg";
import nodeIcon from "!!raw-loader!assets/nodeIcon.svg";
import { Labels } from "ccl/src/views/clusterviz/components/nodeOrLocality/labels";
import { CapacityArc } from "ccl/src/views/clusterviz/components/nodeOrLocality/capacityArc";
import { StatsBars } from "ccl/src/views/clusterviz/components/nodeOrLocality/statsBars";

type NodeLivenessStatus = cockroach.storage.NodeLivenessStatus;

interface NodeViewProps {
  node: NodeStatus$Properties;
  liveness: { [id: string]: NodeLivenessStatus };

  nodeHistory?: NodeHistory;
  maxClientActivityRate: number;
}

export class NodeView extends React.Component<NodeViewProps> {
  renderLivenessIcon() {
    // TODO(vilterp): pipe in real liveness data; add icons for other states
    return (
      <g dangerouslySetInnerHTML={trustIcon(liveIcon)} />
    );
  }

  render() {
    const { node, liveness } = this.props;
    const { capacityUsable, capacityUsed } = sumNodeStats([node], liveness);

    return (
      <g fill="none" transform="translate(-90 -100)">
        <Labels
          label={node.desc.address.address_field}
          subLabel={""} // TODO(vilterp): uptime
        />
        <g dangerouslySetInnerHTML={trustIcon(nodeIcon)} transform="translate(14 14)" />
        {this.renderLivenessIcon()}
        <CapacityArc
          usableCapacity={capacityUsable}
          usedCapacity={capacityUsed}
        />
        <StatsBars />
      </g>
    );
  }
}
