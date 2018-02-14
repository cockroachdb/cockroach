// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { withRouter, WithRouterProps } from "react-router";

import { LocalityTree } from "src/redux/localities";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { generateLocalityRoute, getLocalityLabel, getLeaves } from "src/util/localities";

import { sumNodeStats, LivenessStatus } from "src/redux/nodes";
import { pluralize } from "src/util/pluralize";
import { trustIcon } from "src/util/trust";
import localityIcon from "!!raw-loader!assets/localityIcon.svg";
import { Sparklines } from "src/views/clusterviz/components/nodeOrLocality/sparklines";
import { CapacityArc } from "src/views/clusterviz/components/nodeOrLocality/capacityArc";
import { Labels } from "src/views/clusterviz/components/nodeOrLocality/labels";

interface LocalityViewProps {
  localityTree: LocalityTree;
  liveness: { [id: string]: LivenessStatus };
}

class LocalityView extends React.Component<LocalityViewProps & WithRouterProps> {
  onClick = () => {
    const localityTree = this.props.localityTree;
    const destination = CLUSTERVIZ_ROOT + "/" + generateLocalityRoute(localityTree.tiers);
    this.props.router.push(destination);
  }

  renderLivenessIcon() {
    // TODO(vilterp): aggregate liveness of child nodes; render composite icon
    return (
      <g />
    );
  }

  render() {
    const { tiers } = this.props.localityTree;

    const leavesUnderMe = getLeaves(this.props.localityTree);
    const { capacityUsable, capacityUsed } = sumNodeStats(leavesUnderMe, this.props.liveness);

    const nodeIds = leavesUnderMe.map((node) => `${node.desc.node_id}`);

    return (
      <g
        onClick={this.onClick}
        style={{ cursor: "pointer" }}
        transform="translate(-90 -100)"
      >
        <Labels
          label={getLocalityLabel(tiers)}
          subLabel={`${leavesUnderMe.length} ${pluralize(leavesUnderMe.length, "Node", "Nodes")}`}
        />
        <g dangerouslySetInnerHTML={trustIcon(localityIcon)} transform="translate(14 14)" />
        {this.renderLivenessIcon()}
        <CapacityArc
          usableCapacity={capacityUsable}
          usedCapacity={capacityUsed}
        />
        <Sparklines nodes={nodeIds} />
      </g>
    );
  }
}

const localityViewWithRouter = withRouter(LocalityView);

export { localityViewWithRouter as LocalityView };
