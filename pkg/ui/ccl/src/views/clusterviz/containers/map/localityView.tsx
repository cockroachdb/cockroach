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

import { StatsView } from "ccl/src/views/clusterviz/containers/map/statsView";
import { sumNodeStats, LivenessStatus } from "src/redux/nodes";
import { pluralize } from "src/util/pluralize";

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

  render() {
    const { tiers } = this.props.localityTree;

    const leavesUnderMe = getLeaves(this.props.localityTree);
    const { capacityUsable, capacityUsed } = sumNodeStats(leavesUnderMe, this.props.liveness);

    return (
      <g onClick={this.onClick} style={{ cursor: "pointer" }}>
        <StatsView
          usableCapacity={capacityUsable}
          usedCapacity={capacityUsed}
          label={getLocalityLabel(tiers)}
          subLabel={`${leavesUnderMe.length} ${pluralize(leavesUnderMe.length, "Node", "Nodes")}`}
        />
      </g>
    );
  }
}

const localityViewWithRouter = withRouter(LocalityView);

export { localityViewWithRouter as LocalityView };
