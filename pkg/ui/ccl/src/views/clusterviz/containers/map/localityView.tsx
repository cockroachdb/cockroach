// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import PropTypes from "prop-types";
import React from "react";
import { InjectedRouter, RouterState } from "react-router";

import { LocalityTree } from "src/redux/localities";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { generateLocalityRoute, getLeaves } from "src/util/localities";

import { StatsView } from "ccl/src/views/clusterviz/containers/map/statsView";
import { sumNodeStats, LivenessStatus } from "src/redux/nodes";

interface LocalityViewProps {
  localityTree: LocalityTree;
  liveness: { [id: string]: LivenessStatus };
}

export class LocalityView extends React.Component<LocalityViewProps, any> {
  static contextTypes = {
    router: PropTypes.object.isRequired,
  };
  context: { router: InjectedRouter & RouterState };

  onClick = () => {
    const localityTree = this.props.localityTree;
    const destination = CLUSTERVIZ_ROOT + "/" + generateLocalityRoute(localityTree.tiers);
    this.context.router.push(destination);
  }

  render() {
    const { tiers } = this.props.localityTree;
    const thisTier = tiers[tiers.length - 1];

    const leavesUnderMe = getLeaves(this.props.localityTree);
    const { capacityUsable, capacityUsed } = sumNodeStats(leavesUnderMe, this.props.liveness);

    return (
      <g onClick={this.onClick} style={{ cursor: "pointer" }}>
        <StatsView
          usableCapacity={capacityUsable}
          usedCapacity={capacityUsed}
          label={`${thisTier.key}=${thisTier.value}`}
        />
      </g>
    );
  }
}
