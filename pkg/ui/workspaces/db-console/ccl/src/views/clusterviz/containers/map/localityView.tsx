// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import { RouteComponentProps } from "react-router";
import { withRouter } from "react-router-dom";

import { LocalityTree } from "src/redux/localities";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import {
  generateLocalityRoute,
  getLocalityLabel,
  getLeaves,
} from "src/util/localities";

import { sumNodeStats, LivenessStatus } from "src/redux/nodes";
import { pluralize } from "src/util/pluralize";
import { trustIcon } from "src/util/trust";
import localityIcon from "!!raw-loader!assets/localityIcon.svg";
import liveIcon from "!!raw-loader!assets/livenessIcons/live.svg";
import { Sparklines } from "src/views/clusterviz/components/nodeOrLocality/sparklines";
import { CapacityArc } from "src/views/clusterviz/components/nodeOrLocality/capacityArc";
import { Labels } from "src/views/clusterviz/components/nodeOrLocality/labels";

interface LocalityViewProps {
  localityTree: LocalityTree;
  livenessStatuses: { [id: string]: LivenessStatus };
}

const SCALE_FACTOR = 0.8;
const TRANSLATE_X = -90 * SCALE_FACTOR;
const TRANSLATE_Y = -100 * SCALE_FACTOR;

// TODO(vilterp): CSS, I guess.
const DEAD_COLOR = "#E75263";
const SUSPECT_COLOR = "#FFAD26";

const LIVENESS_ICON_SPACING = 3;
const LIVENESS_ICON_RADIUS = 8;
const LIVENESS_TOTAL_WIDTH = LIVENESS_ICON_RADIUS * 2 + LIVENESS_ICON_SPACING;

class LocalityView extends React.Component<
  LocalityViewProps & RouteComponentProps
> {
  onClick = () => {
    const localityTree = this.props.localityTree;
    const destination =
      CLUSTERVIZ_ROOT + generateLocalityRoute(localityTree.tiers);
    this.props.history.push(destination);
  };

  renderLivenessIcons(nodeCounts: {
    healthy: number;
    suspect: number;
    dead: number;
  }) {
    // note: there's also a count of decommissioned nodes; it's ignored here.

    // If all nodes live, render green checkmark.
    if (nodeCounts.dead === 0 && nodeCounts.suspect === 0) {
      return <g dangerouslySetInnerHTML={trustIcon(liveIcon)} />;
    }

    const pairs: { color: string; count: number }[] = [];
    if (nodeCounts.suspect > 0) {
      pairs.push({ color: SUSPECT_COLOR, count: nodeCounts.suspect });
    }
    if (nodeCounts.dead > 0) {
      pairs.push({ color: DEAD_COLOR, count: nodeCounts.dead });
    }

    return (
      <g transform="translate(12 12)">
        {pairs.map(({ color, count }, idx) => (
          <g transform={`translate(${LIVENESS_TOTAL_WIDTH * idx * -1} 0)`}>
            <circle r={LIVENESS_ICON_RADIUS} fill={color} />
            <text
              fill="white"
              textAnchor="middle"
              fontFamily="Lato-Regular"
              fontSize="10px"
              y="3"
            >
              {count}
            </text>
          </g>
        ))}
      </g>
    );
  }

  render() {
    const { tiers } = this.props.localityTree;

    const leavesUnderMe = getLeaves(this.props.localityTree);
    const { capacityUsable, capacityUsed, nodeCounts } = sumNodeStats(
      leavesUnderMe,
      this.props.livenessStatuses,
    );

    const nodeIds = leavesUnderMe.map((node) => `${node.desc.node_id}`);

    return (
      <g
        onClick={this.onClick}
        style={{ cursor: "pointer" }}
        transform={`translate(${TRANSLATE_X},${TRANSLATE_Y})scale(${SCALE_FACTOR})`}
      >
        <rect width={180} height={210} opacity={0} />
        <Labels
          label={getLocalityLabel(tiers)}
          subLabel={`${leavesUnderMe.length} ${pluralize(
            leavesUnderMe.length,
            "Node",
            "Nodes",
          )}`}
        />
        <g
          dangerouslySetInnerHTML={trustIcon(localityIcon)}
          transform="translate(14 14)"
        />
        {this.renderLivenessIcons(nodeCounts)}
        <CapacityArc
          usableCapacity={capacityUsable}
          usedCapacity={capacityUsed}
          localityLabel={getLocalityLabel(tiers)}
        />
        <Sparklines nodes={nodeIds} />
      </g>
    );
  }
}

const localityViewWithRouter = withRouter(LocalityView);

export { localityViewWithRouter as LocalityView };
