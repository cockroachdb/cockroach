// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import moment from "moment";
import { Link } from "react-router";

import { NodeStatus$Properties } from "src/util/proto";
import { sumNodeStats } from "src/redux/nodes";
import { trustIcon } from "src/util/trust";
import liveIcon from "!!raw-loader!assets/livenessIcons/live.svg";
import suspectIcon from "!!raw-loader!assets/livenessIcons/suspect.svg";
import deadIcon from "!!raw-loader!assets/livenessIcons/dead.svg";
import nodeIcon from "!!raw-loader!assets/nodeIcon.svg";
import { Labels } from "src/views/clusterviz/components/nodeOrLocality/labels";
import { CapacityArc } from "src/views/clusterviz/components/nodeOrLocality/capacityArc";
import { Sparklines } from "src/views/clusterviz/components/nodeOrLocality/sparklines";
import { LongToMoment } from "src/util/convert";
import { cockroach } from "src/js/protos";
import NodeLivenessStatus = cockroach.storage.NodeLivenessStatus;
import Liveness$Properties = cockroach.storage.Liveness$Properties;

interface NodeViewProps {
  node: NodeStatus$Properties;
  livenessStatus: { [id: string]: NodeLivenessStatus };
  liveness: Liveness$Properties;
}

const SCALE_FACTOR = 0.6;
const TRANSLATE_X = -90 * SCALE_FACTOR;
const TRANSLATE_Y = -100 * SCALE_FACTOR;

export class NodeView extends React.Component<NodeViewProps> {
  getLivenessIcon(nodeCounts: { suspect: number, dead: number }) {
    if (nodeCounts.dead > 0) {
      return deadIcon;
    }
    if (nodeCounts.suspect > 0) {
      return suspectIcon;
    }
    return liveIcon;
  }

  getUptimeText() {
    const { node, livenessStatus, liveness } = this.props;

    const thisLiveness = livenessStatus[node.desc.node_id];

    switch (thisLiveness) {
      case NodeLivenessStatus.DEAD: {
        if (!liveness) {
          return "no information";
        }

        const deadTime = liveness.expiration.wall_time;
        const deadMoment = LongToMoment(deadTime);
        return `dead for ${moment.duration(deadMoment.diff(moment())).humanize()}`;
      }
      case NodeLivenessStatus.LIVE: {
        const startTime = LongToMoment(node.started_at);
        return "up for " + moment.duration(startTime.diff(moment())).humanize();
      }
      case NodeLivenessStatus.DECOMMISSIONED:
        return "decommissioned";
      case NodeLivenessStatus.DECOMMISSIONING:
        return "decommissioning";
      case NodeLivenessStatus.UNKNOWN:
        return "unknown";
      case NodeLivenessStatus.UNAVAILABLE:
        return "unavailable";
      default:
        return ""; // idk man
    }
  }

  render() {
    const { node, livenessStatus } = this.props;
    const { capacityUsable, capacityUsed, nodeCounts } = sumNodeStats([node], livenessStatus);

    return (
      <Link
        to={`/node/${node.desc.node_id}`}
        style={{ cursor: "pointer" }}
      >
        <g transform={`translate(${TRANSLATE_X},${TRANSLATE_Y})scale(${SCALE_FACTOR})`}>
          <Labels
            label={`Node ${node.desc.node_id}`}
            subLabel={this.getUptimeText()}
            tooltip={node.desc.address.address_field}
          />
          <g dangerouslySetInnerHTML={trustIcon(nodeIcon)} transform="translate(14 14)" />
          <g
            dangerouslySetInnerHTML={trustIcon(this.getLivenessIcon(nodeCounts))}
            transform="translate(9, 9)"
          />
          <CapacityArc
            usableCapacity={capacityUsable}
            usedCapacity={capacityUsed}
          />
          <Sparklines nodes={[`${node.desc.node_id}`]} />
        </g>
      </Link>
    );
  }
}
