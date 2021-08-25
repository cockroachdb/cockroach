// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";
import moment from "moment";
import { Link } from "react-router-dom";

import { INodeStatus } from "src/util/proto";
import { nodeCapacityStats, livenessNomenclature } from "src/redux/nodes";
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

import NodeLivenessStatus = cockroach.kv.kvserver.liveness.livenesspb.NodeLivenessStatus;
type ILiveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

interface NodeViewProps {
  node: INodeStatus;
  livenessStatus: NodeLivenessStatus;
  liveness: ILiveness;
}

const SCALE_FACTOR = 0.8;
const TRANSLATE_X = -90 * SCALE_FACTOR;
const TRANSLATE_Y = -100 * SCALE_FACTOR;

export class NodeView extends React.Component<NodeViewProps> {
  getLivenessIcon(livenessStatus: NodeLivenessStatus) {
    switch (livenessStatus) {
      case NodeLivenessStatus.NODE_STATUS_LIVE:
        return liveIcon;
      case NodeLivenessStatus.NODE_STATUS_DEAD:
        return deadIcon;
      default:
        return suspectIcon;
    }
  }

  getUptimeText() {
    const { node, livenessStatus, liveness } = this.props;

    switch (livenessStatus) {
      case NodeLivenessStatus.NODE_STATUS_DEAD: {
        if (!liveness) {
          return "dead";
        }

        const deadTime = liveness.expiration.wall_time;
        const deadMoment = LongToMoment(deadTime);
        return `dead for ${moment
          .duration(deadMoment.diff(moment()))
          .humanize()}`;
      }
      case NodeLivenessStatus.NODE_STATUS_LIVE: {
        const startTime = LongToMoment(node.started_at);
        return `up for ${moment.duration(startTime.diff(moment())).humanize()}`;
      }
      default:
        return livenessNomenclature(livenessStatus);
    }
  }

  render() {
    const { node, livenessStatus } = this.props;
    const { used, usable } = nodeCapacityStats(node);

    return (
      <Link to={`/node/${node.desc.node_id}`} style={{ cursor: "pointer" }}>
        <g
          transform={`translate(${TRANSLATE_X},${TRANSLATE_Y})scale(${SCALE_FACTOR})`}
        >
          <rect width={180} height={210} opacity={0} />
          <Labels
            label={`Node ${node.desc.node_id}`}
            subLabel={this.getUptimeText()}
            tooltip={node.desc.address.address_field}
          />
          <g
            dangerouslySetInnerHTML={trustIcon(nodeIcon)}
            transform="translate(14 14)"
          />
          <g
            dangerouslySetInnerHTML={trustIcon(
              this.getLivenessIcon(livenessStatus),
            )}
            transform="translate(9, 9)"
          />
          <CapacityArc
            usableCapacity={usable}
            usedCapacity={used}
            nodeLabel={`Node ${node.desc.node_id}`}
          />
          <Sparklines nodes={[`${node.desc.node_id}`]} />
        </g>
      </Link>
    );
  }
}
