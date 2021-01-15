// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { LocalityTree } from "src/redux/localities";
import { getChildLocalities } from "src/util/localities";

import { LocalityView } from "./localityView";
import { NodeView } from "./nodeView";
import { LivenessStatus } from "src/redux/nodes";
import { cockroach } from "src/js/protos";

type Liveness = cockroach.kv.kvserver.liveness.livenesspb.ILiveness;

const MIN_RADIUS = 150;
const PADDING = 150;

interface CircleLayoutProps {
  localityTree: LocalityTree;
  livenessStatuses: { [id: string]: LivenessStatus };
  livenesses: { [id: string]: Liveness };
  viewportSize: [number, number];
}

export class CircleLayout extends React.Component<CircleLayoutProps> {
  coordsFor(index: number, total: number, radius: number) {
    if (total === 1) {
      return [0, 0];
    }

    if (total === 2) {
      const leftOrRight = index === 0 ? -radius : radius;
      return [leftOrRight, 0];
    }

    const angle = (2 * Math.PI * index) / total - Math.PI / 2;
    return [radius * Math.cos(angle), radius * Math.sin(angle)];
  }

  render() {
    const { localityTree, viewportSize } = this.props;
    const childLocalities = getChildLocalities(localityTree);

    const total = localityTree.nodes.length + childLocalities.length;

    const calculatedRadius = Math.min(...viewportSize) / 2 - PADDING;
    const radius = Math.max(MIN_RADIUS, calculatedRadius);

    return (
      <g transform={`translate(${viewportSize[0] / 2},${viewportSize[1] / 2})`}>
        {childLocalities.map((locality, i) => (
          <g transform={`translate(${this.coordsFor(i, total, radius)})`}>
            <LocalityView
              localityTree={locality}
              livenessStatuses={this.props.livenessStatuses}
            />
          </g>
        ))}
        {localityTree.nodes.map((node, i) => {
          return (
            <g
              transform={`translate(${this.coordsFor(
                i + childLocalities.length,
                total,
                radius,
              )})`}
            >
              <NodeView
                node={node}
                livenessStatus={this.props.livenessStatuses[node.desc.node_id]}
                liveness={this.props.livenesses[node.desc.node_id]}
              />
            </g>
          );
        })}
      </g>
    );
  }
}
