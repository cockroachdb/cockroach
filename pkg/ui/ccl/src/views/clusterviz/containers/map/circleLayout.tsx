// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import React from "react";

import { LocalityTree } from "src/redux/localities";
import { getChildLocalities } from "src/util/localities";

import { LocalityView } from "./localityView";
import { NodeView } from "./nodeView";
import { LivenessStatus } from "src/redux/nodes";

const MIN_RADIUS = 150;
const PADDING = 150;

interface CircleLayoutProps {
  localityTree: LocalityTree;
  liveness: { [id: string]: LivenessStatus };
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

    const angle = 2 * Math.PI * index / total - Math.PI / 2;
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
        {
          childLocalities.map((locality, i) => (
            <g transform={`translate(${this.coordsFor(i, total, radius)})`}>
              <LocalityView localityTree={locality} liveness={this.props.liveness} />
            </g>
          ))
        }
        {
          localityTree.nodes.map((node, i) => {
            return (
              <g transform={`translate(${this.coordsFor(i + childLocalities.length, total, radius)})`}>
                <NodeView
                  node={node}
                  liveness={this.props.liveness}
                />
              </g>
            );
          })
        }
      </g>
    );
  }
}
