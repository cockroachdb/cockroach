// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import React from "react";
import * as d3 from "d3";

import { SimulatedNodeStatus } from "./nodeSimulator";
import { LocalityCollection, LocalityTreeNode } from "./localityCollection";
import { NodeView } from "./nodeView";
import { Box, ZoomTransformer } from "./zoom";

type BoxDimensions = {x: number, y: number, w: number, h: number};

interface LocalityViewProps {
  locality: LocalityTreeNode;
}

class LocalityView extends React.Component<LocalityViewProps, any> {
  render() {
    return (
      <text x={15} y={15}>
        {
          // TODO Resolve name of locality.
          (this.props.locality.data as any).key
        }
      </text>
    );
  }
}

interface LocalityBoxProps {
  box: Box;
  data: LocalityTreeNode;
}

class LocalityBox extends React.Component<LocalityBoxProps, any> {
  render() {
    return (
      <g transform={`translate(${this.props.box.center()})`}>
        <LocalityView locality={this.props.data} />
      </g>
    );
  }
}

interface NodeBoxProps {
  box: Box;
  data: SimulatedNodeStatus;
}

class NodeBox extends React.Component<NodeBoxProps, any> {
  render() {
    return (
      <g transform={`translate(${this.props.box.center()})`}>
        <NodeView
          nodeHistory={this.props.data}
          maxClientActivityRate={10000}
        />
      </g>
    );
  }
}

interface ModalTreeNodeViewProps {
  node: LocalityTreeNode;
  projection: d3.geo.Projection;
}

class ModalTreeNodeView extends React.Component<ModalTreeNodeViewProps, any> {
  renderContents = (b: BoxDimensions) => {
    const { node } = this.props;
    const box = new Box(b.x, b.y, b.w, b.h);
    return node.isLocality()
      ? <LocalityBox box={box} data={node} />
      : <NodeBox box={box} data={node.data as SimulatedNodeStatus} />;
  }

  render() {
    const { node } = this.props;

    const center = this.props.projection(node.longLat());
    let coords: BoxDimensions = {x: center[0] - 50, y: center[1] - 50, w: 100, h: 100};

    return this.renderContents(coords);
  }
}

interface ModalLocalitiesViewProps {
  nodeHistories: SimulatedNodeStatus[];
  projection: d3.geo.Projection;
  zoom: ZoomTransformer;
}

export class ModalLocalitiesView extends React.Component<ModalLocalitiesViewProps, any> {
  render() {
    const localities = new LocalityCollection();
    this.props.nodeHistories.forEach(nh => localities.addNode(nh));
    localities.computeTreeMapLayout(this.props.zoom.viewportSize());
    return (
      <g>
        {localities.tree.children
          ? localities.tree.children.map(l => <ModalTreeNodeView projection={this.props.projection} node={l} />)
          : null
        }
      </g>
    );
  }
}
