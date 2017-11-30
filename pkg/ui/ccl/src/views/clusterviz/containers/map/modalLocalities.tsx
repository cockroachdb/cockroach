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
import { Motion, spring } from "react-motion";

import { SimulatedNodeStatus } from "./nodeSimulator";
import { LocalityCollection, LocalityTreeNode } from "./localityCollection";
import { NodeView } from "./nodeView";
import { Box, ZoomTransformer } from "./zoom";

type BoxDimensions = {x: number, y: number, w: number, h: number};

interface LocalityBoxProps {
  box: Box;
  mode: "map" | "tree";
  showChildren: boolean;
  projection: d3.geo.Projection;
  data: LocalityTreeNode;
}

class LocalityBox extends React.Component<LocalityBoxProps, any> {
  render() {
    const { box, mode, data, showChildren } = this.props;
    let childNodes: JSX.Element[];
    if (showChildren && data.children) {
      /* tslint:disable-next-line:no-use-before-declare */
      childNodes = data.children.map(c => <ModalTreeNodeView node={c} projection={this.props.projection} mode={mode} />);
    }
    return (
      <g>
        <rect
          x={box.left()}
          y={box.top()}
          width={box.width()}
          height={box.height()}
          style={{
            fill: "white",
            color: "black",
            stroke: "black",
            strokeWidth: "1px",
          }}
        />
        {
          <text x={box.left() + 15} y={box.top() + 15}>
            {
              // TODO Resolve name of locality.
              (data.data as any).key
            }
          </text>
        }
        { childNodes }
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
  mode: "map" | "tree";
}

interface ModalTreeNodeViewState {
  inTransition: boolean;
}

class ModalTreeNodeView extends React.Component<ModalTreeNodeViewProps, ModalTreeNodeViewState> {
  constructor(props: any) {
    super(props);
    this.state = {
      inTransition: false,
    };
  }

  onRest = () => {
    this.setState({inTransition: false});
  }

  shouldAnimate() {
    return this.state.inTransition || this.props.mode === "tree";
  }

  componentWillReceiveProps(newProps: ModalTreeNodeViewProps) {
    if (this.props.mode !== newProps.mode) {
      this.setState({inTransition: true});
    }
  }

  renderContents = (b: BoxDimensions) => {
    const { node, mode } = this.props;
    const box = new Box(b.x, b.y, b.w, b.h);
    const showChildren = mode === "tree" && !this.state.inTransition;
    return node.isLocality()
      ? <LocalityBox
          box={box}
          mode={mode}
          showChildren={showChildren}
          data={node}
          projection={this.props.projection}
        />
      : <NodeBox box={box} data={node.data as SimulatedNodeStatus} />;
  }

  render() {
    const { node, mode } = this.props;

    // Determine the coordinates of this components bounding box based on the
    // current display mode; either its position in the computed tree map,
    // or its position on the world map.
    let coords: BoxDimensions;
    switch (mode) {
      case "map":
        const center = this.props.projection(node.longLat());
        coords = {x: center[0] - 50, y: center[1] - 50, w: 100, h: 100};
        break;
      default:
        coords = {x: node.x, y: node.y, w: node.dx, h: node.dy};
    }

    const targetStyle = this.shouldAnimate()
      ? _.mapValues(coords, n => spring(n, { stiffness: 250, precision: 1 }))
      : coords;
    return (
      <Motion
        defaultStyle={coords}
        style={targetStyle}
        onRest={this.onRest}
      >
        {this.renderContents}
      </Motion>
    );
  }
}
interface ModalLocalitiesViewProps {
  nodeHistories: SimulatedNodeStatus[];
  projection: d3.geo.Projection;
  zoom: ZoomTransformer;
}

export class ModalLocalitiesView extends React.Component<ModalLocalitiesViewProps, any> {
  constructor(props: any) {
    super(props);
    this.state = {
      mode: "tree",
    };
  }

  toggleMode = () => {
    const mode = this.state.mode === "tree" ? "map" : "tree";
    this.setState({ mode });
  }

  render() {
    const localities = new LocalityCollection();
    this.props.nodeHistories.forEach(nh => localities.addNode(nh));
    localities.computeTreeMapLayout(this.props.zoom.viewportSize());
    return (
      <g onClick={this.toggleMode} className="swappable-nodes">
        {localities.tree.children
          ? localities.tree.children.map(l => <ModalTreeNodeView projection={this.props.projection} mode={this.state.mode} node={l} />)
          : null
        }
      </g>
    );
  }
}
