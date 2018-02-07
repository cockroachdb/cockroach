// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import React from "react";

import "./sim.css";

import { ModalLocalitiesView } from "./modalLocalities";
import { NodeHistory } from "./nodeHistory";
import { LivenessStatus } from "src/redux/nodes";
import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";

interface NodeCanvasProps {
  nodeHistories: { [id: string]: NodeHistory };
  localityTree: LocalityTree;
  locationTree: LocationTree;
  liveness: { [id: string]: LivenessStatus };
  tiers: LocalityTier[];
}

interface NodeCanvasState {
  viewportSize: [number, number];
}

export class NodeCanvas extends React.Component<NodeCanvasProps, NodeCanvasState> {
  graphEl: SVGElement;
  debouncedOnResize: () => void;

  constructor(props: any) {
    super(props);

    // Add debounced resize listener.
    this.debouncedOnResize = _.debounce(this.onResize, 200);
  }

  updateViewport = () => {
    this.setState({
      viewportSize: [this.graphEl.clientWidth, this.graphEl.clientHeight],
    });
  }

  onResize = () => {
    this.updateViewport();
  }

  componentDidMount() {
    window.addEventListener("resize", this.debouncedOnResize);

    this.updateViewport();
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.debouncedOnResize);
  }

  renderContent() {
    if (!this.state) {
      return null;
    }

    const { nodeHistories, localityTree, locationTree, liveness, tiers } = this.props;

    return (
      <g>
        <ModalLocalitiesView
          nodeHistories={nodeHistories}
          localityTree={localityTree}
          locationTree={locationTree}
          liveness={liveness}
          tiers={tiers}
          viewportSize={this.state.viewportSize}
        />
      </g>
    );
  }

  render() {
    // We must render the SVG even before initializing the state, because we
    // need to read its dimensions from the DOM in order to initialize the
    // state.
    return (
      <div style={{ width: "100%", height: "100%" }}>
        <svg
          style={{ width: "100%", height: "100%" }}
          className="cluster-viz"
          ref={svg => this.graphEl = svg}
        >
          { this.renderContent() }
        </svg>
      </div>
    );
  }
}
