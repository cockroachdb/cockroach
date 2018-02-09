// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import React from "react";
import { Link } from "react-router";

import { CircleLayout } from "./circleLayout";
import { renderAsMap } from "./layout";
import { MapLayout } from "./mapLayout";
import { NodeHistory } from "./nodeHistory";

import { LivenessStatus } from "src/redux/nodes";
import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { generateLocalityRoute, getLocalityLabel } from "src/util/localities";

const BACK_BUTTON_OFFSET = 26;

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

    const { nodeHistories, localityTree, locationTree, liveness } = this.props;
    const { viewportSize } = this.state;

    if (renderAsMap(locationTree, localityTree)) {
      return <MapLayout
        localityTree={localityTree}
        locationTree={locationTree}
        liveness={liveness}
        viewportSize={viewportSize}
      />;
    }

    return <CircleLayout
      localityTree={localityTree}
      liveness={liveness}
      nodeHistories={nodeHistories}
      viewportSize={viewportSize}
    />;
  }

  renderBackButton() {
    const { tiers } = this.props;

    if (!this.state || _.isEmpty(tiers)) {
      return null;
    }

    const parentLocality = tiers.splice(0, tiers.length - 1);

    return (
      <div style={{ position: "absolute", left: BACK_BUTTON_OFFSET, bottom: BACK_BUTTON_OFFSET }}>
        <Link to={ CLUSTERVIZ_ROOT + "/" + generateLocalityRoute(parentLocality) }>
          Back to { getLocalityLabel(parentLocality) }
        </Link>
      </div>
    );
  }

  render() {
    // We must render the SVG even before initializing the state, because we
    // need to read its dimensions from the DOM in order to initialize the
    // state.
    return (
      <div style={{ flexGrow: 1, position: "relative" }}>
        <div style={{ width: "100%", height: "100%", position: "absolute"  }}>
          <svg
            style={{
              width: "100%",
              height: "100%",
              marginBottom: -3, // WHYYYYYYYYY?!?!?!?!?
            }}
            className="cluster-viz"
            ref={svg => this.graphEl = svg}
          >
            { this.renderContent() }
          </svg>
        </div>
        { this.renderBackButton() }
      </div>
    );
  }
}
