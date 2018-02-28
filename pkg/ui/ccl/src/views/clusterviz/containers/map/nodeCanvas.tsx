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

import { LivenessStatus } from "src/redux/nodes";
import { LocalityTier, LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { CLUSTERVIZ_ROOT } from "src/routes/visualization";
import { generateLocalityRoute, getLocalityLabel } from "src/util/localities";
import arrowUpIcon from "!!raw-loader!assets/arrowUp.svg";
import { trustIcon } from "src/util/trust";
import { cockroach } from "src/js/protos";
import Liveness = cockroach.storage.Liveness;

const BACK_BUTTON_OFFSET = 26;

interface NodeCanvasProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  livenessStatus: { [id: string]: LivenessStatus };
  liveness: { [id: string]: Liveness };
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
    const rect = this.graphEl.getBoundingClientRect();
    this.setState({
      viewportSize: [rect.width, rect.height],
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

    const { localityTree, locationTree, livenessStatus, liveness } = this.props;
    const { viewportSize } = this.state;

    if (renderAsMap(locationTree, localityTree)) {
      return <MapLayout
        localityTree={localityTree}
        locationTree={locationTree}
        livenessStatus={livenessStatus}
        viewportSize={viewportSize}
      />;
    }

    return <CircleLayout
      viewportSize={viewportSize}
      localityTree={localityTree}
      livenessStatus={livenessStatus}
      liveness={liveness}
    />;
  }

  renderBackButton() {
    const { tiers } = this.props;

    if (!this.state || _.isEmpty(tiers)) {
      return null;
    }

    const parentLocality = tiers.slice(0, tiers.length - 1);

    return (
      <div
        style={{
          position: "absolute",
          left: BACK_BUTTON_OFFSET,
          bottom: BACK_BUTTON_OFFSET,
          backgroundColor: "white",
          border: "1px solid #EDEDED",
          borderRadius: 3,
          padding: 12,
          boxShadow: "0px 0px 4px 0px rgba(0, 0, 0, 0.2)",
          letterSpacing: 0.5,
        }}
      >
        <Link
          to={ CLUSTERVIZ_ROOT + generateLocalityRoute(parentLocality) }
          style={{ textDecoration: "none", color: "#595f6c" }}
        >
          <span dangerouslySetInnerHTML={trustIcon(arrowUpIcon)} style={{ position: "relative", top: 1 }} />
          Up to{" "}
          <span style={{ textTransform: "uppercase" }}>
            { getLocalityLabel(parentLocality) }
          </span>
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
              position: "absolute",
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
