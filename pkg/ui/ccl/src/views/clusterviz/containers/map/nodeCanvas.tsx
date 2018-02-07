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

import * as vector from "src/util/vector";

import "./sim.css";

import { ModalLocalitiesView } from "./modalLocalities";
import { WorldMap } from "./worldmap";
import { NodeHistory } from "./nodeHistory";
import { Box, ZoomTransformer } from "./zoom";
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
  zoomTransform: ZoomTransformer;
}

export class NodeCanvas extends React.Component<NodeCanvasProps, NodeCanvasState> {
  graphEl: SVGElement;
  zoom: d3.behavior.Zoom<any>;
  maxLatitude = 63;
  debouncedOnResize: () => void;

  constructor(props: any) {
    super(props);

    // Add debounced resize listener.
    this.debouncedOnResize = _.debounce(this.onResize, 200);
  }

  updateZoomState(zt: ZoomTransformer) {
    const minScale = zt.minScale();

    // Update both the d3 zoom behavior and the local state.
    this.zoom
      .scaleExtent([minScale, minScale * 10])
      .size(zt.viewportSize())
      .scale(zt.scale())
      .translate(zt.translate());

    this.setState({
      zoomTransform: zt,
    });
  }

  onZoom = () => {
    this.updateZoomState(this.state.zoomTransform.withScaleAndTranslate(
      this.zoom.scale(), this.zoom.translate(),
    ));
  }

  onResize = () => {
    this.updateZoomState(this.state.zoomTransform.withViewportSize(
      [this.graphEl.clientWidth, this.graphEl.clientHeight],
    ));
  }

  componentDidMount() {
    // Create a new zoom behavior and apply it to the svg element.
    this.zoom = d3.behavior.zoom()
      .on("zoom", this.onZoom);
    d3.select(this.graphEl).call(this.zoom);

    window.addEventListener("resize", this.debouncedOnResize);

    // Compute zoomable area bounds based on the default mercator projection.
    const projection = d3.geo.mercator();
    const topLeft = projection([-180, this.maxLatitude]);
    const botRight = projection([180, -this.maxLatitude]);
    const bounds = new Box(
      topLeft[0],
      topLeft[1],
      botRight[0] - topLeft[0],
      botRight[1] - topLeft[1],
    );

    // Set initial zoom state.
    this.updateZoomState(new ZoomTransformer(
      bounds, [this.graphEl.clientWidth, this.graphEl.clientHeight],
    ));
  }

  componentWillUnmount() {
    window.removeEventListener("resize", this.debouncedOnResize);
  }

  renderContent() {
    if (!this.state) {
      return null;
    }

    // Apply the current zoom transform to a  mercator projection to pass to
    // components of the ClusterVisualization.  Our zoom bounds are computed
    // from the default projection, so we apply the scale and translation on
    // top of the default scale and translation.
    const scale = this.state.zoomTransform.scale();
    const translate = this.state.zoomTransform.translate();
    const projection = d3.geo.mercator();
    projection.scale(projection.scale() * scale);
    projection.translate(vector.add(vector.mult(projection.translate(), scale), translate));

    const { nodeHistories, localityTree, locationTree, liveness, tiers } = this.props;

    return (
      <g>
        <WorldMap projection={projection} />
        <ModalLocalitiesView
          nodeHistories={nodeHistories}
          localityTree={localityTree}
          locationTree={locationTree}
          liveness={liveness}
          tiers={tiers}
          projection={projection}
          zoom={this.state.zoomTransform}
        />
      </g>
    );
  }

  render() {
    // We must render the SVG even before initializing the state, because we
    // need to read its dimensions from the DOM in order to initialize the
    // state.
    return (
      <div style={{ width: "100%", height: "100%", backgroundColor: "lavender" }}>
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
