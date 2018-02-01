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
import { RouterState } from "react-router";

import { parseLocalityRoute } from "src/util/localities";
import * as vector from "src/util/vector";

import "./sim.css";

import NodeSimulator from "./nodeSimulator";
import { WorldMap } from "./worldmap";
import { Box, ZoomTransformer } from "./zoom";
import { LocalityTier } from "src/redux/localities";
import { Breadcrumbs } from "ccl/src/views/clusterviz/containers/map/breadcrumbs";

interface ClusterVisualizationState {
  zoomTransform: ZoomTransformer;
}

export default class ClusterVisualization extends React.Component<RouterState, ClusterVisualizationState> {
  graphEl: SVGElement;
  zoom: d3.behavior.Zoom<any>;
  maxLatitude = 63;
  debouncedOnResize: () => void;

  constructor(props: any) {
    super(props);
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

    // Add debounced resize listener.
    this.debouncedOnResize = _.debounce(this.onResize, 200);
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

  renderContent(tiers: LocalityTier[]) {
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

    return (
      <g>
        <WorldMap projection={projection} />
        <NodeSimulator projection={projection} zoom={this.state.zoomTransform} tiers={tiers} />
      </g>
    );
  }

  render() {
    const tiers = parseLocalityRoute(this.props.params.splat);

    // We must render the SVG even before initializing the state, because we
    // need to read its dimensions from the DOM in order to initialize the
    // state.
    return (
      <div style={{ height: "100%" }}>
        <Breadcrumbs tiers={tiers} />
        <svg
          style={{ width: "100%", height: "100%" }}
          className="cluster-viz"
          ref={svg => this.graphEl = svg}
        >
          { this.renderContent(tiers) }
        </svg>
      </div>
    );
  }
}
