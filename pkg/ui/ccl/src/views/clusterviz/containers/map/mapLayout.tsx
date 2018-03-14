// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

import _ from "lodash";
import * as d3 from "d3";
import React from "react";
import { createSelector } from "reselect";

import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { getChildLocalities } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";
import * as vector from "src/util/vector";

import { LocalityView } from "./localityView";
import { WorldMap } from "./worldmap";
import { Box, ZoomTransformer } from "./zoom";
import { LivenessStatus } from "src/redux/nodes";

import "./mapLayout.styl";

interface MapLayoutProps {
  localityTree: LocalityTree;
  locationTree: LocationTree;
  livenessStatuses: { [id: string]: LivenessStatus };
  viewportSize: [number, number];
}

interface LocalityLocation {
  locality: LocalityTree;
  location: any;
}

interface MapLayoutState {
  zoomTransform: ZoomTransformer;
  prevLocalityLocations: LocalityLocation[];
}

export class MapLayout extends React.Component<MapLayoutProps, MapLayoutState> {
  gEl: any;
  zoom: d3.behavior.Zoom<any>;
  maxLatitude = 80;

  localityLocations = createSelector(
    (props: MapLayoutProps) => props.localityTree,
    (props: MapLayoutProps) => props.locationTree,
    (localityTree, locationTree) => {
      return _.map(getChildLocalities(localityTree), locality => {
        const location = findOrCalculateLocation(locationTree, locality);
        return {
          locality,
          location,
        };
      });
    },
  );

  constructor(props: MapLayoutProps) {
    super(props);

    // Create a new zoom behavior and apply it to the svg element.
    this.zoom = d3.behavior.zoom()
      .on("zoom", this.onZoom);

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
    const zoomTransform = new ZoomTransformer(bounds, props.viewportSize);
    this.updateZoomState(zoomTransform);
    this.state = {
      zoomTransform,
      prevLocalityLocations: [],
    };
  }

  updateZoomState(zt: ZoomTransformer) {
    const minScale = zt.minScale();

    // Update both the d3 zoom behavior and the local state.
    this.zoom
      .scaleExtent([minScale, minScale * 10])
      .size(zt.viewportSize())
      .scale(zt.scale())
      .translate(zt.translate());
  }

  onZoom = () => {
    const zoomTransform = this.state.zoomTransform.withScaleAndTranslate(
      this.zoom.scale(), this.zoom.translate(),
    );
    this.updateZoomState(zoomTransform);
    this.setState({ zoomTransform });
  }

  // rezoomToLocalities is called to properly re-zoom the map to display all
  // localities.
  rezoomToLocalities() {
    const { prevLocalityLocations } = this.state;
    const localityLocations = this.localityLocations(this.props);

    // Deep comparison to previous locality set. If anything has changed, this
    // indicates that the user has navigated to a different level of the
    // locality tree OR that new data has been added to the currently visible
    // locality.
    if (_.isEqual(localityLocations, prevLocalityLocations)) {
      return;
    }

    const projection = d3.geo.mercator();
    const boxes = localityLocations.map(localityLocation => {
      const { location } = localityLocation;
      const center = projection([location.longitude, location.latitude]);

      // Create a 100 unit box centered on each locality. This is an arbitrary
      // size in order to reserve enough space to display each locality.
      return new Box(center[0] - 50, center[1] - 50, 100, 100);
    });
    const zoomTransform = this.state.zoomTransform.zoomedToBox(Box.boundingBox(...boxes));
    this.updateZoomState(zoomTransform);
    this.setState({
      zoomTransform,
      prevLocalityLocations: localityLocations,
    });
  }

  componentDidMount() {
    d3.select(this.gEl).call(this.zoom);
    this.rezoomToLocalities();
  }

  componentWillReceiveProps(props: MapLayoutProps) {
    const zoomTransform = this.state.zoomTransform.withViewportSize(props.viewportSize);
    this.updateZoomState(zoomTransform);
    this.setState({ zoomTransform });
    this.rezoomToLocalities();
  }

  renderChildLocalities(projection: d3.geo.Projection) {
    return this.localityLocations(this.props).map((localityLocation) => {
      const { locality, location } = localityLocation;
      const center = projection([location.longitude, location.latitude]);

      return (
        <g transform={`translate(${center})`}>
          <LocalityView localityTree={locality} livenessStatuses={this.props.livenessStatuses} />
        </g>
      );
    });
  }

  render() {
    // Apply the current zoom transform to a  mercator projection to pass to
    // components of the ClusterVisualization.  Our zoom bounds are computed
    // from the default projection, so we apply the scale and translation on
    // top of the default scale and translation.
    const scale = this.state.zoomTransform.scale();
    const translate = this.state.zoomTransform.translate();
    const projection = d3.geo.mercator();
    projection.scale(projection.scale() * scale);
    projection.translate(vector.add(vector.mult(projection.translate(), scale), translate));

    const { viewportSize } = this.props;

    return (
      <g ref={el => this.gEl = el}>
        <rect width={viewportSize[0]} height={viewportSize[1]} fill="lavender" />
        <WorldMap projection={projection} />
        { this.renderChildLocalities(projection) }
      </g>
    );
  }
}
