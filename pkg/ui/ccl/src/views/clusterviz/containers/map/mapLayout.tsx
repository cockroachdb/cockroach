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

import * as protos from "src/js/protos";
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

interface MapLayoutState {
  zoomTransform: ZoomTransformer;
  prevLocations: protos.cockroach.server.serverpb.LocationsResponse.Location$Properties[];
}

export class MapLayout extends React.Component<MapLayoutProps, MapLayoutState> {
  gEl: any;
  zoom: d3.behavior.Zoom<any>;
  maxLatitude = 80;

  constructor(props: MapLayoutProps) {
    super(props);

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

    const zoomTransform = new ZoomTransformer(bounds, props.viewportSize);
    this.state = {
      zoomTransform,
      prevLocations: [],
    };

    // Create a new zoom behavior and apply it to the svg element.
    this.zoom = d3.behavior.zoom()
      .on("zoom", this.onZoom);

    // Set initial zoom state.
    this.updateZoom(zoomTransform);
  }

  // updateZoom programmatically requests zoom transition to the target
  // specified by the provided ZoomTransformer. If 'animate' is true, this
  // transition is animated; otherwise, the transition is instant. Note that the
  // non-animated updates must still go through a d3 transition of 0 duration,
  // or else the next animated zoom will not have the correct initial state - it
  // establishes the starting point of future animations.
  //
  // During the transition, d3 will repeatedly call the 'onZoom' method with the
  // appropriate translations for the animation; that is the point where this
  // component will actually be re-rendered.
  updateZoom(zt: ZoomTransformer, animate = false) {
    const minScale = zt.minScale();

    this.zoom
      .scaleExtent([minScale, minScale * 10])
      .size(zt.viewportSize());

    // Update both the d3 zoom behavior and the local state.
    d3.select(this.gEl)
      .transition()
      .duration(animate ? 750 : 0)
      .call(this.zoom
        .scale(zt.scale())
        .translate(zt.translate())
        .event);
  }

  // onZoom is called by d3 whenever the zoom needs to be updated. We apply
  // the translations from d3 to our react-land zoomTransform state, causing
  // the component to re-render with the new zoom.
  onZoom = () => {
    const zoomTransform = this.state.zoomTransform.withScaleAndTranslate(
      this.zoom.scale(), this.zoom.translate(),
    );
    this.setState({ zoomTransform });
  }

  // rezoomToLocalities is called to properly re-zoom the map to display all
  // localities.
  rezoomToLocalities() {
    const { prevLocations } = this.state;
    const { localityTree, locationTree } = this.props;
    const locations = _.map(
      getChildLocalities(localityTree), l => findOrCalculateLocation(locationTree, l),
    );

    // Deep comparison to previous location set. If any locations have changed,
    // this indicates that the user has navigated to a different level of the
    // locality tree OR that new data has been added to the currently visible
    // locality.
    if (_.isEqual(locations, prevLocations)) {
      return;
    }

    // Compute a new zoom based on the new set of localities.
    const projection = d3.geo.mercator();
    const boxes = locations.map(location => {
      const center = projection([location.longitude, location.latitude]);

      // Create a 100 unit box centered on each mapped location. This is an
      // arbitrary size in order to reserve enough space to display each
      // location.
      return new Box(center[0] - 50, center[1] - 50, 100, 100);
    });
    const zoomTransform = this.state.zoomTransform.zoomedToBox(Box.boundingBox(...boxes));
    this.setState({
      prevLocations: locations,
    });

    this.updateZoom(zoomTransform, !_.isEmpty(prevLocations));
  }

  componentDidMount() {
    d3.select(this.gEl).call(this.zoom);
    this.rezoomToLocalities();
  }

  componentWillReceiveProps(props: MapLayoutProps) {
    this.setState({
      zoomTransform: this.state.zoomTransform.withViewportSize(props.viewportSize),
    });
    this.rezoomToLocalities();
  }

  renderChildLocalities(projection: d3.geo.Projection) {
    const { localityTree, locationTree } = this.props;
    return _.map(getChildLocalities(localityTree), locality => {
      const location = findOrCalculateLocation(locationTree, locality);
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
