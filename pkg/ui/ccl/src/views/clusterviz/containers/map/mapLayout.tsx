// Copyright 2017 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
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
  prevLocations: protos.cockroach.server.serverpb.LocationsResponse.ILocation[];
}

export class MapLayout extends React.Component<MapLayoutProps, MapLayoutState> {
  gEl: React.RefObject<SVGGElement> = React.createRef();
  zoom: d3.behavior.Zoom<any>;

  constructor(props: MapLayoutProps) {
    super(props);

    const projection = d3.geo.equirectangular();
    const topLeft = projection([-180, 140]);
    const botRight = projection([180, -120]);
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
    this.zoom = d3.behavior.zoom().on("zoom", this.onZoom);

    // Set initial zoom state.
    this.updateZoom(zoomTransform);
  }

  // updateZoom programmatically requests zoom transition to the target
  // specified by the provided ZoomTransformer. If 'animate' is true, this
  // transition is animated; otherwise, the transition is instant.
  //
  // During the transition, d3 will repeatedly call the 'onZoom' method with the
  // appropriate translations for the animation; that is the point where this
  // component will actually be re-rendered.
  updateZoom(zt: ZoomTransformer, animate = false) {
    const minScale = zt.minScale();

    this.zoom.scaleExtent([minScale, minScale * 10]).size(zt.viewportSize());

    if (animate) {
      // Call zoom.event on the current zoom state, then transition to the
      // target zoom state. This is needed because free pan-and-zoom does not
      // update the internal animation state used by zoom.event, and will cause
      // animations after the first to have the wrong starting position.
      d3.select(this.gEl.current)
        .call(this.zoom.event)
        .transition()
        .duration(750)
        .call(this.zoom.scale(zt.scale()).translate(zt.translate()).event);
    } else {
      // Call zoom.event on the element itself, rather than a transition.
      d3.select(this.gEl.current).call(
        this.zoom.scale(zt.scale()).translate(zt.translate()).event,
      );
    }
  }

  // onZoom is called by d3 whenever the zoom needs to be updated. We apply
  // the translations from d3 to our react-land zoomTransform state, causing
  // the component to re-render with the new zoom.
  onZoom = () => {
    const zoomTransform = this.state.zoomTransform.withScaleAndTranslate(
      this.zoom.scale(),
      this.zoom.translate(),
    );

    // In case the transform was adjusted, apply the scale and translation back
    // to the d3 zoom behavior.
    this.zoom.scale(zoomTransform.scale()).translate(zoomTransform.translate());

    this.setState({ zoomTransform });
  };

  // rezoomToLocalities is called to properly re-zoom the map to display all
  // localities. Should be supplied with the current ZoomTransformer setting.
  rezoomToLocalities(zoomTransform: ZoomTransformer) {
    const { prevLocations } = this.state;
    const { localityTree, locationTree } = this.props;
    const locations = _.map(getChildLocalities(localityTree), (l) =>
      findOrCalculateLocation(locationTree, l),
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
    const boxes = locations.map((location) => {
      const center = projection([location.longitude, location.latitude]);

      // Create a 100 unit box centered on each mapped location. This is an
      // arbitrary size in order to reserve enough space to display each
      // location.
      return new Box(center[0] - 50, center[1] - 50, 100, 100);
    });
    zoomTransform = zoomTransform.zoomedToBox(Box.boundingBox(...boxes));
    this.setState({
      prevLocations: locations,
    });

    this.updateZoom(zoomTransform, !_.isEmpty(prevLocations));
  }

  componentDidMount() {
    d3.select(this.gEl.current).call(this.zoom);
    this.rezoomToLocalities(this.state.zoomTransform);
  }

  componentDidUpdate() {
    const zoomTransform = this.state.zoomTransform.withViewportSize(
      this.props.viewportSize,
    );
    if (!_.isEqual(this.state.zoomTransform, zoomTransform)) {
      this.setState({
        zoomTransform,
      });
    }
    this.rezoomToLocalities(zoomTransform);
  }

  renderChildLocalities(projection: d3.geo.Projection) {
    const { localityTree, locationTree } = this.props;
    return _.map(getChildLocalities(localityTree), (locality) => {
      const location = findOrCalculateLocation(locationTree, locality);
      const center = projection([location.longitude, location.latitude]);

      return (
        <g transform={`translate(${center})`}>
          <LocalityView
            localityTree={locality}
            livenessStatuses={this.props.livenessStatuses}
          />
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
    projection.translate(
      vector.add(vector.mult(projection.translate(), scale), translate),
    );

    const { viewportSize } = this.props;

    return (
      <g ref={this.gEl}>
        <rect width={viewportSize[0]} height={viewportSize[1]} fill="#E2E5EE" />
        <WorldMap projection={projection} />
        {this.renderChildLocalities(projection)}
      </g>
    );
  }
}
