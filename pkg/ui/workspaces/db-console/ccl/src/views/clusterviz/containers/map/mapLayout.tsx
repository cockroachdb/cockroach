// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

import { geoEquirectangular, geoMercator, GeoProjection } from "d3-geo";
import { select } from "d3-selection";
import { zoom, ZoomBehavior, ZoomTransform } from "d3-zoom";
import isEqual from "lodash/isEqual";
import map from "lodash/map";
import React from "react";

import * as protos from "src/js/protos";
import { LocalityTree } from "src/redux/localities";
import { LocationTree } from "src/redux/locations";
import { LivenessStatus } from "src/redux/nodes";
import { getChildLocalities } from "src/util/localities";
import { findOrCalculateLocation } from "src/util/locations";
import * as vector from "src/util/vector";

import { LocalityView } from "./localityView";
import { WorldMap } from "./worldmap";
import { Box, ZoomTransformer } from "./zoom";

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
  zoomBehavior: ZoomBehavior<SVGGElement, unknown>;

  constructor(props: MapLayoutProps) {
    super(props);

    const projection = geoEquirectangular();
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
    this.zoomBehavior = zoom<SVGGElement, unknown>().on("zoom", this.onZoom);

    // Set initial zoom state.
    this.updateZoom(zoomTransform);
  }

  // updateZoom programmatically requests zoom transition to the target
  // specified by the provided ZoomTransformer.
  updateZoom(zt: ZoomTransformer) {
    const minScale = zt.minScale();
    this.zoomBehavior
      .extent([[0, 0], zt.viewportSize()])
      .scaleExtent([minScale, minScale * 10]);
    // Call zoom.event on the element itself, rather than a transition.
    select(this.gEl.current).call(
      this.zoomBehavior.transform as any,
      new ZoomTransform(zt.scale(), zt.translate()[0], zt.translate()[1]),
    );
  }

  // onZoom is called by d3 whenever the zoom needs to be updated. We apply
  // the translations from d3 to our react-land zoomTransform state, causing
  // the component to re-render with the new zoom.
  onZoom = (event: any) => {
    const zoomTransform = this.state.zoomTransform.withScaleAndTranslate(
      event.transform.k,
      [event.transform.x, event.transform.y],
    );

    this.setState({ zoomTransform });
  };

  // rezoomToLocalities is called to properly re-zoom the map to display all
  // localities. Should be supplied with the current ZoomTransformer setting.
  rezoomToLocalities(zoomTransform: ZoomTransformer) {
    const { prevLocations } = this.state;
    const { localityTree, locationTree } = this.props;
    const locations = map(getChildLocalities(localityTree), l =>
      findOrCalculateLocation(locationTree, l),
    );

    // Deep comparison to previous location set. If any locations have changed,
    // this indicates that the user has navigated to a different level of the
    // locality tree OR that new data has been added to the currently visible
    // locality.
    if (isEqual(locations, prevLocations)) {
      return;
    }

    // Compute a new zoom based on the new set of localities.
    const projection = geoMercator();
    const boxes = locations.map(location => {
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

    this.updateZoom(zoomTransform);
  }

  componentDidMount() {
    select(this.gEl.current).call(this.zoomBehavior);
    this.rezoomToLocalities(this.state.zoomTransform);
  }

  componentDidUpdate() {
    const zoomTransform = this.state.zoomTransform.withViewportSize(
      this.props.viewportSize,
    );
    if (!isEqual(this.state.zoomTransform, zoomTransform)) {
      this.setState({
        zoomTransform,
      });
    }
    this.rezoomToLocalities(zoomTransform);
  }

  renderChildLocalities(projection: GeoProjection) {
    const { localityTree, locationTree } = this.props;
    return map(getChildLocalities(localityTree), locality => {
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
    const projection = geoMercator();
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
