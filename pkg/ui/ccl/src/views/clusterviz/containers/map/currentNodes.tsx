// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Cockroach Community Licence (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/LICENSE

import _ from "lodash";
import React from "react";
import { connect } from "react-redux";
import * as d3 from "d3";

import { refreshNodes, refreshLiveness } from "src/redux/apiReducers";
import { nodesSummarySelector, NodesSummary } from "src/redux/nodes";
import { AdminUIState } from "src/redux/state";

import NodeStatusHistory from "./statusHistory";
import * as Vector from "./vector";
import * as PathMath from "./pathmath";
import { Locality } from "./locality";
import { LocalityLink, LocalityLinkProps } from "./localityLink";

export interface CurrentNodesProps {
  nodesSummary: NodesSummary;
  statusesValid: boolean;
  refreshNodes: typeof refreshNodes;
  refreshLiveness: typeof refreshLiveness;
}

export interface CurrentNodesOwnProps {
  projection: d3.geo.Projection;
}

// List of fake location data in order to place nodes on the map for visual
// effect.
const locations: [number, number][] = [
  [-74.00597, 40.71427],
  [-80.19366, 25.77427],
  [-93.60911, 41.60054],
  [-118.24368, 34.05223],
  [-122.33207, 47.60621],
  [-0.12574, 51.50853],
  [13.41053, 52.52437],
  [18.0649, 59.33258],
  [151.20732, -33.86785],
  [144.96332, -37.814],
  [153.02809, -27.46794],
  [116.39723, 39.9075],
  [121.45806, 31.22222],
  [114.0683, 22.54554],
  [72.88261, 19.07283],
  [77.59369, 12.97194],
  [77.22445, 28.63576],
];

// CurrentNodes places all current nodes on the map based on their long/lat
// coordinates (currently simulated). Also generates a link between each node
// pair, visualized on the map as a directed path.
class CurrentNodes extends React.Component<CurrentNodesProps & CurrentNodesOwnProps, any> {
  // HACK: nodeHistories is used to maintain a list of previous node statuses
  // for individual nodes. This is used to display rates of change without
  // having to query time series data. This is a hack because this behavior
  // should be moved into the reducer.
  nodeHistories: { [id: string]: NodeStatusHistory } = {};

  // accumulateHistory parses incoming nodeStatus properties and accumulates
  // a history for each node.
  accumulateHistory() {
    if (!this.props.nodesSummary.nodeStatuses) {
      return;
    }

    this.props.nodesSummary.nodeStatuses.map((status) => {
      const id = status.desc.node_id;
      if (!this.nodeHistories.hasOwnProperty(id)) {
        this.nodeHistories[id] = new NodeStatusHistory(status);
      } else {
        this.nodeHistories[id].update(status);
      }
    });
  }

  componentWillMount() {
    this.props.refreshNodes();
    this.props.refreshLiveness();
  }

  componentWillReceiveProps(props: CurrentNodesProps & CurrentNodesOwnProps) {
    props.refreshNodes();
    props.refreshLiveness();
  }

  render() {
    // TODO(mrtracy): Move this to lifecycle methods.
    this.accumulateHistory();
    const { projection } = this.props;
    if (_.isEmpty(this.nodeHistories)) {
      return null;
    }

    // Attach a generated location to each node to create the data for a
    // locality.
    const nodeHistoryList = _.values(this.nodeHistories);
    const localities = nodeHistoryList.map((nh, i) => ({
      nodeHistory: nh,
      translate: projection(locations[i % locations.length]),
    }));

    // Create locality links for each pair.
    type localityInfo = typeof localities[0];
    const linkPairs: [localityInfo, localityInfo][] = [];
    localities.forEach(l1 =>
      localities.forEach(l2 => {
        if (l1 === l2) {
          return;
        }

        linkPairs.push([l1, l2]);
      }),
    );

    // Localities are scaled to the viewport, not the map; however, when the map
    // is zoomed out very far, localities may overlap at their default size. The
    // scale factor is computed based on the closest locality pair, but applied
    // to all localities in order to give a uniform appearance.
    let localityScale = 1;
    const minDistance = Locality.maxRadius * 3;
    linkPairs.forEach(linkPair => {
      // Compute the distance between this locality pair. If the distance
      // is shorter than the minimum required distance, adjust the scale.
      const d = Vector.distance(linkPair[0].translate, linkPair[1].translate);
      if (d < minDistance) {
        const scale = d / minDistance;
        if (scale < localityScale) {
          localityScale = scale;
        }
      }
    });

    // Adjust locality links paths by routing them around other localities.
    // TODO(mrtracy): This math should be extracted into the PathMath library.
    const scaledRadius = Locality.outerRadius * localityScale;
    const localityLinks = linkPairs.map(linkPair => {
      const link: LocalityLinkProps = {
        id: `${linkPair[0].nodeHistory.id()}-${linkPair[1].nodeHistory.id()}`,
        reversed: false,
        points: [],
      };

      // If the link goes from right to left, mark it as reversed.
      if (linkPair[0].translate[0] > linkPair[1].translate[0]) {
        link.reversed = true;
      }

      const vec = Vector.sub(linkPair[1].translate, linkPair[0].translate);
      const norm = Vector.normalize(vec);
      const skip = scaledRadius;

      // Create the first points in the curve between the two localties.
      link.points = [linkPair[0].translate, Vector.add(linkPair[0].translate, Vector.mult(norm, skip))];

      // Bend the curve around any localities which come too close to
      // the line drawn to represent this locality link. This inner
      // loop just adds additional points to the cardinal curve.
      for (let j = 0; j < localities.length; j++) {
        // First, find the closest point on the locality link segment to
        // the center of each locality.
        const loc = localities[j];
        const closest = PathMath.findClosestPoint(linkPair[0].translate, linkPair[1].translate, loc.translate);
        // Only consider bending the locality link IF the closest point
        // lies on the segment _between_ the two localities.
        if (closest[0] !== 0 || closest[1] !== 0) {
          // We bend if the distance is within 2x the max radius (2x is
          // determined empirically for aesthetics).
          const dist = Vector.distance(closest, loc.translate);
          if (dist < scaledRadius * 2) {
            // This part is a bit dicey, so here's an explanation of the
            // algorithm:
            // - Compute the vector from the locality center to closest point.
            // - Measure the angle; if between 45 degrees and 135 degrees:
            //   - If vector points down, bend 2x the max radius to clear the
            //     locality name tag.
            //   - Otherwise, bend 1.5x max radius.
            const cVec = Vector.sub(closest, loc.translate);
            const angle = (cVec[0] === 0) ? Math.PI / 2 : Math.abs(Math.atan(cVec[1] / cVec[0]));
            const magnitude = (angle < Math.PI * 3 / 4 && angle > Math.PI / 4)
                ? (cVec[1] > 1 ? scaledRadius * 2 : scaledRadius * 1.5)
                : scaledRadius * 1.5;
            const invertNorm = Vector.invert(norm);
            const perpV = Vector.mult(invertNorm, magnitude);
            const dir1 = Vector.add(loc.translate, perpV);
            const dir2 = Vector.sub(loc.translate, perpV);
            if (dist < magnitude) {
              if (Vector.distance(closest, dir1) < Vector.distance(closest, dir2)) {
                link.points.push(dir1);
              } else {
                link.points.push(dir2);
              }
            }
          }
        }
      }

      // Finish up the curve by adding the final points.
      link.points.push(Vector.sub(linkPair[1].translate, Vector.mult(norm, skip)));
      link.points.push(linkPair[1].translate);

      // Ensure that points sort from left to right.
      link.points.sort(function (a, b) { return a[0] - b[0]; });

      return link;
    });

    // Maximum client activity rate, used to normalize network bars across all
    // displayed localities.
    // TODO(mrtracy): A different criteria should be used to establish a maximum
    // range for these numbers (or they will be displayed differently).
    const maxClientActivityRate =
      _.max(localities.map(locality => locality.nodeHistory.clientActivityRate)) || 1;

    return (
      <g>
        {localities.map((locality) =>
          <g
            key={locality.nodeHistory.id()}
            transform={
              `translate(${locality.translate[0]}, ${locality.translate[1]})`
              + `scale(${localityScale})`
            }
          >
            <Locality
              nodeHistory={locality.nodeHistory}
              maxClientActivityRate={maxClientActivityRate}
            />
          </g>,
        )}
        {localityLinks.map(link =>
          <LocalityLink
            key={link.id}
            {...link}
          />,
        )}
      </g>
    );
  }
}

export default connect(
  (state: AdminUIState, _ownProps: CurrentNodesOwnProps) => ({
    nodesSummary: nodesSummarySelector(state),
    statusesValid: state.cachedData.nodes.valid && state.cachedData.liveness.valid,
  }),
  {
    refreshNodes,
    refreshLiveness,
  },
)(CurrentNodes);
