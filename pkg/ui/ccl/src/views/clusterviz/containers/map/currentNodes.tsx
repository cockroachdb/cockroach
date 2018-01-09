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

import { SimulatedNodeStatus } from "./nodeSimulator";
import * as Vector from "./vector";
import * as PathMath from "./pathmath";
import { NodeView } from "./nodeView";
import { LocalityLink, LocalityLinkProps } from "./localityLink";

export interface CurrentNodesProps {
  nodeHistories: SimulatedNodeStatus[];
  projection: d3.geo.Projection;
}

// CurrentNodes places all current nodes on the map based on their long/lat
// coordinates (currently simulated). Also generates a link between each node
// pair, visualized on the map as a directed path.
export class CurrentNodes extends React.Component<CurrentNodesProps, any> {
  render() {
    const { projection } = this.props;
    if (_.isEmpty(this.props.nodeHistories)) {
      return null;
    }

    // Attach a generated location to each node to create the data for a
    // locality.
    const localities = this.props.nodeHistories.map((nh) => ({
      nodeHistory: nh,
      translate: projection(nh.longLat()),
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
    const minDistance = NodeView.maxRadius * 3;
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
    const scaledRadius = NodeView.outerRadius * localityScale;
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
            <NodeView
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
