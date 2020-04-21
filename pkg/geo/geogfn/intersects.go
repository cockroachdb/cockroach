// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geogfn

import "github.com/golang/geo/s2"

// polygonIntersectsPolyline returns whether polygon a intersects with
// polyline b.
// We do not consider edges which are touching / the same as intersecting.
func polygonIntersectsPolyline(a *s2.Polygon, b *s2.Polyline) bool {
	// Avoid using NumEdges / Edge of the Polygon type as it is not O(1).
	for _, loop := range a.Loops() {
		for loopEdgeIdx := 0; loopEdgeIdx < loop.NumEdges(); loopEdgeIdx++ {
			loopEdge := loop.Edge(loopEdgeIdx)
			crosser := s2.NewChainEdgeCrosser(loopEdge.V0, loopEdge.V1, (*b)[0])
			for _, nextVertex := range (*b)[1:] {
				if crosser.ChainCrossingSign(nextVertex) == s2.Cross {
					return true
				}
			}
		}
	}
	return false
}
