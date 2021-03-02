// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geomfn

import (
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
	"github.com/cockroachdb/errors"
)

// BufferParams is a wrapper around the geos.BufferParams.
type BufferParams struct {
	p geos.BufferParams
}

// MakeDefaultBufferParams returns the default BufferParams/
func MakeDefaultBufferParams() BufferParams {
	return BufferParams{
		p: geos.BufferParams{
			EndCapStyle:      geos.BufferParamsEndCapStyleRound,
			JoinStyle:        geos.BufferParamsJoinStyleRound,
			SingleSided:      false,
			QuadrantSegments: 8,
			MitreLimit:       5.0,
		},
	}
}

// WithQuadrantSegments returns a copy of the BufferParams with the quadrantSegments set.
func (b BufferParams) WithQuadrantSegments(quadrantSegments int) BufferParams {
	ret := b
	ret.p.QuadrantSegments = quadrantSegments
	return ret
}

// ParseBufferParams parses the given buffer params from a SQL string into
// the BufferParams form.
// The string must be of the same format as specified by https://postgis.net/docs/ST_Buffer.html.
// Returns the BufferParams, as well as the modified distance.
func ParseBufferParams(s string, distance float64) (BufferParams, float64, error) {
	p := MakeDefaultBufferParams()
	fields := strings.Fields(s)
	for _, field := range fields {
		fParams := strings.Split(field, "=")
		if len(fParams) != 2 {
			return BufferParams{}, 0, errors.Newf("unknown buffer parameter: %s", fParams)
		}
		f, val := fParams[0], fParams[1]
		switch strings.ToLower(f) {
		case "quad_segs":
			valInt, err := strconv.ParseInt(val, 10, 64)
			if err != nil {
				return BufferParams{}, 0, errors.Wrapf(err, "invalid int for %s: %s", f, val)
			}
			p.p.QuadrantSegments = int(valInt)
		case "endcap":
			switch strings.ToLower(val) {
			case "round":
				p.p.EndCapStyle = geos.BufferParamsEndCapStyleRound
			case "flat", "butt":
				p.p.EndCapStyle = geos.BufferParamsEndCapStyleFlat
			case "square":
				p.p.EndCapStyle = geos.BufferParamsEndCapStyleSquare
			default:
				return BufferParams{}, 0, errors.Newf("unknown endcap: %s (accepted: round, flat, square)", val)
			}
		case "join":
			switch strings.ToLower(val) {
			case "round":
				p.p.JoinStyle = geos.BufferParamsJoinStyleRound
			case "mitre", "miter":
				p.p.JoinStyle = geos.BufferParamsJoinStyleMitre
			case "bevel":
				p.p.JoinStyle = geos.BufferParamsJoinStyleBevel
			default:
				return BufferParams{}, 0, errors.Newf("unknown join: %s (accepted: round, mitre, bevel)", val)
			}
		case "mitre_limit", "miter_limit":
			valFloat, err := strconv.ParseFloat(val, 64)
			if err != nil {
				return BufferParams{}, 0, errors.Wrapf(err, "invalid float for %s: %s", f, val)
			}
			p.p.MitreLimit = valFloat
		case "side":
			switch strings.ToLower(val) {
			case "both":
				p.p.SingleSided = false
			case "left":
				p.p.SingleSided = true
			case "right":
				p.p.SingleSided = true
				distance *= -1
			default:
				return BufferParams{}, 0, errors.Newf("unknown side: %s (accepted: both, left, right)", val)
			}
		default:
			return BufferParams{}, 0, errors.Newf("unknown field: %s (accepted fields: quad_segs, endcap, join, mitre_limit, side)", f)
		}
	}
	return p, distance, nil
}

// Buffer buffers a given Geometry by the supplied parameters.
func Buffer(g geo.Geometry, params BufferParams, distance float64) (geo.Geometry, error) {
	if params.p.QuadrantSegments < 1 {
		return geo.Geometry{}, errors.Newf(
			"must request at least 1 quadrant segment, requested %d quadrant segments",
			params.p.QuadrantSegments,
		)

	}
	if params.p.QuadrantSegments > geo.MaxAllowedSplitPoints {
		return geo.Geometry{}, errors.Newf(
			"attempting to split buffered geometry into too many quadrant segments; requested %d quadrant segments, max %d",
			params.p.QuadrantSegments,
			geo.MaxAllowedSplitPoints,
		)
	}
	bufferedGeom, err := geos.Buffer(g.EWKB(), params.p, distance)
	if err != nil {
		return geo.Geometry{}, err
	}
	return geo.ParseGeometryFromEWKB(bufferedGeom)
}
