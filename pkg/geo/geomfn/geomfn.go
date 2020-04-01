// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geomfn contains functions for operating on geo.Geometry types.
package geomfn

import (
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geos"
)

// ParseGeometry parses a Geometry from a given text.
// TODO(otan): when we have our own WKT parser, move this to geo.
func ParseGeometry(str geo.WKT) (*geo.Geometry, error) {
	// TODO(otan): set SRID of EWKB to 4326.
	wkb, err := geos.WKTToWKB(str)
	if err != nil {
		return nil, err
	}
	return geo.NewGeometry(geo.EWKB(wkb)), nil
}
