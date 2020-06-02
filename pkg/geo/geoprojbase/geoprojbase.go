// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geoprojbase is a minimal dependency package that contains
// basic metadata and data structures for SRIDs and their CRS
// transofmrations.
package geoprojbase

import "github.com/cockroachdb/cockroach/pkg/geo/geopb"

// Proj4Text is the text representation of a PROJ4 transformation.
type Proj4Text string

// ProjInfo is a struct containing metadata related to a given SRID.
type ProjInfo struct {
	SRID      geopb.SRID
	AuthName  string
	AuthSRID  int
	SRText    string
	Proj4Text Proj4Text
	IsLatLng  bool
}

// Projection returns the ProjInfo identifier for the given SRID, as well as an bool
// indicating whether the projection exists.
func Projection(srid geopb.SRID) (ProjInfo, bool) {
	p, exists := projections[srid]
	return p, exists
}
