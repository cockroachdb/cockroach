// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	// Force these into vendor until they're used.
	_ "github.com/golang/geo/s2"
	_ "github.com/twpayne/go-geom"
	_ "github.com/twpayne/go-geom/encoding/ewkb"
	_ "github.com/twpayne/go-geom/encoding/ewkbhex"
	_ "github.com/twpayne/go-geom/encoding/geojson"
	_ "github.com/twpayne/go-geom/encoding/kml"
	_ "github.com/twpayne/go-geom/encoding/wkb"
	_ "github.com/twpayne/go-geom/encoding/wkbhex"
	_ "github.com/twpayne/go-geom/encoding/wkt"
)
