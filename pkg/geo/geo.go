// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package geo contains types that represent spatial objects on a plane or
// sphere. These respectively correspond to the SQL GEOMETRY and GEOGRAPHY
// types.
//
// Subpackages are available that perform operations using these types:
// - geo/geofn implements the logic of the PostGIS/OGC functions over GEOMETRY
//   and GEOGRAPHY types.
// - geo/geoindex implements index-accelerated versions of a small number of the
//   functions in geofn. These are used with a precomputed SQL inverted index.
package geo

import (
	"bytes"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
)

// Region represents a geometrical object in planar or spherical space.
//
// WIP This needs a better name. GeoJSON calls it a "Geometry", but that's
// confusing with the SQL GEOMETRY type. Does PostGIS have a name for this? A
// casual scan seems to show that they totally separate the two. Now that I
// think about it, maybe we actually do need two types, on that wraps `r2` and
// one that wraps `s2`.
type Region struct {
	// WIP The prototype has made it pretty clear that using s2.Region here
	// results in awkward type switches all over the place. There's got to be
	// something better for us to do here.
	s2.Region
	// TODO(dan): Most operations on geometrical objects need to have SRIDs
	// associated with them to either do the conversion or error. Where should
	// those SRIDs live? Here?
}

func (r Region) String() string {
	// WIP
	return fmt.Sprintf("%s", r.Region)
}

// Marshal serializes this object into a binary represention suitable for long
// term storage. When passed back to Unmarshal, this representation will exactly
// round trip the original object with no loss of precision.
//
// The primary user of this is the SQL value encoding for geographic datums.
func (r *Region) Marshal() []byte {
	// TODO(dan): We likely want to a standard format for marshalling and
	// unmarshalling, instead of relying on the s2 one, which doesn't seem to have
	// a documented guarantee of being safe for persistence.
	var buf bytes.Buffer
	switch r := r.Region.(type) {
	case s2.Point:
		if err := r.Encode(&buf); err != nil {
			panic(err)
		}
	case s2.Rect:
		if err := r.Encode(&buf); err != nil {
			panic(err)
		}
	default:
		panic(errors.AssertionFailedf(`unhandled feature type %T: %v`, r, r))
	}
	return buf.Bytes()
}
