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

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/stretchr/testify/require"
)

var mismatchingSRIDGeographyA = geo.MustParseGeography("SRID=4004;POINT(1.0 1.0)")
var mismatchingSRIDGeographyB = geo.MustParseGeography("SRID=4326;LINESTRING(1.0 1.0, 2.0 2.0)")

// requireMismatchingSRIDError checks errors fall as expected for mismatching SRIDs.
func requireMismatchingSRIDError(t *testing.T, err error) {
	require.Error(t, err)
	require.EqualError(t, err, `operation on mixed SRIDs forbidden: (Point, 4004) != (LineString, 4326)`)
}
