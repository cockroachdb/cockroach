// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build windows

package geos

import (
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func EnsureInit(errDisplay EnsureInitErrorDisplay) error {
	return nil
}

func WKTToEWKB(wkt geopb.WKT, srid geopb.SRID) (geopb.EWKB, error) {
	return nil, unimplemented.NewWithIssue(46876, "operation not supported on Windows")
}

func ClipEWKBByRect(
	wkb geopb.WKB, xmin float64, ymin float64, xmax float64, ymax float64,
) (geopb.EWKB, error) {
	return nil, unimplemented.NewWithIssue(46876, "operation not supported on Windows")
}
