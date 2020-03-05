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
	"github.com/cockroachdb/errors"
	"github.com/golang/geo/s2"
	"github.com/twpayne/go-geom"
)

func STArea(f *Feature) (float64, error) {
	if f.gm != nil {
		switch s := f.gm.(type) {
		case *geom.Point:
			return s.Area(), nil
		default:
			return 0.0, errors.Errorf(`unhandled geom type %T: %s`, s, s)
		}
	}
	switch s := f.gg.(type) {
	case s2.Point:
		return 0.0, nil
	default:
		return 0.0, errors.Errorf(`unhandled geog type %T: %s`, s, s)
	}
}
