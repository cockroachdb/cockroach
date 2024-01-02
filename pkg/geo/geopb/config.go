// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geopb

// IsEmpty returns whether the config contains a geospatial index
// configuration.
func (cfg Config) IsEmpty() bool {
	return cfg.S2Geography == nil && cfg.S2Geometry == nil
}

// IsGeography returns whether the config is a geography geospatial index
// configuration.
func (cfg Config) IsGeography() bool {
	return cfg.S2Geography != nil
}

// IsGeometry returns whether the config is a geometry geospatial index
// configuration.
func (cfg Config) IsGeometry() bool {
	return cfg.S2Geometry != nil
}
