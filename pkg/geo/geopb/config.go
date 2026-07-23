// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
