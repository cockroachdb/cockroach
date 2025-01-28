// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecpb

// Equal returns true if this config is equal to the given config.
func (c *Config) Equal(other *Config) bool {
	return c.Dims == other.Dims && c.Seed == other.Seed
}
