// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

// RegionName is an alias for a region stored on the database.
type RegionName string

// RegionNames is an alias for a slice of regions.
type RegionNames []RegionName

// ToStrings converts the RegionNames slice to a string slice.
func (regions RegionNames) ToStrings() []string {
	ret := make([]string, len(regions))
	for i, region := range regions {
		ret[i] = string(region)
	}
	return ret
}

// IsValidRegionNameString implements the tree.DatabaseRegionConfig interface.
func (cfg *DatabaseDescriptor_RegionConfig) IsValidRegionNameString(r string) bool {
	if cfg == nil {
		return false
	}
	for _, region := range cfg.Regions {
		if string(region.Name) == r {
			return true
		}
	}
	return false
}

// PrimaryRegionString implements the tree.DatabaseRegionConfig interface.
func (cfg *DatabaseDescriptor_RegionConfig) PrimaryRegionString() string {
	if cfg == nil {
		return ""
	}
	return string(cfg.PrimaryRegion)
}
