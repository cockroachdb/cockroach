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

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

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

// TelemetryName returns the name to use for the given locality.
func (cfg *TableDescriptor_LocalityConfig) TelemetryName() string {
	switch l := cfg.Locality.(type) {
	case *TableDescriptor_LocalityConfig_Global_:
		return tree.TelemetryNameGlobal
	case *TableDescriptor_LocalityConfig_RegionalByTable_:
		if l.RegionalByTable.Region != nil {
			return tree.TelemetryNameRegionalByTableIn
		}
		return tree.TelemetryNameRegionalByTable
	case *TableDescriptor_LocalityConfig_RegionalByRow_:
		if l.RegionalByRow.As != nil {
			return tree.TelemetryNameRegionalByRowAs
		}
		return tree.TelemetryNameRegionalByRow
	}
	panic(errors.AssertionFailedf("unknown locality: %#v", *cfg))
}
