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

// Region is an alias for a region stored on the database.
type Region string

// Regions is an alias for a slice of regions.
type Regions []Region

// ToStrings converts the Regions slice to a string slice.
func (regions Regions) ToStrings() []string {
	ret := make([]string, len(regions))
	for i, region := range regions {
		ret[i] = string(region)
	}
	return ret
}
