// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package catpb

// RegionName is an alias for a region stored on the database.
type RegionName string

// String implements fmt.Stringer.
func (r RegionName) String() string {
	return string(r)
}

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

// Contains returns true if the NameList contains the name.
func (regions RegionNames) Contains(name RegionName) bool {
	for _, r := range regions {
		if r == name {
			return true
		}
	}
	return false
}
