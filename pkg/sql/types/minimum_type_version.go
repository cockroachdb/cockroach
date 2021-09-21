// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types

import "github.com/cockroachdb/cockroach/pkg/clusterversion"

// minimumTypeUsageVersions defines the minimum version needed for a new
// data type.
var minimumTypeUsageVersions = map[*T]clusterversion.Key{
	RegRole: clusterversion.RegroleType,
}

// IsTypeSupportedInVersion returns whether a given type is supported in the given version.
func IsTypeSupportedInVersion(v clusterversion.ClusterVersion, t *T) (bool, error) {
	// For these checks, if we have an array, we only want to find whether
	// we support the array contents.
	if t.Family() == ArrayFamily {
		t = t.ArrayContents()
	}

	minVersion, ok := minimumTypeUsageVersions[t]
	if !ok {
		return true, nil
	}
	return v.IsActive(minVersion), nil
}
