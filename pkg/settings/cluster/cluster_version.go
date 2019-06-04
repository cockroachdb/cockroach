// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

package cluster

import "github.com/cockroachdb/cockroach/pkg/roachpb"

// IsActiveVersion returns true if the features of the supplied version are active at the running
// version.
func (cv ClusterVersion) IsActiveVersion(v roachpb.Version) bool {
	return !cv.Less(v)
}

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (cv ClusterVersion) IsActive(versionKey VersionKey) bool {
	v := VersionByKey(versionKey)
	return cv.IsActiveVersion(v)
}
