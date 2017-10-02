// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package cluster

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// IsActiveVersion returns true if the features of the supplied version are active at the running
// version.
func (cv ClusterVersion) IsActiveVersion(v roachpb.Version) bool {
	return !cv.UseVersion.Less(v)
}

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (cv ClusterVersion) IsActive(versionKey VersionKey) bool {
	v := VersionByKey(versionKey)
	return cv.IsActiveVersion(v)
}

// IsMinSupported returns true if the features of the supplied version will be
// permanently available (i.e. cannot be downgraded away).
func (cv ClusterVersion) IsMinSupported(versionKey VersionKey) bool {
	v := VersionByKey(versionKey)
	return !cv.MinimumVersion.Less(v)
}
