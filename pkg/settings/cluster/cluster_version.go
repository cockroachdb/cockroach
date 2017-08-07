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

import "github.com/cockroachdb/cockroach/pkg/roachpb"

var (
	// MinimumSupportedVersion is the earliest version of data supported
	// by this binary. If this binary is started using a store that has
	// data marked with an earlier version than MinimumSupportedVersion,
	// then the binary will exit with an error.
	MinimumSupportedVersion = VersionBase

	// ServerVersion is the version of this binary.
	ServerVersion = VersionSplitHardStateBelowRaft
)

// List all historical versions here in reverse chronological order, with
// comments describing what backwards-incompatible features were introduced.
//
// NB: when adding a version, don't forget to bump ServerVersion above (and
// perhaps MinimumSupportedVersion, if necessary).
var (
	// VersionSplitHardStateBelowRaft is https://github.com/cockroachdb/cockroach/pull/17051.
	VersionSplitHardStateBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 2}

	// VersionRaftLogTruncationBelowRaft is https://github.com/cockroachdb/cockroach/pull/16993.
	VersionRaftLogTruncationBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 1}

	// VersionBase is the empty version and corresponds to any binary
	// older than 1.0.1, though these binaries won't know anything
	// about the mechanism in which this version is used.
	VersionBase = roachpb.Version{Major: 1}
)

// FIXME(tschottdorf): remove these with the version migration PR.
var _ = MinimumSupportedVersion
var _ = VersionBase
