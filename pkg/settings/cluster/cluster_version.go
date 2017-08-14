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
	MinimumSupportedVersion = Version1_0

	// ServerVersion is the version of this binary.
	ServerVersion = Version1_1
)

// List all historical versions here in reverse chronological order, with
// comments describing what backwards-incompatible features were introduced.
//
// NB: when adding a version, don't forget to bump ServerVersion above (and
// perhaps MinimumSupportedVersion, if necessary).
var (
	// Version1_1 is v1.1.
	// Note that 1.1-alpha versions already self-identify as v1.1.
	Version1_1 = roachpb.Version{Major: 1, Minor: 1}

	// Version1_0_0_2SplitHardStateBelowRaft is https://github.com/cockroachdb/cockroach/pull/17051.
	Version1_0_0_2SplitHardStateBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 2}

	// Version1_0_0_1RaftLogTruncationBelowRaft is https://github.com/cockroachdb/cockroach/pull/16993.
	Version1_0_0_1RaftLogTruncationBelowRaft = roachpb.Version{Major: 1, Minor: 0, Unstable: 1}

	// Version1_0 corresponds to any binary older than 1.0-1,
	// though these binaries won't know anything about the mechanism in which
	// this version is used.
	Version1_0 = roachpb.Version{Major: 1}
)

// BootstrapVersion is the version that a new cluster bootstrapped from this
// binary should have.
func BootstrapVersion() ClusterVersion {
	return ClusterVersion{
		UseVersion:     ServerVersion,
		MinimumVersion: ServerVersion,
	}
}
