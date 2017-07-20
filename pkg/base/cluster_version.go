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
//
// Author: Spencer Kimball (spencer@cockroachlabs.com)

package base

import "github.com/cockroachdb/cockroach/pkg/roachpb"

var (
	// List all historical versions here, with comments describing
	// what backwards-incompatible features were introduced.

	// VersionBase is the empty version and corresponds to any binary
	// newer than 1.0.0.1.
	VersionBase = roachpb.Version{}
	// VersionVersioning marks the introduction of the versioning
	// mechanism.
	VersionVersioning = roachpb.Version{Major: 1, Minor: 0, Patch: 0, Unstable: 1}
	// ...

	// MinimumSupportedVersion is the earliest version of data supported
	// by this binary. If this binary is started using a store that has
	// data marked with an earlier version than MinimumSupportedVersion,
	// then the binary will exit with an error.
	MinimumSupportedVersion = VersionBase

	// ServerVersion is the version of this binary.
	ServerVersion = VersionVersioning
)
