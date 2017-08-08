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

package migration

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

// ExposedClusterVersion manages the active version of a running node.
type ExposedClusterVersion struct {
	getter  func() cluster.ClusterVersion
	engines []engine.Engine
}

// NewExposedClusterVersion initializes an ExposedClusterVersion which takes
// into account the gossiped cluster version (if any) and the cluster version
// (if any) persisted on the given engines, and updates the engines as the
// cluster-wide version is bumped before exposing these new versions.
//
// While this variable might hold a stale value (owing to the fact that the
// source of truth may not be local to the node), the MinimumVersion represented
// by it does not decrease.
func NewExposedClusterVersion(
	getter func() cluster.ClusterVersion, engines []engine.Engine,
) *ExposedClusterVersion {
	// TODO(tschottdorf): check the engines.
	return &ExposedClusterVersion{
		getter:  getter,
		engines: engines,
	}
}

// NewDefaultExposedClusterVersion returns an *ExposedClusterVersion which takes its
// updates from the global version cluster setting.
func NewDefaultExposedClusterVersion(engines []engine.Engine) *ExposedClusterVersion {
	getter := func() cluster.ClusterVersion {
		return cluster.ClusterVersion{}
		// FIXME(tschottdorf): actually build this stuff properly.
	}
	return NewExposedClusterVersion(getter, engines)
}

// Version returns the minimum cluster version the caller may assume is in
// effect.
func (ecv *ExposedClusterVersion) Version() cluster.ClusterVersion {
	// TODO(tschottdorf): whenever the settings value changes, persist the
	// change to all engines before returning.
	return ecv.getter()
}

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (ecv *ExposedClusterVersion) IsActive(v roachpb.Version) bool {
	return !ecv.Version().UseVersion.Less(v)
}
