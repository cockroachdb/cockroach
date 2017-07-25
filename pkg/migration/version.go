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
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"golang.org/x/net/context"

	"github.com/pkg/errors"
)

func parseAndValidateVersion(s string) (roachpb.Version, error) {
	var c roachpb.Version
	parts := strings.Split(s, ".")

	if len(parts) == 2 {
		parts = append(parts, "0")
	}

	if len(parts) != 3 {
		return c, errors.Errorf("invalid version %s", s)
	}
	ints := make([]int64, len(parts))
	for i := range parts {
		var err error
		if ints[i], err = strconv.ParseInt(parts[i], 10, 32); err != nil {
			return c, errors.Errorf("invalid version %s: %s", s, err)
		}
	}

	c.Major = int32(ints[0])
	c.Minor = int32(ints[1])
	c.Unstable = int32(ints[2])

	return c, nil
}

type stringedVersion base.ClusterVersion

func (sv *stringedVersion) String() string {
	if sv == nil {
		sv = &stringedVersion{}
	}
	return sv.MinimumVersion.String()
}

var versionTransformer settings.TransformerFn = func(curRawProto []byte, versionBump *string) (newRawProto []byte, versionStringer interface{}, _ error) {
	defer func() {
		if versionStringer != nil {
			versionStringer = (*stringedVersion)(versionStringer.(*base.ClusterVersion))
		}
	}()
	var oldV base.ClusterVersion

	// If no old value supplied, fill in the default.
	if curRawProto == nil {
		oldV = base.ClusterVersion{
			MinimumVersion: base.MinimumSupportedVersion,
			UseVersion:     base.MinimumSupportedVersion,
		}
		var err error
		curRawProto, err = oldV.Marshal()
		if err != nil {
			return nil, nil, err
		}
	}

	if err := oldV.Unmarshal(curRawProto); err != nil {
		return nil, nil, err
	}

	if versionBump == nil {
		// Round-trip the existing value, but only if it passes sanity checks.
		b, err := oldV.Marshal()
		if err != nil {
			return nil, nil, err
		}
		return b, &oldV, err
	}

	// We have a new proposed update to the value, validate it.
	minVersion, err := parseAndValidateVersion(*versionBump)
	if err != nil {
		return nil, nil, err
	}

	newV := oldV
	newV.UseVersion = minVersion
	newV.MinimumVersion = minVersion

	if minVersion.Less(oldV.MinimumVersion) {
		return nil, nil, errors.Errorf("cannot downgrade from %s to %s", oldV.MinimumVersion, minVersion)
	}

	distance := func(o, n roachpb.Version) int32 {
		unstableN := n.Unstable - o.Unstable
		if unstableN > 1 {
			unstableN = 1
		}

		var dist int32
		for _, d := range []int32{
			n.Major - o.Major,
			n.Minor - o.Minor,
			n.Patch - o.Patch,
			unstableN,
		} {
			if d > 0 {
				dist += d
			}
		}
		return dist
	}

	if distance(oldV.MinimumVersion, minVersion) > 1 {
		return nil, nil, errors.Errorf("cannot upgrade directly from %s to %s", oldV.MinimumVersion, minVersion)
	}

	if base.ServerVersion.Less(minVersion) {
		// TODO(tschottdorf): also ask gossip about other nodes.
		return nil, nil, errors.Errorf("cannot upgrade to %s: node running %s",
			minVersion, base.ServerVersion)
	}

	b, err := newV.Marshal()
	return b, &newV, err
}

// ClusterVersion tracks the configured minimum cluster version.
var clusterVersion = func() *settings.StatemachineSetting {
	s := settings.RegisterStatemachineSetting(
		"version",
		"set the active cluster version in the format '<major>.<minor>'.", // hide optional `.<unstable>`
		versionTransformer,
	)
	s.Hide()
	return s
}()

// ExposedClusterVersion manages the active version of a running node.
type ExposedClusterVersion struct {
	getter  func() base.ClusterVersion
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
	getter func() base.ClusterVersion, engines []engine.Engine,
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
	getter := func() base.ClusterVersion {
		// TODO(tschottdorf): persist any updates to all of our stores before making it visible.
		_, obj, err := versionTransformer([]byte(clusterVersion.Get()), nil)
		if err != nil {
			log.Fatalf(context.Background(), "error loading cluster version: %s", err)
		}
		return *(*base.ClusterVersion)(obj.(*stringedVersion))
	}
	return NewExposedClusterVersion(getter, engines)
}

// Version returns the minimum cluster version the caller may assume is in
// effect.
func (ecv *ExposedClusterVersion) Version() base.ClusterVersion {
	// TODO(tschottdorf): whenever the settings value changes, persist the
	// change to all engines before returning.
	return ecv.getter()
}

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (ecv *ExposedClusterVersion) IsActive(v roachpb.Version) bool {
	return !ecv.Version().UseVersion.Less(v)
}
