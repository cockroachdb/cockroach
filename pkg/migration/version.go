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
	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"golang.org/x/net/context"

	"github.com/pkg/errors"
)

type stringedVersion base.ClusterVersion

func (sv *stringedVersion) String() string {
	if sv == nil {
		sv = &stringedVersion{}
	}
	return sv.MinimumVersion.String()
}

func versionTransformer(defaultVersion base.ClusterVersion) settings.TransformerFn {
	return func(curRawProto []byte, versionBump *string) (newRawProto []byte, versionStringer interface{}, _ error) {
		defer func() {
			if versionStringer != nil {
				versionStringer = (*stringedVersion)(versionStringer.(*base.ClusterVersion))
			}
		}()
		var oldV base.ClusterVersion

		// If no old value supplied, fill in the default.
		if curRawProto == nil {
			oldV = defaultVersion
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
		minVersion, err := roachpb.ParseVersion(*versionBump)
		if err != nil {
			return nil, nil, err
		}

		newV := oldV
		newV.UseVersion = minVersion
		newV.MinimumVersion = minVersion

		if minVersion.Less(oldV.MinimumVersion) {
			return nil, nil, errors.Errorf("cannot downgrade from %s to %s", oldV.MinimumVersion, minVersion)
		}

		if !oldV.MinimumVersion.CanBump(minVersion) {
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
}

// clusterVersionVar is the *StateMachineSetting which tracks the configured
// minimum cluster version.
var clusterVersion = func() *settings.StateMachineSetting {
	s := settings.RegisterStateMachineSetting(
		"version",
		"set the active cluster version in the format '<major>.<minor>'.", // hide optional `-<unstable>`
		nil, // intentional hack, see NewDefaultExposedClusterVersion.
	)
	s.Hide()
	return s
}()

// ExposedClusterVersion manages the active version of a running node.
type ExposedClusterVersion struct {
	mu struct {
		syncutil.RWMutex
		base.ClusterVersion
		callbacks []func(base.ClusterVersion)
	}
}

// NewExposedClusterVersion initializes an ExposedClusterVersion which takes
// into account the gossiped cluster version (if any) and the cluster version
// (if any) persisted on the given engines, and updates the engines as the
// cluster-wide version is bumped before exposing these new versions.
//
// While this variable might hold a stale value (owing to the fact that the
// source of truth may not be local to the node), the MinimumVersion represented
// by it does not decrease.
func NewExposedClusterVersion(initialVersion base.ClusterVersion) *ExposedClusterVersion {
	ecv := &ExposedClusterVersion{}
	ecv.mu.ClusterVersion = initialVersion
	return ecv
}

// NewDefaultExposedClusterVersion returns an *ExposedClusterVersion which takes its
// updates from the global version cluster setting and starts out with the supplied
// ClusterVersion.
func NewDefaultExposedClusterVersion(initialCV base.ClusterVersion) *ExposedClusterVersion {
	transformer := versionTransformer(initialCV)
	// This is all a hack, but thanks to the singleton-ness of Settings we can't
	// set this up at init time. The explicit reset to nil serves to disarm an
	// assertion that fires since during tests, this method is called
	// repeatedly.
	clusterVersion.SetTransformer(nil)
	clusterVersion.SetTransformer(transformer)

	ecv := NewExposedClusterVersion(initialCV)
	clusterVersion.OnChange(func() {
		_, obj, err := transformer([]byte(clusterVersion.Get()), nil)
		if err != nil {
			log.Fatalf(context.Background(), "error loading cluster version: %s", err)
		}
		ecv.SetTo(*(*base.ClusterVersion)(obj.(*stringedVersion)))
	})
	return ecv
}

// Version returns the cluster version the caller may assume is in
// effect.
func (ecv *ExposedClusterVersion) Version() base.ClusterVersion {
	ecv.mu.RLock()
	cv := ecv.mu.ClusterVersion
	ecv.mu.RUnlock()
	return cv
}

// IsActive returns true if the features of the supplied version are active at
// the running version.
func (ecv *ExposedClusterVersion) IsActive(v roachpb.Version) bool {
	return !ecv.Version().UseVersion.Less(v)
}

// OnChange registers the given closure as a callback which is invoked whenenver
// SetTo() is called. Multiple registrations are possible. Callbacks must not
// block as they are invoked synchronously on SetTo, but they may call Version().
func (ecv *ExposedClusterVersion) OnChange(f func(base.ClusterVersion)) {
	ecv.mu.Lock()
	ecv.mu.callbacks = append(ecv.mu.callbacks, f)
	ecv.mu.Unlock()
}

// SetTo changes the exposed cluster version. It is typically called from a
// gossip callback.
func (ecv *ExposedClusterVersion) SetTo(cv base.ClusterVersion) {
	ecv.mu.Lock()
	ecv.mu.ClusterVersion = cv
	ecv.mu.Unlock()

	ecv.mu.RLock()
	callbacks := append(([]func(base.ClusterVersion))(nil), ecv.mu.callbacks...)
	ecv.mu.RUnlock()

	for _, cb := range callbacks {
		cb(cv)
	}
}
