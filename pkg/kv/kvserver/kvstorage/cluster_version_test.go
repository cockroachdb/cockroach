// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvstorage

import (
	"context"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// TestStoresClusterVersionIncompatible verifies an error occurs when
// setting up the cluster version from stores that are incompatible with the
// running binary.
func TestStoresClusterVersionIncompatible(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	current := clusterversion.Latest.Version()
	minSupported := clusterversion.MinSupported.Version()

	future := current
	future.Major++

	tooOld := minSupported
	tooOld.Major--

	type testCase struct {
		binV, minV roachpb.Version // binary version and min supported version
		engV       roachpb.Version // version found on engine in test
		expErr     string
	}
	for name, tc := range map[string]testCase{
		"StoreTooNew": {
			// This is what the node is running.
			binV: current,
			// This is what the running node requires from its stores.
			minV: minSupported,
			// Version is way too high for this node.
			engV:   future,
			expErr: `cockroach version .* is incompatible with data in store <no-attributes>=<in-mem>; use version .* or later`,
		},
		"StoreTooOldVersion": {
			// This is what the node is running.
			binV: current,
			// This is what the running node requires from its stores.
			minV: minSupported,
			// Version is way too low.
			engV:   tooOld,
			expErr: `store <no-attributes>=<in-mem>, last used with cockroach version .*, is too old for running version .* \(which requires data from .* or later\)`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			engs := []storage.Engine{storage.NewDefaultInMemForTesting()}
			defer engs[0].Close()
			// Configure versions and write.
			cv := clusterversion.ClusterVersion{Version: tc.engV}
			err := WriteClusterVersionToEngines(ctx, engs, cv)
			if err == nil {
				cv, err = SynthesizeClusterVersionFromEngines(ctx, engs, tc.binV, tc.minV)
			}
			if !testutils.IsError(err, tc.expErr) {
				t.Fatalf("unexpected error: %+v, got version %v", err, cv)
			}
		})
	}
}

// TestStoresClusterVersionWriteSynthesize verifies that the cluster version is
// written to all stores and that missing versions are filled in appropriately.
func TestClusterVersionWriteSynthesize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	// We can't hardcode versions here because the can become too old to create
	// stores with. create a store.
	// For example's sake, let's assume that minV is 1.0. Then binV is 1.1 and
	// development versions are 1.0-1 and 1.0-2.
	minV := clusterversion.MinSupported.Version()
	binV := minV
	binV.Minor += 1

	// Development versions.
	versionA := minV
	versionA.Internal = 1

	versionB := minV
	versionB.Internal = 2

	engines := make([]storage.Engine, 3)
	for i := range engines {
		// Create the stores without an initialized cluster version.
		st := cluster.MakeTestingClusterSettingsWithVersions(binV, minV, false /* initializeVersion */)
		eng, err := storage.Open(
			ctx, storage.InMemory(), st,
			storage.ForTesting, storage.MaxSizeBytes(1<<20),
		)
		if err != nil {
			t.Fatal(err)
		}
		stopper.AddCloser(eng)
		engines[i] = eng
	}
	e0 := engines[:1]
	e01 := engines[:2]
	e2 := engines[2:3]
	e012 := engines[:3]

	// If there are no stores, default to minV.
	if initialCV, err := SynthesizeClusterVersionFromEngines(ctx, nil, binV, minV); err != nil {
		t.Fatal(err)
	} else {
		expCV := clusterversion.ClusterVersion{
			Version: minV,
		}
		if !reflect.DeepEqual(initialCV, expCV) {
			t.Fatalf("expected %+v; got %+v", expCV, initialCV)
		}
	}

	// Verify that the initial read of an empty store synthesizes minV. This
	// is the code path that runs after starting the binV binary for the first
	// time after the rolling upgrade from minV.
	if initialCV, err := SynthesizeClusterVersionFromEngines(ctx, e0, binV, minV); err != nil {
		t.Fatal(err)
	} else {
		expCV := clusterversion.ClusterVersion{
			Version: minV,
		}
		if !reflect.DeepEqual(initialCV, expCV) {
			t.Fatalf("expected %+v; got %+v", expCV, initialCV)
		}
	}

	// Bump a version to something more modern (but supported by this binary).
	// Note that there's still only one store.
	{
		cv := clusterversion.ClusterVersion{
			Version: versionB,
		}
		if err := WriteClusterVersionToEngines(ctx, e0, cv); err != nil {
			t.Fatal(err)
		}

		// Verify the same thing comes back on read.
		if newCV, err := SynthesizeClusterVersionFromEngines(ctx, e0, binV, minV); err != nil {
			t.Fatal(err)
		} else {
			expCV := cv
			if !reflect.DeepEqual(newCV, cv) {
				t.Fatalf("expected %+v; got %+v", expCV, newCV)
			}
		}
	}

	// Use stores 0 and 1. It reads as minV because store 1 has no entry, lowering
	// the use version to minV.
	{
		expCV := clusterversion.ClusterVersion{
			Version: minV,
		}
		if cv, err := SynthesizeClusterVersionFromEngines(ctx, e01, binV, minV); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(cv, expCV) {
			t.Fatalf("expected %+v, got %+v", expCV, cv)
		}

		// Write an updated Version to both stores.
		cv := clusterversion.ClusterVersion{
			Version: versionB,
		}
		if err := WriteClusterVersionToEngines(ctx, e01, cv); err != nil {
			t.Fatal(err)
		}
	}

	// Third node comes along, for now it's alone. It has a lower use version.
	cv := clusterversion.ClusterVersion{
		Version: versionA,
	}

	if err := WriteClusterVersionToEngines(ctx, e2, cv); err != nil {
		t.Fatal(err)
	}

	// Reading across all stores, we expect to pick up the lowest useVersion both
	// from the third store.
	expCV := clusterversion.ClusterVersion{
		Version: versionA,
	}
	if cv, err := SynthesizeClusterVersionFromEngines(ctx, e012, binV, minV); err != nil {
		t.Fatal(err)
	} else if !reflect.DeepEqual(cv, expCV) {
		t.Fatalf("expected %+v, got %+v", expCV, cv)
	}
}
