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

package server_test

import (
	"fmt"
	"os"
	"testing"

	gosql "database/sql"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

type testClusterWithHelpers struct {
	*testing.T
	*testcluster.TestCluster
}

func (th *testClusterWithHelpers) getVersionFromShow(i int) string {
	var version string
	if err := th.ServerConn(i).QueryRow("SHOW CLUSTER SETTING version").Scan(&version); err != nil {
		th.Fatalf("%d: %s", i, err)
	}
	return version
}

func (th *testClusterWithHelpers) getVersionFromSelect(i int) string {
	var version string
	if err := th.ServerConn(i).QueryRow("SELECT value FROM system.settings WHERE name = 'version'").Scan(&version); err != nil {
		if err == gosql.ErrNoRows {
			return ""
		}
		th.Fatalf("%d: %s (%T)", i, err, err)
	}
	var v cluster.ClusterVersion
	if err := v.Unmarshal([]byte(version)); err != nil {
		th.Fatalf("%d: %s", i, err)
	}
	return v.MinimumVersion.String()
}

func (th *testClusterWithHelpers) getVersionFromSetting(i int) *cluster.ExposedClusterVersion {
	return &th.Servers[i].ClusterSettings().Version
}

func (th *testClusterWithHelpers) setVersion(i int, version string) error {
	// TODO(tschottdorf):
	// https://github.com/cockroachdb/cockroach/issues/17563 required before
	// we can avoid Sprintf.
	_, err := th.ServerConn(i).Exec(fmt.Sprintf("SET CLUSTER SETTING version = '%s'", version))
	return err
}

func (th *testClusterWithHelpers) mustSetVersion(i int, version string) {
	if err := th.setVersion(i, version); err != nil {
		th.Fatalf("%d: %s", i, err)
	}
}

// Set up a mixed cluster with the given initial bootstrap version and
// len(versions) servers that each run at MinSupportedVersion=v[0] and
// ServerVersion=v[1] (i.e. they identify as a binary that can run with
// at least a v[0] mixed cluster and is itself v[1]).
func setupMixedCluster(t *testing.T, bootstrapVersion cluster.ClusterVersion, versions [][2]string) testClusterWithHelpers {
	serverArgsPerNode := map[int]base.TestServerArgs{}
	for i, v := range versions {
		st := cluster.MakeClusterSettings(roachpb.MustParseVersion(v[0]), roachpb.MustParseVersion(v[1]))
		serverArgsPerNode[i] = base.TestServerArgs{
			Settings: st,
			Knobs: base.TestingKnobs{
				Store: &storage.StoreTestingKnobs{
					BootstrapVersion: &bootstrapVersion,
				},
			},
		}
	}

	tc := testcluster.StartTestCluster(t, len(versions), base.TestClusterArgs{
		ServerArgsPerNode: serverArgsPerNode,
	})
	// We simulate crashes using this cluster, and having this enabled (which is
	// a default migration) causes leaktest to complain.
	if _, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING diagnostics.reporting.enabled = 'false'"); err != nil {
		t.Fatal(err)
	}
	return testClusterWithHelpers{t, tc}
}

func TestClusterVersionUpgrade1_0To1_1(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Four nodes that are all compatible with 1.0, but are really 1.1. This is
	// what the official v1.1 binary will look like.
	versions := [][2]string{{"1.0", "1.1"}, {"1.0", "1.1"}, {"1.0", "1.1"}, {"1.0", "1.1"}}

	// Start by running 1.0.
	bootstrapVersion := cluster.ClusterVersion{
		UseVersion:     cluster.VersionBase,
		MinimumVersion: cluster.VersionBase,
	}

	tc := setupMixedCluster(t, bootstrapVersion, versions)
	defer tc.Stopper().Stop(ctx)

	// TODO(tschottdorf): once the migration in #17389 is in place, verify that
	// there's a suitable entry in the `system.settings` table for a freshly
	// bootstrapped cluster (at 1.1). It's acceptable that an 1.0 cluster (as
	// this one) doesn't write one.

	for i := 0; i < tc.NumServers(); i++ {
		if exp, version := bootstrapVersion.MinimumVersion.String(), tc.getVersionFromShow(i); version != exp {
			t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
		}
	}

	for _, newVersion := range []roachpb.Version{
		bootstrapVersion.MinimumVersion, // v1.0
		{Major: 1, Unstable: 500},
		{Major: 1, Minor: 1},
	} {
		for i := 0; i < tc.NumServers(); i++ {
			v := tc.getVersionFromSetting(i)
			wantActive := newVersion == bootstrapVersion.MinimumVersion
			if isActive := v.IsActive(newVersion); isActive != wantActive {
				t.Fatalf("%d: v%s active=%t (wanted %t)", i, newVersion, isActive, wantActive)
			}
		}

		exp := newVersion.String()
		tc.mustSetVersion(tc.NumServers()-1, exp)

		// Read the versions from the table from each node. Note that under the
		// hood, everything goes to the lease holder and so it's pretty much
		// guaranteed that they all read the same, but it doesn't hurt to check.
		for i := 0; i < tc.NumServers(); i++ {
			if version := tc.getVersionFromSelect(i); version != exp {
				t.Fatalf("%d: incorrect version %q (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromShow(i); version != exp {
				t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
		}

		// Now check the Settings.Version variable. That is the tricky one for which
		// we "hold back" a gossip update until we've written to the engines. We may
		// have to wait a bit until we see the new version here, even though it's
		// already in the table.
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < tc.NumServers(); i++ {
				vers := tc.getVersionFromSetting(i)
				if v := vers.Version().MinimumVersion.String(); v == bootstrapVersion.String() {
					return errors.Errorf("%d: still waiting for %s (now at %s)", i, exp, v)
				} else if v != exp {
					t.Fatalf("%d: should never see version %s (wanted %s)", i, v, exp)
				}
			}
			// Everyone is at the version we're waiting for.
			return nil
		})

		// Since the wrapped version setting exposes the new versions, it must
		// definitely be present on all stores on the first try.
		if err := tc.Servers[1].GetStores().(*storage.Stores).VisitStores(func(s *storage.Store) error {
			cv, err := storage.ReadVersionFromEngineOrDefault(ctx, s.Engine())
			if err != nil {
				return err
			}
			if act := cv.MinimumVersion.String(); act != exp {
				t.Fatalf("%s: %s persisted, but should be %s", s, act, exp)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestClusterVersionBootstrapStrict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Four nodes that are all strictly version X without accepting anything else.
	for _, versions := range [][][2]string{
		[][2]string{{"1.1", "1.1"}, {"1.1", "1.1"}, {"1.1", "1.1"}, {"1.1", "1.1"}},
		[][2]string{{"4.7", "4.7"}, {"4.7", "4.7"}, {"4.7", "4.7"}, {"4.7", "4.7"}},
	} {
		func() {
			bootstrapVersion := cluster.ClusterVersion{
				UseVersion:     roachpb.MustParseVersion(versions[0][0]),
				MinimumVersion: roachpb.MustParseVersion(versions[0][0]),
			}

			tc := setupMixedCluster(t, bootstrapVersion, versions)
			defer tc.Stopper().Stop(ctx)

			exp := versions[0][0]

			for i := 0; i < tc.NumServers(); i++ {
				if version := tc.getVersionFromSetting(i).Version().MinimumVersion.String(); version != exp {
					t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
				}
				if version := tc.getVersionFromShow(i); version != exp {
					t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
				}

				// TODO(tschottdorf): this documents the oddity that exists at
				// the moment, see
				// https://github.com/cockroachdb/cockroach/issues/17389.
				//
				// In short, there's nothing that initially populates the
				// settings table as this is difficult to do during
				// bootstrapping, and using a sql migration is awkward since
				// we'd need a new migration for each release of CockroachDB and
				// must not have users restart into a newer version immediately
				// after bootstrap.
				//
				// Details here are TBD and should be updated with the outcome
				// of the aforementioned issue.
				brokenExp := "" // in a better world, would have brokenExp == exp

				if version := tc.getVersionFromSelect(i); version != brokenExp {
					t.Fatalf("%d: incorrect version %q (wanted %s)", i, version, brokenExp)
				}
			}
		}()
	}
}

func TestClusterVersionMixedIllegal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	exits := make(chan int, 100)

	log.SetExitFunc(func(i int) { exits <- i })
	defer log.SetExitFunc(os.Exit)

	// Three nodes at minimum version 1.1 and a fourth one at 1.0.
	versions := [][2]string{{"1.0", "1.1"}, {"1.0", "1.1"}, {"1.0", "1.1"}, {"1.0", "1.0"}}

	// Start by running 1.1.
	bootstrapVersion := cluster.ClusterVersion{
		UseVersion:     cluster.VersionBase,
		MinimumVersion: cluster.VersionBase,
	}

	tc := setupMixedCluster(t, bootstrapVersion, versions)
	defer tc.Stopper().Stop(ctx)

	exp := "1.1"

	// The last node refuses to perform an upgrade that would risk its own life.
	if err := tc.setVersion(len(versions)-1, exp); !testutils.IsError(err, "cannot upgrade to 1.1: node running 1.0") {
		t.Fatal(err)
	}

	// The other nodes are less careful.
	tc.mustSetVersion(0, exp)

	<-exits // wait for fourth node to die

	// Check that we can still talk to the first three nodes.
	for i := 0; i < tc.NumServers()-1; i++ {
		if version := tc.getVersionFromSetting(i).Version().MinimumVersion.String(); version != exp {
			t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
		}
		testutils.SucceedsSoon(tc, func() error {
			if version := tc.getVersionFromShow(i); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			return nil
		})
	}
}
