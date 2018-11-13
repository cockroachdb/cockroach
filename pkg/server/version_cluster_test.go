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
	"context"
	gosql "database/sql"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
)

type testClusterWithHelpers struct {
	*testing.T
	*testcluster.TestCluster
	args func() map[int]base.TestServerArgs
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
	if err := protoutil.Unmarshal([]byte(version), &v); err != nil {
		th.Fatalf("%d: %s", i, err)
	}
	return v.MinimumVersion.String()
}

func (th *testClusterWithHelpers) getVersionFromSetting(i int) *cluster.ExposedClusterVersion {
	return &th.Servers[i].ClusterSettings().Version
}

func (th *testClusterWithHelpers) setVersion(i int, version string) error {
	_, err := th.ServerConn(i).Exec("SET CLUSTER SETTING version = $1", version)
	return err
}

func (th *testClusterWithHelpers) mustSetVersion(i int, version string) {
	if err := th.setVersion(i, version); err != nil {
		th.Fatalf("%d: %s", i, err)
	}
}

func (th *testClusterWithHelpers) setDowngrade(i int, version string) error {
	_, err := th.ServerConn(i).Exec("SET CLUSTER SETTING cluster.preserve_downgrade_option = $1", version)
	return err
}

func (th *testClusterWithHelpers) resetDowngrade(i int) error {
	_, err := th.ServerConn(i).Exec("RESET CLUSTER SETTING cluster.preserve_downgrade_option")
	return err
}

// Set up a mixed cluster with the given initial bootstrap version and
// len(versions) servers that each run at MinSupportedVersion=v[0] and
// ServerVersion=v[1] (i.e. they identify as a binary that can run with
// at least a v[0] mixed cluster and is itself v[1]). A directory can
// optionally be passed in.
func setupMixedCluster(
	t *testing.T, knobs base.TestingKnobs, versions [][2]string, dir string,
) testClusterWithHelpers {

	twh := testClusterWithHelpers{
		T: t,
		args: func() map[int]base.TestServerArgs {
			serverArgsPerNode := map[int]base.TestServerArgs{}
			for i, v := range versions {
				st := cluster.MakeClusterSettings(roachpb.MustParseVersion(v[0]), roachpb.MustParseVersion(v[1]))
				args := base.TestServerArgs{
					Settings: st,
					Knobs:    knobs,
				}
				if dir != "" {
					args.StoreSpecs = []base.StoreSpec{{Path: filepath.Join(dir, strconv.Itoa(i))}}
				}
				serverArgsPerNode[i] = args
			}
			return serverArgsPerNode
		}}

	tc := testcluster.StartTestCluster(t, len(versions), base.TestClusterArgs{
		ReplicationMode:   base.ReplicationManual, // speeds up test
		ServerArgsPerNode: twh.args(),
	})

	// We simulate crashes using this cluster, and having this enabled (which is
	// a default migration) causes leaktest to complain.
	if _, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING diagnostics.reporting.enabled = 'false'"); err != nil {
		t.Fatal(err)
	}

	twh.TestCluster = tc
	return twh
}

// Prev returns the previous version of the given version.
// eg. prev(2.0) = 1.0, prev(2.1) == 2.0, prev(2.1-5) == 2.1.
func prev(version roachpb.Version) roachpb.Version {
	if version.Unstable != 0 {
		return roachpb.Version{Major: version.Major, Minor: version.Minor}
	} else if version.Minor != 0 {
		return roachpb.Version{Major: version.Major}
	} else {
		// version will be at least 2.0-X, so it's safe to set new Major to be version.Major-1.
		return roachpb.Version{Major: version.Major - 1}
	}
}

func TestClusterVersionPersistedOnJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var newVersion = cluster.BinaryServerVersion
	var oldVersion = prev(newVersion)

	// Starts 3 nodes that have cluster versions set to be oldVersion and
	// self-declared binary version set to be newVersion with a cluster
	// running at the new version (i.e. a very regular setup). Want to check
	// that after joining the cluster, the second two servers persist the
	// new version (and not the old one).
	versions := [][2]string{{oldVersion.String(), newVersion.String()}, {oldVersion.String(), newVersion.String()}, {oldVersion.String(), newVersion.String()}}

	bootstrapVersion := cluster.ClusterVersion{
		UseVersion:     newVersion,
		MinimumVersion: newVersion,
	}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
		Upgrade: &server.UpgradeTestingKnobs{
			DisableUpgrade: 1,
		},
	}

	ctx := context.Background()
	dir, finish := testutils.TempDir(t)
	defer finish()
	tc := setupMixedCluster(t, knobs, versions, dir)
	defer tc.TestCluster.Stopper().Stop(ctx)

	for i := 0; i < len(tc.TestCluster.Servers); i++ {
		testutils.SucceedsSoon(t, func() error {
			for _, engine := range tc.TestCluster.Servers[i].Engines() {
				cv, err := storage.ReadClusterVersion(ctx, engine)
				if err != nil {
					t.Fatal(err)
				}
				if cv.MinimumVersion != newVersion {
					return errors.Errorf("n%d: expected version %v, got %v", i+1, newVersion, cv)
				}
			}
			return nil
		})
	}
}

func TestClusterVersionUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	dir, finish := testutils.TempDir(t)
	defer finish()

	var newVersion = cluster.BinaryServerVersion
	var oldVersion = prev(newVersion)

	// Starts 3 nodes that have cluster versions set to be oldVersion and
	// self-declared binary version set to be newVersion. Expect cluster
	// version to upgrade automatically from oldVersion to newVersion.
	versions := [][2]string{{oldVersion.String(), newVersion.String()}, {oldVersion.String(), newVersion.String()}, {oldVersion.String(), newVersion.String()}}

	bootstrapVersion := cluster.ClusterVersion{
		UseVersion:     oldVersion,
		MinimumVersion: oldVersion,
	}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
		Upgrade: &server.UpgradeTestingKnobs{
			DisableUpgrade: 1,
		},
	}
	tc := setupMixedCluster(t, knobs, versions, dir)
	defer tc.TestCluster.Stopper().Stop(ctx)

	// Set CLUSTER SETTING cluster.preserve_downgrade_option to oldVersion to prevent upgrade.
	if err := tc.setDowngrade(0, oldVersion.String()); err != nil {
		t.Fatalf("error setting CLUSTER SETTING cluster.preserve_downgrade_option: %s", err)
	}
	atomic.StoreInt32(&knobs.Upgrade.(*server.UpgradeTestingKnobs).DisableUpgrade, 0)

	// Check the cluster version is still oldVersion.
	curVersion := tc.getVersionFromSelect(0)
	if curVersion != oldVersion.String() {
		t.Fatalf("cluster version should still be %s, but get %s", oldVersion, curVersion)
	}

	// Reset cluster.preserve_downgrade_option to enable auto upgrade.
	if err := tc.resetDowngrade(0); err != nil {
		t.Fatalf("error resetting CLUSTER SETTING cluster.preserve_downgrade_option: %s", err)
	}

	// Check the cluster version is bumped to newVersion.
	testutils.SucceedsSoon(t, func() error {
		if version := tc.getVersionFromSelect(0); version != newVersion.String() {
			return errors.Errorf("cluster version is still %s, should be %s", oldVersion, newVersion)
		}
		return nil
	})
	curVersion = tc.getVersionFromSelect(0)
	isNoopUpdate := curVersion == newVersion.String()

	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			v := tc.getVersionFromSetting(i)
			wantActive := isNoopUpdate
			if isActive := v.Version().IsActiveVersion(newVersion); isActive != wantActive {
				return errors.Errorf("%d: v%s active=%t (wanted %t)", i, newVersion, isActive, wantActive)
			}

			if tableV, curV := tc.getVersionFromSelect(i), v.Version().MinimumVersion.String(); tableV != curV {
				return errors.Errorf("%d: read v%s from table, v%s from setting", i, tableV, curV)
			}
		}
		return nil
	})

	exp := newVersion.String()

	// Read the versions from the table from each node. Note that under the
	// hood, everything goes to the lease holder and so it's pretty much
	// guaranteed that they all read the same, but it doesn't hurt to check.
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			if version := tc.getVersionFromSelect(i); version != exp {
				return errors.Errorf("%d: incorrect version %q (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromShow(i); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
		}
		return nil
	})

	// Now check the Settings.Version variable. That is the tricky one for which
	// we "hold back" a gossip update until we've written to the engines. We may
	// have to wait a bit until we see the new version here, even though it's
	// already in the table.
	testutils.SucceedsSoon(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			vers := tc.getVersionFromSetting(i)
			if v := vers.Version().MinimumVersion.String(); v == curVersion {
				if isNoopUpdate {
					continue
				}
				return errors.Errorf("%d: still waiting for %s (now at %s)", i, exp, v)
			} else if v != exp {
				t.Fatalf("%d: should never see version %s (wanted %s)", i, v, exp)
			}
		}
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

func TestClusterVersionBootstrapStrict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Four nodes that are all strictly version X without accepting anything else.
	for _, versions := range [][][2]string{
		{{"1.1", "1.1"}, {"1.1", "1.1"}, {"1.1", "1.1"}, {"1.1", "1.1"}},
		{{"4.7", "4.7"}, {"4.7", "4.7"}, {"4.7", "4.7"}, {"4.7", "4.7"}},
	} {
		func() {
			bootstrapVersion := cluster.ClusterVersion{
				UseVersion:     roachpb.MustParseVersion(versions[0][0]),
				MinimumVersion: roachpb.MustParseVersion(versions[0][0]),
			}

			knobs := base.TestingKnobs{
				Store: &storage.StoreTestingKnobs{
					BootstrapVersion: &bootstrapVersion,
				},
			}
			tc := setupMixedCluster(t, knobs, versions, "")
			defer tc.Stopper().Stop(ctx)

			exp := versions[0][0]

			for i := 0; i < tc.NumServers(); i++ {
				if version := tc.getVersionFromSetting(i).Version().MinimumVersion.String(); version != exp {
					t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
				}
				if version := tc.getVersionFromShow(i); version != exp {
					t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
				}

				if version := tc.getVersionFromSelect(i); version != exp {
					t.Fatalf("%d: incorrect version %q (wanted %s)", i, version, exp)
				}
			}
		}()
	}
}

func TestClusterVersionMixedVersionTooOld(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Prevent node crashes from generating several megabytes of stacks when
	// GOTRACEBACK=all, as it is on CI.
	defer log.DisableTracebacks()()

	exits := make(chan int, 100)

	log.SetExitFunc(true /* hideStack */, func(i int) { exits <- i })
	defer log.ResetExitFunc()

	// Three nodes at v1.1 and a fourth one at 1.0, but all operating at v1.0.
	versions := [][2]string{{"1.0", "1.1"}, {"1.0", "1.1"}, {"1.0", "1.1"}, {"1.0", "1.0"}}

	// Start by running 1.0.
	bootstrapVersion := cluster.ClusterVersion{
		UseVersion:     cluster.VersionByKey(cluster.VersionBase),
		MinimumVersion: cluster.VersionByKey(cluster.VersionBase),
	}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
	}
	tc := setupMixedCluster(t, knobs, versions, "")
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
		testutils.SucceedsSoon(tc, func() error {
			if version := tc.getVersionFromSetting(i).Version().MinimumVersion.String(); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromShow(i); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			return nil
		})
	}
}

func TestClusterVersionMixedVersionTooNew(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	// Prevent node crashes from generating several megabytes of stacks when
	// GOTRACEBACK=all, as it is on CI.
	defer log.DisableTracebacks()()

	exits := make(chan int, 100)

	log.SetExitFunc(true /* hideStack */, func(i int) { exits <- i })
	defer log.ResetExitFunc()

	// Three nodes at v1.1 and a fourth one (started later) at 1.1-2 (and
	// incompatible with anything earlier).
	versions := [][2]string{{"1.1", "1.1"}, {"1.1", "1.1"}, {"1.1", "1.1"}}

	// Try running 1.1.
	bootstrapVersion := cluster.ClusterVersion{
		UseVersion:     roachpb.Version{Major: 1, Minor: 1},
		MinimumVersion: roachpb.Version{Major: 1, Minor: 1},
	}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
	}
	tc := setupMixedCluster(t, knobs, versions, "")
	defer tc.Stopper().Stop(ctx)

	tc.AddServer(t, base.TestServerArgs{
		Settings: cluster.MakeClusterSettings(
			roachpb.Version{Major: 1, Minor: 1, Unstable: 2},
			roachpb.Version{Major: 1, Minor: 1, Unstable: 2}),
	})

	// TODO(tschottdorf): the cluster remains running even though we're running
	// an illegal combination of versions. The root cause is that nothing
	// populates the version setting table entry, and so each node implicitly
	// assumes its own version. We also use versions prior to 1.1-5 to avoid
	// the version compatibility check in the RPC heartbeat.
	//
	// TODO(tschottdorf): validate something about the on-disk contents of the
	// nodes at this point.
	exp := "1.1"

	// Write the de facto cluster version (v1.1) into the table. Note that we
	// can do this from the node running 1.1-2 (it could be prevented, but doesn't
	// seem too interesting).
	if err := tc.setVersion(3, exp); err != nil {
		t.Fatal(err)
	}

	<-exits // wait for fourth node to die

	// Check that we can still talk to the first three nodes.
	for i := 0; i < tc.NumServers()-1; i++ {
		testutils.SucceedsSoon(tc, func() error {
			if version := tc.getVersionFromSetting(i).Version().MinimumVersion.String(); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromShow(i); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			return nil
		})
	}
}
