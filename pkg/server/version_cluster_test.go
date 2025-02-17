// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/upgrade"
	"github.com/cockroachdb/cockroach/pkg/upgrade/upgradebase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
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
		if errors.Is(err, gosql.ErrNoRows) {
			return ""
		}
		th.Fatalf("%d: %s (%T)", i, err, err)
	}
	var v clusterversion.ClusterVersion
	if err := protoutil.Unmarshal([]byte(version), &v); err != nil {
		th.Fatalf("%d: %s", i, err)
	}
	return v.Version.String()
}

func (th *testClusterWithHelpers) setVersion(i int, version string) error {
	_, err := th.ServerConn(i).Exec("SET CLUSTER SETTING version = $1", version)
	return err
}

func (th *testClusterWithHelpers) setDowngrade(i int, version string) error {
	_, err := th.ServerConn(i).Exec("SET CLUSTER SETTING cluster.preserve_downgrade_option = $1", version)
	return err
}

func (th *testClusterWithHelpers) resetDowngrade(i int) error {
	_, err := th.ServerConn(i).Exec("RESET CLUSTER SETTING cluster.preserve_downgrade_option")
	return err
}

// Set up a mixed cluster with the following setup:
// - len(versions) servers
// - server[i] runs at binary version `versions[i][0]`
// - server[i] runs with minimum supported version `versions[i][1]`
// A directory can optionally be passed in.
func setupMixedCluster(
	t *testing.T, knobs base.TestingKnobs, versions [][2]string, dir string,
) testClusterWithHelpers {

	twh := testClusterWithHelpers{
		T: t,
		args: func() map[int]base.TestServerArgs {
			serverArgsPerNode := map[int]base.TestServerArgs{}
			for i, v := range versions {
				v0, v1 := roachpb.MustParseVersion(v[0]), roachpb.MustParseVersion(v[1])
				st := cluster.MakeTestingClusterSettingsWithVersions(v0, v1, false /* initializeVersion */)
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
	// a default upgrade) causes leaktest to complain.
	if _, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING diagnostics.reporting.enabled = 'false'"); err != nil {
		t.Fatal(err)
	}

	twh.TestCluster = tc
	return twh
}

// Prev returns the previous version of the given version.
// eg. prev(20.1) = 19.2, prev(19.2) = 19.1, prev(19.1) = 2.1,
// prev(2.0) = 1.0, prev(2.1) == 2.0, prev(2.1-5) == 2.1.
func prev(version roachpb.Version) roachpb.Version {
	if version.Internal != 0 {
		return roachpb.Version{Major: version.Major, Minor: version.Minor}
	}

	v19_1 := roachpb.Version{Major: 19, Minor: 1}

	if v19_1.Less(version) {
		if version.Minor > 1 {
			return roachpb.Version{Major: version.Major, Minor: version.Minor - 1}
		}

		// version is the first release of that year, e.g. MM.1

		v25_1 := roachpb.Version{Major: 25, Minor: 1}
		if v25_1.Equal(version) {
			// For 2024, we had 3 releases that year.
			return roachpb.Version{Major: 24, Minor: 3}
		}
		if v25_1.Less(version) {
			// 2025 onwards, we had 4 releases per year.
			return roachpb.Version{Major: version.Major - 1, Minor: 4}
		}

		// Prior to 2024, we had 2 releases per year.
		return roachpb.Version{Major: version.Major - 1, Minor: 2}
	}

	if version == v19_1 {
		return roachpb.Version{Major: 2, Minor: 1}
	}

	// Logic for versions below 19.1.

	if version.Major > 2 {
		log.Fatalf(context.Background(), "can't compute previous version for %s", version)
	}

	if version.Minor != 0 {
		return roachpb.Version{Major: version.Major}
	} else {
		// version will be at least 2.0-X, so it's safe to set new Major to be version.Major-1.
		return roachpb.Version{Major: version.Major - 1}
	}
}

func TestPrev(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		this, prev roachpb.Version
	}{
		// releases 2025+: 4 releases per year.
		{
			this: roachpb.Version{Major: 28, Minor: 4},
			prev: roachpb.Version{Major: 28, Minor: 3},
		},
		{
			this: roachpb.Version{Major: 28, Minor: 1},
			prev: roachpb.Version{Major: 27, Minor: 4},
		},
		{
			this: roachpb.Version{Major: 26, Minor: 1},
			prev: roachpb.Version{Major: 25, Minor: 4},
		},
		// releases 2024: 3 releases per year.
		{
			this: roachpb.Version{Major: 25, Minor: 1},
			prev: roachpb.Version{Major: 24, Minor: 3},
		},
		// releases 2019-2023: Calendar versioning, with 2 releases per year
		{
			this: roachpb.Version{Major: 24, Minor: 1},
			prev: roachpb.Version{Major: 23, Minor: 2},
		},
		{
			this: roachpb.Version{Major: 21, Minor: 1},
			prev: roachpb.Version{Major: 20, Minor: 2},
		},
	}
	for _, test := range tests {
		t.Run("", func(t *testing.T) {
			if p := prev(test.this); p != test.prev {
				t.Errorf("expected %s, got %s", test.prev, p)
			}
		})
	}
}

func TestClusterVersionPersistedOnJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var newVersion = clusterversion.Latest.Version()
	var oldVersion = prev(newVersion)

	// Starts 3 nodes that have cluster versions set to be oldVersion and
	// self-declared binary version set to be newVersion with a cluster
	// running at the new version (i.e. a very regular setup). Want to check
	// that after joining the cluster, the second two servers persist the
	// new version (and not the old one).
	versions := [][2]string{
		{newVersion.String(), oldVersion.String()},
		{newVersion.String(), oldVersion.String()},
		{newVersion.String(), oldVersion.String()},
	}

	knobs := base.TestingKnobs{
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
		},
	}

	ctx := context.Background()
	dir, finish := testutils.TempDir(t)
	defer finish()
	tc := setupMixedCluster(t, knobs, versions, dir)
	defer tc.TestCluster.Stopper().Stop(ctx)

	for i := 0; i < len(tc.TestCluster.Servers); i++ {
		for _, engine := range tc.TestCluster.Servers[i].Engines() {
			cv := engine.MinVersion()
			if cv != newVersion {
				t.Fatalf("n%d: expected version %v, got %v", i+1, newVersion, cv)
			}
		}
	}
}

func TestClusterVersionUpgrade(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t, "test takes minutes")
	skip.UnderRace(t, "takes >5mn under race")
	skip.UnderStress(t, "takes >3mn under stress")

	ctx := context.Background()

	var newVersion = clusterversion.Latest.Version()
	var oldVersion = prev(newVersion)

	disableUpgradeCh := make(chan struct{})
	rawTC := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ReplicationMode: base.ReplicationManual, // speeds up test
		ServerArgs: base.TestServerArgs{
			Knobs: base.TestingKnobs{
				Server: &server.TestingKnobs{
					ClusterVersionOverride:         oldVersion,
					DisableAutomaticVersionUpgrade: disableUpgradeCh,
				},
			},
		},
	})
	defer rawTC.Stopper().Stop(ctx)
	tc := testClusterWithHelpers{
		T:           t,
		TestCluster: rawTC,
	}

	{
		// Regression test for the fix for this issue:
		// https://github.com/cockroachdb/cockroach/pull/39640#pullrequestreview-275532068
		//
		// This can be removed when VersionLearnerReplicas is always-on.
		k := tc.ScratchRange(t)
		tc.AddVotersOrFatal(t, k, tc.Target(2))
		_, err := tc.RemoveVoters(k, tc.Target(2))
		require.NoError(t, err)
	}

	// Set CLUSTER SETTING cluster.preserve_downgrade_option to oldVersion to prevent upgrade.
	if err := tc.setDowngrade(0, oldVersion.String()); err != nil {
		t.Fatalf("error setting CLUSTER SETTING cluster.preserve_downgrade_option: %s", err)
	}
	close(disableUpgradeCh)

	// Check the cluster version is still oldVersion.
	curVersion := tc.getVersionFromSelect(0)
	if curVersion != oldVersion.String() {
		t.Fatalf("cluster version should still be %s, but got %s", oldVersion, curVersion)
	}

	// Reset cluster.preserve_downgrade_option to enable auto upgrade.
	if err := tc.resetDowngrade(0); err != nil {
		t.Fatalf("error resetting CLUSTER SETTING cluster.preserve_downgrade_option: %s", err)
	}

	// Check the cluster version is bumped to newVersion.
	testutils.SucceedsWithin(t, func() error {
		if version := tc.getVersionFromSelect(0); version != newVersion.String() {
			return errors.Errorf("cluster version is still %s, should be %s", version, newVersion)
		}
		return nil
	}, 3*time.Minute)
	curVersion = tc.getVersionFromSelect(0)
	isNoopUpdate := curVersion == newVersion.String()

	testutils.SucceedsWithin(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			st := tc.Servers[i].ClusterSettings()
			v := st.Version.ActiveVersion(ctx)
			wantActive := isNoopUpdate
			if isActive := v.IsActiveVersion(newVersion); isActive != wantActive {
				return errors.Errorf("%d: v%s active=%t (wanted %t)", i, newVersion, isActive, wantActive)
			}

			if tableV, curV := tc.getVersionFromSelect(i), v.String(); tableV != curV {
				return errors.Errorf("%d: read v%s from table, v%s from setting", i, tableV, curV)
			}
		}
		return nil
	}, 3*time.Minute)

	exp := newVersion.String()

	// Read the versions from the table from each node. Note that under the
	// hood, everything goes to the lease holder and so it's pretty much
	// guaranteed that they all read the same, but it doesn't hurt to check.
	testutils.SucceedsWithin(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			if version := tc.getVersionFromSelect(i); version != exp {
				return errors.Errorf("%d: incorrect version %q (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromShow(i); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
		}
		return nil
	}, 3*time.Minute)

	// Now check the Settings.Version variable. That is the tricky one for which
	// we "hold back" a gossip update until we've written to the engines. We may
	// have to wait a bit until we see the new version here, even though it's
	// already in the table.
	testutils.SucceedsWithin(t, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			vers := tc.Servers[i].ClusterSettings().Version.ActiveVersion(ctx)
			if v := vers.String(); v == curVersion {
				if isNoopUpdate {
					continue
				}
				return errors.Errorf("%d: still waiting for %s (now at %s)", i, exp, v)
			} else if v != exp {
				t.Fatalf("%d: should never see version %s (wanted %s)", i, v, exp)
			}
		}
		return nil
	}, 3*time.Minute)

	// Since the wrapped version setting exposes the new versions, it must
	// definitely be present on all stores on the first try.
	if err := tc.Servers[1].GetStores().(*kvserver.Stores).VisitStores(func(s *kvserver.Store) error {
		cv := s.TODOEngine().MinVersion()
		if act := cv.String(); act != exp {
			t.Fatalf("%s: %s persisted, but should be %s", s, act, exp)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

// Test that, after cluster bootstrap, the different ways of getting the cluster
// version all agree.
func TestAllVersionsAgree(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tcRaw := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tcRaw.Stopper().Stop(ctx)
	tc := testClusterWithHelpers{
		T:           t,
		TestCluster: tcRaw,
	}

	exp := clusterversion.Latest.String()

	// The node bootstrapping the cluster starts at TestingBinaryVersion, the
	// others start at TestingMinimumSupportedVersion and it takes them a gossip
	// update to get to TestingBinaryVersion. Hence, we loop until that gossip
	// comes.
	testutils.SucceedsSoon(tc, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			if version := tc.Servers[i].ClusterSettings().Version.ActiveVersion(ctx); version.String() != exp {
				return fmt.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromShow(i); version != exp {
				return fmt.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromSelect(i); version != exp {
				return fmt.Errorf("%d: incorrect version %q (wanted %s)", i, version, exp)
			}
		}
		return nil
	})
}

// TestClusterVersionMixedVersionTooOld verifies that we're unable to bump a
// cluster version in a mixed node cluster where one of the nodes is running a
// binary that cannot support the targeted cluster version.
func TestClusterVersionMixedVersionTooOld(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	// Prevent node crashes from generating several megabytes of stacks when
	// GOTRACEBACK=all, as it is on CI.
	defer log.DisableTracebacks()()

	v0 := clusterversion.MinSupported.Version()
	v1 := clusterversion.Latest.Version()
	v0s := v0.String()
	v1s := v1.String()

	// Three nodes at v1 and a fourth one at v0, but all operating at v0.
	versions := [][2]string{
		{v1s, v0s},
		{v1s, v0s},
		{v1s, v0s},
		{v0s, v0s},
	}

	// Start by running v0.
	knobs := base.TestingKnobs{
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: make(chan struct{}),
			ClusterVersionOverride:         v0,
		},
		// Inject an upgrade which would run to upgrade the cluster.
		// We'll validate that we never create a job for this upgrade.
		UpgradeManager: &upgradebase.TestingKnobs{
			ListBetweenOverride: func(from, to roachpb.Version) []roachpb.Version {
				return []roachpb.Version{to}
			},
			RegistryOverride: func(cv roachpb.Version) (upgradebase.Upgrade, bool) {
				if !cv.Equal(v1) {
					return nil, false
				}
				return upgrade.NewTenantUpgrade("testing",
					v1,
					upgrade.NoPrecondition,
					func(ctx context.Context, version clusterversion.ClusterVersion, deps upgrade.TenantDeps) error {
						return nil
					},
					upgrade.RestoreActionNotRequired("test"),
				), true
			},
		},
	}
	tc := setupMixedCluster(t, knobs, versions, "")
	defer tc.Stopper().Stop(ctx)

	// The last node refuses to perform an upgrade that would risk its own life.
	if err := tc.setVersion(len(versions)-1, v1s); !testutils.IsError(err,
		fmt.Sprintf("cannot upgrade to %s: node running %s", v1s, v0s),
	) {
		t.Fatal(err)
	}

	// The other nodes are just as careful.
	for i := 0; i < len(versions)-2; i++ {
		testutils.SucceedsSoon(t, func() error {
			err := tc.setVersion(i, v1s)
			if testutils.IsError(err, "required, but unavailable") {
				// Paper over transient unavailability errors. Because we're
				// setting the cluster version so soon after cluster startup,
				// it's possible that we're doing so before all the nodes have
				// had a chance to heartbeat their liveness records for the very
				// first time. To other nodes it appears that the node in
				// question is unavailable.
				return err
			}

			if !testutils.IsError(err, fmt.Sprintf("binary version %s less than target cluster version", v0s)) {
				t.Error(i, err)
			}
			return nil
		})
	}

	// Ensure that no upgrade jobs got created.
	{
		sqlutils.MakeSQLRunner(tc.ServerConn(0)).CheckQueryResults(
			t, "SELECT * FROM crdb_internal.jobs WHERE job_type = 'upgrade'", [][]string{})
	}

	// Check that we can still talk to the first three nodes.
	for i := 0; i < tc.NumServers()-1; i++ {
		testutils.SucceedsSoon(tc, func() error {
			if version := tc.Servers[i].ClusterSettings().Version.ActiveVersion(ctx).String(); version != v0s {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, v0s)
			}
			if version := tc.getVersionFromShow(i); version != v0s {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, v0s)
			}
			return nil
		})
	}
}
