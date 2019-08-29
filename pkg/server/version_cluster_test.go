// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server_test

import (
	"context"
	gosql "database/sql"
	"fmt"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
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
		if err == gosql.ErrNoRows {
			return ""
		}
		th.Fatalf("%d: %s (%T)", i, err, err)
	}
	var v cluster.ClusterVersion
	if err := protoutil.Unmarshal([]byte(version), &v); err != nil {
		th.Fatalf("%d: %s", i, err)
	}
	return v.Version.String()
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

	bootstrapVersion := cluster.ClusterVersion{Version: newVersion}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: 1,
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
				if cv.Version != newVersion {
					return errors.Errorf("n%d: expected version %v, got %v", i+1, newVersion, cv)
				}
			}
			return nil
		})
	}
}

// TestClusterVersionUnreplicatedRaftTruncatedState exercises the
// VersionUnreplicatedRaftTruncatedState migration in as much detail as possible
// in a unit test.
//
// It starts a four node cluster with a pre-migration version and upgrades into
// the new version while traffic and scattering are active, verifying that the
// truncated states are rewritten.
func TestClusterVersionUnreplicatedRaftTruncatedState(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	dir, finish := testutils.TempDir(t)
	defer finish()

	// NB: this test can be removed when that version is always-on.
	oldVersion := cluster.VersionByKey(cluster.VersionUnreplicatedRaftTruncatedState - 1)
	oldVersionS := oldVersion.String()
	newVersionS := cluster.VersionByKey(cluster.VersionUnreplicatedRaftTruncatedState).String()

	// Four node cluster in which all versions support newVersion (i.e. would in
	// principle upgrade to it) but are bootstrapped at oldVersion.
	versions := [][2]string{
		{oldVersionS, newVersionS},
		{oldVersionS, newVersionS},
		{oldVersionS, newVersionS},
		{oldVersionS, newVersionS},
	}

	bootstrapVersion := cluster.ClusterVersion{Version: oldVersion}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: 1,
		},
	}

	tc := setupMixedCluster(t, knobs, versions, dir)
	defer tc.TestCluster.Stopper().Stop(ctx)

	if _, err := tc.ServerConn(0).Exec(`
CREATE TABLE kv (id INT PRIMARY KEY, v INT);
ALTER TABLE kv SPLIT AT SELECT i FROM generate_series(1, 9) AS g(i);
`); err != nil {
		t.Fatal(err)
	}

	scatter := func() {
		t.Helper()
		if _, err := tc.ServerConn(0).Exec(
			`ALTER TABLE kv EXPERIMENTAL_RELOCATE SELECT ARRAY[i%$1+1], i FROM generate_series(0, 9) AS g(i)`, len(versions),
		); err != nil {
			t.Log(err)
		}
	}

	var n int
	insert := func() {
		t.Helper()
		n++
		// Write only to a subset of our ranges to guarantee log truncations there.
		_, err := tc.ServerConn(0).Exec(`UPSERT INTO kv VALUES($1, $2)`, n%2, n)
		if err != nil {
			t.Fatal(err)
		}
	}

	for i := 0; i < 500; i++ {
		insert()
	}
	scatter()

	for _, server := range tc.Servers {
		assert.NoError(t, server.GetStores().(*storage.Stores).VisitStores(func(s *storage.Store) error {
			s.VisitReplicas(func(r *storage.Replica) bool {
				key := keys.RaftTruncatedStateKey(r.RangeID)
				var truncState roachpb.RaftTruncatedState
				found, err := engine.MVCCGetProto(
					context.Background(), s.Engine(), key,
					hlc.Timestamp{}, &truncState, engine.MVCCGetOptions{},
				)
				if err != nil {
					t.Fatal(err)
				}
				if found {
					t.Errorf("unexpectedly found unreplicated TruncatedState at %s", key)
				}
				return true // want more
			})
			return nil
		}))
	}

	if v := tc.getVersionFromSelect(0); v != oldVersionS {
		t.Fatalf("running %s, wanted %s", v, oldVersionS)
	}

	assert.NoError(t, tc.setVersion(0, newVersionS))
	for i := 0; i < 500; i++ {
		insert()
	}
	scatter()

	for _, server := range tc.Servers {
		testutils.SucceedsSoon(t, func() error {
			err := server.GetStores().(*storage.Stores).VisitStores(func(s *storage.Store) error {
				// We scattered and so old copies of replicas may be laying around.
				// If we're not proactive about removing them, the test gets pretty
				// slow because those replicas aren't caught up any more.
				s.MustForceReplicaGCScanAndProcess()
				var err error
				s.VisitReplicas(func(r *storage.Replica) bool {
					snap := s.Engine().NewSnapshot()
					defer snap.Close()

					keyLegacy := keys.RaftTruncatedStateLegacyKey(r.RangeID)
					keyUnreplicated := keys.RaftTruncatedStateKey(r.RangeID)

					if found, innerErr := engine.MVCCGetProto(
						context.Background(), snap, keyLegacy,
						hlc.Timestamp{}, nil, engine.MVCCGetOptions{},
					); innerErr != nil {
						t.Fatal(innerErr)
					} else if found {
						if err == nil {
							err = errors.New("found legacy TruncatedState")
						}
						err = errors.Wrap(err, r.String())

						// Force a log truncation to prove that this rectifies
						// the situation.
						status := r.RaftStatus()
						if status != nil {
							desc := r.Desc()
							truncate := &roachpb.TruncateLogRequest{}
							truncate.Key = desc.StartKey.AsRawKey()
							truncate.RangeID = desc.RangeID
							truncate.Index = status.HardState.Commit
							var ba roachpb.BatchRequest
							ba.RangeID = r.RangeID
							ba.Add(truncate)
							if _, pErr := s.DB().NonTransactionalSender().Send(ctx, ba); pErr != nil {
								// The index may no longer be around if the system decided to truncate the
								// logs after we read it. Don't fail the test when that happens.
								if !testutils.IsPError(pErr, "requested entry at index is unavailable") {
									t.Fatal(err)
								}
							}
						}
						return true // want more
					}

					if found, err := engine.MVCCGetProto(
						context.Background(), snap, keyUnreplicated,
						hlc.Timestamp{}, nil, engine.MVCCGetOptions{},
					); err != nil {
						t.Fatal(err)
					} else if !found {
						// We can't have neither of the keys present.
						t.Fatalf("%s: unexpectedly did not find unreplicated TruncatedState at %s", r, keyUnreplicated)
					}

					return true // want more
				})
				return err
			})
			return err
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

	bootstrapVersion := cluster.ClusterVersion{Version: oldVersion}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: 1,
		},
	}
	tc := setupMixedCluster(t, knobs, versions, dir)
	defer tc.TestCluster.Stopper().Stop(ctx)

	{
		// Regression test for the fix for this issue:
		// https://github.com/cockroachdb/cockroach/pull/39640#pullrequestreview-275532068
		//
		// This can be removed when VersionLearnerReplicas is always-on.
		k := tc.ScratchRange(t)
		tc.AddReplicasOrFatal(t, k, tc.Target(2))
		_, err := tc.RemoveReplicas(k, tc.Target(2))
		require.NoError(t, err)
	}

	// Set CLUSTER SETTING cluster.preserve_downgrade_option to oldVersion to prevent upgrade.
	if err := tc.setDowngrade(0, oldVersion.String()); err != nil {
		t.Fatalf("error setting CLUSTER SETTING cluster.preserve_downgrade_option: %s", err)
	}
	atomic.StoreInt32(&knobs.Server.(*server.TestingKnobs).DisableAutomaticVersionUpgrade, 0)

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

			if tableV, curV := tc.getVersionFromSelect(i), v.Version().Version.String(); tableV != curV {
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
			if v := vers.Version().Version.String(); v == curVersion {
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
		cv, err := storage.ReadVersionFromEngineOrZero(ctx, s.Engine())
		if err != nil {
			return err
		}
		if act := cv.Version.String(); act != exp {
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
	ctx := context.Background()

	tcRaw := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{})
	defer tcRaw.Stopper().Stop(ctx)
	tc := testClusterWithHelpers{
		T:           t,
		TestCluster: tcRaw,
	}

	exp := cluster.BinaryServerVersion.String()

	// The node bootstrapping the cluster starts at BinaryServerVersion, the
	// others start at MinimumSupportedVersion and it takes them a gossip update
	// to get to BinaryServerVersion. Hence, we loop until that gossip comes.
	testutils.SucceedsSoon(tc, func() error {
		for i := 0; i < tc.NumServers(); i++ {
			if version := tc.getVersionFromSetting(i).Version().Version.String(); version != exp {
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

// Returns two versions v0 and v1 which correspond to adjacent releases. v1 will
// equal the MinSupportedVersion to avoid rot in tests using this (as we retire
// old versions).
func v0v1() (roachpb.Version, roachpb.Version) {
	v1 := cluster.BinaryMinimumSupportedVersion
	v0 := cluster.BinaryMinimumSupportedVersion
	if v0.Minor > 0 {
		v0.Minor--
	} else {
		v0.Major--
	}
	return v0, v1
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

	v0, v1 := v0v1()
	v0s := v0.String()
	v1s := v1.String()

	// Three nodes at v1.1 and a fourth one at 1.0, but all operating at v1.0.
	versions := [][2]string{{v0s, v1s}, {v0s, v1s}, {v0s, v1s}, {v0s, v0s}}

	// Start by running v1.
	bootstrapVersion := cluster.ClusterVersion{
		Version: v0,
	}

	knobs := base.TestingKnobs{
		Store: &storage.StoreTestingKnobs{
			BootstrapVersion: &bootstrapVersion,
		},
		Server: &server.TestingKnobs{
			DisableAutomaticVersionUpgrade: 1,
		},
	}
	tc := setupMixedCluster(t, knobs, versions, "")
	defer tc.Stopper().Stop(ctx)

	exp := v1s

	// The last node refuses to perform an upgrade that would risk its own life.
	if err := tc.setVersion(len(versions)-1, exp); !testutils.IsError(err,
		fmt.Sprintf("cannot upgrade to %s: node running %s", v1s, v0s),
	) {
		t.Fatal(err)
	}

	// The other nodes are less careful.
	tc.mustSetVersion(0, exp)

	<-exits // wait for fourth node to die

	// Check that we can still talk to the first three nodes.
	for i := 0; i < tc.NumServers()-1; i++ {
		testutils.SucceedsSoon(tc, func() error {
			if version := tc.getVersionFromSetting(i).Version().Version.String(); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			if version := tc.getVersionFromShow(i); version != exp {
				return errors.Errorf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
			return nil
		})
	}
}
