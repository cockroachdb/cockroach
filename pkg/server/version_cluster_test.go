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
	gosql "database/sql"
	"fmt"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/pkg/errors"
)

func TestMixedVersionCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const nodeCount = 4
	bootstrapVersion := cluster.ClusterVersion{
		UseVersion:     cluster.VersionMajorOneMinorZero,
		MinimumVersion: cluster.VersionMajorOneMinorZero,
	}

	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &storage.StoreTestingKnobs{
				BootstrapVersion: &bootstrapVersion,
			},
		},
	}

	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, nodeCount, base.TestClusterArgs{
		ServerArgs: serverArgs,
	})
	defer tc.Stopper().Stop(ctx)

	var dbs []*gosql.DB

	getVersionFromTable := func(i int) string {
		var version string
		if err := dbs[i].QueryRow("SHOW CLUSTER SETTING version").Scan(&version); err != nil {
			t.Fatalf("%d: %s", i, err)
		}
		return version
	}

	getVersionFromSetting := func(i int) *cluster.ExposedClusterVersion {
		return &tc.Servers[i].ClusterSettings().Version
	}

	setVersion := func(i int, version string) {
		// TODO(tschottdorf):
		// https://github.com/cockroachdb/cockroach/issues/17563 required before
		// we can avoid Sprintf.
		if _, err := dbs[i].Exec(fmt.Sprintf("SET CLUSTER SETTING version = '%s'", version)); err != nil {
			t.Fatalf("%d: %s", i, err)
		}
	}

	// TODO(tschottdorf): once the migration in #17389 is in place, verify that
	// there's a suitable entry in the `system.settings` table for a freshly
	// bootstrapped cluster (at 1.1). It's acceptable that an 1.0 cluster (as
	// this one) doesn't write one.

	for i := 0; i < tc.NumServers(); i++ {
		dbs = append(dbs, tc.ServerConn(i))

		if exp, version := "1.0", getVersionFromTable(i); version != exp {
			t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
		}
	}

	for _, newVersion := range []roachpb.Version{
		bootstrapVersion.MinimumVersion, // v1.0
		{Major: 1, Unstable: 500},
		{Major: 1, Minor: 1},
	} {
		for i := 0; i < tc.NumServers(); i++ {
			v := getVersionFromSetting(i)
			wantActive := newVersion == bootstrapVersion.MinimumVersion
			if isActive := v.IsActive(newVersion); isActive != wantActive {
				t.Fatalf("%d: v%s active=%t (wanted %t)", i, newVersion, isActive, wantActive)
			}
		}

		exp := newVersion.String()
		setVersion(tc.NumServers()-1, exp)

		// Read the versions from the table from each node. Note that under the
		// hood, everything goes to the lease holder and so it's pretty much
		// guaranteed that they all read the same, but it doesn't hurt to check.
		for i := 0; i < tc.NumServers(); i++ {
			if version := getVersionFromTable(i); version != exp {
				t.Fatalf("%d: incorrect version %s (wanted %s)", i, version, exp)
			}
		}

		// Now check the Settings.Version variable. That is the tricky one for which
		// we "hold back" a gossip update until we've written to the engines. We may
		// have to wait a bit until we see the new version here, even though it's
		// already in the table.
		testutils.SucceedsSoon(t, func() error {
			for i := 0; i < tc.NumServers(); i++ {
				vers := getVersionFromSetting(i)
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
