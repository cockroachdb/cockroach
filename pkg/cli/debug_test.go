// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/gossip"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func createStore(t *testing.T, path string) {
	t.Helper()
	db, err := storage.Open(
		context.Background(),
		fs.MustInitPhysicalTestingEnv(path),
		cluster.MakeClusterSettingsWithVersions(
			clusterversion.Latest.Version(),
			// We use PreviousRelease so that we don't have the enable version
			// skipping when running tests against this store.
			clusterversion.PreviousRelease.Version(),
		),
		storage.CacheSize(server.DefaultCacheSize))
	if err != nil {
		t.Fatal(err)
	}
	db.Close()
}

func TestOpenReadOnlyStore(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	stopper := stop.NewStopper()
	defer stopper.Stop(context.Background())

	baseDir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	storePath := filepath.Join(baseDir, "store")
	createStore(t, storePath)

	for _, test := range []struct {
		rw     fs.RWMode
		expErr string
	}{
		{
			rw:     fs.ReadWrite,
			expErr: "",
		},
		{
			rw:     fs.ReadOnly,
			expErr: `Not supported operation in read only mode|pebble: read-only`,
		},
	} {
		t.Run(fmt.Sprintf("readOnly=%t", test.rw == fs.ReadOnly), func(t *testing.T) {
			db, err := OpenEngine(storePath, stopper, test.rw)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()

			key := roachpb.Key("key")
			val := []byte("value")
			err = db.PutUnversioned(key, val)
			if !testutils.IsError(err, test.expErr) {
				t.Fatalf("wanted %s but got %v", test.expErr, err)
			}
		})
	}
}

func TestParseGossipValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	tc := testcluster.StartTestCluster(t, 3, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
		},
	})
	defer tc.Stopper().Stop(ctx)

	sys := tc.Server(0).SystemLayer()

	var gossipInfo gossip.InfoStatus
	if err := serverutils.GetJSONProto(sys, "/_status/gossip/1", &gossipInfo); err != nil {
		t.Fatal(err)
	}

	debugOutput, err := parseGossipValues(&gossipInfo)
	if err != nil {
		t.Fatal(err)
	}
	debugLines := strings.Split(debugOutput, "\n")
	if len(debugLines) != len(gossipInfo.Infos) {
		var gossipInfoKeys []string
		for key := range gossipInfo.Infos {
			gossipInfoKeys = append(gossipInfoKeys, key)
		}
		sort.Strings(gossipInfoKeys)
		t.Errorf("`debug gossip-values` output contains %d entries, but the source gossip contains %d:\ndebug output:\n%v\n\ngossipInfos:\n%v",
			len(debugLines), len(gossipInfo.Infos), debugOutput, strings.Join(gossipInfoKeys, "\n"))
	}
}

func TestParsePositiveDuration(t *testing.T) {
	defer leaktest.AfterTest(t)()
	duration, _ := parsePositiveDuration("1h")
	if duration != time.Hour {
		t.Errorf("Expected %v, got %v", time.Hour, duration)
	}
	_, err := parsePositiveDuration("-5m")
	if err == nil {
		t.Errorf("Expected to fail parsing negative duration -5m")
	}
}
