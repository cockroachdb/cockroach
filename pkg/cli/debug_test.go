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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
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

// TestDebugDecodeKeyEngineKeyPaths tests the new engine key decoding logic in debug.go
func TestDebugDecodeKeyEngineKeyPaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("lock table keys", func(t *testing.T) {
		// Create lock table keys and test they decode properly
		uuid1 := uuid.Must(uuid.FromString("6ba7b810-9dad-11d1-80b4-00c04fd430c8"))

		lockKeys := []storage.LockTableKey{
			{Key: roachpb.Key("test"), Strength: lock.Shared, TxnUUID: uuid1},
			{Key: roachpb.Key("foo"), Strength: lock.Exclusive, TxnUUID: uuid1},
			{Key: roachpb.Key("bar"), Strength: lock.Intent, TxnUUID: uuid1},
		}

		for i, lockKey := range lockKeys {
			t.Run(fmt.Sprintf("lock %d %s", i, lockKey.Strength), func(t *testing.T) {
				engineKey, _ := lockKey.ToEngineKey(nil)
				encoded := engineKey.Encode()
				hexKey := fmt.Sprintf("%x", encoded)

				out, err := TestCLI{}.RunWithCaptureArgs([]string{
					"debug", "decode-key", "--encoding", "hex", hexKey,
				})

				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// Verify the output contains lock strength and key
				if !strings.Contains(out, lockKey.Strength.String()) {
					t.Errorf("Expected output to contain lock strength %s, got: %s", lockKey.Strength, out)
				}
				if !strings.Contains(out, string(lockKey.Key)) {
					t.Errorf("Expected output to contain key %s, got: %s", lockKey.Key, out)
				}
			})
		}
	})

	t.Run("mvcc keys via engine key", func(t *testing.T) {
		// Test MVCC keys that are decoded via the EngineKey.IsMVCCKey() path
		mvccKey := storage.MVCCKey{
			Key:       roachpb.Key("mvcc_test"),
			Timestamp: hlc.Timestamp{WallTime: 123456},
		}

		encoded := storage.EncodeMVCCKey(mvccKey)
		hexKey := fmt.Sprintf("%x", encoded)

		out, err := TestCLI{}.RunWithCaptureArgs([]string{
			"debug", "decode-key", "--encoding", "hex", hexKey,
		})

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if !strings.Contains(out, "mvcc_test") {
			t.Errorf("Expected output to contain key content, got: %s", out)
		}
	})

	t.Run("engine key other types", func(t *testing.T) {
		// Test engine keys that are neither lock table nor MVCC keys
		// These should be printed as-is via the "else" branch
		// For now, we'll test with a simple case that should trigger this path

		// This will test the fallback path when DecodeEngineKey returns false
		invalidEngineKey := "ff"

		out, err := TestCLI{}.RunWithCaptureArgs([]string{
			"debug", "decode-key", "--encoding", "hex", invalidEngineKey,
		})

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should trigger the MVCC fallback path and show an error
		if !strings.Contains(out, "ERROR") {
			t.Errorf("Expected output to contain error from MVCC fallback, got: %s", out)
		}
	})

	t.Run("backwards compatibility fallback", func(t *testing.T) {
		// Test the fallback path: when DecodeEngineKey fails,
		// it should fall back to DecodeMVCCKey for backwards compatibility

		// Use the same test case from existing tests that should trigger fallback
		invalidKey := "jg==" // base64 encoded "8e"

		out, err := TestCLI{}.RunWithCaptureArgs([]string{
			"debug", "decode-key", "--encoding", "base64", invalidKey,
		})

		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Should contain error message from MVCC key decoding fallback
		if !strings.Contains(out, "ERROR: invalid encoded mvcc key") {
			t.Errorf("Expected fallback to MVCC decoding with error message, got: %s", out)
		}
	})
}
