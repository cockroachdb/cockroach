// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage_test

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storageconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

// Minimal test that creates an encrypted Pebble that exercises creation and
// reading of encrypted files, rereading data after reopening the engine, and
// stats code.
func TestPebbleEncryption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const stickyVFSID = `foo`

	ctx := context.Background()
	stickyRegistry := fs.NewStickyRegistry()
	keyFile128 := "111111111111111111111111111111111234567890123456"
	writeToFile(t, stickyRegistry.Get(stickyVFSID), "16.key", []byte(keyFile128))

	encOptions := &storageconfig.EncryptionOptions{
		KeySource: storageconfig.EncryptionKeyFromFiles,
		KeyFiles: &storageconfig.EncryptionKeyFiles{
			CurrentKey: "16.key",
			OldKey:     "plain",
		},
		RotationPeriod: time.Hour,
	}

	func() {
		// Initialize the filesystem env.
		settings := cluster.MakeTestingClusterSettings()
		env, err := fs.InitEnvFromStoreSpec(
			ctx,
			base.StoreSpec{
				InMemory:          true,
				Size:              storageconfig.BytesSize(512 << 20),
				EncryptionOptions: encOptions,
				StickyVFSID:       stickyVFSID,
			},
			fs.EnvConfig{
				RW:      fs.ReadWrite,
				Version: settings.Version,
			},
			stickyRegistry, /* sticky registry */
			nil,            /* statsCollector */
		)
		require.NoError(t, err)
		db, err := storage.Open(ctx, env, settings)
		require.NoError(t, err)
		defer db.Close()

		// TODO(sbhola): Ensure that we are not returning the secret data keys by mistake.
		r, err := db.GetEncryptionRegistries()
		require.NoError(t, err)

		var fileRegistry enginepb.FileRegistry
		require.NoError(t, protoutil.Unmarshal(r.FileRegistry, &fileRegistry))
		var keyRegistry enginepb.DataKeysRegistry
		require.NoError(t, protoutil.Unmarshal(r.KeyRegistry, &keyRegistry))

		stats, err := db.GetEnvStats()
		require.NoError(t, err)
		// Opening the DB should've created OPTIONS, MANIFEST, and the WAL.
		require.GreaterOrEqual(t, stats.TotalFiles, uint64(3))
		// We also created markers for the format version and the manifest.
		require.Equal(t, uint64(5), stats.ActiveKeyFiles)
		var s enginepb.EncryptionStatus
		require.NoError(t, protoutil.Unmarshal(stats.EncryptionStatus, &s))
		require.Equal(t, "16.key", s.ActiveStoreKey.Source)
		require.Equal(t, int32(enginepb.EncryptionType_AES128_CTR), stats.EncryptionType)
		t.Logf("EnvStats:\n%+v\n\n", *stats)

		batch := db.NewWriteBatch()
		defer batch.Close()
		require.NoError(t, batch.PutUnversioned(roachpb.Key("a"), []byte("a")))
		require.NoError(t, batch.Commit(true))
		require.NoError(t, db.Flush())
		require.Equal(t, []byte("a"), storageutils.MVCCGetRaw(t, db, storageutils.PointKey(keys.SystemSQLCodec, "a", 0)))
	}()

	func() {
		// Initialize the filesystem env again, replaying the file registries.
		settings := cluster.MakeTestingClusterSettings()
		env, err := fs.InitEnvFromStoreSpec(
			ctx,
			base.StoreSpec{
				InMemory:          true,
				Size:              storageconfig.BytesSize(512 << 20),
				EncryptionOptions: encOptions,
				StickyVFSID:       stickyVFSID,
			},
			fs.EnvConfig{
				RW:      fs.ReadWrite,
				Version: settings.Version,
			},
			stickyRegistry, /* sticky registry */
			nil,            /* statsCollector */
		)
		require.NoError(t, err)
		db, err := storage.Open(ctx, env, settings)
		require.NoError(t, err)
		defer db.Close()
		require.Equal(t, []byte("a"), storageutils.MVCCGetRaw(t, db, storageutils.PointKey(keys.SystemSQLCodec, "a", 0)))

		// Flushing should've created a new sstable under the active key.
		stats, err := db.GetEnvStats()
		require.NoError(t, err)
		t.Logf("EnvStats:\n%+v\n\n", *stats)
		require.GreaterOrEqual(t, stats.TotalFiles, uint64(5))
		require.LessOrEqual(t, uint64(5), stats.ActiveKeyFiles)
		require.Equal(t, stats.TotalBytes, stats.ActiveKeyBytes)
	}()
}

func TestPebbleEncryption2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const stickyVFSID = `foo`
	stickyRegistry := fs.NewStickyRegistry()

	memFS := stickyRegistry.Get(stickyVFSID)
	firstKeyFile128 := "111111111111111111111111111111111234567890123456"
	secondKeyFile128 := "111111111111111111111111111111198765432198765432"
	writeToFile(t, memFS, "16v1.key", []byte(firstKeyFile128))
	writeToFile(t, memFS, "16v2.key", []byte(secondKeyFile128))

	keys := make(map[string]bool)
	validateKeys := func(reader storage.Reader) bool {
		keysCopy := make(map[string]bool)
		for k, v := range keys {
			keysCopy[k] = v
		}

		foundUnknown := false
		kvFunc := func(kv roachpb.KeyValue) error {
			key := kv.Key
			val := kv.Value
			expected := keysCopy[string(key)]
			if !expected || len(val.RawBytes) == 0 {
				foundUnknown = true
				return nil
			}
			delete(keysCopy, string(key))
			return nil
		}

		_, err := storage.MVCCIterate(
			context.Background(),
			reader,
			nil,
			roachpb.KeyMax,
			hlc.Timestamp{},
			storage.MVCCScanOptions{},
			kvFunc,
		)
		require.NoError(t, err)
		return len(keysCopy) == 0 && !foundUnknown
	}

	addKeyAndValidate := func(
		key string, val string, encKeyFile string, oldEncFileKey string,
	) {
		encOptions := &storageconfig.EncryptionOptions{
			KeySource: storageconfig.EncryptionKeyFromFiles,
			KeyFiles: &storageconfig.EncryptionKeyFiles{
				CurrentKey: encKeyFile,
				OldKey:     oldEncFileKey,
			},
			RotationPeriod: time.Hour,
		}

		// Initialize the filesystem env.
		ctx := context.Background()
		settings := cluster.MakeTestingClusterSettings()
		env, err := fs.InitEnvFromStoreSpec(
			ctx,
			base.StoreSpec{
				InMemory:          true,
				Size:              storageconfig.BytesSize(512 << 20),
				EncryptionOptions: encOptions,
				StickyVFSID:       stickyVFSID,
			},
			fs.EnvConfig{
				RW:      fs.ReadWrite,
				Version: settings.Version,
			},
			stickyRegistry, /* sticky registry */
			nil,            /* statsCollector */
		)
		require.NoError(t, err)
		db, err := storage.Open(ctx, env, settings)
		require.NoError(t, err)
		defer db.Close()

		require.True(t, validateKeys(db))

		keys[key] = true
		_, err = storage.MVCCPut(
			context.Background(),
			db,
			roachpb.Key(key),
			hlc.Timestamp{},
			roachpb.MakeValueFromBytes([]byte(val)),
			storage.MVCCWriteOptions{},
		)
		require.NoError(t, err)
		require.NoError(t, db.Flush())
		require.NoError(t, db.Compact(context.Background()))
		require.True(t, validateKeys(db))
		db.Close()
	}

	addKeyAndValidate("a", "a", "16v1.key", "plain")
	addKeyAndValidate("b", "b", "plain", "16v1.key")
	addKeyAndValidate("c", "c", "16v2.key", "plain")
	addKeyAndValidate("d", "d", "plain", "16v2.key")
}

func writeToFile(t *testing.T, vfs vfs.FS, filename string, b []byte) {
	f, err := vfs.Create(filename, fs.UnspecifiedWriteCategory)
	require.NoError(t, err)
	breader := bytes.NewReader(b)
	_, err = io.Copy(f, breader)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
}
