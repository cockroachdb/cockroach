// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package engineccl

import (
	"bytes"
	"context"
	"io"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/ccl/baseccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/storageccl/engineccl/enginepbccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestEncryptedFS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()

	fileRegistry := &storage.PebbleFileRegistry{FS: memFS, DBDir: "/bar"}
	require.NoError(t, fileRegistry.Load())

	// Using a StoreKeyManager for the test since it is easy to create. Write a key for the
	// StoreKeyManager.
	var b []byte
	for i := 0; i < keyIDLength+16; i++ {
		b = append(b, 'a')
	}
	f, err := memFS.Create("keyfile")
	require.NoError(t, err)
	bReader := bytes.NewReader(b)
	_, err = io.Copy(f, bReader)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	keyManager := &StoreKeyManager{fs: memFS, activeKeyFilename: "keyfile", oldKeyFilename: "plain"}
	require.NoError(t, keyManager.Load(context.Background()))

	streamCreator := &FileCipherStreamCreator{keyManager: keyManager, envType: enginepb.EnvType_Store}

	fs := &encryptedFS{FS: memFS, fileRegistry: fileRegistry, streamCreator: streamCreator}

	// Style (and most code) is from Pebble's mem_fs_test.go. We are mainly testing the integration of
	// encryptedFS with FileRegistry and FileCipherStreamCreator. This uses real encryption but the
	// strings here are not very long since we've tested that in lower-level unit tests.
	testCases := []string{
		// Make the /bar/baz directory; create a third-level file.
		"1a: mkdirall /bar/baz",
		"1b: f = create /bar/baz/y",
		"1c: f.stat.name == y",
		// Write more than a block of data; read it back.
		"2a: f.write abcdefghijklmnopqrstuvwxyz",
		"2b: f.close",
		"2c: f = open /bar/baz/y",
		"2d: f.read 5 == abcde",
		"2e: f.readat 2 1 == bc",
		"2f: f.readat 5 20 == uvwxy",
		"2g: f.close",
		// Link /bar/baz/y to /bar/z. We should be able to read from both files
		// and remove them independently.
		"3a: link /bar/baz/y /bar/z",
		"3b: f = open /bar/z",
		"3c: f.read 5 == abcde",
		"3d: f.close",
		"3e: remove /bar/baz/y",
		"3f: f = open /bar/z",
		"3g: f.read 5 == abcde",
		"3h: f.close",
		// Rename /bar/z to /foo
		"4a: rename /bar/z /foo",
		"4b: f = open /foo",
		"4c: f.readat 5 20 == uvwxy",
		"4d: f.close",
		"4e: open /bar/z fails",
		// ReuseForWrite /foo /baz
		"5a: f = reuseForWrite /foo /baz",
		"5b: f.write abc",
		"5c: f.close",
		"5d: f = open /baz",
		"5e: f.read 3 == abc",
	}

	for _, tc := range testCases {
		s := strings.Split(tc, " ")[1:]

		saveF := s[0] == "f" && s[1] == "="
		if saveF {
			s = s[2:]
		}

		fails := s[len(s)-1] == "fails"
		if fails {
			s = s[:len(s)-1]
		}

		var (
			fi  os.FileInfo
			g   vfs.File
			err error
		)
		switch s[0] {
		case "create":
			g, err = fs.Create(s[1])
		case "link":
			err = fs.Link(s[1], s[2])
		case "open":
			g, err = fs.Open(s[1])
		case "mkdirall":
			err = fs.MkdirAll(s[1], 0755)
		case "remove":
			err = fs.Remove(s[1])
		case "rename":
			err = fs.Rename(s[1], s[2])
		case "reuseForWrite":
			g, err = fs.ReuseForWrite(s[1], s[2])
		case "f.write":
			_, err = f.Write([]byte(s[1]))
		case "f.read":
			n, _ := strconv.Atoi(s[1])
			buf := make([]byte, n)
			_, err = io.ReadFull(f, buf)
			if err != nil {
				break
			}
			if got, want := string(buf), s[3]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		case "f.readat":
			n, _ := strconv.Atoi(s[1])
			off, _ := strconv.Atoi(s[2])
			buf := make([]byte, n)
			_, err = f.ReadAt(buf, int64(off))
			if err != nil {
				break
			}
			if got, want := string(buf), s[4]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		case "f.close":
			f, err = nil, f.Close()
		case "f.stat.name":
			fi, err = f.Stat()
			if err != nil {
				break
			}
			if got, want := fi.Name(), s[2]; got != want {
				t.Fatalf("%q: got %q, want %q", tc, got, want)
			}
		default:
			t.Fatalf("bad test case: %q", tc)
		}

		if saveF {
			f, g = g, nil
		} else if g != nil {
			g.Close()
		}

		if fails {
			if err == nil {
				t.Fatalf("%q: got nil error, want non-nil", tc)
			}
		} else {
			if err != nil {
				t.Fatalf("%q: %v", tc, err)
			}
		}
	}
}

// Minimal test that creates an encrypted Pebble that exercises creation and reading of encrypted
// files, rereading data after reopening the engine, and stats code.
func TestPebbleEncryption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	opts := storage.DefaultPebbleOptions()
	opts.Cache = pebble.NewCache(1 << 20)
	defer opts.Cache.Unref()

	memFS := vfs.NewMem()
	opts.FS = memFS
	keyFile128 := "111111111111111111111111111111111234567890123456"
	writeToFile(t, opts.FS, "16.key", []byte(keyFile128))
	var encOptions baseccl.EncryptionOptions
	encOptions.KeySource = baseccl.EncryptionKeySource_KeyFiles
	encOptions.KeyFiles = &baseccl.EncryptionKeyFiles{
		CurrentKey: "16.key",
		OldKey:     "plain",
	}
	encOptions.DataKeyRotationPeriod = 1000 // arbitrary seconds
	encOptionsBytes, err := protoutil.Marshal(&encOptions)
	require.NoError(t, err)
	db, err := storage.NewPebble(
		context.Background(),
		storage.PebbleConfig{
			StorageConfig: base.StorageConfig{
				Attrs:             roachpb.Attributes{},
				MaxSize:           512 << 20,
				UseFileRegistry:   true,
				EncryptionOptions: encOptionsBytes,
			},
			Opts: opts,
		})
	require.NoError(t, err)
	// TODO(sbhola): Ensure that we are not returning the secret data keys by mistake.
	r, err := db.GetEncryptionRegistries()
	require.NoError(t, err)

	var fileRegistry enginepb.FileRegistry
	require.NoError(t, protoutil.Unmarshal(r.FileRegistry, &fileRegistry))
	var keyRegistry enginepbccl.DataKeysRegistry
	require.NoError(t, protoutil.Unmarshal(r.KeyRegistry, &keyRegistry))

	stats, err := db.GetEnvStats()
	require.NoError(t, err)
	// Opening the DB should've created OPTIONS, CURRENT, MANIFEST and the
	// WAL, all under the active key.
	require.Equal(t, uint64(4), stats.TotalFiles)
	require.Equal(t, uint64(4), stats.ActiveKeyFiles)
	var s enginepbccl.EncryptionStatus
	require.NoError(t, protoutil.Unmarshal(stats.EncryptionStatus, &s))
	require.Equal(t, "16.key", s.ActiveStoreKey.Source)
	require.Equal(t, int32(enginepbccl.EncryptionType_AES128_CTR), stats.EncryptionType)
	t.Logf("EnvStats:\n%+v\n\n", *stats)

	batch := db.NewUnindexedBatch(true /* writeOnly */)
	require.NoError(t, batch.PutUnversioned(roachpb.Key("a"), []byte("a")))
	require.NoError(t, batch.Commit(true))
	require.NoError(t, db.Flush())
	val, err := db.MVCCGet(storage.MVCCKey{Key: roachpb.Key("a")})
	require.NoError(t, err)
	require.Equal(t, "a", string(val))
	db.Close()

	opts2 := storage.DefaultPebbleOptions()
	opts2.Cache = pebble.NewCache(1 << 20)
	defer opts2.Cache.Unref()

	opts2.FS = memFS
	db, err = storage.NewPebble(
		context.Background(),
		storage.PebbleConfig{
			StorageConfig: base.StorageConfig{
				Attrs:             roachpb.Attributes{},
				MaxSize:           512 << 20,
				UseFileRegistry:   true,
				EncryptionOptions: encOptionsBytes,
			},
			Opts: opts2,
		})
	require.NoError(t, err)
	val, err = db.MVCCGet(storage.MVCCKey{Key: roachpb.Key("a")})
	require.NoError(t, err)
	require.Equal(t, "a", string(val))

	// Flushing should've created a new sstable under the active key.
	stats, err = db.GetEnvStats()
	require.NoError(t, err)
	t.Logf("EnvStats:\n%+v\n\n", *stats)
	require.Equal(t, uint64(5), stats.TotalFiles)
	require.LessOrEqual(t, uint64(5), stats.ActiveKeyFiles)
	require.Equal(t, stats.TotalBytes, stats.ActiveKeyBytes)

	db.Close()
}

func TestPebbleEncryption2(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()
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
		encOptions := baseccl.EncryptionOptions{
			KeySource: baseccl.EncryptionKeySource_KeyFiles,
			KeyFiles: &baseccl.EncryptionKeyFiles{
				CurrentKey: encKeyFile,
				OldKey:     oldEncFileKey,
			},
			DataKeyRotationPeriod: 1000,
		}
		encOptionsBytes, err := protoutil.Marshal(&encOptions)
		require.NoError(t, err)

		opts := storage.DefaultPebbleOptions()
		opts.FS = memFS
		opts.Cache = pebble.NewCache(1 << 20)
		defer opts.Cache.Unref()

		db, err := storage.NewPebble(
			context.Background(),
			storage.PebbleConfig{
				StorageConfig: base.StorageConfig{
					Attrs:             roachpb.Attributes{},
					MaxSize:           512 << 20,
					UseFileRegistry:   true,
					EncryptionOptions: encOptionsBytes,
				},
				Opts: opts,
			})
		require.NoError(t, err)
		require.True(t, validateKeys(db))

		keys[key] = true
		err = storage.MVCCPut(
			context.Background(),
			db,
			nil, /* ms */
			roachpb.Key(key),
			hlc.Timestamp{},
			roachpb.MakeValueFromBytes([]byte(val)),
			nil, /* txn */
		)
		require.NoError(t, err)
		require.NoError(t, db.Flush())
		require.NoError(t, db.Compact())
		require.True(t, validateKeys(db))
		db.Close()
	}

	addKeyAndValidate("a", "a", "16v1.key", "plain")
	addKeyAndValidate("b", "b", "plain", "16v1.key")
	addKeyAndValidate("c", "c", "16v2.key", "plain")
	addKeyAndValidate("d", "d", "plain", "16v2.key")
}
