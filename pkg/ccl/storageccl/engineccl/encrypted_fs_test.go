// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package engineccl

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/errorfs"
	"github.com/stretchr/testify/require"
)

func TestEncryptedFS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()

	require.NoError(t, memFS.MkdirAll("/bar", os.ModePerm))
	fileRegistry := &fs.FileRegistry{FS: memFS, DBDir: "/bar"}
	require.NoError(t, fileRegistry.Load(context.Background()))

	// Using a StoreKeyManager for the test since it is easy to create. Write a key for the
	// StoreKeyManager.
	var b []byte
	for i := 0; i < keyIDLength+16; i++ {
		b = append(b, 'a')
	}
	f, err := memFS.Create("keyfile", fs.UnspecifiedWriteCategory)
	require.NoError(t, err)
	bReader := bytes.NewReader(b)
	_, err = io.Copy(f, bReader)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	keyManager := &StoreKeyManager{fs: memFS, activeKeyFilename: "keyfile", oldKeyFilename: "plain"}
	require.NoError(t, keyManager.Load(context.Background()))

	streamCreator := &FileCipherStreamCreator{keyManager: keyManager, envType: enginepb.EnvType_Store}

	encryptedFS := &encryptedFS{FS: memFS, fileRegistry: fileRegistry, streamCreator: streamCreator}

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
			g, err = encryptedFS.Create(s[1], fs.UnspecifiedWriteCategory)
		case "link":
			err = encryptedFS.Link(s[1], s[2])
		case "open":
			g, err = encryptedFS.Open(s[1])
		case "mkdirall":
			err = encryptedFS.MkdirAll(s[1], 0755)
		case "remove":
			err = encryptedFS.Remove(s[1])
		case "rename":
			err = encryptedFS.Rename(s[1], s[2])
		case "reuseForWrite":
			g, err = encryptedFS.ReuseForWrite(s[1], s[2], fs.UnspecifiedWriteCategory)
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

func TestEncryptedFSUnencryptedFiles(t *testing.T) {
	defer leaktest.AfterTest(t)()

	memFS := vfs.NewMem()
	require.NoError(t, memFS.MkdirAll("/foo", os.ModeDir))

	fileRegistry := &fs.FileRegistry{FS: memFS, DBDir: "/foo"}
	require.NoError(t, fileRegistry.Load(context.Background()))

	keyManager := &StoreKeyManager{fs: memFS, activeKeyFilename: "plain", oldKeyFilename: "plain"}
	require.NoError(t, keyManager.Load(context.Background()))

	streamCreator := &FileCipherStreamCreator{keyManager: keyManager, envType: enginepb.EnvType_Store}

	encryptedFS := &encryptedFS{FS: memFS, fileRegistry: fileRegistry, streamCreator: streamCreator}

	var filesCreated []string
	for i := 0; i < 5; i++ {
		filename := fmt.Sprintf("file%d", i)
		f, err := encryptedFS.Create(filename, fs.UnspecifiedWriteCategory)
		require.NoError(t, err)
		filesCreated = append(filesCreated, filename)
		require.NoError(t, f.Close())
	}

	// The file registry should be empty since we only created unencrypted files.
	for _, filename := range filesCreated {
		require.Nil(t, fileRegistry.GetFileEntry(filename))
	}
}

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

	encOptions := &storagepb.EncryptionOptions{
		KeySource: storagepb.EncryptionKeySource_KeyFiles,
		KeyFiles: &storagepb.EncryptionKeyFiles{
			CurrentKey: "16.key",
			OldKey:     "plain",
		},
		DataKeyRotationPeriod: 1000, // arbitrary seconds
	}

	func() {
		// Initialize the filesystem env.
		env, err := fs.InitEnvFromStoreSpec(
			ctx,
			base.StoreSpec{
				InMemory:          true,
				Attributes:        roachpb.Attributes{},
				Size:              storagepb.SizeSpec{Capacity: 512 << 20},
				EncryptionOptions: encOptions,
				StickyVFSID:       stickyVFSID,
			},
			fs.ReadWrite,
			stickyRegistry, /* sticky registry */
			nil,            /* statsCollector */
		)
		require.NoError(t, err)
		db, err := storage.Open(ctx, env, cluster.MakeTestingClusterSettings())
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
		require.Equal(t, []byte("a"), storageutils.MVCCGetRaw(t, db, storageutils.PointKey("a", 0)))
	}()

	func() {
		// Initialize the filesystem env again, replaying the file registries.
		env, err := fs.InitEnvFromStoreSpec(
			ctx,
			base.StoreSpec{
				InMemory:          true,
				Attributes:        roachpb.Attributes{},
				Size:              storagepb.SizeSpec{Capacity: 512 << 20},
				EncryptionOptions: encOptions,
				StickyVFSID:       stickyVFSID,
			},
			fs.ReadWrite,
			stickyRegistry, /* sticky registry */
			nil,            /* statsCollector */
		)
		require.NoError(t, err)
		db, err := storage.Open(ctx, env, cluster.MakeTestingClusterSettings())
		require.NoError(t, err)
		defer db.Close()
		require.Equal(t, []byte("a"), storageutils.MVCCGetRaw(t, db, storageutils.PointKey("a", 0)))

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
		encOptions := &storagepb.EncryptionOptions{
			KeySource: storagepb.EncryptionKeySource_KeyFiles,
			KeyFiles: &storagepb.EncryptionKeyFiles{
				CurrentKey: encKeyFile,
				OldKey:     oldEncFileKey,
			},
			DataKeyRotationPeriod: 1000,
		}

		// Initialize the filesystem env.
		ctx := context.Background()
		env, err := fs.InitEnvFromStoreSpec(
			ctx,
			base.StoreSpec{
				InMemory:          true,
				Attributes:        roachpb.Attributes{},
				Size:              storagepb.SizeSpec{Capacity: 512 << 20},
				EncryptionOptions: encOptions,
				StickyVFSID:       stickyVFSID,
			},
			fs.ReadWrite,
			stickyRegistry, /* sticky registry */
			nil,            /* statsCollector */
		)
		require.NoError(t, err)
		db, err := storage.Open(ctx, env, cluster.MakeTestingClusterSettings())
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
		require.NoError(t, db.Compact())
		require.True(t, validateKeys(db))
		db.Close()
	}

	addKeyAndValidate("a", "a", "16v1.key", "plain")
	addKeyAndValidate("b", "b", "plain", "16v1.key")
	addKeyAndValidate("c", "c", "16v2.key", "plain")
	addKeyAndValidate("d", "d", "plain", "16v2.key")
}

func TestCanRegistryElide(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var entry *enginepb.FileEntry = nil
	require.True(t, canRegistryElide(entry))

	entry = &enginepb.FileEntry{EnvType: enginepb.EnvType_Store}
	settings := &enginepb.EncryptionSettings{EncryptionType: enginepb.EncryptionType_Plaintext}
	b, err := protoutil.Marshal(settings)
	require.NoError(t, err)
	entry.EncryptionSettings = b
	require.True(t, canRegistryElide(entry))

	settings = &enginepb.EncryptionSettings{EncryptionType: enginepb.EncryptionType_AES128_CTR}
	b, err = protoutil.Marshal(settings)
	require.NoError(t, err)
	entry.EncryptionSettings = b
	require.False(t, canRegistryElide(entry))
}

// errorInjector injects errors into metadata writes involving the
// encryptedFS, i.e., writes by the file registry and key manager. Data files
// in the test below have names prefixed by TEST and are spared this error
// injection, since the goal of the test is to discover bugs in the metadata
// processing.
type errorInjector struct {
	prob float64
	rand *rand.Rand

	// The test is single threaded, and there is no async IO in the lower
	// layers, so this field does not need synchronization.
	startInjecting bool
}

func (i *errorInjector) MaybeError(op errorfs.Op) error {
	if i.startInjecting && op.Kind.ReadOrWrite() == errorfs.OpIsWrite &&
		!strings.HasPrefix(op.Path, "TEST") && i.rand.Float64() < i.prob {
		return errors.WithStack(errorfs.ErrInjected)
	}
	return nil
}

func (i *errorInjector) startErrors() {
	i.startInjecting = true
}

// String implements fmt.Stringer.
func (i *errorInjector) String() string { return "<opaque>" }

// testFS is the interface implemented by a plain FS and encrypted FS being
// tested.
type testFS interface {
	fs() vfs.FS
	// restart is used to "reset" to the synced state. It is used for 2 reasons:
	// - explicit testing of node restarts.
	// - encryptedFS has unrecoverable errors that cause panics. We do want to
	//   test situations where the node is restarted after the panic.
	restart() error
	// syncDir is a convenience function to sync the dir being used by the test.
	syncDir(t *testing.T)
}

// plainTestFS is not encrypted and does not require syncs for state to be
// preserved. It defined the expected output of the randomized test.
type plainTestFS struct {
	vfs vfs.FS
}

func (ptfs *plainTestFS) fs() vfs.FS {
	return ptfs.vfs
}

func (ptfs *plainTestFS) restart() error { return nil }

func (ptfs *plainTestFS) syncDir(t *testing.T) {}

// encryptedTestFS is the encrypted FS being tested.
type encryptedTestFS struct {
	// The base strict FS which is wrapped for error injection and encryption.
	mem        *vfs.MemFS
	encOptions *storagepb.EncryptionOptions
	errorProb  float64
	errorRand  *rand.Rand

	encEnv *fs.EncryptionEnv
}

func (etfs *encryptedTestFS) fs() vfs.FS {
	return etfs.encEnv.FS
}

func (etfs *encryptedTestFS) syncDir(t *testing.T) {
	dir, err := etfs.mem.OpenDir("/")
	// These operations are on the base FS, so there should be no errors.
	require.NoError(t, err)
	require.NoError(t, dir.Sync())
	require.NoError(t, dir.Close())
}

func (etfs *encryptedTestFS) restart() error {
	if etfs.encEnv != nil {
		etfs.encEnv.Closer.Close()
		etfs.encEnv = nil
	}
	etfs.mem = etfs.mem.CrashClone(vfs.CrashCloneCfg{})
	ei := &errorInjector{prob: etfs.errorProb, rand: etfs.errorRand}
	fsMeta := errorfs.Wrap(etfs.mem, ei)
	// TODO(sumeer): Do deterministic rollover of file registry after small
	// number of operations.
	fileRegistry := &fs.FileRegistry{
		FS: fsMeta, DBDir: "", ReadOnly: false, NumOldRegistryFiles: 2}
	if err := fileRegistry.Load(context.Background()); err != nil {
		return err
	}
	encEnv, err := newEncryptedEnv(
		fsMeta, fileRegistry, "", false, etfs.encOptions)
	if err != nil {
		return err
	}
	etfs.encEnv = encEnv
	// Error injection starts after initialization, to simplify the test (the
	// caller does not need to handle errors and call restart again).
	ei.startErrors()
	return nil
}

func makeEncryptedTestFS(t *testing.T, errorProb float64, errorRand *rand.Rand) *encryptedTestFS {
	mem := vfs.NewCrashableMem()
	keyFile128 := "111111111111111111111111111111111234567890123456"
	writeToFile(t, mem, "16.key", []byte(keyFile128))
	dir, err := mem.OpenDir("/")
	require.NoError(t, err)
	require.NoError(t, dir.Sync())
	require.NoError(t, dir.Close())

	var encOptions storagepb.EncryptionOptions
	encOptions.KeySource = storagepb.EncryptionKeySource_KeyFiles
	encOptions.KeyFiles = &storagepb.EncryptionKeyFiles{
		CurrentKey: "16.key",
		OldKey:     "plain",
	}
	// Effectively infinite period.
	//
	// TODO(sumeer): Do deterministic data key rotation. Inject kmTimeNow and
	// operations that advance time.
	encOptions.DataKeyRotationPeriod = 100000
	etfs := &encryptedTestFS{
		mem:        mem,
		encOptions: &encOptions,
		errorProb:  errorProb,
		errorRand:  errorRand,
		encEnv:     nil,
	}
	require.NoError(t, etfs.restart())
	return etfs
}

// fsTest is used by the various operations.
type fsTest struct {
	t  *testing.T
	fs testFS

	output strings.Builder
}

func (t *fsTest) outputOkOrError(err error) {
	if err != nil {
		fmt.Fprintf(&t.output, " %s\n", err.Error())
	} else {
		fmt.Fprintf(&t.output, " ok\n")
	}
}

// The operations in the test.
type fsTestOp interface {
	run(t *fsTest)
}

// createOp creates a file with name and the given value.
type createOp struct {
	name  string
	value []byte
}

func (op *createOp) run(t *fsTest) {
	var err error
	buf := make([]byte, len(op.value))
	// Create is idempotent, so we simply retry on injected errors.
	withRetry(t, func() error {
		var f vfs.File
		f, err := t.fs.fs().Create(op.name, fs.UnspecifiedWriteCategory)
		if err != nil {
			return err
		}
		// copy the value since Write can modify the value.
		copy(buf, op.value)
		_, err = f.Write(buf)
		if err != nil {
			f.Close()
			return err
		}
		if err = f.Sync(); err != nil {
			f.Close()
			return err
		}
		return f.Close()
	})
	t.fs.syncDir(t.t)
	fmt.Fprintf(&t.output, "createOp(%s, %s):", op.name, string(op.value))
	t.outputOkOrError(err)
}

// removeOp removed the file with name.
type removeOp struct {
	name string
}

func (op *removeOp) run(t *fsTest) {
	var err error
	observedError := false
	// Remove can fail if the file got removed from the underlying FS and we did
	// not remove from the registry. It can also fail even when it was removed
	// from the registry, if the registry rollover fails. So we swallow
	// ErrNotExist, which will happen if the file has been removed from the
	// underlying FS. And additionally call restart to get the registry into a
	// clean state (it elides files that are not found in the underlying FS when
	// loading the registry) -- this is just defensive code since we want to
	// avoid relying too much on the internal implementation details of
	// encryptedFS.
	withRetry(t, func() error {
		err = t.fs.fs().Remove(op.name)
		if err != nil {
			observedError = true
			if oserror.IsNotExist(err) {
				// Removal is considered done.
				err = nil
			}
		}
		return err
	})
	t.fs.syncDir(t.t)
	fmt.Fprintf(&t.output, "removeOp(%s):", op.name)
	t.outputOkOrError(err)
	if observedError {
		require.NoError(t.t, t.fs.restart())
	}
}

// linkOp exercises vfs.FS.Link.
type linkOp struct {
	fromName string
	toName   string
}

func (op *linkOp) run(t *fsTest) {
	var err error
	// Link is not idempotent, in that the underlying FS linking happens first,
	// and if the subsequent changes to the registry fail, the underlying FS
	// will not permit linking again (since from file already exists). But we do
	// want to test failure handling of encryptedFS, so we massage this to make
	// it idempotent.
	_, err = t.fs.fs().Stat(op.toName)
	// Do something only if the to file does not exist.
	if oserror.IsNotExist(err) {
		withRetry(t, func() error {
			err = t.fs.fs().Link(op.fromName, op.toName)
			if oserror.IsExist(err) {
				// Partially done and failed, so remove the to file and try again.
				err = errorfs.ErrInjected
				_ = t.fs.fs().Remove(op.toName)
			}
			return err
		})
	}
	t.fs.syncDir(t.t)
	fmt.Fprintf(&t.output, "linkOp(%s, %s):", op.fromName, op.toName)
	t.outputOkOrError(err)
}

// renameOp exercises vfs.FS.Rename
type renameOp struct {
	fromName string
	toName   string
}

func (op *renameOp) run(t *fsTest) {
	var err error
	observedError := false
	// Only do the rename if from file exists: encryptedFS will delete the
	// toName entry from the registry if the fromName file is not in the
	// registry. And the underlying FS.Rename will return an error since the
	// from file does not exist, so the semantics of encrypted FS and plain FS
	// are not compatible for this case.
	_, err = t.fs.fs().Stat(op.fromName)
	if err == nil {
		withRetry(t, func() error {
			err = t.fs.fs().Rename(op.fromName, op.toName)
			if err != nil {
				observedError = true
				_, err2 := t.fs.fs().Stat(op.fromName)
				if oserror.IsNotExist(err2) {
					// Swallow the error since rename must have happened.
					err = nil
				}
			}
			return err
		})
	}
	t.fs.syncDir(t.t)
	fmt.Fprintf(&t.output, "renameOp(%s, %s):", op.fromName, op.toName)
	t.outputOkOrError(err)
	if observedError {
		require.NoError(t.t, t.fs.restart())
	}
}

type restartOp struct {
}

func (op *restartOp) run(t *fsTest) {
	err := t.fs.restart()
	fmt.Fprintf(&t.output, "restartOp:")
	t.outputOkOrError(err)
}

type readOp struct {
	name string
}

func (op *readOp) run(t *fsTest) {
	var err error
	var value []byte
	withRetry(t, func() error {
		var f vfs.File
		f, err = t.fs.fs().Open(op.name)
		if err != nil {
			return err
		}
		defer f.Close()
		value, err = io.ReadAll(f)
		return err
	})
	fmt.Fprintf(&t.output, "readOp(%s):", op.name)
	if err != nil {
		fmt.Fprintf(&t.output, " %s\n", err.Error())
	} else {
		fmt.Fprintf(&t.output, " %s\n", string(value))
	}
}

func fillRand(rng *rand.Rand, buf []byte) {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = uint64(len(letters))
	const lettersCharsPerRand = 12 // floor(log(math.MaxUint64)/log(lettersLen))

	var r uint64
	var q int
	for i := 0; i < len(buf); i++ {
		if q == 0 {
			r = rng.Uint64()
			q = lettersCharsPerRand
		}
		buf[i] = letters[r%lettersLen]
		r = r / lettersLen
		q--
	}
}

func withRetry(t *fsTest, fn func() error) {
	retryFunc := func() (err error) {
		defer func() {
			// Recover from a panic by restarting and retrying.
			if r := recover(); r != nil {
				err = errorfs.ErrInjected
				require.NoError(t.t, t.fs.restart())
			}
		}()
		err = fn()
		return
	}
	i := 0
	for {
		err := retryFunc()
		if !errors.Is(err, errorfs.ErrInjected) {
			break
		}
		i++
	}
}

func makeOps(rng *rand.Rand, numOps int) []fsTestOp {
	var filenames []string
	newFilename := func() string {
		var fileNameBytes [8]byte
		fillRand(rng, fileNameBytes[:])
		name := "TEST" + string(fileNameBytes[:])
		filenames = append(filenames, name)
		return name
	}
	pickFileName := func(newProb float64) string {
		if len(filenames) == 0 || rng.Float64() < newProb {
			return newFilename()
		}
		return filenames[rng.Intn(len(filenames))]
	}
	ops := make([]fsTestOp, 0, numOps)
	for len(ops) < numOps {
		v := rng.Float64()
		if v < 0.5 {
			// Read
			if len(filenames) == 0 {
				continue
			}
			name := filenames[rng.Intn(len(filenames))]
			ops = append(ops, &readOp{name: name})
		} else if v < 0.75 {
			// Create
			name := newFilename()
			var buf [10]byte
			fillRand(rng, buf[:])
			ops = append(ops, &createOp{
				name:  name,
				value: buf[:],
			})
		} else if v < 0.80 {
			// Link
			fromName := pickFileName(0.01)
			var toName string
			for {
				toName = pickFileName(0.9)
				if fromName != toName {
					break
				}
			}
			ops = append(ops, &linkOp{
				fromName: fromName,
				toName:   toName,
			})
		} else if v < 0.85 {
			// Rename
			fromName := pickFileName(0.01)
			var toName string
			for {
				toName = pickFileName(0.9)
				if fromName != toName {
					break
				}
			}
			ops = append(ops, &renameOp{
				fromName: fromName,
				toName:   toName,
			})
		} else if v < 0.9 {
			// Remove
			name := pickFileName(0.9)
			ops = append(ops, &removeOp{name: name})
		} else {
			// Restart
			ops = append(ops, &restartOp{})
		}
	}
	return ops
}

var seed = flag.Uint64("seed", 0, "a pseudorandom number generator seed")

func TestEncryptedFSRandomizedWithErrors(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if *seed == 0 {
		*seed = uint64(timeutil.Now().UnixNano())
	}
	t.Logf("seed %d", *seed)
	rng := rand.New(rand.NewSource(int64(*seed)))

	errorProb := 0.0
	if rng.Float64() < 0.9 {
		errorProb = rng.Float64() / 8
	}
	ptfs := &plainTestFS{vfs: vfs.NewMem()}
	etfs := makeEncryptedTestFS(t, errorProb, rng)
	pTest := fsTest{t: t, fs: ptfs}
	eTest := fsTest{t: t, fs: etfs}
	ops := makeOps(rng, 500)
	for i := range ops {
		fmt.Fprintf(&pTest.output, "%d: ", i)
		ops[i].run(&pTest)
		fmt.Fprintf(&eTest.output, "%d: ", i)
		ops[i].run(&eTest)
	}
	expectedStr := pTest.output.String()
	actualStr := eTest.output.String()
	if true || expectedStr != actualStr {
		t.Logf("---- expected\n%s\n", expectedStr)
		t.Logf("---- actual\n%s\n", actualStr)
	}
	require.Equal(t, pTest.output.String(), eTest.output.String())
}
