// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestMinVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	version1 := roachpb.Version{Major: 21, Minor: 1, Patch: 0, Internal: 122}
	version2 := roachpb.Version{Major: 21, Minor: 1, Patch: 0, Internal: 126}

	mem := vfs.NewMem()
	dir := "/foo"
	require.NoError(t, mem.MkdirAll(dir, os.ModeDir))

	// Expect !ok min version file doesn't exist.
	v, ok, err := getMinVersion(mem, dir)
	require.NoError(t, err)
	require.Equal(t, roachpb.Version{}, v)
	require.False(t, ok)

	// Expect min version to not be at least any target version.
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version1)
	require.NoError(t, err)
	require.False(t, ok)
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version2)
	require.NoError(t, err)
	require.False(t, ok)

	// Expect no error when updating min version if no file currently exists.
	require.NoError(t, writeMinVersionFile(mem, dir, version1))

	// Expect min version to be version1.
	v, ok, err = getMinVersion(mem, dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, version1.Equal(v))

	// Expect min version to be at least version1 but not version2.
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version2)
	require.NoError(t, err)
	require.False(t, ok)

	// Expect no error when updating min version to a higher version.
	require.NoError(t, writeMinVersionFile(mem, dir, version2))

	// Expect min version to be at least version1 and version2.
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version1)
	require.NoError(t, err)
	require.True(t, ok)
	ok, err = MinVersionIsAtLeastTargetVersion(mem, dir, version2)
	require.NoError(t, err)
	require.True(t, ok)

	// Expect min version to be version2.
	v, ok, err = getMinVersion(mem, dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, version2.Equal(v))

	// Expect no-op when trying to update min version to a lower version.
	require.NoError(t, writeMinVersionFile(mem, dir, version1))
	v, ok, err = getMinVersion(mem, dir)
	require.NoError(t, err)
	require.True(t, ok)
	require.True(t, version2.Equal(v))
}

func TestSetMinVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	p, err := Open(context.Background(), InMemory(), cluster.MakeClusterSettings(), CacheSize(0))
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, MinimumSupportedFormatVersion, p.db.FormatMajorVersion())

	// Advancing the store cluster version to one that supports a new feature
	// should also advance the store's format major version.
	err = p.SetMinVersion(clusterversion.Latest.Version())
	require.NoError(t, err)
	require.Equal(t, pebbleFormatVersion(clusterversion.Latest.Version()), p.db.FormatMajorVersion())
}

func TestMinVersion_IsNotEncrypted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Replace the NewEncryptedEnvFunc global for the duration of this
	// test. We'll use it to initialize a test caesar cipher
	// encryption-at-rest implementation.
	oldNewEncryptedEnvFunc := fs.NewEncryptedEnvFunc
	defer func() { fs.NewEncryptedEnvFunc = oldNewEncryptedEnvFunc }()
	fs.NewEncryptedEnvFunc = fauxNewEncryptedEnvFunc

	ctx := context.Background()
	st := cluster.MakeClusterSettings()
	baseFS := vfs.NewMem()
	env, err := fs.InitEnv(ctx, baseFS, "", fs.EnvConfig{
		EncryptionOptions: &storagepb.EncryptionOptions{},
	}, nil /* statsCollector */)
	require.NoError(t, err)

	p, err := Open(ctx, env, st)
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.SetMinVersion(st.Version.LatestVersion()))

	// Reading the file directly through the unencrypted MemFS should
	// succeed and yield the correct version.
	v, ok, err := getMinVersion(env.UnencryptedFS, "")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, st.Version.LatestVersion(), v)
}

func fauxNewEncryptedEnvFunc(
	unencryptedFS vfs.FS,
	fr *fs.FileRegistry,
	dbDir string,
	readOnly bool,
	_ *storagepb.EncryptionOptions,
) (*fs.EncryptionEnv, error) {
	return &fs.EncryptionEnv{
		Closer: nopCloser{},
		FS:     fauxEncryptedFS{FS: unencryptedFS},
	}, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

type fauxEncryptedFS struct {
	vfs.FS
}

func (fs fauxEncryptedFS) Create(path string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(path, category)
	if err != nil {
		return nil, err
	}
	return fauxEncryptedFile{f}, nil
}

func (fs fauxEncryptedFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return fauxEncryptedFile{f}, nil
}

type fauxEncryptedFile struct {
	vfs.File
}

func (f fauxEncryptedFile) Write(b []byte) (int, error) {
	for i := range b {
		b[i] = b[i] + 1
	}
	return f.File.Write(b)
}

func (f fauxEncryptedFile) Read(b []byte) (int, error) {
	n, err := f.File.Read(b)
	for i := 0; i < n; i++ {
		b[i] = b[i] - 1
	}
	return n, err
}

func (f fauxEncryptedFile) ReadAt(p []byte, off int64) (int, error) {
	n, err := f.File.ReadAt(p, off)
	for i := 0; i < n; i++ {
		p[i] = p[i] - 1
	}
	return n, err
}
