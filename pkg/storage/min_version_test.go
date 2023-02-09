// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble"
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

	st := cluster.MakeClusterSettings()
	p, err := Open(context.Background(), InMemory(), cluster.MakeClusterSettings(), CacheSize(0))
	require.NoError(t, err)
	defer p.Close()
	require.Equal(t, pebble.FormatPrePebblev1Marked, p.db.FormatMajorVersion())

	ValueBlocksEnabled.Override(context.Background(), &st.SV, true)
	// Advancing the store cluster version to one that supports value blocks
	// should also advance the store's format major version.
	err = p.SetMinVersion(clusterversion.ByKey(clusterversion.V23_1EnablePebbleFormatSSTableValueBlocks))
	require.NoError(t, err)
	require.Equal(t, pebble.FormatSSTableValueBlocks, p.db.FormatMajorVersion())
}

func TestMinVersion_IsNotEncrypted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Replace the NewEncryptedEnvFunc global for the duration of this
	// test. We'll use it to initialize a test caesar cipher
	// encryption-at-rest implementation.
	oldNewEncryptedEnvFunc := NewEncryptedEnvFunc
	defer func() { NewEncryptedEnvFunc = oldNewEncryptedEnvFunc }()
	NewEncryptedEnvFunc = fauxNewEncryptedEnvFunc

	st := cluster.MakeClusterSettings()
	fs := vfs.NewMem()
	p, err := Open(
		context.Background(),
		Location{dir: "", fs: fs},
		st,
		EncryptionAtRest(nil))
	require.NoError(t, err)
	defer p.Close()
	require.NoError(t, p.SetMinVersion(st.Version.BinaryVersion()))

	// Reading the file directly through the unencrypted MemFS should
	// succeed and yield the correct version.
	v, ok, err := getMinVersion(fs, "")
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, st.Version.BinaryVersion(), v)
}

func fauxNewEncryptedEnvFunc(
	fs vfs.FS, fr *PebbleFileRegistry, dbDir string, readOnly bool, optionBytes []byte,
) (*EncryptionEnv, error) {
	return &EncryptionEnv{
		Closer: nopCloser{},
		FS:     fauxEncryptedFS{FS: fs},
	}, nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

type fauxEncryptedFS struct {
	vfs.FS
}

func (fs fauxEncryptedFS) Create(path string) (vfs.File, error) {
	f, err := fs.FS.Create(path)
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
