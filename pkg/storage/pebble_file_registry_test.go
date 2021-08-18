// Copyright 2019 The Cockroach Authors.
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
	"runtime/debug"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func TestFileRegistryRelativePaths(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mem := vfs.NewMem()
	fileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	type TestCase struct {
		dbDir            string
		filename         string
		expectedFilename string
	}
	testCases := []TestCase{
		{"/", "/foo", "foo"},
		{"/rocksdir", "/rocksdirfoo", "/rocksdirfoo"},
		{"/rocksdir", "/rocksdir/foo", "foo"},
		// We get the occasional double-slash.
		{"/rocksdir", "/rocksdir//foo", "foo"},
		{"/mydir", "/mydir", ""},
		{"/mydir", "/mydir/", ""},
		{"/mydir", "/mydir//", ""},
		{"/mnt/otherdevice/", "/mnt/otherdevice/myfile", "myfile"},
		{"/mnt/otherdevice/myfile", "/mnt/otherdevice/myfile", ""},
	}

	for _, tc := range testCases {
		require.NoError(t, mem.MkdirAll(tc.dbDir, 0755))
		registry := &PebbleFileRegistry{FS: mem, DBDir: tc.dbDir}
		require.NoError(t, registry.Load())
		require.NoError(t, registry.SetFileEntry(tc.filename, fileEntry))
		entry := registry.GetFileEntry(tc.expectedFilename)
		if diff := pretty.Diff(entry, fileEntry); diff != nil {
			t.Fatalf("filename: %s: %s\n%v", tc.filename, strings.Join(diff, "\n"), entry)
		}
	}
}

func TestFileRegistryOps(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mem := vfs.NewMem()
	fooFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	barFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("bar")}
	bazFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("baz")}

	require.NoError(t, mem.MkdirAll("/mydb", 0755))
	registry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb"}
	require.NoError(t, registry.Load())
	require.Nil(t, registry.GetFileEntry("file1"))

	expected := make(map[string]*enginepb.FileEntry)

	checkEquality := func() {
		// Ensure all the expected paths exist, otherwise Load will elide
		// them and this test is not designed to test elision.
		for path := range expected {
			path = mem.PathJoin("/mydb", path)
			require.NoError(t, mem.MkdirAll(mem.PathDir(path), 0655))
			f, err := mem.Create(path)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}

		registry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb"}
		require.NoError(t, registry.Load())
		registry.mu.Lock()
		defer registry.mu.Unlock()
		if diff := pretty.Diff(registry.mu.currProto.Files, expected); diff != nil {
			t.Log(string(debug.Stack()))
			t.Fatalf("%s\n%v", strings.Join(diff, "\n"), registry.mu.currProto.Files)
		}
	}

	// {file1 => foo}
	require.NoError(t, registry.SetFileEntry("file1", fooFileEntry))
	expected["file1"] = fooFileEntry
	checkEquality()

	// {file1 => foo, file2 => bar}
	require.NoError(t, registry.SetFileEntry("file2", barFileEntry))
	expected["file2"] = barFileEntry
	checkEquality()

	// {file3 => foo, file2 => bar}
	require.NoError(t, registry.MaybeRenameEntry("file1", "file3"))
	expected["file3"] = fooFileEntry
	delete(expected, "file1")
	checkEquality()

	// {file3 => foo, file2 => bar, file4 => bar}
	require.NoError(t, registry.MaybeLinkEntry("file2", "file4"))
	expected["file4"] = barFileEntry
	checkEquality()

	// {file3 => foo, file4 => bar}
	require.NoError(t, registry.MaybeLinkEntry("file5", "file2"))
	delete(expected, "file2")
	checkEquality()

	// {file3 => foo}
	require.NoError(t, registry.MaybeRenameEntry("file7", "file4"))
	delete(expected, "file4")
	checkEquality()

	// {file3 => foo, blue/baz => baz} (since latter file uses relative path).
	require.NoError(t, registry.SetFileEntry("/mydb/blue/baz", bazFileEntry))
	expected["blue/baz"] = bazFileEntry
	checkEquality()

	entry := registry.GetFileEntry("/mydb/blue/baz")
	if diff := pretty.Diff(entry, bazFileEntry); diff != nil {
		t.Fatalf("%s\n%v", strings.Join(diff, "\n"), entry)
	}

	// {file3 => foo}
	require.NoError(t, registry.MaybeDeleteEntry("/mydb/blue/baz"))
	delete(expected, "blue/baz")
	checkEquality()

	// {file3 => foo, green/baz => baz} (since latter file uses relative path).
	require.NoError(t, registry.SetFileEntry("/mydb//green/baz", bazFileEntry))
	expected["green/baz"] = bazFileEntry
	checkEquality()

	// Noops
	require.NoError(t, registry.MaybeDeleteEntry("file1"))
	require.NoError(t, registry.MaybeRenameEntry("file4", "file5"))
	require.NoError(t, registry.MaybeLinkEntry("file6", "file7"))
	checkEquality()

	// Open a read-only registry. All updates should fail.
	roRegistry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb", ReadOnly: true}
	require.NoError(t, roRegistry.Load())
	require.Error(t, roRegistry.SetFileEntry("file3", bazFileEntry))
	require.Error(t, roRegistry.MaybeDeleteEntry("file3"))
	require.Error(t, roRegistry.MaybeRenameEntry("file3", "file4"))
	require.Error(t, roRegistry.MaybeLinkEntry("file3", "file4"))
}

func TestFileRegistryCheckNoFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mem := vfs.NewMem()
	fileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	registry := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry.CheckNoRegistryFile())
	require.NoError(t, registry.Load())
	require.NoError(t, registry.SetFileEntry("/foo", fileEntry))
	registry = &PebbleFileRegistry{FS: mem}
	require.Error(t, registry.CheckNoRegistryFile())
}

func TestFileRegistryElideUnencrypted(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Temporarily change the global CanRegistryElideFunc to test it.
	prevRegistryElideFunc := CanRegistryElideFunc
	defer func() {
		CanRegistryElideFunc = prevRegistryElideFunc
	}()
	CanRegistryElideFunc = func(entry *enginepb.FileEntry) bool {
		return entry == nil || len(entry.EncryptionSettings) == 0
	}

	// Create a new pebble file registry and inject a registry with an unencrypted file.
	mem := vfs.NewMem()

	for _, name := range []string{"test1", "test2"} {
		f, err := mem.Create(name)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	registry := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry.Load())
	newProto := &enginepb.FileRegistry{}
	newProto.Files = make(map[string]*enginepb.FileEntry)
	newProto.Files["test1"] = &enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte(nil)}
	newProto.Files["test2"] = &enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("foo")}
	require.NoError(t, registry.rewriteOldRegistry(newProto))

	// Create another pebble file registry to verify that the unencrypted file is elided on startup.
	registry2 := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry2.Load())
	require.NotContains(t, registry2.mu.currProto.Files, "test1")
	entry := registry2.mu.currProto.Files["test2"]
	require.NotNil(t, entry)
	require.Equal(t, entry.EncryptionSettings, []byte("foo"))
}

func TestFileRegistryElideNonexistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mem := vfs.NewMem()
	f, err := mem.Create("bar")
	require.NoError(t, err)
	require.NoError(t, f.Close())
	{
		registry := &PebbleFileRegistry{FS: mem}
		require.NoError(t, registry.Load())
		require.NoError(t, registry.rewriteOldRegistry(&enginepb.FileRegistry{
			Files: map[string]*enginepb.FileEntry{
				"foo": {EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")},
				"bar": {EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("bar")},
			},
		}))
	}

	// Create another registry and verify that the nonexistent `foo` file
	// entry is elided on startup.
	{
		registry := &PebbleFileRegistry{FS: mem}
		require.NoError(t, registry.Load())
		require.NotContains(t, registry.mu.currProto.Files, "foo")
		require.Contains(t, registry.mu.currProto.Files, "bar")
		require.NotNil(t, registry.mu.currProto.Files["bar"])
		require.Equal(t, []byte("bar"), registry.mu.currProto.Files["bar"].EncryptionSettings)
	}
}

func TestFileRegistryRecordsReadAndWrite(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	files := map[string]*enginepb.FileEntry{
		"test1": {EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("foo")},
		"test2": {EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("bar")},
	}

	mem := vfs.NewMem()

	// Ensure all the expected paths exist, otherwise Load will elide
	// them and this test is not designed to test elision.
	for name := range files {
		f, err := mem.Create(name)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	// Create a file registry and add entries for a few files.
	registry1 := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry1.CheckNoRegistryFile())
	require.NoError(t, registry1.Load())
	for filename, entry := range files {
		require.NoError(t, registry1.SetFileEntry(filename, entry))
	}
	require.NoError(t, registry1.Close())

	// Create another file registry and load in the registry file.
	// It should use the monolithic one.
	registry2 := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry2.Load())
	for filename, entry := range files {
		require.Equal(t, entry, registry2.GetFileEntry(filename))
	}
	require.NoError(t, registry2.Close())

	// Signal that we no longer need the monolithic one.
	require.NoError(t, registry2.StopUsingOldRegistry())
	require.NoError(t, registry2.checkNoBaseRegistry())
	require.NoError(t, registry2.Close())

	registry3 := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry3.Load())
	for filename, entry := range files {
		require.Equal(t, entry, registry3.GetFileEntry(filename))
	}
	require.NoError(t, registry3.Close())
}
