// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
	"runtime/debug"
	"strings"
	"testing"
)

func checkEquality(t *testing.T, fs vfs.FS, expected map[string]*enginepb.FileEntry) {
	registry := &PebbleFileRegistry{FS: fs, DBDir: "/mydb"}
	require.NoError(t, registry.Load())
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if diff := pretty.Diff(registry.mu.currProto.Files, expected); diff != nil {
		t.Log(string(debug.Stack()))
		t.Fatalf("%s\n%v", strings.Join(diff, "\n"), registry.mu.currProto.Files)
	}
}

func TestFileRegistryRelativePaths(t *testing.T) {
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
		mem.MkdirAll(tc.dbDir, 0755)
		registry := &PebbleFileRegistry{FS: mem, DBDir: tc.dbDir}
		require.NoError(t, registry.Load())
		registry.SetFileEntry(tc.filename, fileEntry)
		entry := registry.GetFileEntry(tc.expectedFilename)
		if diff := pretty.Diff(entry, fileEntry); diff != nil {
			t.Fatalf("filename: %s: %s\n%v", tc.filename, strings.Join(diff, "\n"), entry)
		}
	}
}

func TestFileRegistryOps(t *testing.T) {
	mem := vfs.NewMem()
	fooFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	barFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("bar")}
	bazFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("baz")}

	mem.MkdirAll("/mydb", 0755)
	registry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb"}
	require.NoError(t, registry.Load())
	require.Nil(t, registry.GetFileEntry("file1"))

	// {file1 => foo}
	require.NoError(t, registry.SetFileEntry("file1", fooFileEntry))
	expected := make(map[string]*enginepb.FileEntry)
	expected["file1"] = fooFileEntry
	checkEquality(t, mem, expected)

	// {file1 => foo, file2 => bar}
	require.NoError(t, registry.SetFileEntry("file2", barFileEntry))
	expected["file2"] = barFileEntry
	checkEquality(t, mem, expected)

	// {file3 => foo, file2 => bar}
	require.NoError(t, registry.EnsureRenameEntry("file1", "file3"))
	expected["file3"] = fooFileEntry
	delete(expected, "file1")
	checkEquality(t, mem, expected)

	// {file3 => foo, file2 => bar, file4 => bar}
	require.NoError(t, registry.EnsureLinkEntry("file2", "file4"))
	expected["file4"] = barFileEntry
	checkEquality(t, mem, expected)

	// {file3 => foo, file4 => bar}
	require.NoError(t, registry.EnsureLinkEntry("file5", "file2"))
	delete(expected, "file2")
	checkEquality(t, mem, expected)

	// {file3 => foo}
	require.NoError(t, registry.EnsureRenameEntry("file7", "file4"))
	delete(expected, "file4")
	checkEquality(t, mem, expected)

	// {file3 => foo, blue/baz => baz} (since latter file uses relative path).
	require.NoError(t, registry.SetFileEntry("/mydb/blue/baz", bazFileEntry))
	expected["blue/baz"] = bazFileEntry
	checkEquality(t, mem, expected)

	entry := registry.GetFileEntry("/mydb/blue/baz")
	if diff := pretty.Diff(entry, bazFileEntry); diff != nil {
		t.Fatalf("%s\n%v", strings.Join(diff, "\n"), entry)
	}

	// {file3 => foo}
	require.NoError(t, registry.EnsureDeleteEntry("/mydb/blue/baz"))
	delete(expected, "blue/baz")
	checkEquality(t, mem, expected)

	// {file3 => foo, green/baz => baz} (since latter file uses relative path).
	require.NoError(t, registry.SetFileEntry("/mydb//green/baz", bazFileEntry))
	expected["green/baz"] = bazFileEntry
	checkEquality(t, mem, expected)

	// Noops
	require.NoError(t, registry.EnsureDeleteEntry("file1"))
	require.NoError(t, registry.EnsureRenameEntry("file4", "file5"))
	require.NoError(t, registry.EnsureLinkEntry("file6", "file7"))
	checkEquality(t, mem, expected)

	// Open a read-only registry. All updates should fail.
	roRegistry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb", ReadOnly: true}
	require.NoError(t, roRegistry.Load())
	require.Error(t, roRegistry.SetFileEntry("file3", bazFileEntry))
	require.Error(t, roRegistry.EnsureDeleteEntry("file3"))
	require.Error(t, roRegistry.EnsureRenameEntry("file3", "file4"))
	require.Error(t, roRegistry.EnsureLinkEntry("file3", "file4"))
}
