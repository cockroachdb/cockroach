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
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
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
		require.NoError(t, registry.Close())
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
	// We need a second instance of the first two entries to make kr/pretty's diff algorithm happy.
	fooFileEntry2 :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	barFileEntry2 :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("bar")}

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
		if diff := pretty.Diff(registry.mu.entries, expected); diff != nil {
			t.Log(string(debug.Stack()))
			t.Fatalf("%s\n%v", strings.Join(diff, "\n"), registry.mu.entries)
		}
	}

	require.NoError(t, mem.MkdirAll("/mydb", 0755))
	registry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb"}
	require.NoError(t, registry.Load())
	require.Nil(t, registry.GetFileEntry("file1"))

	// {file1 => foo}
	require.NoError(t, registry.SetFileEntry("file1", fooFileEntry))
	expected["file1"] = fooFileEntry
	checkEquality()

	// {file1 => foo, file2 => bar}
	require.NoError(t, registry.SetFileEntry("file2", barFileEntry))
	expected["file2"] = barFileEntry
	checkEquality()

	// {file1 => foo, file3 => foo, file2 => bar}
	require.NoError(t, registry.MaybeCopyEntry("file1", "file3"))
	expected["file3"] = fooFileEntry2
	checkEquality()

	// {file3 => foo, file2 => bar}
	require.NoError(t, registry.MaybeDeleteEntry("file1"))
	delete(expected, "file1")
	checkEquality()

	// {file3 => foo, file2 => bar, file4 => bar}
	require.NoError(t, registry.MaybeLinkEntry("file2", "file4"))
	expected["file4"] = barFileEntry2
	checkEquality()

	// {file3 => foo, file4 => bar}
	require.NoError(t, registry.MaybeLinkEntry("file5", "file2"))
	delete(expected, "file2")
	checkEquality()

	// {file3 => foo}
	require.NoError(t, registry.MaybeCopyEntry("file7", "file4"))
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
	require.NoError(t, registry.MaybeCopyEntry("file4", "file5"))
	require.NoError(t, registry.MaybeLinkEntry("file6", "file7"))
	checkEquality()

	// Open a read-only registry. All updates should fail.
	roRegistry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb", ReadOnly: true}
	require.NoError(t, roRegistry.Load())
	require.Error(t, roRegistry.SetFileEntry("file3", bazFileEntry))
	require.Error(t, roRegistry.MaybeDeleteEntry("file3"))
	require.Error(t, roRegistry.MaybeCopyEntry("file3", "file4"))
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
	require.NoError(t, registry.writeToRegistryFile(&enginepb.RegistryUpdateBatch{
		Updates: []*enginepb.RegistryUpdate{
			{Filename: "test1", Entry: &enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte(nil)}},
			{Filename: "test2", Entry: &enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("foo")}},
		},
	}))
	require.NoError(t, registry.Close())

	// Create another pebble file registry to verify that the unencrypted file is elided on startup.
	registry2 := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry2.Load())
	require.NotContains(t, registry2.mu.entries, "test1")
	entry := registry2.mu.entries["test2"]
	require.NotNil(t, entry)
	require.Equal(t, entry.EncryptionSettings, []byte("foo"))
	require.NoError(t, registry2.Close())
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
		require.NoError(t, registry.writeToRegistryFile(&enginepb.RegistryUpdateBatch{
			Updates: []*enginepb.RegistryUpdate{
				{Filename: "foo", Entry: &enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}},
				{Filename: "bar", Entry: &enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("bar")}},
			},
		}))
		require.NoError(t, registry.Close())
	}

	// Create another registry and verify that the nonexistent `foo` file
	// entry is elided on startup.
	{
		registry := &PebbleFileRegistry{FS: mem}
		require.NoError(t, registry.Load())
		require.NotContains(t, registry.mu.entries, "foo")
		require.Contains(t, registry.mu.entries, "bar")
		require.NotNil(t, registry.mu.entries["bar"])
		require.Equal(t, []byte("bar"), registry.mu.entries["bar"].EncryptionSettings)
		require.NoError(t, registry.Close())
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

	registry3 := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry3.Load())
	for filename, entry := range files {
		require.Equal(t, entry, registry3.GetFileEntry(filename))
	}
	require.NoError(t, registry3.Close())
}

func TestFileRegistry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var buf bytes.Buffer
	fs := loggingFS{FS: vfs.NewMem(), w: &buf}
	var registry *PebbleFileRegistry

	datadriven.RunTest(t, testutils.TestDataPath(t, "file_registry"), func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()

		switch d.Cmd {
		case "check-no-registry-file":
			require.Nil(t, registry)
			registry = &PebbleFileRegistry{FS: fs}
			err := registry.CheckNoRegistryFile()
			registry = nil
			if err == nil {
				fmt.Fprintf(&buf, "OK\n")
			} else {
				fmt.Fprintf(&buf, "Error: %s\n", err)
			}
			return buf.String()
		case "close":
			require.NotNil(t, registry)
			require.NoError(t, registry.Close())
			registry = nil
			return buf.String()
		case "get":
			var filename string
			d.ScanArgs(t, "filename", &filename)
			entry := registry.GetFileEntry(filename)
			if entry == nil {
				return ""
			}
			return string(entry.EncryptionSettings)
		case "load":
			require.Nil(t, registry)
			registry = &PebbleFileRegistry{FS: fs}
			require.NoError(t, registry.Load())
			return buf.String()
		case "reset":
			require.Nil(t, registry)
			fs = loggingFS{FS: vfs.NewMem(), w: &buf}
			return ""
		case "set":
			var filename, settings string
			d.ScanArgs(t, "filename", &filename)
			d.ScanArgs(t, "settings", &settings)

			var entry *enginepb.FileEntry
			if settings != "" {
				entry = &enginepb.FileEntry{
					EnvType:            enginepb.EnvType_Data,
					EncryptionSettings: []byte(settings),
				}
			}
			require.NoError(t, registry.SetFileEntry(filename, entry))
			return buf.String()
		case "touch":
			for _, filename := range strings.Split(d.Input, "\n") {
				f, err := fs.Create(filename)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			return buf.String()
		default:
			panic("unrecognized command " + d.Cmd)
		}
	})
}

type loggingFS struct {
	vfs.FS
	w io.Writer
}

func (fs loggingFS) Create(name string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "create(%q)\n", name)
	f, err := fs.FS.Create(name)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Link(oldname, newname string) error {
	fmt.Fprintf(fs.w, "link(%q, %q)\n", oldname, newname)
	return fs.FS.Link(oldname, newname)
}

func (fs loggingFS) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	fmt.Fprintf(fs.w, "open(%q)\n", name)
	f, err := fs.FS.Open(name, opts...)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) OpenDir(name string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "open-dir(%q)\n", name)
	f, err := fs.FS.OpenDir(name)
	if err != nil {
		return nil, err
	}
	return loggingFile{f, name, fs.w}, nil
}

func (fs loggingFS) Remove(name string) error {
	fmt.Fprintf(fs.w, "remove(%q)\n", name)
	return fs.FS.Remove(name)
}

func (fs loggingFS) Rename(oldname, newname string) error {
	fmt.Fprintf(fs.w, "rename(%q, %q)\n", oldname, newname)
	return fs.FS.Rename(oldname, newname)
}

func (fs loggingFS) ReuseForWrite(oldname, newname string) (vfs.File, error) {
	fmt.Fprintf(fs.w, "reuseForWrite(%q, %q)\n", oldname, newname)
	f, err := fs.FS.ReuseForWrite(oldname, newname)
	if err == nil {
		f = loggingFile{f, newname, fs.w}
	}
	return f, err
}

func (fs loggingFS) Stat(path string) (os.FileInfo, error) {
	fmt.Fprintf(fs.w, "stat(%q)\n", path)
	return fs.FS.Stat(path)
}

func (fs loggingFS) MkdirAll(dir string, perm os.FileMode) error {
	fmt.Fprintf(fs.w, "mkdir-all(%q, %#o)\n", dir, perm)
	return fs.FS.MkdirAll(dir, perm)
}

func (fs loggingFS) Lock(name string) (io.Closer, error) {
	fmt.Fprintf(fs.w, "lock: %q\n", name)
	return fs.FS.Lock(name)
}

type loggingFile struct {
	vfs.File
	name string
	w    io.Writer
}

func (f loggingFile) Write(p []byte) (n int, err error) {
	fmt.Fprintf(f.w, "write(%q, <...%d bytes...>)\n", f.name, len(p))
	return f.File.Write(p)
}

func (f loggingFile) Close() error {
	fmt.Fprintf(f.w, "close(%q)\n", f.name)
	return f.File.Close()
}

func (f loggingFile) Sync() error {
	fmt.Fprintf(f.w, "sync(%q)\n", f.name)
	return f.File.Sync()
}
