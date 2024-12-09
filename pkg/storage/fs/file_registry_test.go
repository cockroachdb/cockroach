// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
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
		registry := &FileRegistry{FS: mem, DBDir: tc.dbDir}
		require.NoError(t, registry.Load(context.Background()))
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
			f, err := mem.Create(path, UnspecifiedWriteCategory)
			require.NoError(t, err)
			require.NoError(t, f.Close())
		}

		registry := &FileRegistry{FS: mem, DBDir: "/mydb"}
		require.NoError(t, registry.Load(context.Background()))
		registry.writeMu.Lock()
		defer registry.writeMu.Unlock()
		if diff := pretty.Diff(registry.writeMu.mu.entries, expected); diff != nil {
			t.Log(debugutil.Stack())
			t.Fatalf("%s\n%v", strings.Join(diff, "\n"), registry.writeMu.mu.entries)
		}
	}

	require.NoError(t, mem.MkdirAll("/mydb", 0755))
	registry := &FileRegistry{FS: mem, DBDir: "/mydb"}
	require.NoError(t, registry.Load(context.Background()))
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
	roRegistry := &FileRegistry{FS: mem, DBDir: "/mydb", ReadOnly: true}
	require.NoError(t, roRegistry.Load(context.Background()))
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
	require.NoError(t, checkNoRegistryFile(mem, "" /* dbDir */))
	registry := &FileRegistry{FS: mem}
	require.NoError(t, registry.Load(context.Background()))
	require.NoError(t, registry.SetFileEntry("/foo", fileEntry))
	require.Error(t, checkNoRegistryFile(mem, "" /* dbDir */))
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
		f, err := mem.Create(name, UnspecifiedWriteCategory)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	registry := &FileRegistry{FS: mem}
	require.NoError(t, registry.Load(context.Background()))
	require.NoError(t, registry.writeToRegistryFileLocked(&enginepb.RegistryUpdateBatch{
		Updates: []*enginepb.RegistryUpdate{
			{Filename: "test1", Entry: &enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte(nil)}},
			{Filename: "test2", Entry: &enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("foo")}},
		},
	}))
	require.NoError(t, registry.Close())

	// Create another pebble file registry to verify that the unencrypted file is elided on startup.
	registry2 := &FileRegistry{FS: mem}
	require.NoError(t, registry2.Load(context.Background()))
	require.NotContains(t, registry2.writeMu.mu.entries, "test1")
	entry := registry2.writeMu.mu.entries["test2"]
	require.NotNil(t, entry)
	require.Equal(t, entry.EncryptionSettings, []byte("foo"))
	require.NoError(t, registry2.Close())
}

func TestFileRegistryElideNonexistent(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mem := vfs.NewMem()
	f, err := mem.Create("bar", UnspecifiedWriteCategory)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	{
		registry := &FileRegistry{FS: mem}
		require.NoError(t, registry.Load(context.Background()))
		require.NoError(t, registry.writeToRegistryFileLocked(&enginepb.RegistryUpdateBatch{
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
		registry := &FileRegistry{FS: mem}
		require.NoError(t, registry.Load(context.Background()))
		require.NotContains(t, registry.writeMu.mu.entries, "foo")
		require.Contains(t, registry.writeMu.mu.entries, "bar")
		require.NotNil(t, registry.writeMu.mu.entries["bar"])
		require.Equal(t, []byte("bar"), registry.writeMu.mu.entries["bar"].EncryptionSettings)
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
		f, err := mem.Create(name, UnspecifiedWriteCategory)
		require.NoError(t, err)
		require.NoError(t, f.Close())
	}

	// Create a file registry and add entries for a few files.
	require.NoError(t, checkNoRegistryFile(mem, "" /* dbDir */))
	registry1 := &FileRegistry{FS: mem}
	require.NoError(t, registry1.Load(context.Background()))
	for filename, entry := range files {
		require.NoError(t, registry1.SetFileEntry(filename, entry))
	}
	require.NoError(t, registry1.Close())

	// Create another file registry and load in the registry file.
	// It should use the monolithic one.
	registry2 := &FileRegistry{FS: mem}
	require.NoError(t, registry2.Load(context.Background()))
	for filename, entry := range files {
		require.Equal(t, entry, registry2.GetFileEntry(filename))
	}
	require.NoError(t, registry2.Close())

	registry3 := &FileRegistry{FS: mem}
	require.NoError(t, registry3.Load(context.Background()))
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
	var registry *FileRegistry

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "file_registry"), func(t *testing.T, d *datadriven.TestData) string {
		buf.Reset()

		switch d.Cmd {
		case "check-no-registry-file":
			require.Nil(t, registry)
			if err := checkNoRegistryFile(fs, "" /* dbDir */); err == nil {
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
			var softMaxSize int64
			d.MaybeScanArgs(t, "soft-max-size", &softMaxSize)
			require.Nil(t, registry)
			registry = &FileRegistry{FS: fs, SoftMaxSize: softMaxSize}
			require.NoError(t, registry.Load(context.Background()))
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
				f, err := fs.Create(filename, UnspecifiedWriteCategory)
				require.NoError(t, err)
				require.NoError(t, f.Close())
			}
			return buf.String()
		case "list":
			type fileEntry struct {
				name  string
				entry *enginepb.FileEntry
			}
			var fileEntries []fileEntry
			for name, entry := range registry.List() {
				fileEntries = append(fileEntries, fileEntry{
					name:  name,
					entry: entry,
				})
			}
			sort.Slice(fileEntries, func(i, j int) bool {
				return fileEntries[i].name < fileEntries[j].name
			})
			var b bytes.Buffer
			for _, fe := range fileEntries {
				b.WriteString(fmt.Sprintf(
					"name=%s,type=%s,settings=%s\n",
					fe.name, fe.entry.EnvType.String(), string(fe.entry.EncryptionSettings),
				))
			}
			return b.String()
		default:
			panic("unrecognized command " + d.Cmd)
		}
	})
}

type loggingFS struct {
	vfs.FS
	w io.Writer
}

func (fs loggingFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	fmt.Fprintf(fs.w, "create(%q)\n", name)
	f, err := fs.FS.Create(name, category)
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

func (fs loggingFS) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	fmt.Fprintf(fs.w, "reuseForWrite(%q, %q)\n", oldname, newname)
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err == nil {
		f = loggingFile{f, newname, fs.w}
	}
	return f, err
}

func (fs loggingFS) Stat(path string) (vfs.FileInfo, error) {
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

// fileRegistryEntryChecked creates new files and corresponding entries in the
// file registry and checks that the expected entries are in the registry.
type fileRegistryEntryChecker struct {
	t   *testing.T
	dir string
	fs  vfs.FS

	dbDir              vfs.File
	encryptionSettings []byte
	numAddedEntries    int
}

func makeFileRegistryEntryChecker(t *testing.T, fs vfs.FS, dir string) *fileRegistryEntryChecker {
	dbDir, err := fs.OpenDir(dir)
	require.NoError(t, err)
	return &fileRegistryEntryChecker{
		t:     t,
		dir:   dir,
		fs:    fs,
		dbDir: dbDir,
		// Large settings slice, so that the test rolls over registry files quickly.
		encryptionSettings: make([]byte, 1<<20),
	}
}

func (c *fileRegistryEntryChecker) addEntry(r *FileRegistry) {
	filename := fmt.Sprintf("%04d", c.numAddedEntries)
	// Create a file for this added entry so that it doesn't get cleaned up
	// when we reopen the file registry.
	f, err := c.fs.Create(c.fs.PathJoin(c.dir, filename), UnspecifiedWriteCategory)
	require.NoError(c.t, err)
	require.NoError(c.t, f.Sync())
	require.NoError(c.t, f.Close())
	require.NoError(c.t, c.dbDir.Sync())
	fileEntry := &enginepb.FileEntry{EnvType: enginepb.EnvType_Data}
	fileEntry.EncryptionSettings = append(c.encryptionSettings, []byte(filename)...)
	require.NoError(c.t, r.SetFileEntry(filename, fileEntry))
	c.numAddedEntries++
}

// checkEntries checks that the entries we have added are in the file registry
// and there isn't an additional entry.
func (c *fileRegistryEntryChecker) checkEntries(r *FileRegistry) {
	for i := 0; i < c.numAddedEntries; i++ {
		filename := fmt.Sprintf("%04d", i)
		entry := r.GetFileEntry(filename)
		require.NotNil(c.t, entry)
		require.Equal(c.t, filename, string(entry.EncryptionSettings[len(entry.EncryptionSettings)-4:]))
	}
	// Does not have an additional entry.
	filename := fmt.Sprintf("%04d", c.numAddedEntries)
	entry := r.GetFileEntry(filename)
	require.Nil(c.t, entry)
}

func TestFileRegistryRollover(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const dir = "/mydb"
	mem := vfs.NewMem()
	require.NoError(t, mem.MkdirAll(dir, 0755))
	registry := &FileRegistry{
		FS:    mem,
		DBDir: dir,
		// SoftMaxSize configures the registry size at which the registry will
		// first roll over to a new registry size. Setting a lower size reduces
		// the memory footprint and runtime of this function.
		SoftMaxSize: 32 << 10, /* 32 KiB */
	}
	require.NoError(t, registry.Load(context.Background()))

	// All the registry files created so far. Some may have been subsequently
	// deleted.
	var registryFiles []string
	accumRegistryFiles := func() {
		registry.writeMu.Lock()
		defer registry.writeMu.Unlock()
		n := len(registryFiles)
		if registry.writeMu.registryFilename != "" &&
			(n == 0 || registry.writeMu.registryFilename != registryFiles[n-1]) {
			registryFiles = append(registryFiles, registry.writeMu.registryFilename)
			n++
		}
	}
	registryChecker := makeFileRegistryEntryChecker(t, mem, dir)
	for {
		created := len(registryFiles)
		accumRegistryFiles()
		if created != len(registryFiles) {
			// Rolled over.
			registryChecker.checkEntries(registry)
		}
		// Rollover a few times.
		if len(registryFiles) == 4 {
			break
		}
		registryChecker.addEntry(registry)
	}
	require.NoError(t, registry.Close())
	registry = &FileRegistry{FS: mem, DBDir: dir}
	require.NoError(t, registry.Load(context.Background()))
	// Check added entries again.
	registryChecker.checkEntries(registry)
	require.NoError(t, registry.Close())
}

// TestFileRegistryKeepOldFilesAndSync tests that the file registry keeps
// older registry files as configured, and correctly syncs writes to disk.
func TestFileRegistryKeepOldFilesAndSync(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderRace(t) // Slow under race.

	const dir = "/mydb"
	mem := vfs.NewCrashableMem()
	{
		require.NoError(t, mem.MkdirAll(dir, 0755))
		// Sync the root dir so that /mydb does not vanish later.
		dir, err := mem.OpenDir("/")
		require.NoError(t, err)
		require.NoError(t, dir.Sync())
		dir.Close()
	}

	// Keep 2 old file registries.
	var numOldRegistryFiles = 3
	registry := &FileRegistry{
		FS:                  mem,
		DBDir:               dir,
		NumOldRegistryFiles: numOldRegistryFiles,
		SoftMaxSize:         1024,
	}
	require.NoError(t, registry.Load(context.Background()))

	// All the registry files created so far. Some may have been subsequently
	// deleted.
	var registryFiles []string
	accumRegistryFiles := func(forceCheck bool) (totalCreated int) {
		registry.writeMu.Lock()
		defer registry.writeMu.Unlock()
		n := len(registryFiles)
		doCheck := forceCheck
		if registry.writeMu.registryFilename != "" &&
			(n == 0 || registry.writeMu.registryFilename != registryFiles[n-1]) {
			registryFiles = append(registryFiles, registry.writeMu.registryFilename)
			n++
			doCheck = true
		}
		if doCheck {
			numObsolete := len(registryFiles) - 1
			if numObsolete > numOldRegistryFiles {
				numObsolete = numOldRegistryFiles
			}
			// The obsolete files are what we expect them to be.
			require.Equal(t, numObsolete, len(registry.writeMu.obsoleteRegistryFiles))
			var expectedFiles []string
			for i := 0; i < numObsolete; i++ {
				require.Equal(t, registryFiles[n-2-i], registry.writeMu.obsoleteRegistryFiles[numObsolete-1-i])
				expectedFiles = append(expectedFiles, registryFiles[n-2-i])
			}
			expectedFiles = append(expectedFiles, registryFiles[n-1])
			// Also check that it matches what is in the filesystem.
			lsFiles, err := registry.FS.List(dir)
			require.NoError(t, err)
			var foundFiles []string
			for _, f := range lsFiles {
				f = registry.FS.PathBase(f)
				if strings.HasPrefix(f, registryFilenameBase) {
					foundFiles = append(foundFiles, f)
				}
			}
			require.ElementsMatch(t, expectedFiles, foundFiles)
		}
		return len(registryFiles)
	}
	registryChecker := makeFileRegistryEntryChecker(t, mem, dir)
	totalCreated := 0
	for {
		created := accumRegistryFiles(false)
		if created != totalCreated {
			registryChecker.checkEntries(registry)
		}
		totalCreated = created
		// Go over the threshold of old registry files to keep, so we exercise
		// cleanup logic a few times.
		if totalCreated > numOldRegistryFiles+3 {
			break
		}
		registryChecker.addEntry(registry)
	}
	// Take a crash-consistent snapshot.
	crashFS := mem.CrashClone(vfs.CrashCloneCfg{})
	// Add another entry, that will be deliberately lost.
	registryChecker.addEntry(registry)
	registryChecker.checkEntries(registry)
	require.NoError(t, registry.Close())

	numAddedEntries := registryChecker.numAddedEntries
	registryChecker = makeFileRegistryEntryChecker(t, crashFS, dir)
	// Remove the lost entry from what we check.
	registryChecker.numAddedEntries = numAddedEntries - 1

	// Keep no old registry files.
	numOldRegistryFiles = 0
	registry = &FileRegistry{
		FS:                  crashFS,
		DBDir:               dir,
		NumOldRegistryFiles: numOldRegistryFiles,
		SoftMaxSize:         1024,
	}
	require.NoError(t, registry.Load(context.Background()))
	// Force check that the old registry files are gone.
	accumRegistryFiles(true)
	// Check added entries again.
	registryChecker.checkEntries(registry)
	require.NoError(t, registry.Close())

	// Another load, with a different NumOldRegistryFiles, just for fun.
	numOldRegistryFiles = 1
	registry = &FileRegistry{FS: crashFS, DBDir: dir, NumOldRegistryFiles: numOldRegistryFiles}
	require.NoError(t, registry.Load(context.Background()))
	registryChecker.checkEntries(registry)
}

func TestFileRegistryBlockedWriteAllowsRead(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	mem := vfs.NewMem()
	fs := &BlockingWriteFSForTesting{FS: mem}

	registry := &FileRegistry{FS: fs}
	require.NoError(t, registry.Load(context.Background()))
	fileEntry := &enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("foo")}
	require.NoError(t, registry.SetFileEntry("test1", fileEntry))
	fs.Block()
	go func() {
		require.NoError(t, registry.SetFileEntry("test2", fileEntry))
	}()
	time.Sleep(time.Millisecond)
	require.Equal(t, fileEntry, registry.GetFileEntry("test1"))
	snap := registry.GetRegistrySnapshot()
	require.Equal(t, 1, len(snap.Files))
	require.Equal(t, 1, len(registry.List()))
	fs.WaitForBlockAndUnblock()
	require.NoError(t, registry.Close())
}
