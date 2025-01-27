// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fs

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/disk"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/cockroachdb/pebble/vfs/vfstest"
)

// RWMode is an enum for whether a filesystem is read-write or read-only.
type RWMode int8

const (
	// ReadWrite indicates a filesystem may be both read from and written to.
	ReadWrite RWMode = iota
	// ReadOnly indicates a filesystem is read-only.
	ReadOnly
)

// Default for MaxSyncDuration below.
var maxSyncDurationDefault = envutil.EnvOrDefaultDuration("COCKROACH_ENGINE_MAX_SYNC_DURATION_DEFAULT", 20*time.Second)

// MaxSyncDuration is the threshold above which an observed engine sync duration
// triggers either a warning or a fatal error.
var MaxSyncDuration = settings.RegisterDurationSetting(
	settings.SystemVisible,
	"storage.max_sync_duration",
	"maximum duration for disk operations; any operations that take longer"+
		" than this setting trigger a warning log entry or process crash",
	maxSyncDurationDefault,
	settings.WithPublic)

// MaxSyncDurationFatalOnExceeded governs whether disk stalls longer than
// MaxSyncDuration fatal the Cockroach process. Defaults to true.
var MaxSyncDurationFatalOnExceeded = settings.RegisterBoolSetting(
	settings.ApplicationLevel, // used for temp storage in virtual cluster servers
	"storage.max_sync_duration.fatal.enabled",
	"if true, fatal the process when a disk operation exceeds storage.max_sync_duration",
	true,
	settings.WithPublic)

// InitEnvsFromStoreSpecs constructs Envs for all the provided store specs.
func InitEnvsFromStoreSpecs(
	ctx context.Context,
	specs []base.StoreSpec,
	rw RWMode,
	stickyRegistry StickyRegistry,
	diskWriteStats disk.WriteStatsManager,
) (Envs, error) {
	envs := make(Envs, len(specs))
	for i := range specs {
		var err error
		envs[i], err = InitEnvFromStoreSpec(ctx, specs[i], rw, stickyRegistry, diskWriteStats)
		if err != nil {
			envs.CloseAll()
			return nil, err
		}
	}
	return envs, nil
}

// Envs holds a set of Env.
type Envs []*Env

// CloseAll closes all the envs.
func (e Envs) CloseAll() {
	for i := range e {
		if e[i] != nil {
			e[i].Close()
		}
	}
}

// InitEnvFromStoreSpec constructs a new Env from a store spec. See the
// documentation for InitEnv for more details.
//
// stickyRegistry may be nil iff the spec's StickyVFSID field is unset.
func InitEnvFromStoreSpec(
	ctx context.Context,
	spec base.StoreSpec,
	rw RWMode,
	stickyRegistry StickyRegistry,
	diskWriteStats disk.WriteStatsManager,
) (*Env, error) {
	fs := vfs.Default
	dir := spec.Path
	if spec.InMemory {
		if spec.StickyVFSID != "" {
			if stickyRegistry == nil {
				return nil, errors.Errorf("missing StickyVFSRegistry")
			}
			fs = stickyRegistry.Get(spec.StickyVFSID)
		} else {
			fs = vfs.NewMem()
		}
	}
	return InitEnv(ctx, fs, dir, EnvConfig{
		RW:                rw,
		EncryptionOptions: spec.EncryptionOptions,
	}, diskWriteStats)
}

// EnvConfig provides additional configuration settings for Envs.
type EnvConfig struct {
	RW                RWMode
	EncryptionOptions *storagepb.EncryptionOptions
}

// InitEnv initializes a new virtual filesystem environment.
//
// If successful the returned Env is returned with 1 reference. It must be
// closed to avoid leaking goroutines and other resources.
func InitEnv(
	ctx context.Context, fs vfs.FS, dir string, cfg EnvConfig, diskWriteStats disk.WriteStatsManager,
) (*Env, error) {
	e := &Env{Dir: dir, UnencryptedFS: fs, rw: cfg.RW}
	e.refs.Store(1)
	// Defer the Close(). The Close() will only actually release resources if we
	// error out early without adding an additional ref just before returning.
	defer e.Close()

	// In CrdbTestBuilds, check during Close that all files are closed. Leaked
	// files can leak goroutines. If there are any, we'll panic and include the
	// stack traces taken when the files were opened.
	if buildutil.CrdbTestBuild {
		var dumpOpenFileStacks func(io.Writer)
		e.UnencryptedFS, dumpOpenFileStacks = vfstest.WithOpenFileTracking(e.UnencryptedFS)
		e.onClose = append(e.onClose, func() {
			var buf bytes.Buffer
			dumpOpenFileStacks(&buf)
			if buf.Len() > 0 {
				panic(errors.AssertionFailedf("during (fs.Env).Close there remain open files; all files must be closed before closing the Env\n\n%s", buf.String()))
			}
		})
	}

	// Set disk-health check interval to min(5s, maxSyncDurationDefault). This
	// is mostly to ease testing; the default of 5s is too infrequent to test
	// conveniently. See the disk-stalled roachtest for an example of how this
	// is used.
	diskHealthCheckInterval := 5 * time.Second
	if diskHealthCheckInterval.Seconds() > maxSyncDurationDefault.Seconds() {
		diskHealthCheckInterval = maxSyncDurationDefault
	}

	// Create the directory if it doesn't already exist. We need to acquire a
	// database lock down below, which requires the directory already exist.
	if cfg.RW == ReadWrite {
		if err := e.UnencryptedFS.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Create the stats collector. This has to be done after the directory has
	// been created above.
	var statsCollector *vfs.DiskWriteStatsCollector
	var err error
	if diskWriteStats != nil && dir != "" {
		if statsCollector, err = diskWriteStats.GetOrCreateCollector(dir); err != nil {
			return nil, errors.Wrap(err, "retrieving stats collector")
		}
	}

	// Instantiate a file system with disk health checking enabled. This FS
	// wraps the filesystem with a layer that times all write-oriented
	// operations. When a slow disk operation occurs, Env.onDiskSlow is invoked
	// (which in turn will invoke Env.onDiskSlowFunc if set).
	e.UnencryptedFS, e.diskHealthChecksCloser = vfs.WithDiskHealthChecks(
		e.UnencryptedFS, diskHealthCheckInterval, statsCollector, e.onDiskSlow)
	// If we encounter ENOSPC, exit with an informative exit code.
	e.UnencryptedFS = vfs.OnDiskFull(e.UnencryptedFS, func() {
		exit.WithCode(exit.DiskFull())
	})

	// Acquire the database lock in the store directory to ensure that no other
	// process is simultaneously accessing the same store. We manually acquire
	// the database lock here (rather than allowing Pebble to acquire the lock)
	// so that we hold the lock when we initialize encryption-at-rest subsystem.
	e.DirectoryLock, err = pebble.LockDirectory(dir, e.UnencryptedFS)
	if err != nil {
		return nil, err
	}

	// Validate and configure encryption-at-rest. If no encryption-at-rest
	// configuration was provided, resolveEncryptedEnvOptions will validate that
	// there is no file registry.
	e.Registry, e.Encryption, err = resolveEncryptedEnvOptions(
		ctx, e.UnencryptedFS, dir, cfg.EncryptionOptions, cfg.RW)
	if err != nil {
		return nil, err
	}
	if e.Encryption != nil {
		e.defaultFS = e.Encryption.FS
	} else {
		e.defaultFS = e.UnencryptedFS
	}
	e.refs.Add(1)
	return e, nil
}

// Env is an abstraction over the storage environment, including the virtual
// file system and related configuration.
type Env struct {
	// Dir is the path to the root directory of the environment.
	Dir string
	// UnencryptedFS is a VFS without any encryption-at-rest enabled. Some files
	// are stored unencrypted (eg, the 'min version' file used to persist the
	// cluster version).
	UnencryptedFS vfs.FS
	// DirectoryLock is a file lock preventing other processes from opening the
	// database or encryption-at-rest registry within this data directory.
	DirectoryLock *pebble.Lock
	// Registry is non-nil if encryption-at-rest has ever been enabled on the
	// store. The registry maintains a mapping of all encrypted keys and the
	// corresponding data key with which they're encrypted.
	Registry *FileRegistry
	// Encryption is non-nil if encryption-at-rest has ever been enabled on
	// the store. It provides access to encryption-at-rest stats, etc.
	Encryption *EncryptionEnv

	// defaultFS is the primary VFS that most users should use. If
	// encryption-at-rest is enabled, this VFS will handle transparently
	// encrypting and decrypting as necessary. When the Env is used as an
	// implementation of a VFS, this is the underlying VFS used.
	defaultFS              vfs.FS
	rw                     RWMode
	diskHealthChecksCloser io.Closer
	onClose                []func()
	onDiskSlowFunc         atomic.Pointer[func(vfs.DiskSlowInfo)]
	refs                   atomic.Int32
}

// Ref adds an additional reference to the Env. Each reference requires a
// corresponding call to Close.
func (e *Env) Ref() {
	e.refs.Add(1)
}

// Assert that Env implements vfs.FS.
var _ vfs.FS = (*Env)(nil)

// IsReadOnly returns true if the environment is opened in read-only mode.
func (e *Env) IsReadOnly() bool {
	return e.rw == ReadOnly
}

// RWMode returns the read-write mode of the environment.
func (e *Env) RWMode() RWMode { return e.rw }

// RegisterOnDiskSlow configures the Env to call the provided function when a
// disk operation is slow.
func (e *Env) RegisterOnDiskSlow(fn func(vfs.DiskSlowInfo)) {
	e.onDiskSlowFunc.Store(&fn)
}

// Close closes all the open resources associated with the Env. If the Env had
// an associated file registry (eg, encryption-at-rest is or was configured) it
// will be closed. All disk-health monitoring goroutines will also exit.
func (e *Env) Close() {
	if v := e.refs.Add(-1); v > 0 {
		// Refs remain.
		return
	}

	var err error
	if e.Encryption != nil {
		err = errors.CombineErrors(err, e.Encryption.Closer.Close())
	}
	if e.Registry != nil {
		err = errors.CombineErrors(err, e.Registry.Close())
	}
	if e.DirectoryLock != nil {
		err = errors.CombineErrors(err, e.DirectoryLock.Close())
	}
	if e.diskHealthChecksCloser != nil {
		err = errors.CombineErrors(err, e.diskHealthChecksCloser.Close())
	}
	if err != nil {
		panic(err)
	}
	for _, closeFn := range e.onClose {
		closeFn()
	}
}

// Unwrap is part of the vfs.FS interface.
func (e *Env) Unwrap() vfs.FS {
	// We don't want to expose the unencrypted FS.
	return nil
}

func (e *Env) onDiskSlow(info vfs.DiskSlowInfo) {
	if fn := e.onDiskSlowFunc.Load(); fn != nil {
		(*fn)(info)
	}
}

// InMemory constructs a new in-memory environment.
func InMemory() *Env {
	e, err := InitEnv(context.Background(), vfs.NewMem(), "" /* dir */, EnvConfig{}, nil /* diskWriteStats */)
	// In practice InitEnv is infallible with this configuration.
	if err != nil {
		panic(err)
	}
	return e
}

// MustInitPhysicalTestingEnv initializes an Env that reads/writees from the
// real physical filesystem in the provided directory, without
// encryption-at-rest. Since this function ignores the possibility of
// encryption-at-rest, it should only be used as a testing convenience.
func MustInitPhysicalTestingEnv(dir string) *Env {
	e, err := InitEnv(context.Background(), vfs.Default, dir, EnvConfig{}, nil /* diskWriteStats */)
	if err != nil {
		panic(err)
	}
	return e
}

func errReadOnly() error {
	return errors.New("filesystem is read-only")
}

// Create creates the named file for reading and writing. If a file
// already exists at the provided name, it's removed first ensuring the
// resulting file descriptor points to a new inode.
func (e *Env) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	if e.rw == ReadOnly {
		return nil, errReadOnly()
	}
	return e.defaultFS.Create(name, category)
}

// Link creates newname as a hard link to the oldname file.
func (e *Env) Link(oldname, newname string) error {
	if e.rw == ReadOnly {
		return errReadOnly()
	}
	return e.defaultFS.Link(oldname, newname)
}

// Open opens the named file for reading. openOptions provides
func (e *Env) Open(name string, opts ...vfs.OpenOption) (vfs.File, error) {
	return e.defaultFS.Open(name, opts...)
}

// OpenReadWrite opens the named file for reading and writing. If the file
// does not exist, it is created.
func (e *Env) OpenReadWrite(
	name string, category vfs.DiskWriteCategory, opts ...vfs.OpenOption,
) (vfs.File, error) {
	if e.rw == ReadOnly {
		return nil, errReadOnly()
	}
	return e.defaultFS.OpenReadWrite(name, category, opts...)
}

// OpenDir opens the named directory for syncing.
func (e *Env) OpenDir(name string) (vfs.File, error) {
	return e.defaultFS.OpenDir(name)
}

// Remove removes the named file or directory.
func (e *Env) Remove(name string) error {
	if e.rw == ReadOnly {
		return errReadOnly()
	}
	return e.defaultFS.Remove(name)
}

// RemoveAll removes the named file or directory and any children it
// contains. It removes everything it can but returns the first error it
// encounters.
func (e *Env) RemoveAll(name string) error {
	if e.rw == ReadOnly {
		return errReadOnly()
	}
	return e.defaultFS.RemoveAll(name)
}

// Rename renames a file. It overwrites the file at newname if one exists,
// the same as os.Rename.
func (e *Env) Rename(oldname, newname string) error {
	if e.rw == ReadOnly {
		return errReadOnly()
	}
	return e.defaultFS.Rename(oldname, newname)

}

// ReuseForWrite attempts to reuse the file with oldname by renaming it to newname and opening
// it for writing without truncation. It is acceptable for the implementation to choose not
// to reuse oldname, and simply create the file with newname -- in this case the implementation
// should delete oldname. If the caller calls this function with an oldname that does not exist,
// the implementation may return an error.
func (e *Env) ReuseForWrite(
	oldname, newname string, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	if e.rw == ReadOnly {
		return nil, errReadOnly()
	}
	return e.defaultFS.ReuseForWrite(oldname, newname, category)
}

// MkdirAll creates a directory and all necessary parents. The permission
// bits perm have the same semantics as in os.MkdirAll. If the directory
// already exists, MkdirAll does nothing and returns nil.
func (e *Env) MkdirAll(dir string, perm os.FileMode) error {
	if e.rw == ReadOnly {
		return errReadOnly()
	}
	return e.defaultFS.MkdirAll(dir, perm)
}

// Lock locks the given file, creating the file if necessary, and
// truncating the file if it already exists. The lock is an exclusive lock
// (a write lock), but locked files should neither be read from nor written
// to. Such files should have zero size and only exist to co-ordinate
// ownership across processes.
//
// A nil Closer is returned if an error occurred. Otherwise, close that
// Closer to release the lock.
//
// On Linux and OSX, a lock has the same semantics as fcntl(2)'s advisory
// locks. In particular, closing any other file descriptor for the same
// file will release the lock prematurely.
//
// Attempting to lock a file that is already locked by the current process
// returns an error and leaves the existing lock untouched.
//
// Lock is not yet implemented on other operating systems, and calling it
// will return an error.
func (e *Env) Lock(name string) (io.Closer, error) {
	return e.defaultFS.Lock(name)
}

// List returns a listing of the given directory. The names returned are
// relative to dir.
func (e *Env) List(dir string) ([]string, error) {
	return e.defaultFS.List(dir)
}

// Stat returns an os.FileInfo describing the named file.
func (e *Env) Stat(name string) (vfs.FileInfo, error) {
	return e.defaultFS.Stat(name)
}

// PathBase returns the last element of path. Trailing path separators are
// removed before extracting the last element. If the path is empty, PathBase
// returns ".".  If the path consists entirely of separators, PathBase returns a
// single separator.
func (e *Env) PathBase(path string) string {
	return e.defaultFS.PathBase(path)
}

// PathJoin joins any number of path elements into a single path, adding a
// separator if necessary.
func (e *Env) PathJoin(elem ...string) string {
	return e.defaultFS.PathJoin(elem...)
}

// PathDir returns all but the last element of path, typically the path's directory.
func (e *Env) PathDir(path string) string {
	return e.defaultFS.PathDir(path)
}

// GetDiskUsage returns disk space statistics for the filesystem where
// path is any file or directory within that filesystem.
func (e *Env) GetDiskUsage(path string) (vfs.DiskUsage, error) {
	return e.defaultFS.GetDiskUsage(path)
}

// CreateWithSync creates a file wrapped with logic to periodically sync
// whenever more than bytesPerSync bytes accumulate. This syncing does not
// provide any persistency guarantees, but can prevent latency spikes.
func CreateWithSync(
	fs vfs.FS, name string, bytesPerSync int, category vfs.DiskWriteCategory,
) (vfs.File, error) {
	f, err := fs.Create(name, category)
	if err != nil {
		return nil, err
	}
	return vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: bytesPerSync}), nil
}

// WriteFile writes data to a file named by filename.
func WriteFile(fs vfs.FS, filename string, data []byte, category vfs.DiskWriteCategory) error {
	f, err := fs.Create(filename, category)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// ReadFile reads data from a file named by filename.
func ReadFile(fs vfs.FS, filename string) ([]byte, error) {
	file, err := fs.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	return io.ReadAll(file)
}
