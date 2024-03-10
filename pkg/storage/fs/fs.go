// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fs

import (
	"context"
	"io"
	"os"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli/exit"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
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
	ctx context.Context, specs []base.StoreSpec, rw RWMode, stickyRegistry StickyRegistry,
) (Envs, error) {
	envs := make(Envs, len(specs))
	for i := range specs {
		var err error
		envs[i], err = InitEnvFromStoreSpec(ctx, specs[i], rw, stickyRegistry)
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
	ctx context.Context, spec base.StoreSpec, rw RWMode, stickyRegistry StickyRegistry,
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
	})
}

// EnvConfig provides additional configuration settings for Envs.
type EnvConfig struct {
	RW                RWMode
	EncryptionOptions []byte
}

// InitEnv initializes a new virtual filesystem environment.
//
// If successful the returned Env is returned with 1 reference. It must be
// closed to avoid leaking goroutines and other resources.
func InitEnv(ctx context.Context, fs vfs.FS, dir string, cfg EnvConfig) (*Env, error) {
	e := &Env{Dir: dir, UnencryptedFS: fs, rw: cfg.RW}
	e.refs.Store(1)
	// Defer the Close(). The Close() will only actually release resources if we
	// error out early without adding an additional ref just before returning.
	defer e.Close()

	// Set disk-health check interval to min(5s, maxSyncDurationDefault). This
	// is mostly to ease testing; the default of 5s is too infrequent to test
	// conveniently. See the disk-stalled roachtest for an example of how this
	// is used.
	diskHealthCheckInterval := 5 * time.Second
	if diskHealthCheckInterval.Seconds() > maxSyncDurationDefault.Seconds() {
		diskHealthCheckInterval = maxSyncDurationDefault
	}

	// Instantiate a file system with disk health checking enabled. This FS
	// wraps the filesystem with a layer that times all write-oriented
	// operations. When a slow disk operation occurs, Env.onDiskSlow is invoked
	// (which in turn will invoke Env.onDiskSlowFunc if set).
	e.UnencryptedFS, e.diskHealthChecksCloser = vfs.WithDiskHealthChecks(
		e.UnencryptedFS, diskHealthCheckInterval, nil /* statsCollector */, e.onDiskSlow)
	// If we encounter ENOSPC, exit with an informative exit code.
	e.UnencryptedFS = vfs.OnDiskFull(e.UnencryptedFS, func() {
		exit.WithCode(exit.DiskFull())
	})

	// Create the directory if it doesn't already exist. We need to acquire a
	// database lock down below, which requires the directory already exist.
	if cfg.RW == ReadWrite {
		if err := e.UnencryptedFS.MkdirAll(dir, os.ModePerm); err != nil {
			return nil, err
		}
	}

	// Acquire the database lock in the store directory to ensure that no other
	// process is simultaneously accessing the same store. We manually acquire
	// the database lock here (rather than allowing Pebble to acquire the lock)
	// so that we hold the lock when we initialize encryption-at-rest subsystem.
	var err error
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
		e.DefaultFS = e.Encryption.FS
	} else {
		e.DefaultFS = e.UnencryptedFS
	}
	e.refs.Add(1)
	return e, nil
}

// Env is an abstraction over the storage environment, including the virtual
// file system and related configuration.
type Env struct {
	// Dir is the path to the root directory of the environment.
	Dir string
	// DefaultFS is the primary VFS that most users should use. If
	// encryption-at-rest is enabled, this VFS will handle transparently
	// encrypting and decrypting as necessary.
	DefaultFS vfs.FS
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

	rw                     RWMode
	diskHealthChecksCloser io.Closer
	onDiskSlowFunc         atomic.Pointer[func(vfs.DiskSlowInfo)]
	refs                   atomic.Int32
}

// Ref adds an additional reference to the Env. Each reference requires a
// corresponding call to Close.
func (e *Env) Ref() {
	e.refs.Add(1)
}

// IsReadOnly returns true if the environment is opened in read-only mode.
func (e *Env) IsReadOnly() bool {
	// TODO(jackson): If `Env` itself implement vfs.FS (passing through to
	// DefaultFS), we could enforce the read-only restriction.
	return e.rw == ReadOnly
}

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
}

func (e *Env) onDiskSlow(info vfs.DiskSlowInfo) {
	if fn := e.onDiskSlowFunc.Load(); fn != nil {
		(*fn)(info)
	}
}

// InMemory constructs a new in-memory environment.
func InMemory() *Env {
	e, err := InitEnv(context.Background(), vfs.NewMem(), "" /* dir */, EnvConfig{})
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
	e, err := InitEnv(context.Background(), vfs.Default, dir, EnvConfig{})
	if err != nil {
		panic(err)
	}
	return e
}

// CreateWithSync creates a file wrapped with logic to periodically sync
// whenever more than bytesPerSync bytes accumulate. This syncing does not
// provide any persistency guarantees, but can prevent latency spikes.
func CreateWithSync(fs vfs.FS, name string, bytesPerSync int) (vfs.File, error) {
	f, err := fs.Create(name)
	if err != nil {
		return nil, err
	}
	return vfs.NewSyncingFile(f, vfs.SyncingFileOptions{BytesPerSync: bytesPerSync}), nil
}

// WriteFile writes data to a file named by filename.
func WriteFile(fs vfs.FS, filename string, data []byte) error {
	f, err := fs.Create(filename)
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
