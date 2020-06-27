// Copyright 2014 The Cockroach Authors.
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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// TODO(tamird): why does rocksdb not link jemalloc,snappy statically?

// #cgo CPPFLAGS: -I../../c-deps/libroach/include
// #cgo LDFLAGS: -lroach
// #cgo LDFLAGS: -lprotobuf
// #cgo LDFLAGS: -lrocksdb
// #cgo LDFLAGS: -lsnappy
// #cgo linux LDFLAGS: -lrt -lpthread
// #cgo windows LDFLAGS: -lshlwapi -lrpcrt4
//
// #include <stdlib.h>
// #include <libroach.h>
import "C"

var minWALSyncInterval = settings.RegisterDurationSetting(
	"rocksdb.min_wal_sync_interval",
	"minimum duration between syncs of the RocksDB WAL",
	0*time.Millisecond,
)

var rocksdbConcurrency = envutil.EnvOrDefaultInt(
	"COCKROACH_ROCKSDB_CONCURRENCY", func() int {
		// Use up to min(numCPU, 4) threads for background RocksDB compactions per
		// store.
		const max = 4
		if n := runtime.NumCPU(); n <= max {
			return n
		}
		return max
	}())

// Set to true to perform expensive iterator debug leak checking. In normal
// operation, we perform inexpensive iterator leak checking but those checks do
// not indicate where the leak arose. The expensive checking tracks the stack
// traces of every iterator allocated. DO NOT ENABLE in production code.
const debugIteratorLeak = false

var rocksdbLogger *log.SecondaryLogger

// InitRocksDBLogger initializes the logger to use for RocksDB log messages. If
// not called, WARNING, ERROR, and FATAL logs will be output to the normal
// CockroachDB log. The caller is responsible for ensuring the
// Close() method is eventually called on the new logger.
func InitRocksDBLogger(ctx context.Context) *log.SecondaryLogger {
	rocksdbLogger = log.NewSecondaryLogger(ctx, nil, "rocksdb",
		true /* enableGC */, false /* forceSyncWrites */, false /* enableMsgCount */)
	return rocksdbLogger
}

//export rocksDBLog
func rocksDBLog(usePrimaryLog C.bool, sevLvl C.int, s *C.char, n C.int) {
	sev := log.Severity(sevLvl)
	if !usePrimaryLog {
		if rocksdbLogger != nil {
			// NB: No need for the rocksdb tag if we're logging to a rocksdb specific
			// file.
			rocksdbLogger.LogSev(context.Background(), sev, C.GoStringN(s, n))
			return
		}

		// Only log INFO logs to the normal CockroachDB log at --v=3 and
		// above. This only applies when we're not using the primary log for
		// RocksDB generated messages (which is utilized by the encryption-at-rest
		// code).
		if sev == log.Severity_INFO && !log.V(3) {
			return
		}
	}

	ctx := logtags.AddTag(context.Background(), "rocksdb", nil)
	switch sev {
	case log.Severity_WARNING:
		log.Warningf(ctx, "%v", C.GoStringN(s, n))
	case log.Severity_ERROR:
		log.Errorf(ctx, "%v", C.GoStringN(s, n))
	case log.Severity_FATAL:
		log.Fatalf(ctx, "%v", C.GoStringN(s, n))
	default:
		log.Infof(ctx, "%v", C.GoStringN(s, n))
	}
}

//export prettyPrintKey
func prettyPrintKey(cKey C.DBKey) *C.char {
	mvccKey := MVCCKey{
		Key: gobytes(unsafe.Pointer(cKey.key.data), int(cKey.key.len)),
		Timestamp: hlc.Timestamp{
			WallTime: int64(cKey.wall_time),
			Logical:  int32(cKey.logical),
		},
	}
	return C.CString(mvccKey.String())
}

const (
	// RecommendedMaxOpenFiles is the recommended value for RocksDB's
	// max_open_files option.
	RecommendedMaxOpenFiles = 10000
	// MinimumMaxOpenFiles is the minimum value that RocksDB's max_open_files
	// option can be set to. While this should be set as high as possible, the
	// minimum total for a single store node must be under 2048 for Windows
	// compatibility. See:
	// https://wpdev.uservoice.com/forums/266908-command-prompt-console-bash-on-ubuntu-on-windo/suggestions/17310124-add-ability-to-change-max-number-of-open-files-for
	MinimumMaxOpenFiles = 1700
)

// RocksDBCache is a wrapper around C.DBCache
type RocksDBCache struct {
	cache *C.DBCache
}

// NewRocksDBCache creates a new cache of the specified size. Note that the
// cache is refcounted internally and starts out with a refcount of one (i.e.
// Release() should be called after having used the cache).
func NewRocksDBCache(cacheSize int64) RocksDBCache {
	return RocksDBCache{cache: C.DBNewCache(C.uint64_t(cacheSize))}
}

func (c RocksDBCache) ref() RocksDBCache {
	if c.cache != nil {
		c.cache = C.DBRefCache(c.cache)
	}
	return c
}

// Release releases the cache. Note that the cache will continue to be used
// until all of the RocksDB engines it was attached to have been closed, and
// that RocksDB engines which use it auto-release when they close.
func (c RocksDBCache) Release() {
	if c.cache != nil {
		C.DBReleaseCache(c.cache)
	}
}

// RocksDBConfig holds all configuration parameters and knobs used in setting
// up a new RocksDB instance.
type RocksDBConfig struct {
	// StorageConfig contains storage configs for all storage engines.
	base.StorageConfig
	// ReadOnly will open the database in read only mode if set to true.
	ReadOnly bool
	// MaxOpenFiles controls the maximum number of file descriptors RocksDB
	// creates. If MaxOpenFiles is zero, this is set to DefaultMaxOpenFiles.
	MaxOpenFiles uint64
	// WarnLargeBatchThreshold controls if a log message is printed when a
	// WriteBatch takes longer than WarnLargeBatchThreshold. If it is set to
	// zero, no log messages are ever printed.
	WarnLargeBatchThreshold time.Duration
	// RocksDBOptions contains RocksDB specific options using a semicolon
	// separated key-value syntax ("key1=value1; key2=value2").
	RocksDBOptions string
}

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {
	cfg   RocksDBConfig
	rdb   *C.DBEngine
	cache RocksDBCache // Shared cache.
	// auxDir is used for storing auxiliary files. Ideally it is a subdirectory of Dir.
	auxDir string

	commit struct {
		syncutil.Mutex
		cond       sync.Cond
		committing bool
		groupSize  int
		pending    []*rocksDBBatch
	}

	syncer struct {
		syncutil.Mutex
		cond    sync.Cond
		closed  bool
		pending []*rocksDBBatch
	}

	iters struct {
		syncutil.Mutex
		m map[*rocksDBIterator][]byte
	}
}

var _ Engine = &RocksDB{}

// SetRocksDBOpenHook sets the DBOpenHook function that will be called during
// RocksDB initialization. It is intended to be called by CCL code.
func SetRocksDBOpenHook(fn unsafe.Pointer) {
	C.DBSetOpenHook(fn)
}

// NewRocksDB allocates and returns a new RocksDB object.
// This creates options and opens the database. If the database
// doesn't yet exist at the specified directory, one is initialized
// from scratch.
// The caller must call the engine's Close method when the engine is no longer
// needed.
func NewRocksDB(cfg RocksDBConfig, cache RocksDBCache) (*RocksDB, error) {
	if cfg.Dir == "" {
		return nil, errors.New("dir must be non-empty")
	}

	r := &RocksDB{
		cfg:    cfg,
		cache:  cache.ref(),
		auxDir: filepath.Join(cfg.Dir, base.AuxiliaryDir),
	}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func newRocksDBInMem(attrs roachpb.Attributes, cacheSize int64) *RocksDB {
	cache := NewRocksDBCache(cacheSize)
	// The cache starts out with a refcount of one, and creating the engine
	// from it adds another refcount, at which point we release one of them.
	defer cache.Release()

	// TODO(bdarnell): The hard-coded 512 MiB is wrong; see
	// https://github.com/cockroachdb/cockroach/issues/16750
	db, err := newMemRocksDB(attrs, cache, 512<<20 /* MaxSize: 512 MiB */)
	if err != nil {
		panic(err)
	}
	return db
}

func newMemRocksDB(attrs roachpb.Attributes, cache RocksDBCache, maxSize int64) (*RocksDB, error) {
	r := &RocksDB{
		cfg: RocksDBConfig{
			StorageConfig: base.StorageConfig{
				Attrs:   attrs,
				MaxSize: maxSize,
			},
		},
		// dir: empty dir == "mem" RocksDB instance.
		cache:  cache.ref(),
		auxDir: "cockroach-auxiliary",
	}
	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

// String formatter.
func (r *RocksDB) String() string {
	dir := r.cfg.Dir
	if r.cfg.Dir == "" {
		dir = "<in-mem>"
	}
	attrs := r.Attrs().String()
	if attrs == "" {
		attrs = "<no-attributes>"
	}
	return fmt.Sprintf("%s=%s", attrs, dir)
}

func (r *RocksDB) open() error {
	var existingVersion, newVersion storageVersion
	if len(r.cfg.Dir) != 0 {
		log.Infof(context.TODO(), "opening rocksdb instance at %q", r.cfg.Dir)

		// Check the version number.
		var err error
		if existingVersion, err = getVersion(r.cfg.Dir); err != nil {
			return err
		}
		if existingVersion < versionMinimum || existingVersion > versionCurrent {
			// Instead of an error, we should call a migration if possible when
			// one is needed immediately following the DBOpen call.
			return fmt.Errorf("incompatible rocksdb data version, current:%d, on disk:%d, minimum:%d",
				versionCurrent, existingVersion, versionMinimum)
		}

		newVersion = existingVersion
		if newVersion == versionNoFile {
			// We currently set the default store version one before the file registry
			// to allow downgrades to older binaries as long as encryption is not in use.
			// TODO(mberhault): once enough releases supporting versionFileRegistry have passed, we can upgrade
			// to it without worry.
			newVersion = versionBeta20160331
		}

		// Using the file registry forces the latest version. We can't downgrade!
		if r.cfg.UseFileRegistry {
			newVersion = versionCurrent
		}
	} else {
		if log.V(2) {
			log.Infof(context.TODO(), "opening in memory rocksdb instance")
		}

		// In memory dbs are always current.
		existingVersion = versionCurrent
	}

	maxOpenFiles := uint64(RecommendedMaxOpenFiles)
	if r.cfg.MaxOpenFiles != 0 {
		maxOpenFiles = r.cfg.MaxOpenFiles
	}
	if r.cfg.Dir != "" {
		if err := os.MkdirAll(r.cfg.Dir, os.ModePerm); err != nil {
			return err
		}
	}
	status := C.DBOpen(&r.rdb, goToCSlice([]byte(r.cfg.Dir)),
		C.DBOptions{
			cache:             r.cache.cache,
			num_cpu:           C.int(rocksdbConcurrency),
			max_open_files:    C.int(maxOpenFiles),
			use_file_registry: C.bool(newVersion == versionCurrent),
			must_exist:        C.bool(r.cfg.MustExist),
			read_only:         C.bool(r.cfg.ReadOnly),
			rocksdb_options:   goToCSlice([]byte(r.cfg.RocksDBOptions)),
			extra_options:     goToCSlice(r.cfg.ExtraOptions),
		})
	if err := statusToError(status); err != nil {
		return errors.Wrap(err, "could not open rocksdb instance")
	}

	// Update or add the version file if needed and if on-disk.
	if len(r.cfg.Dir) != 0 && existingVersion < newVersion {
		if err := writeVersionFile(r.cfg.Dir, newVersion); err != nil {
			return err
		}
	}

	// Create the auxiliary directory if necessary.
	if !r.cfg.ReadOnly {
		if err := r.MkdirAll(r.auxDir); err != nil {
			return err
		}
	}

	r.commit.cond.L = &r.commit.Mutex
	r.syncer.cond.L = &r.syncer.Mutex
	r.iters.m = make(map[*rocksDBIterator][]byte)

	// NB: The sync goroutine acts as a check that the RocksDB instance was
	// properly closed as the goroutine will leak otherwise.
	go r.syncLoop()

	return nil
}

func (r *RocksDB) syncLoop() {
	s := &r.syncer
	s.Lock()

	var lastSync time.Time
	var err error

	for {
		for len(s.pending) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			s.Unlock()
			return
		}

		var min time.Duration
		if r.cfg.Settings != nil {
			min = minWALSyncInterval.Get(&r.cfg.Settings.SV)
		}
		if delta := timeutil.Since(lastSync); delta < min {
			s.Unlock()
			time.Sleep(min - delta)
			s.Lock()
		}

		pending := s.pending
		s.pending = nil

		s.Unlock()

		// Linux only guarantees we'll be notified of a writeback error once
		// during a sync call. After sync fails once, we cannot rely on any
		// future data written to WAL being crash-recoverable. That's because
		// any future writes will be appended after a potential corruption in
		// the WAL, and RocksDB's recovery terminates upon encountering any
		// corruption. So, we must not call `DBSyncWAL` again after it has
		// failed once.
		if r.cfg.Dir != "" && err == nil {
			err = statusToError(C.DBSyncWAL(r.rdb))
			lastSync = timeutil.Now()
		}

		for _, b := range pending {
			b.commitErr = err
			b.commitWG.Done()
		}

		s.Lock()
	}
}

// Close closes the database by deallocating the underlying handle.
func (r *RocksDB) Close() {
	if r.rdb == nil {
		log.Errorf(context.TODO(), "closing unopened rocksdb instance")
		return
	}
	if len(r.cfg.Dir) == 0 {
		if log.V(1) {
			log.Infof(context.TODO(), "closing in-memory rocksdb instance")
		}
	} else {
		log.Infof(context.TODO(), "closing rocksdb instance at %q", r.cfg.Dir)
	}
	if r.rdb != nil {
		if err := statusToError(C.DBClose(r.rdb)); err != nil {
			if debugIteratorLeak {
				r.iters.Lock()
				for _, stack := range r.iters.m {
					fmt.Printf("%s\n", stack)
				}
				r.iters.Unlock()
			}
			panic(err)
		}
		r.rdb = nil
	}
	r.cache.Release()
	r.syncer.Lock()
	r.syncer.closed = true
	r.syncer.cond.Signal()
	r.syncer.Unlock()
}

// CreateCheckpoint creates a RocksDB checkpoint in the given directory (which
// must not exist). This directory should be located on the same file system, or
// copies of all data are used instead of hard links, which is very expensive.
func (r *RocksDB) CreateCheckpoint(dir string) error {
	status := C.DBCreateCheckpoint(r.rdb, goToCSlice([]byte(dir)))
	return errors.Wrap(statusToError(status), "unable to take RocksDB checkpoint")
}

// Closed returns true if the engine is closed.
func (r *RocksDB) Closed() bool {
	return r.rdb == nil
}

// ExportToSst is part of the engine.Reader interface.
func (r *RocksDB) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	start := MVCCKey{Key: startKey, Timestamp: startTS}
	end := MVCCKey{Key: endKey, Timestamp: endTS}

	var data C.DBString
	var intentErr C.DBString
	var bulkopSummary C.DBString
	var resumeKey C.DBString

	err := statusToError(C.DBExportToSst(goToCKey(start), goToCKey(end),
		C.bool(exportAllRevisions),
		C.uint64_t(targetSize), C.uint64_t(maxSize),
		goToCIterOptions(io), r.rdb, &data, &intentErr, &bulkopSummary, &resumeKey))

	if err != nil {
		if err.Error() == "WriteIntentError" {
			var e roachpb.WriteIntentError
			if err := protoutil.Unmarshal(cStringToGoBytes(intentErr), &e); err != nil {
				return nil, roachpb.BulkOpSummary{}, nil, errors.Wrap(err, "failed to decode write intent error")
			}

			return nil, roachpb.BulkOpSummary{}, nil, &e
		}
		return nil, roachpb.BulkOpSummary{}, nil, err
	}

	var summary roachpb.BulkOpSummary
	if err := protoutil.Unmarshal(cStringToGoBytes(bulkopSummary), &summary); err != nil {
		return nil, roachpb.BulkOpSummary{}, nil, errors.Wrap(err, "failed to decode BulkopSummary")
	}

	return cStringToGoBytes(data), summary, roachpb.Key(cStringToGoBytes(resumeKey)), nil
}

// Attrs returns the list of attributes describing this engine. This
// may include a specification of disk type (e.g. hdd, ssd, fio, etc.)
// and potentially other labels to identify important attributes of
// the engine.
func (r *RocksDB) Attrs() roachpb.Attributes {
	return r.cfg.Attrs
}

// Put sets the given key to the value provided.
//
// It is safe to modify the contents of the arguments after Put returns.
func (r *RocksDB) Put(key MVCCKey, value []byte) error {
	return dbPut(r.rdb, key, value)
}

// Merge implements the RocksDB merge operator using the function goMergeInit
// to initialize missing values and goMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goMerge and goMergeInit for details.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (r *RocksDB) Merge(key MVCCKey, value []byte) error {
	return dbMerge(r.rdb, key, value)
}

// LogData is part of the Writer interface.
//
// It is safe to modify the contents of the arguments after LogData returns.
func (r *RocksDB) LogData(data []byte) error {
	panic("unimplemented")
}

// LogLogicalOp is part of the Writer interface.
func (r *RocksDB) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// ApplyBatchRepr atomically applies a set of batched updates. Created by
// calling Repr() on a batch. Using this method is equivalent to constructing
// and committing a batch whose Repr() equals repr.
//
// It is safe to modify the contents of the arguments after ApplyBatchRepr
// returns.
func (r *RocksDB) ApplyBatchRepr(repr []byte, sync bool) error {
	return dbApplyBatchRepr(r.rdb, repr, sync)
}

// Get returns the value for the given key.
func (r *RocksDB) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.rdb, key)
}

// GetProto fetches the value at the specified key and unmarshals it.
func (r *RocksDB) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.rdb, key, msg)
}

// Clear removes the item from the db with the given key.
//
// It is safe to modify the contents of the arguments after Clear returns.
func (r *RocksDB) Clear(key MVCCKey) error {
	return dbClear(r.rdb, key)
}

// SingleClear removes the most recent item from the db with the given key.
//
// It is safe to modify the contents of the arguments after SingleClear returns.
func (r *RocksDB) SingleClear(key MVCCKey) error {
	return dbSingleClear(r.rdb, key)
}

// ClearRange removes a set of entries, from start (inclusive) to end
// (exclusive).
//
// It is safe to modify the contents of the arguments after ClearRange returns.
func (r *RocksDB) ClearRange(start, end MVCCKey) error {
	return dbClearRange(r.rdb, start, end)
}

// ClearIterRange removes a set of entries, from start (inclusive) to end
// (exclusive).
//
// It is safe to modify the contents of the arguments after ClearIterRange
// returns.
func (r *RocksDB) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	return dbClearIterRange(r.rdb, iter, start, end)
}

// Iterate iterates from start to end keys, invoking f on each
// key/value pair. See engine.Iterate for details.
func (r *RocksDB) Iterate(start, end roachpb.Key, f func(MVCCKeyValue) (bool, error)) error {
	return iterateOnReader(r, start, end, f)
}

// Capacity queries the underlying file system for disk capacity information.
func (r *RocksDB) Capacity() (roachpb.StoreCapacity, error) {
	return computeCapacity(r.cfg.Dir, r.cfg.MaxSize)
}

// Compact forces compaction over the entire database.
func (r *RocksDB) Compact() error {
	return statusToError(C.DBCompact(r.rdb))
}

// CompactRange forces compaction over a specified range of keys in the database.
func (r *RocksDB) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	return statusToError(C.DBCompactRange(r.rdb, goToCSlice(start), goToCSlice(end), C.bool(forceBottommost)))
}

// disableAutoCompaction disables automatic compactions. For testing use only.
func (r *RocksDB) disableAutoCompaction() error {
	return statusToError(C.DBDisableAutoCompaction(r.rdb))
}

// ApproximateDiskBytes returns the approximate on-disk size of the specified key range.
func (r *RocksDB) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	start := MVCCKey{Key: from}
	end := MVCCKey{Key: to}
	var result C.uint64_t
	err := statusToError(C.DBApproximateDiskBytes(r.rdb, goToCKey(start), goToCKey(end), &result))
	return uint64(result), err
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (r *RocksDB) Flush() error {
	return statusToError(C.DBFlush(r.rdb))
}

// NewIterator returns an iterator over this rocksdb engine.
func (r *RocksDB) NewIterator(opts IterOptions) Iterator {
	return newRocksDBIterator(r.rdb, opts, r, r)
}

// NewSnapshot creates a snapshot handle from engine and returns a
// read-only rocksDBSnapshot engine.
func (r *RocksDB) NewSnapshot() Reader {
	if r.rdb == nil {
		panic("RocksDB is not initialized yet")
	}
	return &rocksDBSnapshot{
		parent: r,
		handle: C.DBNewSnapshot(r.rdb),
	}
}

// Type implements the Engine interface.
func (r *RocksDB) Type() enginepb.EngineType {
	return enginepb.EngineTypeRocksDB
}

// NewReadOnly returns a new ReadWriter wrapping this rocksdb engine.
func (r *RocksDB) NewReadOnly() ReadWriter {
	return &rocksDBReadOnly{
		parent:   r,
		isClosed: false,
	}
}

type rocksDBReadOnly struct {
	parent     *RocksDB
	prefixIter reusableIterator
	normalIter reusableIterator
	isClosed   bool
}

func (r *rocksDBReadOnly) Close() {
	if r.isClosed {
		panic("closing an already-closed rocksDBReadOnly")
	}
	r.isClosed = true
	if i := &r.prefixIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
}

// Read-only batches are not committed
func (r *rocksDBReadOnly) Closed() bool {
	return r.isClosed
}

// ExportToSst is part of the engine.Reader interface.
func (r *rocksDBReadOnly) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return r.parent.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

func (r *rocksDBReadOnly) Get(key MVCCKey) ([]byte, error) {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	return dbGet(r.parent.rdb, key)
}

func (r *rocksDBReadOnly) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	return dbGetProto(r.parent.rdb, key, msg)
}

func (r *rocksDBReadOnly) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (bool, error),
) error {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	return iterateOnReader(r, start, end, f)
}

// NewIterator returns an iterator over the underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// rocksDBReadOnly. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same rocksDBReadOnly.
func (r *rocksDBReadOnly) NewIterator(opts IterOptions) Iterator {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		return newRocksDBIterator(r.parent.rdb, opts, r, r.parent)
	}
	iter := &r.normalIter
	if opts.Prefix {
		iter = &r.prefixIter
	}
	if iter.rocksDBIterator.iter == nil {
		iter.rocksDBIterator.init(r.parent.rdb, opts, r, r.parent)
	} else {
		iter.rocksDBIterator.setOptions(opts)
	}
	if iter.inuse {
		panic("iterator already in use")
	}
	iter.inuse = true
	return iter
}

// Writer methods are not implemented for rocksDBReadOnly. Ideally, the code
// could be refactored so that a Reader could be supplied to evaluateBatch

// Writer is the write interface to an engine's data.
func (r *rocksDBReadOnly) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) Clear(key MVCCKey) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) SingleClear(key MVCCKey) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) ClearRange(start, end MVCCKey) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) Merge(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) Put(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) LogData(data []byte) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	panic("not implemented")
}

// NewBatch returns a new batch wrapping this rocksdb engine.
func (r *RocksDB) NewBatch() Batch {
	b := newRocksDBBatch(r, false /* writeOnly */)
	return b
}

// NewWriteOnlyBatch returns a new write-only batch wrapping this rocksdb
// engine.
func (r *RocksDB) NewWriteOnlyBatch() Batch {
	return newRocksDBBatch(r, true /* writeOnly */)
}

// GetSSTables retrieves metadata about this engine's live sstables.
func (r *RocksDB) GetSSTables() SSTableInfos {
	var n C.int
	tables := C.DBGetSSTables(r.rdb, &n)
	// We can't index into tables because it is a pointer, not a slice. The
	// hackery below treats the pointer as an array and then constructs a slice
	// from it.

	tableSize := unsafe.Sizeof(C.DBSSTable{})
	tableVal := func(i int) C.DBSSTable {
		return *(*C.DBSSTable)(unsafe.Pointer(uintptr(unsafe.Pointer(tables)) + uintptr(i)*tableSize))
	}

	res := make(SSTableInfos, n)
	for i := range res {
		r := &res[i]
		tv := tableVal(i)
		r.Level = int(tv.level)
		r.Size = int64(tv.size)
		r.Start = cToGoKey(tv.start_key)
		r.End = cToGoKey(tv.end_key)
		if ptr := tv.start_key.key.data; ptr != nil {
			C.free(unsafe.Pointer(ptr))
		}
		if ptr := tv.end_key.key.data; ptr != nil {
			C.free(unsafe.Pointer(ptr))
		}
	}
	C.free(unsafe.Pointer(tables))

	sort.Sort(res)
	return res
}

// WALFileInfo contains metadata about a single write-ahead log file. Note this
// mirrors the C.DBWALFile struct.
type WALFileInfo struct {
	LogNumber int64
	Size      int64
}

// GetSortedWALFiles retrievews information about all of the write-ahead log
// files in this engine in order from oldest to newest.
func (r *RocksDB) GetSortedWALFiles() ([]WALFileInfo, error) {
	var n C.int
	var files *C.DBWALFile
	status := C.DBGetSortedWALFiles(r.rdb, &files, &n)
	if err := statusToError(status); err != nil {
		return nil, errors.Wrap(err, "could not get sorted WAL files")
	}
	defer C.free(unsafe.Pointer(files))

	// We can't index into files because it is a pointer, not a slice. The hackery
	// below treats the pointer as an array and then constructs a slice from it.

	structSize := unsafe.Sizeof(C.DBWALFile{})
	getWALFile := func(i int) *C.DBWALFile {
		return (*C.DBWALFile)(unsafe.Pointer(uintptr(unsafe.Pointer(files)) + uintptr(i)*structSize))
	}

	res := make([]WALFileInfo, n)
	for i := range res {
		wf := getWALFile(i)
		res[i].LogNumber = int64(wf.log_number)
		res[i].Size = int64(wf.size)
	}
	return res, nil
}

// GetUserProperties fetches the user properties stored in each sstable's
// metadata.
func (r *RocksDB) GetUserProperties() (enginepb.SSTUserPropertiesCollection, error) {
	buf := cStringToGoBytes(C.DBGetUserProperties(r.rdb))
	var ssts enginepb.SSTUserPropertiesCollection
	if err := protoutil.Unmarshal(buf, &ssts); err != nil {
		return enginepb.SSTUserPropertiesCollection{}, err
	}
	if ssts.Error != "" {
		return enginepb.SSTUserPropertiesCollection{}, errors.Newf("%s", ssts.Error)
	}
	return ssts, nil
}

// GetStats retrieves stats from this engine's RocksDB instance and
// returns it in a new instance of Stats.
func (r *RocksDB) GetStats() (*Stats, error) {
	var s C.DBStatsResult
	if err := statusToError(C.DBGetStats(r.rdb, &s)); err != nil {
		return nil, err
	}
	return &Stats{
		BlockCacheHits:                 int64(s.block_cache_hits),
		BlockCacheMisses:               int64(s.block_cache_misses),
		BlockCacheUsage:                int64(s.block_cache_usage),
		BlockCachePinnedUsage:          int64(s.block_cache_pinned_usage),
		BloomFilterPrefixChecked:       int64(s.bloom_filter_prefix_checked),
		BloomFilterPrefixUseful:        int64(s.bloom_filter_prefix_useful),
		MemtableTotalSize:              int64(s.memtable_total_size),
		Flushes:                        int64(s.flushes),
		FlushedBytes:                   int64(s.flush_bytes),
		Compactions:                    int64(s.compactions),
		IngestedBytes:                  0, // Not exposed by RocksDB.
		CompactedBytesRead:             int64(s.compact_read_bytes),
		CompactedBytesWritten:          int64(s.compact_write_bytes),
		TableReadersMemEstimate:        int64(s.table_readers_mem_estimate),
		PendingCompactionBytesEstimate: int64(s.pending_compaction_bytes_estimate),
		L0FileCount:                    int64(s.l0_file_count),
		L0SublevelCount:                -1, // Not a RocksDB feature.
	}, nil
}

// GetTickersAndHistograms retrieves maps of all RocksDB tickers and histograms.
// It differs from `GetStats` by getting _every_ ticker and histogram, and by not
// getting anything else (DB properties, for example).
func (r *RocksDB) GetTickersAndHistograms() (*enginepb.TickersAndHistograms, error) {
	res := new(enginepb.TickersAndHistograms)
	var s C.DBTickersAndHistogramsResult
	if err := statusToError(C.DBGetTickersAndHistograms(r.rdb, &s)); err != nil {
		return nil, err
	}

	tickers := (*[MaxArrayLen / C.sizeof_TickerInfo]C.TickerInfo)(
		unsafe.Pointer(s.tickers))[:s.tickers_len:s.tickers_len]
	res.Tickers = make(map[string]uint64)
	for _, ticker := range tickers {
		name := cStringToGoString(ticker.name)
		value := uint64(ticker.value)
		res.Tickers[name] = value
	}
	C.free(unsafe.Pointer(s.tickers))

	res.Histograms = make(map[string]enginepb.HistogramData)
	histograms := (*[MaxArrayLen / C.sizeof_HistogramInfo]C.HistogramInfo)(
		unsafe.Pointer(s.histograms))[:s.histograms_len:s.histograms_len]
	for _, histogram := range histograms {
		name := cStringToGoString(histogram.name)
		value := enginepb.HistogramData{
			Mean:  float64(histogram.mean),
			P50:   float64(histogram.p50),
			P95:   float64(histogram.p95),
			P99:   float64(histogram.p99),
			Max:   float64(histogram.max),
			Count: uint64(histogram.count),
			Sum:   uint64(histogram.sum),
		}
		res.Histograms[name] = value
	}
	C.free(unsafe.Pointer(s.histograms))
	return res, nil
}

// GetCompactionStats returns the internal RocksDB compaction stats. See
// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#rocksdb-statistics.
func (r *RocksDB) GetCompactionStats() string {
	s := cStringToGoString(C.DBGetCompactionStats(r.rdb)) +
		"estimated_pending_compaction_bytes: "
	stats, err := r.GetStats()
	if err != nil {
		return s + err.Error()
	}
	return s + humanizeutil.IBytes(stats.PendingCompactionBytesEstimate)
}

// GetEnvStats returns stats for the RocksDB env. This may include encryption stats.
func (r *RocksDB) GetEnvStats() (*EnvStats, error) {
	var s C.DBEnvStatsResult
	if err := statusToError(C.DBGetEnvStats(r.rdb, &s)); err != nil {
		return nil, err
	}

	return &EnvStats{
		TotalFiles:       uint64(s.total_files),
		TotalBytes:       uint64(s.total_bytes),
		ActiveKeyFiles:   uint64(s.active_key_files),
		ActiveKeyBytes:   uint64(s.active_key_bytes),
		EncryptionType:   int32(s.encryption_type),
		EncryptionStatus: cStringToGoBytes(s.encryption_status),
	}, nil
}

// GetEncryptionRegistries returns the file and key registries when encryption is enabled
// on the store.
func (r *RocksDB) GetEncryptionRegistries() (*EncryptionRegistries, error) {
	var s C.DBEncryptionRegistries
	if err := statusToError(C.DBGetEncryptionRegistries(r.rdb, &s)); err != nil {
		return nil, err
	}

	return &EncryptionRegistries{
		FileRegistry: cStringToGoBytes(s.file_registry),
		KeyRegistry:  cStringToGoBytes(s.key_registry),
	}, nil
}

type rocksDBSnapshot struct {
	parent *RocksDB
	handle *C.DBEngine
}

// Close releases the snapshot handle.
func (r *rocksDBSnapshot) Close() {
	C.DBClose(r.handle)
	r.handle = nil
}

// Closed returns true if the engine is closed.
func (r *rocksDBSnapshot) Closed() bool {
	return r.handle == nil
}

// ExportToSst is part of the engine.Reader interface.
func (r *rocksDBSnapshot) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	return r.parent.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
}

// Get returns the value for the given key, nil otherwise using
// the snapshot handle.
func (r *rocksDBSnapshot) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.handle, key)
}

func (r *rocksDBSnapshot) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.handle, key, msg)
}

// Iterate iterates over the keys between start inclusive and end
// exclusive, invoking f() on each key/value pair using the snapshot
// handle.
func (r *rocksDBSnapshot) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (bool, error),
) error {
	return iterateOnReader(r, start, end, f)
}

// NewIterator returns a new instance of an Iterator over the
// engine using the snapshot handle.
func (r *rocksDBSnapshot) NewIterator(opts IterOptions) Iterator {
	return newRocksDBIterator(r.handle, opts, r, r.parent)
}

// reusableIterator wraps rocksDBIterator and allows reuse of an iterator
// for the lifetime of a batch.
type reusableIterator struct {
	rocksDBIterator
	inuse bool
}

func (r *reusableIterator) Close() {
	// reusableIterator.Close() leaves the underlying rocksdb iterator open until
	// the associated batch is closed.
	if !r.inuse {
		panic("closing idle iterator")
	}
	r.inuse = false
}

type distinctBatch struct {
	*rocksDBBatch
	prefixIter reusableIterator
	normalIter reusableIterator
}

func (r *distinctBatch) Close() {
	if !r.distinctOpen {
		panic("distinct batch not open")
	}
	r.distinctOpen = false
}

// NewIterator returns an iterator over the batch and underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// batch. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same batch.
func (r *distinctBatch) NewIterator(opts IterOptions) Iterator {
	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		if r.writeOnly {
			return newRocksDBIterator(r.parent.rdb, opts, r, r.parent)
		}
		r.ensureBatch()
		return newRocksDBIterator(r.batch, opts, r, r.parent)
	}

	// Use the cached iterator, creating it on first access.
	iter := &r.normalIter
	if opts.Prefix {
		iter = &r.prefixIter
	}
	if iter.rocksDBIterator.iter == nil {
		if r.writeOnly {
			iter.rocksDBIterator.init(r.parent.rdb, opts, r, r.parent)
		} else {
			r.ensureBatch()
			iter.rocksDBIterator.init(r.batch, opts, r, r.parent)
		}
	} else {
		iter.rocksDBIterator.setOptions(opts)
	}
	if iter.inuse {
		panic("iterator already in use")
	}
	iter.inuse = true
	return iter
}

func (r *distinctBatch) Get(key MVCCKey) ([]byte, error) {
	if r.writeOnly {
		return dbGet(r.parent.rdb, key)
	}
	r.ensureBatch()
	return dbGet(r.batch, key)
}

func (r *distinctBatch) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if r.writeOnly {
		return dbGetProto(r.parent.rdb, key, msg)
	}
	r.ensureBatch()
	return dbGetProto(r.batch, key, msg)
}

func (r *distinctBatch) Iterate(start, end roachpb.Key, f func(MVCCKeyValue) (bool, error)) error {
	r.ensureBatch()
	return iterateOnReader(r, start, end, f)
}

func (r *distinctBatch) Put(key MVCCKey, value []byte) error {
	r.builder.Put(key, value)
	return nil
}

func (r *distinctBatch) Merge(key MVCCKey, value []byte) error {
	r.builder.Merge(key, value)
	return nil
}

func (r *distinctBatch) LogData(data []byte) error {
	r.builder.LogData(data)
	return nil
}

func (r *distinctBatch) Clear(key MVCCKey) error {
	r.builder.Clear(key)
	return nil
}

func (r *distinctBatch) SingleClear(key MVCCKey) error {
	r.builder.SingleClear(key)
	return nil
}

func (r *distinctBatch) ClearRange(start, end MVCCKey) error {
	if !r.writeOnly {
		panic("readable batch")
	}
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearRange(r.batch, start, end)
}

func (r *distinctBatch) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearIterRange(r.batch, iter, start, end)
}

func (r *distinctBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

func (r *distinctBatch) close() {
	if r.prefixIter.inuse {
		panic("iterator still inuse")
	}
	if r.normalIter.inuse {
		panic("iterator still inuse")
	}
	if i := &r.prefixIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
}

// batchIterator wraps rocksDBIterator and ensures that the buffered mutations
// in a batch are flushed before performing read operations.
type batchIterator struct {
	iter  rocksDBIterator
	batch *rocksDBBatch
}

func (r *batchIterator) Stats() IteratorStats {
	return r.iter.Stats()
}

func (r *batchIterator) Close() {
	if r.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
	r.iter.destroy()
}

func (r *batchIterator) SeekGE(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.SeekGE(key)
}

func (r *batchIterator) SeekLT(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.SeekLT(key)
}

func (r *batchIterator) Valid() (bool, error) {
	return r.iter.Valid()
}

func (r *batchIterator) Next() {
	r.batch.flushMutations()
	r.iter.Next()
}

func (r *batchIterator) Prev() {
	r.batch.flushMutations()
	r.iter.Prev()
}

func (r *batchIterator) NextKey() {
	r.batch.flushMutations()
	r.iter.NextKey()
}

func (r *batchIterator) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	r.batch.flushMutations()
	return r.iter.ComputeStats(start, end, nowNanos)
}

func (r *batchIterator) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	r.batch.flushMutations()
	return r.iter.FindSplitKey(start, end, minSplitKey, targetSize)
}

func (r *batchIterator) MVCCOpsSpecialized() bool {
	return r.iter.MVCCOpsSpecialized()
}

func (r *batchIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	r.batch.flushMutations()
	return r.iter.MVCCGet(key, timestamp, opts)
}

func (r *batchIterator) MVCCScan(
	start, end roachpb.Key, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (MVCCScanResult, error) {
	r.batch.flushMutations()
	return r.iter.MVCCScan(start, end, timestamp, opts)
}

func (r *batchIterator) SetUpperBound(key roachpb.Key) {
	r.iter.SetUpperBound(key)
}

func (r *batchIterator) Key() MVCCKey {
	return r.iter.Key()
}

func (r *batchIterator) Value() []byte {
	return r.iter.Value()
}

func (r *batchIterator) ValueProto(msg protoutil.Message) error {
	return r.iter.ValueProto(msg)
}

func (r *batchIterator) UnsafeKey() MVCCKey {
	return r.iter.UnsafeKey()
}

func (r *batchIterator) UnsafeValue() []byte {
	return r.iter.UnsafeValue()
}

func (r *batchIterator) getIter() *C.DBIterator {
	return r.iter.iter
}

func (r *batchIterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	return r.iter.CheckForKeyCollisions(sstData, start, end)
}

// reusableBatchIterator wraps batchIterator and makes the Close method a no-op
// to allow reuse of the iterator for the lifetime of the batch. The batch must
// call iter.destroy() when it closes itself.
type reusableBatchIterator struct {
	batchIterator
}

func (r *reusableBatchIterator) Close() {
	// reusableBatchIterator.Close() leaves the underlying rocksdb iterator open
	// until the associated batch is closed.
	if r.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
}

type rocksDBBatch struct {
	parent             *RocksDB
	batch              *C.DBEngine
	flushes            int
	flushedCount       int
	flushedSize        int
	prefixIter         reusableBatchIterator
	normalIter         reusableBatchIterator
	builder            RocksDBBatchBuilder
	distinct           distinctBatch
	distinctOpen       bool
	distinctNeedsFlush bool
	writeOnly          bool
	syncCommit         bool
	closed             bool
	committed          bool
	commitErr          error
	commitWG           sync.WaitGroup
}

var batchPool = sync.Pool{
	New: func() interface{} {
		return &rocksDBBatch{}
	},
}

func newRocksDBBatch(parent *RocksDB, writeOnly bool) *rocksDBBatch {
	// Get a new batch from the pool. Batches in the pool may have their closed
	// fields set to true to facilitate some sanity check assertions. Reset this
	// field and set others.
	r := batchPool.Get().(*rocksDBBatch)
	r.closed = false
	r.parent = parent
	r.writeOnly = writeOnly
	r.distinct.rocksDBBatch = r
	return r
}

func (r *rocksDBBatch) ensureBatch() {
	if r.batch == nil {
		r.batch = C.DBNewBatch(r.parent.rdb, C.bool(r.writeOnly))
	}
}

func (r *rocksDBBatch) Close() {
	if r.closed {
		panic("this batch was already closed")
	}
	r.distinct.close()
	if r.prefixIter.batch != nil {
		panic("iterator still inuse")
	}
	if r.normalIter.batch != nil {
		panic("iterator still inuse")
	}
	if i := &r.prefixIter.iter; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.iter; i.iter != nil {
		i.destroy()
	}
	if r.batch != nil {
		C.DBClose(r.batch)
		r.batch = nil
	}
	r.builder.reset()
	r.closed = true

	// Zero all the remaining fields individually. We can't just copy a new
	// struct onto r, since r.builder has a sync.NoCopy.
	r.batch = nil
	r.parent = nil
	r.flushes = 0
	r.flushedCount = 0
	r.flushedSize = 0
	r.prefixIter = reusableBatchIterator{}
	r.normalIter = reusableBatchIterator{}
	r.distinctOpen = false
	r.distinctNeedsFlush = false
	r.writeOnly = false
	r.syncCommit = false
	r.committed = false
	r.commitErr = nil
	r.commitWG = sync.WaitGroup{}

	batchPool.Put(r)
}

// Closed returns true if the engine is closed.
func (r *rocksDBBatch) Closed() bool {
	return r.closed || r.committed
}

// ExportToSst is part of the engine.Reader interface.
func (r *rocksDBBatch) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	panic("unimplemented")
}

func (r *rocksDBBatch) Put(key MVCCKey, value []byte) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.Put(key, value)
	return nil
}

func (r *rocksDBBatch) Merge(key MVCCKey, value []byte) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.Merge(key, value)
	return nil
}

func (r *rocksDBBatch) LogData(data []byte) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.LogData(data)
	return nil
}

// ApplyBatchRepr atomically applies a set of batched updates to the current
// batch (the receiver).
func (r *rocksDBBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	return r.builder.ApplyRepr(repr)
}

func (r *rocksDBBatch) Get(key MVCCKey) ([]byte, error) {
	if r.writeOnly {
		panic("write-only batch")
	}
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.ensureBatch()
	return dbGet(r.batch, key)
}

func (r *rocksDBBatch) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if r.writeOnly {
		panic("write-only batch")
	}
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.ensureBatch()
	return dbGetProto(r.batch, key, msg)
}

func (r *rocksDBBatch) Iterate(start, end roachpb.Key, f func(MVCCKeyValue) (bool, error)) error {
	if r.writeOnly {
		panic("write-only batch")
	}
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.ensureBatch()
	return iterateOnReader(r, start, end, f)
}

func (r *rocksDBBatch) Clear(key MVCCKey) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.Clear(key)
	return nil
}

func (r *rocksDBBatch) SingleClear(key MVCCKey) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.SingleClear(key)
	return nil
}

func (r *rocksDBBatch) ClearRange(start, end MVCCKey) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearRange(r.batch, start, end)
}

func (r *rocksDBBatch) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearIterRange(r.batch, iter, start, end)
}

func (r *rocksDBBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// NewIterator returns an iterator over the batch and underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// batch. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same batch.
func (r *rocksDBBatch) NewIterator(opts IterOptions) Iterator {
	if r.writeOnly {
		panic("write-only batch")
	}
	if r.distinctOpen {
		panic("distinct batch open")
	}

	if opts.MinTimestampHint != (hlc.Timestamp{}) {
		// Iterators that specify timestamp bounds cannot be cached.
		r.ensureBatch()
		iter := &batchIterator{batch: r}
		iter.iter.init(r.batch, opts, r, r.parent)
		return iter
	}

	// Use the cached iterator, creating it on first access.
	iter := &r.normalIter
	if opts.Prefix {
		iter = &r.prefixIter
	}
	if iter.iter.iter == nil {
		r.ensureBatch()
		iter.iter.init(r.batch, opts, r, r.parent)
	} else {
		iter.iter.setOptions(opts)
	}
	if iter.batch != nil {
		panic("iterator already in use")
	}
	iter.batch = r
	return iter
}

const maxBatchGroupSize = 1 << 20 // 1 MiB

// makeBatchGroup add the specified batch to the pending list of batches to
// commit. Groups are delimited by a nil batch in the pending list. Group
// leaders are the first batch in the pending list and the first batch after a
// nil batch. The size of a group is limited by the maxSize parameter which is
// measured as the number of bytes in the group's batches. The groupSize
// parameter is the size of the current group being formed. Returns the new
// list of pending batches, the new size of the current group and whether the
// batch that was added is the leader of its group.
func makeBatchGroup(
	pending []*rocksDBBatch, b *rocksDBBatch, groupSize, maxSize int,
) (_ []*rocksDBBatch, _ int, leader bool) {
	leader = len(pending) == 0
	if n := len(b.unsafeRepr()); leader {
		groupSize = n
	} else if groupSize+n > maxSize {
		leader = true
		groupSize = n
		pending = append(pending, nil)
	} else {
		groupSize += n
	}
	pending = append(pending, b)
	return pending, groupSize, leader
}

// nextBatchGroup extracts the group of batches from the pending list. See
// makeBatchGroup for an explanation of how groups are encoded into the pending
// list. Returns the next group in the prefix return value, and the remaining
// groups in the suffix parameter (the next group is always a prefix of the
// pending argument).
func nextBatchGroup(pending []*rocksDBBatch) (prefix []*rocksDBBatch, suffix []*rocksDBBatch) {
	for i := 1; i < len(pending); i++ {
		if pending[i] == nil {
			return pending[:i], pending[i+1:]
		}
	}
	return pending, pending[len(pending):]
}

func (r *rocksDBBatch) Commit(syncCommit bool) error {
	if r.Closed() {
		panic("this batch was already committed")
	}
	r.distinctOpen = false

	if r.Empty() {
		// Nothing was written to this batch. Fast path.
		r.committed = true
		return nil
	}

	// Combine multiple write-only batch commits into a single call to
	// RocksDB. RocksDB is supposed to be performing such batching internally,
	// but whether Cgo or something else, it isn't achieving the same degree of
	// batching. Instrumentation shows that internally RocksDB almost never
	// batches commits together. While the batching below often can batch 20 or
	// 30 concurrent commits.
	c := &r.parent.commit
	r.commitWG.Add(1)
	r.syncCommit = syncCommit

	// The leader for the commit is the first batch to be added to the pending
	// slice. Every batch has an associated wait group which is signaled when
	// the commit is complete.
	c.Lock()

	var leader bool
	c.pending, c.groupSize, leader = makeBatchGroup(c.pending, r, c.groupSize, maxBatchGroupSize)

	if leader {
		// We're the leader of our group. Wait for any running commit to finish and
		// for our batch to make it to the head of the pending queue.
		for c.committing || c.pending[0] != r {
			c.cond.Wait()
		}

		var pending []*rocksDBBatch
		pending, c.pending = nextBatchGroup(c.pending)
		c.committing = true
		c.Unlock()

		// We want the batch that is performing the commit to be write-only in
		// order to avoid the (significant) overhead of indexing the operations in
		// the other batches when they are applied.
		committer := r
		merge := pending[1:]
		if !r.writeOnly && len(merge) > 0 {
			committer = newRocksDBBatch(r.parent, true /* writeOnly */)
			defer committer.Close()
			merge = pending
		}

		// Bundle all of the batches together.
		var err error
		for _, b := range merge {
			if err = committer.ApplyBatchRepr(b.unsafeRepr(), false /* sync */); err != nil {
				break
			}
		}

		if err == nil {
			err = committer.commitInternal(false /* sync */)
		}

		// We're done committing the batch, let the next group of batches
		// proceed.
		c.Lock()
		c.committing = false
		// NB: Multiple leaders can be waiting.
		c.cond.Broadcast()
		c.Unlock()

		// Propagate the error to all of the batches involved in the commit. If a
		// batch requires syncing and the commit was successful, add it to the
		// syncing list. Note that we're reusing the pending list here for the
		// syncing list. We need to be careful to cap the capacity so that
		// extending this slice past the length of the pending list will result in
		// reallocation. Otherwise we have a race between appending to this list
		// while holding the sync lock below, and appending to the commit pending
		// list while holding the commit lock above.
		syncing := pending[:0:len(pending)]
		for _, b := range pending {
			if err != nil || !b.syncCommit {
				b.commitErr = err
				b.commitWG.Done()
			} else {
				syncing = append(syncing, b)
			}
		}

		if len(syncing) > 0 {
			// The commit was successful and one or more of the batches requires
			// syncing: notify the sync goroutine.
			s := &r.parent.syncer
			s.Lock()
			if len(s.pending) == 0 {
				s.pending = syncing
			} else {
				s.pending = append(s.pending, syncing...)
			}
			s.cond.Signal()
			s.Unlock()
		}
	} else {
		c.Unlock()
	}
	// Wait for the commit/sync to finish.
	r.commitWG.Wait()
	return r.commitErr
}

func (r *rocksDBBatch) commitInternal(sync bool) error {
	start := timeutil.Now()
	var count, size int

	if r.flushes > 0 {
		// We've previously flushed mutations to the C++ batch, so we have to flush
		// any remaining mutations as well and then commit the batch.
		r.flushMutations()
		r.ensureBatch()
		if err := statusToError(C.DBCommitAndCloseBatch(r.batch, C.bool(sync))); err != nil {
			return err
		}
		r.batch = nil
		count, size = r.flushedCount, r.flushedSize
	} else if r.builder.Len() > 0 {
		count, size = int(r.builder.Count()), r.builder.Len()

		// Fast-path which avoids flushing mutations to the C++ batch. Instead, we
		// directly apply the mutations to the database.
		if err := dbApplyBatchRepr(r.parent.rdb, r.builder.Finish(), sync); err != nil {
			return err
		}
		if r.batch != nil {
			C.DBClose(r.batch)
			r.batch = nil
		}
	} else {
		panic("commitInternal called on empty batch")
	}
	r.committed = true

	warnLargeBatches := r.parent.cfg.WarnLargeBatchThreshold > 0
	if elapsed := timeutil.Since(start); warnLargeBatches && (elapsed >= r.parent.cfg.WarnLargeBatchThreshold) {
		log.Warningf(context.TODO(), "batch [%d/%d/%d] commit took %s (>= warning threshold %s)",
			count, size, r.flushes, elapsed, r.parent.cfg.WarnLargeBatchThreshold)
	}

	return nil
}

func (r *rocksDBBatch) Empty() bool {
	return r.flushes == 0 && r.builder.Count() == 0 && !r.builder.logData
}

func (r *rocksDBBatch) Len() int {
	return len(r.unsafeRepr())
}

func (r *rocksDBBatch) unsafeRepr() []byte {
	if r.flushes == 0 {
		// We've never flushed to C++. Return the mutations only.
		return r.builder.getRepr()
	}
	r.flushMutations()
	return cSliceToUnsafeGoBytes(C.DBBatchRepr(r.batch))
}

func (r *rocksDBBatch) Repr() []byte {
	if r.flushes == 0 {
		// We've never flushed to C++. Return the mutations only. We make a copy
		// of the builder's byte slice so that the return []byte is valid even
		// if the builder is reset or finished.
		repr := r.builder.getRepr()
		cpy := make([]byte, len(repr))
		copy(cpy, repr)
		return cpy
	}
	r.flushMutations()
	return cSliceToGoBytes(C.DBBatchRepr(r.batch))
}

func (r *rocksDBBatch) Distinct() ReadWriter {
	if r.distinctNeedsFlush {
		r.flushMutations()
	}
	if r.distinctOpen {
		panic("distinct batch already open")
	}
	r.distinctOpen = true
	return &r.distinct
}

func (r *rocksDBBatch) flushMutations() {
	if r.builder.Count() == 0 {
		return
	}
	r.ensureBatch()
	r.distinctNeedsFlush = false
	r.flushes++
	r.flushedCount += int(r.builder.Count())
	r.flushedSize += r.builder.Len()
	if err := dbApplyBatchRepr(r.batch, r.builder.Finish(), false); err != nil {
		panic(err)
	}
	// Force a seek of the underlying iterator on the next Seek/ReverseSeek.
	r.prefixIter.iter.reseek = true
	r.normalIter.iter.reseek = true
}

type dbIteratorGetter interface {
	getIter() *C.DBIterator
}

type rocksDBIterator struct {
	parent *RocksDB
	reader Reader
	iter   *C.DBIterator
	valid  bool
	reseek bool
	prefix bool
	err    error
	key    C.DBKey
	value  C.DBSlice
}

// TODO(peter): Is this pool useful now that rocksDBBatch.NewIterator doesn't
// allocate by returning internal pointers?
var iterPool = sync.Pool{
	New: func() interface{} {
		return &rocksDBIterator{}
	},
}

// newRocksDBIterator returns a new iterator over the supplied RocksDB
// instance. If snapshotHandle is not nil, uses the indicated snapshot.
// The caller must call rocksDBIterator.Close() when finished with the
// iterator to free up resources.
func newRocksDBIterator(
	rdb *C.DBEngine, opts IterOptions, reader Reader, parent *RocksDB,
) MVCCIterator {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	r := iterPool.Get().(*rocksDBIterator)
	r.init(rdb, opts, reader, parent)
	return r
}

func (r *rocksDBIterator) getIter() *C.DBIterator {
	return r.iter
}

func (r *rocksDBIterator) init(rdb *C.DBEngine, opts IterOptions, reader Reader, parent *RocksDB) {
	r.parent = parent
	if debugIteratorLeak && r.parent != nil {
		r.parent.iters.Lock()
		r.parent.iters.m[r] = debug.Stack()
		r.parent.iters.Unlock()
	}

	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}

	r.iter = C.DBNewIter(rdb, goToCIterOptions(opts))
	if r.iter == nil {
		panic("unable to create iterator")
	}
	r.reader = reader
	r.prefix = opts.Prefix
}

func (r *rocksDBIterator) setOptions(opts IterOptions) {
	if opts.MinTimestampHint != (hlc.Timestamp{}) || opts.MaxTimestampHint != (hlc.Timestamp{}) {
		panic("iterator with timestamp hints cannot be reused")
	}
	if !opts.Prefix && len(opts.UpperBound) == 0 && len(opts.LowerBound) == 0 {
		panic("iterator must set prefix or upper bound or lower bound")
	}
	C.DBIterSetLowerBound(r.iter, goToCKey(MakeMVCCMetadataKey(opts.LowerBound)))
	C.DBIterSetUpperBound(r.iter, goToCKey(MakeMVCCMetadataKey(opts.UpperBound)))
}

func (r *rocksDBIterator) checkEngineOpen() {
	if r.reader.Closed() {
		panic("iterator used after backing engine closed")
	}
}

func (r *rocksDBIterator) destroy() {
	if debugIteratorLeak && r.parent != nil {
		r.parent.iters.Lock()
		delete(r.parent.iters.m, r)
		r.parent.iters.Unlock()
	}
	C.DBIterDestroy(r.iter)
	*r = rocksDBIterator{}
}

// The following methods implement the Iterator interface.

func (r *rocksDBIterator) Stats() IteratorStats {
	stats := C.DBIterStats(r.iter)
	return IteratorStats{
		TimeBoundNumSSTs:           int(stats.timebound_num_ssts),
		InternalDeleteSkippedCount: int(stats.internal_delete_skipped_count),
	}
}

func (r *rocksDBIterator) Close() {
	r.destroy()
	iterPool.Put(r)
}

func (r *rocksDBIterator) SeekGE(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		// start=Key("") needs special treatment since we need
		// to access start[0] in an explicit seek.
		r.setState(C.DBIterSeekToFirst(r.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if r.valid && !r.reseek && key.Equal(r.UnsafeKey()) {
			return
		}
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
	}
}

func (r *rocksDBIterator) SeekLT(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		r.setState(C.DBIterSeekToLast(r.iter))
	} else {
		// SeekForPrev positions the iterator at the last key that is less
		// than or equal to key, so we may need to iterate backwards once.
		r.setState(C.DBIterSeekForPrev(r.iter, goToCKey(key)))
		if r.valid && key.Equal(r.UnsafeKey()) {
			r.Prev()
		}
	}
}

func (r *rocksDBIterator) Valid() (bool, error) {
	return r.valid, r.err
}

func (r *rocksDBIterator) Next() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, C.bool(false) /* skip_current_key_versions */))
}

var errReversePrefixIteration = fmt.Errorf("unsupported reverse prefix iteration")

func (r *rocksDBIterator) Prev() {
	r.checkEngineOpen()
	if r.prefix {
		r.valid = false
		r.err = errReversePrefixIteration
		return
	}
	r.setState(C.DBIterPrev(r.iter, C.bool(false) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) NextKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, C.bool(true) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) Key() MVCCKey {
	// The data returned by rocksdb_iter_{key,value} is not meant to be
	// freed by the client. It is a direct reference to the data managed
	// by the iterator, so it is copied instead of freed.
	return cToGoKey(r.key)
}

func (r *rocksDBIterator) Value() []byte {
	return cSliceToGoBytes(r.value)
}

func (r *rocksDBIterator) ValueProto(msg protoutil.Message) error {
	if r.value.len == 0 {
		return nil
	}
	return protoutil.Unmarshal(r.UnsafeValue(), msg)
}

func (r *rocksDBIterator) UnsafeKey() MVCCKey {
	return cToUnsafeGoKey(r.key)
}

func (r *rocksDBIterator) UnsafeValue() []byte {
	return cSliceToUnsafeGoBytes(r.value)
}

func (r *rocksDBIterator) clearState() {
	r.valid = false
	r.reseek = true
	r.key = C.DBKey{}
	r.value = C.DBSlice{}
	r.err = nil
}

func (r *rocksDBIterator) setState(state C.DBIterState) {
	r.valid = bool(state.valid)
	r.reseek = false
	r.key = state.key
	r.value = state.value
	r.err = statusToError(state.status)
}

func (r *rocksDBIterator) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	r.clearState()
	result := C.MVCCComputeStats(r.iter,
		goToCKey(MakeMVCCMetadataKey(start)),
		goToCKey(MakeMVCCMetadataKey(end)),
		C.int64_t(nowNanos))
	stats, err := cStatsToGoStats(result, nowNanos)
	if util.RaceEnabled {
		// If we've come here via batchIterator, then flushMutations (which forces
		// reseek) was called just before C.MVCCComputeStats. Set it here as well
		// to match.
		r.reseek = true
		// C.MVCCComputeStats and ComputeStatsGo must behave identically.
		// There are unit tests to ensure that they return the same result, but
		// as an additional check, use the race builds to check any edge cases
		// that the tests may miss.
		verifyStats, verifyErr := ComputeStatsGo(r, start, end, nowNanos)
		if (err != nil) != (verifyErr != nil) {
			panic(fmt.Sprintf("C.MVCCComputeStats differed from ComputeStatsGo: err %v vs %v", err, verifyErr))
		}
		if !stats.Equal(verifyStats) {
			panic(fmt.Sprintf("C.MVCCComputeStats differed from ComputeStatsGo: stats %+v vs %+v", stats, verifyStats))
		}
	}
	return stats, err
}

func (r *rocksDBIterator) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	var splitKey C.DBString
	r.clearState()
	status := C.MVCCFindSplitKey(r.iter,
		goToCKey(MakeMVCCMetadataKey(start)),
		goToCKey(MakeMVCCMetadataKey(minSplitKey)),
		C.int64_t(targetSize), &splitKey)
	if err := statusToError(status); err != nil {
		return MVCCKey{}, err
	}
	return MVCCKey{Key: cStringToGoBytes(splitKey)}, nil
}

func (r *rocksDBIterator) MVCCOpsSpecialized() bool {
	// rocksDBIterator provides specialized implementations of MVCCGet and
	// MVCCScan.
	return true
}

func (r *rocksDBIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(key) == 0 {
		return nil, nil, emptyKeyError()
	}

	r.clearState()
	state := C.MVCCGet(
		r.iter, goToCSlice(key), goToCTimestamp(timestamp), goToCTxn(opts.Txn),
		C.bool(opts.Inconsistent), C.bool(opts.Tombstones), C.bool(opts.FailOnMoreRecent),
	)

	if err := statusToError(state.status); err != nil {
		return nil, nil, err
	}
	if err := writeTooOldToError(timestamp, state.write_too_old_timestamp); err != nil {
		return nil, nil, err
	}
	if err := uncertaintyToError(timestamp, state.uncertainty_timestamp, opts.Txn); err != nil {
		return nil, nil, err
	}

	intents, err := buildScanIntents(cSliceToGoBytes(state.intents))
	if err != nil {
		return nil, nil, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		return nil, nil, &roachpb.WriteIntentError{Intents: intents}
	}

	var intent *roachpb.Intent
	if len(intents) > 1 {
		return nil, nil, errors.Errorf("expected 0 or 1 intents, got %d", len(intents))
	} else if len(intents) == 1 {
		intent = &intents[0]
	}
	if state.data.len == 0 {
		return nil, intent, nil
	}

	count := state.data.count
	if count > 1 {
		return nil, nil, errors.Errorf("expected 0 or 1 result, found %d", count)
	}
	if count == 0 {
		return nil, intent, nil
	}

	// Extract the value from the batch data.
	repr := copyFromSliceVector(state.data.bufs, state.data.len)
	mvccKey, rawValue, _, err := MVCCScanDecodeKeyValue(repr)
	if err != nil {
		return nil, nil, err
	}
	value := &roachpb.Value{
		RawBytes:  rawValue,
		Timestamp: mvccKey.Timestamp,
	}
	return value, intent, nil
}

func (r *rocksDBIterator) MVCCScan(
	start, end roachpb.Key, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (MVCCScanResult, error) {
	if opts.Inconsistent && opts.Txn != nil {
		return MVCCScanResult{}, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(end) == 0 {
		return MVCCScanResult{}, emptyKeyError()
	}
	if opts.MaxKeys < 0 {
		resumeSpan := &roachpb.Span{Key: start, EndKey: end}
		return MVCCScanResult{ResumeSpan: resumeSpan}, nil
	}

	r.clearState()
	state := C.MVCCScan(
		r.iter, goToCSlice(start), goToCSlice(end), goToCTimestamp(timestamp),
		C.int64_t(opts.MaxKeys), C.int64_t(opts.TargetBytes),
		goToCTxn(opts.Txn), C.bool(opts.Inconsistent),
		C.bool(opts.Reverse), C.bool(opts.Tombstones),
		C.bool(opts.FailOnMoreRecent),
	)

	if err := statusToError(state.status); err != nil {
		return MVCCScanResult{}, err
	}
	if err := writeTooOldToError(timestamp, state.write_too_old_timestamp); err != nil {
		return MVCCScanResult{}, err
	}
	if err := uncertaintyToError(timestamp, state.uncertainty_timestamp, opts.Txn); err != nil {
		return MVCCScanResult{}, err
	}

	kvData := [][]byte{copyFromSliceVector(state.data.bufs, state.data.len)}
	numKVs := int64(state.data.count)
	numBytes := int64(state.data.bytes)

	var resumeSpan *roachpb.Span
	if resumeKey := cSliceToGoBytes(state.resume_key); resumeKey != nil {
		if opts.Reverse {
			resumeSpan = &roachpb.Span{Key: start, EndKey: roachpb.Key(resumeKey).Next()}
		} else {
			resumeSpan = &roachpb.Span{Key: resumeKey, EndKey: end}
		}
	}

	intents, err := buildScanIntents(cSliceToGoBytes(state.intents))
	if err != nil {
		return MVCCScanResult{}, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		return MVCCScanResult{}, &roachpb.WriteIntentError{Intents: intents}
	}

	return MVCCScanResult{
		KVData:     kvData,
		NumKeys:    numKVs,
		NumBytes:   numBytes,
		ResumeSpan: resumeSpan,
		Intents:    intents,
	}, nil
}

func (r *rocksDBIterator) SetUpperBound(key roachpb.Key) {
	C.DBIterSetUpperBound(r.iter, goToCKey(MakeMVCCMetadataKey(key)))
}

// CheckForKeyCollisions indicates if the provided SST data collides with this
// iterator in the specified range.
func (r *rocksDBIterator) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	// Create a C++ iterator over the SST being added. This iterator is used to
	// perform a check for key collisions between the SST being ingested, and the
	// exisiting data. As the collision check is in C++ we are unable to use a
	// pure go iterator as in verifySSTable.
	sst := MakeRocksDBSstFileReader()
	defer sst.Close()
	emptyStats := enginepb.MVCCStats{}

	if err := sst.IngestExternalFile(sstData); err != nil {
		return emptyStats, err
	}
	sstIterator := sst.NewIterator(IterOptions{UpperBound: end}).(*rocksDBIterator)
	defer sstIterator.Close()
	sstIterator.SeekGE(MakeMVCCMetadataKey(start))
	if ok, err := sstIterator.Valid(); err != nil || !ok {
		return emptyStats, errors.Wrap(err, "checking for key collisions")
	}

	var intentErr C.DBString
	var skippedKVStats C.MVCCStatsResult

	state := C.DBCheckForKeyCollisions(r.iter, sstIterator.iter, &skippedKVStats, &intentErr)

	err := statusToError(state.status)
	if err != nil {
		if err.Error() == "WriteIntentError" {
			var e roachpb.WriteIntentError
			if err := protoutil.Unmarshal(cStringToGoBytes(intentErr), &e); err != nil {
				return emptyStats, errors.Wrap(err, "failed to decode write intent error")
			}
			return emptyStats, &e
		} else if err.Error() == "InlineError" {
			return emptyStats, errors.Errorf("inline values are unsupported when checking for key collisions")
		}
		err = errors.Wrap(&Error{msg: cToGoKey(state.key).String()}, "ingested key collides with an existing one")
		return emptyStats, err
	}

	skippedStats, err := cStatsToGoStats(skippedKVStats, 0)
	return skippedStats, err
}

func copyFromSliceVector(bufs *C.DBSlice, len C.int32_t) []byte {
	if bufs == nil {
		return nil
	}

	// Interpret the C pointer as a pointer to a Go array, then slice.
	slices := (*[1 << 20]C.DBSlice)(unsafe.Pointer(bufs))[:len:len]
	neededBytes := 0
	for i := range slices {
		neededBytes += int(slices[i].len)
	}
	data := nonZeroingMakeByteSlice(neededBytes)[:0]
	for i := range slices {
		data = append(data, cSliceToUnsafeGoBytes(slices[i])...)
	}
	return data
}

func cStatsToGoStats(stats C.MVCCStatsResult, nowNanos int64) (enginepb.MVCCStats, error) {
	ms := enginepb.MVCCStats{}
	if err := statusToError(stats.status); err != nil {
		return ms, err
	}

	ms.ContainsEstimates = 0
	ms.LiveBytes = int64(stats.live_bytes)
	ms.KeyBytes = int64(stats.key_bytes)
	ms.ValBytes = int64(stats.val_bytes)
	ms.IntentBytes = int64(stats.intent_bytes)
	ms.LiveCount = int64(stats.live_count)
	ms.KeyCount = int64(stats.key_count)
	ms.ValCount = int64(stats.val_count)
	ms.IntentCount = int64(stats.intent_count)
	ms.IntentAge = int64(stats.intent_age)
	ms.GCBytesAge = int64(stats.gc_bytes_age)
	ms.SysBytes = int64(stats.sys_bytes)
	ms.SysCount = int64(stats.sys_count)
	ms.LastUpdateNanos = nowNanos
	return ms, nil
}

// goToCSlice converts a go byte slice to a DBSlice. Note that this is
// potentially dangerous as the DBSlice holds a reference to the go
// byte slice memory that the Go GC does not know about. This method
// is only intended for use in converting arguments to C
// functions. The C function must copy any data that it wishes to
// retain once the function returns.
func goToCSlice(b []byte) C.DBSlice {
	if len(b) == 0 {
		return C.DBSlice{data: nil, len: 0}
	}
	return C.DBSlice{
		data: (*C.char)(unsafe.Pointer(&b[0])),
		len:  C.size_t(len(b)),
	}
}

func goToCIgnoredSeqNums(b []enginepb.IgnoredSeqNumRange) C.DBIgnoredSeqNums {
	if len(b) == 0 {
		return C.DBIgnoredSeqNums{ranges: nil, len: 0}
	}
	return C.DBIgnoredSeqNums{
		ranges: (*C.DBIgnoredSeqNumRange)(unsafe.Pointer(&b[0])),
		len:    C.int(len(b)),
	}
}

func goToCKey(key MVCCKey) C.DBKey {
	return C.DBKey{
		key:       goToCSlice(key.Key),
		wall_time: C.int64_t(key.Timestamp.WallTime),
		logical:   C.int32_t(key.Timestamp.Logical),
	}
}

func cToGoKey(key C.DBKey) MVCCKey {
	// When converting a C.DBKey to an MVCCKey, give the underlying slice an
	// extra byte of capacity in anticipation of roachpb.Key.Next() being
	// called. The extra byte is trivial extra space, but allows callers to avoid
	// an allocation and copy when calling roachpb.Key.Next(). Note that it is
	// important that the extra byte contain the value 0 in order for the
	// roachpb.Key.Next() fast-path to be invoked. This is true for the code
	// below because make() zero initializes all of the bytes.
	unsafeKey := cSliceToUnsafeGoBytes(key.key)
	safeKey := make([]byte, len(unsafeKey), len(unsafeKey)+1)
	copy(safeKey, unsafeKey)

	return MVCCKey{
		Key: safeKey,
		Timestamp: hlc.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cToUnsafeGoKey(key C.DBKey) MVCCKey {
	return MVCCKey{
		Key: cSliceToUnsafeGoBytes(key.key),
		Timestamp: hlc.Timestamp{
			WallTime: int64(key.wall_time),
			Logical:  int32(key.logical),
		},
	}
}

func cStringToGoString(s C.DBString) string {
	if s.data == nil {
		return ""
	}
	// Reinterpret the string as a slice, then cast to string which does a copy.
	result := string(cSliceToUnsafeGoBytes(C.DBSlice(s)))
	C.free(unsafe.Pointer(s.data))
	return result
}

func cStringToGoBytes(s C.DBString) []byte {
	if s.data == nil {
		return nil
	}
	result := gobytes(unsafe.Pointer(s.data), int(s.len))
	C.free(unsafe.Pointer(s.data))
	return result
}

func cSliceToGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	return gobytes(unsafe.Pointer(s.data), int(s.len))
}

func cSliceToUnsafeGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[MaxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func goToCTimestamp(ts hlc.Timestamp) C.DBTimestamp {
	return C.DBTimestamp{
		wall_time: C.int64_t(ts.WallTime),
		logical:   C.int32_t(ts.Logical),
	}
}

func cToGoTimestamp(ts C.DBTimestamp) hlc.Timestamp {
	return hlc.Timestamp{
		WallTime: int64(ts.wall_time),
		Logical:  int32(ts.logical),
	}
}

func goToCTxn(txn *roachpb.Transaction) C.DBTxn {
	var r C.DBTxn
	if txn != nil {
		r.id = goToCSlice(txn.ID.GetBytesMut())
		r.epoch = C.uint32_t(txn.Epoch)
		r.sequence = C.int32_t(txn.Sequence)
		r.max_timestamp = goToCTimestamp(txn.MaxTimestamp)
		r.ignored_seqnums = goToCIgnoredSeqNums(txn.IgnoredSeqNums)
	}
	return r
}

func goToCIterOptions(opts IterOptions) C.DBIterOptions {
	return C.DBIterOptions{
		prefix:             C.bool(opts.Prefix),
		lower_bound:        goToCKey(MakeMVCCMetadataKey(opts.LowerBound)),
		upper_bound:        goToCKey(MakeMVCCMetadataKey(opts.UpperBound)),
		min_timestamp_hint: goToCTimestamp(opts.MinTimestampHint),
		max_timestamp_hint: goToCTimestamp(opts.MaxTimestampHint),
		with_stats:         C.bool(opts.WithStats),
	}
}

func statusToError(s C.DBStatus) error {
	if s.data == nil {
		return nil
	}
	return &Error{msg: cStringToGoString(s)}
}

func writeTooOldToError(readTS hlc.Timestamp, existingCTS C.DBTimestamp) error {
	existingTS := cToGoTimestamp(existingCTS)
	if !existingTS.IsEmpty() {
		// The txn can't write at the existing timestamp, so we provide the
		// error with the timestamp immediately after it.
		return roachpb.NewWriteTooOldError(readTS, existingTS.Next())
	}
	return nil
}

func uncertaintyToError(
	readTS hlc.Timestamp, existingCTS C.DBTimestamp, txn *roachpb.Transaction,
) error {
	existingTS := cToGoTimestamp(existingCTS)
	if !existingTS.IsEmpty() {
		return roachpb.NewReadWithinUncertaintyIntervalError(readTS, existingTS, txn)
	}
	return nil
}

// goMerge takes existing and update byte slices that are expected to
// be marshaled roachpb.Values and merges the two values returning a
// marshaled roachpb.Value or an error.
func goMerge(existing, update []byte) ([]byte, error) {
	var result C.DBString
	status := C.DBMergeOne(goToCSlice(existing), goToCSlice(update), &result)
	if status.data != nil {
		return nil, errors.Errorf("%s: existing=%q, update=%q",
			cStringToGoString(status), existing, update)
	}
	return cStringToGoBytes(result), nil
}

// goPartialMerge takes existing and update byte slices that are expected to
// be marshaled roachpb.Values and performs a partial merge using C++ code,
// marshaled roachpb.Value or an error.
func goPartialMerge(existing, update []byte) ([]byte, error) {
	var result C.DBString
	status := C.DBPartialMergeOne(goToCSlice(existing), goToCSlice(update), &result)
	if status.data != nil {
		return nil, errors.Errorf("%s: existing=%q, update=%q",
			cStringToGoString(status), existing, update)
	}
	return cStringToGoBytes(result), nil
}

func emptyKeyError() error {
	return errors.Errorf("attempted access to empty key")
}

func dbPut(rdb *C.DBEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// *Put, *Get, and *Delete call memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices
	// being reclaimed by the GC.
	return statusToError(C.DBPut(rdb, goToCKey(key), goToCSlice(value)))
}

func dbMerge(rdb *C.DBEngine, key MVCCKey, value []byte) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}

	// DBMerge calls memcpy() (by way of MemTable::Add)
	// when called, so we do not need to worry about these byte slices being
	// reclaimed by the GC.
	return statusToError(C.DBMerge(rdb, goToCKey(key), goToCSlice(value)))
}

func dbApplyBatchRepr(rdb *C.DBEngine, repr []byte, sync bool) error {
	return statusToError(C.DBApplyBatchRepr(rdb, goToCSlice(repr), C.bool(sync)))
}

// dbGet returns the value for the given key.
func dbGet(rdb *C.DBEngine, key MVCCKey) ([]byte, error) {
	if len(key.Key) == 0 {
		return nil, emptyKeyError()
	}
	var result C.DBString
	err := statusToError(C.DBGet(rdb, goToCKey(key), &result))
	if err != nil {
		return nil, err
	}
	return cStringToGoBytes(result), nil
}

func dbGetProto(
	rdb *C.DBEngine, key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		err = emptyKeyError()
		return
	}
	var result C.DBString
	if err = statusToError(C.DBGet(rdb, goToCKey(key), &result)); err != nil {
		return
	}
	if result.len == 0 {
		msg.Reset()
		return
	}
	ok = true
	if msg != nil {
		// Make a byte slice that is backed by result.data. This slice
		// cannot live past the lifetime of this method, but we're only
		// using it to unmarshal the roachpb.
		data := cSliceToUnsafeGoBytes(C.DBSlice(result))
		err = protoutil.Unmarshal(data, msg)
	}
	C.free(unsafe.Pointer(result.data))
	keyBytes = int64(key.EncodedSize())
	valBytes = int64(result.len)
	return
}

func dbClear(rdb *C.DBEngine, key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return statusToError(C.DBDelete(rdb, goToCKey(key)))
}

func dbSingleClear(rdb *C.DBEngine, key MVCCKey) error {
	if len(key.Key) == 0 {
		return emptyKeyError()
	}
	return statusToError(C.DBSingleDelete(rdb, goToCKey(key)))
}

func dbClearRange(rdb *C.DBEngine, start, end MVCCKey) error {
	if err := statusToError(C.DBDeleteRange(rdb, goToCKey(start), goToCKey(end))); err != nil {
		return err
	}
	// This is a serious hack. RocksDB generates sstables which cover an
	// excessively large amount of the key space when range tombstones are
	// present. The crux of the problem is that the logic for determining sstable
	// boundaries depends on actual keys being present. So we help that logic
	// along by adding deletions of the first key covered by the range tombstone,
	// and a key near the end of the range (previous is difficult). See
	// TestRocksDBDeleteRangeCompaction which verifies that either this hack is
	// working, or the upstream problem was fixed in RocksDB.
	if err := dbClear(rdb, start); err != nil {
		return err
	}
	prev := make(roachpb.Key, len(end.Key))
	copy(prev, end.Key)
	if n := len(prev) - 1; prev[n] > 0 {
		prev[n]--
	} else {
		prev = prev[:n]
	}
	if start.Key.Compare(prev) < 0 {
		if err := dbClear(rdb, MakeMVCCMetadataKey(prev)); err != nil {
			return err
		}
	}
	return nil
}

func dbClearIterRange(rdb *C.DBEngine, iter Iterator, start, end roachpb.Key) error {
	getter, ok := iter.(dbIteratorGetter)
	if !ok {
		return errors.Errorf("%T is not a RocksDB iterator", iter)
	}
	return statusToError(C.DBDeleteIterRange(rdb, getter.getIter(),
		goToCKey(MakeMVCCMetadataKey(start)), goToCKey(MakeMVCCMetadataKey(end))))
}

// TODO(dan): Rename this to RocksDBSSTFileReader and RocksDBSSTFileWriter.

// RocksDBSstFileReader allows iteration over a number of non-overlapping
// sstables exported by `RocksDBSstFileWriter`.
type RocksDBSstFileReader struct {
	rocksDB         *RocksDB
	filenameCounter int
}

// MakeRocksDBSstFileReader creates a RocksDBSstFileReader backed by an
// in-memory RocksDB instance.
func MakeRocksDBSstFileReader() RocksDBSstFileReader {
	// cacheSize was selected because it's used for almost all other newRocksDBInMem
	// calls. It's seemed to work well so far, but there's probably more tuning
	// to be done here.
	const cacheSize = 1 << 20
	return RocksDBSstFileReader{rocksDB: newRocksDBInMem(roachpb.Attributes{}, cacheSize)}
}

// IngestExternalFile links a file with the given contents into a database. See
// the RocksDB documentation on `IngestExternalFile` for the various
// restrictions on what can be added.
func (fr *RocksDBSstFileReader) IngestExternalFile(data []byte) error {
	if fr.rocksDB == nil {
		return errors.New("cannot call IngestExternalFile on a closed reader")
	}

	filename := fmt.Sprintf("ingest-%d", fr.filenameCounter)
	fr.filenameCounter++
	if err := fr.rocksDB.WriteFile(filename, data); err != nil {
		return err
	}

	cPaths := make([]*C.char, 1)
	cPaths[0] = C.CString(filename)
	cPathLen := C.size_t(len(cPaths))
	defer C.free(unsafe.Pointer(cPaths[0]))

	const noMove = false
	return statusToError(C.DBIngestExternalFiles(fr.rocksDB.rdb, &cPaths[0], cPathLen, noMove))
}

// Iterate iterates over the keys between start inclusive and end
// exclusive, invoking f() on each key/value pair.
func (fr *RocksDBSstFileReader) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (bool, error),
) error {
	if fr.rocksDB == nil {
		return errors.New("cannot call Iterate on a closed reader")
	}
	return fr.rocksDB.Iterate(start, end, f)
}

// NewIterator returns an iterator over this sst reader.
func (fr *RocksDBSstFileReader) NewIterator(opts IterOptions) Iterator {
	return newRocksDBIterator(fr.rocksDB.rdb, opts, fr.rocksDB, fr.rocksDB)
}

// Close finishes the reader.
func (fr *RocksDBSstFileReader) Close() {
	if fr.rocksDB == nil {
		return
	}
	fr.rocksDB.Close()
	fr.rocksDB = nil
}

// RocksDBSstFileWriter creates a file suitable for importing with
// RocksDBSstFileReader. It implements the Writer interface.
type RocksDBSstFileWriter struct {
	fw *C.DBSstFileWriter
	// dataSize tracks the total key and value bytes added so far.
	dataSize int64
}

var _ Writer = &RocksDBSstFileWriter{}

// MakeRocksDBSstFileWriter creates a new RocksDBSstFileWriter with the default
// configuration.
//
// NOTE: This is deprecated - and should only be used in tests to check for
// equivalence with engine.SSTWriter.
//
// TODO(itsbilal): Move all tests to SSTWriter and then delete this function
// and struct.
func MakeRocksDBSstFileWriter() (RocksDBSstFileWriter, error) {
	fw := C.DBSstFileWriterNew()
	err := statusToError(C.DBSstFileWriterOpen(fw))
	return RocksDBSstFileWriter{fw: fw}, err
}

// ApplyBatchRepr implements the Writer interface.
func (fw *RocksDBSstFileWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("unimplemented")
}

// Clear implements the Writer interface. Note that it inserts a tombstone
// rather than actually remove the entry from the storage engine. An error is
// returned if it is not greater than any previous key used in Put or Clear
// (according to the comparator configured during writer creation). Close
// cannot have been called.
func (fw *RocksDBSstFileWriter) Clear(key MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call Clear on a closed writer")
	}
	fw.dataSize += int64(len(key.Key))
	return statusToError(C.DBSstFileWriterDelete(fw.fw, goToCKey(key)))
}

// DataSize returns the total key and value bytes added so far.
func (fw *RocksDBSstFileWriter) DataSize() int64 {
	return fw.dataSize
}

// SingleClear implements the Writer interface.
func (fw *RocksDBSstFileWriter) SingleClear(key MVCCKey) error {
	panic("unimplemented")
}

// ClearRange implements the Writer interface. Note that it inserts a range deletion
// tombstone rather than actually remove the entries from the storage engine.
// It can be called at any time with respect to Put and Clear.
func (fw *RocksDBSstFileWriter) ClearRange(start, end MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call ClearRange on a closed writer")
	}
	fw.dataSize += int64(len(start.Key)) + int64(len(end.Key))
	return statusToError(C.DBSstFileWriterDeleteRange(fw.fw, goToCKey(start), goToCKey(end)))
}

// ClearIterRange implements the Writer interface.
//
// NOTE: This method is fairly expensive as it performs a Cgo call for every
// key deleted.
func (fw *RocksDBSstFileWriter) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	if fw.fw == nil {
		return errors.New("cannot call ClearIterRange on a closed writer")
	}
	mvccEndKey := MakeMVCCMetadataKey(end)
	iter.SeekGE(MakeMVCCMetadataKey(start))
	for {
		valid, err := iter.Valid()
		if err != nil {
			return err
		}
		if !valid || !iter.Key().Less(mvccEndKey) {
			break
		}
		if err := fw.Clear(iter.Key()); err != nil {
			return err
		}
		iter.Next()
	}
	return nil
}

// Merge implements the Writer interface.
func (fw *RocksDBSstFileWriter) Merge(key MVCCKey, value []byte) error {
	panic("unimplemented")
}

// Put implements the Writer interface. It puts a kv entry into the sstable
// being built. An error is returned if it is not greater than any previous key
// used in Put or Clear (according to the comparator configured during writer
// creation). Close cannot have been called.
func (fw *RocksDBSstFileWriter) Put(key MVCCKey, value []byte) error {
	if fw.fw == nil {
		return errors.New("cannot call Put on a closed writer")
	}
	fw.dataSize += int64(len(key.Key)) + int64(len(value))
	return statusToError(C.DBSstFileWriterAdd(fw.fw, goToCKey(key), goToCSlice(value)))
}

// LogData implements the Writer interface.
func (fw *RocksDBSstFileWriter) LogData(data []byte) error {
	panic("unimplemented")
}

// LogLogicalOp implements the Writer interface.
func (fw *RocksDBSstFileWriter) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

// Truncate truncates the writer's current memory buffer and returns the
// contents it contained. May be called multiple times. The function may not
// truncate and return all keys if the underlying RocksDB blocks have not been
// flushed. Close cannot have been called.
func (fw *RocksDBSstFileWriter) Truncate() ([]byte, error) {
	if fw.fw == nil {
		return nil, errors.New("cannot call Truncate on a closed writer")
	}
	var contents C.DBString
	if err := statusToError(C.DBSstFileWriterTruncate(fw.fw, &contents)); err != nil {
		return nil, err
	}
	return cStringToGoBytes(contents), nil
}

// Finish finalizes the writer and returns the constructed file's contents. At
// least one kv entry must have been added.
func (fw *RocksDBSstFileWriter) Finish() ([]byte, error) {
	if fw.fw == nil {
		return nil, errors.New("cannot call Finish on a closed writer")
	}
	var contents C.DBString
	if err := statusToError(C.DBSstFileWriterFinish(fw.fw, &contents)); err != nil {
		return nil, err
	}
	return cStringToGoBytes(contents), nil
}

// Close finishes and frees memory and other resources. Close is idempotent.
func (fw *RocksDBSstFileWriter) Close() {
	if fw.fw == nil {
		return
	}
	C.DBSstFileWriterClose(fw.fw)
	fw.fw = nil
}

// RunLDB runs RocksDB's ldb command-line tool. The passed
// command-line arguments should not include argv[0].
func RunLDB(args []string) {
	// Prepend "ldb" as argv[0].
	args = append([]string{"ldb"}, args...)
	argv := make([]*C.char, len(args))
	for i := range args {
		argv[i] = C.CString(args[i])
	}
	defer func() {
		for i := range argv {
			C.free(unsafe.Pointer(argv[i]))
		}
	}()

	C.DBRunLDB(C.int(len(argv)), &argv[0])
}

// RunSSTDump runs RocksDB's sst_dump command-line tool. The passed
// command-line arguments should not include argv[0].
func RunSSTDump(args []string) {
	// Prepend "sst_dump" as argv[0].
	args = append([]string{"sst_dump"}, args...)
	argv := make([]*C.char, len(args))
	for i := range args {
		argv[i] = C.CString(args[i])
	}
	defer func() {
		for i := range argv {
			C.free(unsafe.Pointer(argv[i]))
		}
	}()

	C.DBRunSSTDump(C.int(len(argv)), &argv[0])
}

// GetAuxiliaryDir returns the auxiliary storage path for this engine.
func (r *RocksDB) GetAuxiliaryDir() string {
	return r.auxDir
}

// PreIngestDelay implements the Engine interface.
func (r *RocksDB) PreIngestDelay(ctx context.Context) {
	preIngestDelay(ctx, r, r.cfg.Settings)
}

// IngestExternalFiles atomically links a slice of files into the RocksDB
// log-structured merge-tree.
func (r *RocksDB) IngestExternalFiles(ctx context.Context, paths []string) error {
	cPaths := make([]*C.char, len(paths))
	for i := range paths {
		cPaths[i] = C.CString(paths[i])
	}
	defer func() {
		for i := range cPaths {
			C.free(unsafe.Pointer(cPaths[i]))
		}
	}()

	return statusToError(C.DBIngestExternalFiles(
		r.rdb,
		&cPaths[0],
		C.size_t(len(cPaths)),
		C._Bool(true), // move_files
	))
}

// InMem returns true if the receiver is an in-memory engine and false
// otherwise.
func (r *RocksDB) InMem() bool {
	return r.cfg.Dir == ""
}

// ReadFile reads the content from a file with the given filename. The file
// must have been opened through Engine.OpenFile. Otherwise an error will be
// returned.
func (r *RocksDB) ReadFile(filename string) ([]byte, error) {
	var data C.DBSlice
	if err := statusToError(C.DBEnvReadFile(r.rdb, goToCSlice([]byte(filename)), &data)); err != nil {
		return nil, notFoundErrOrDefault(err)
	}
	defer C.free(unsafe.Pointer(data.data))
	return cSliceToGoBytes(data), nil
}

// WriteFile writes data to a file in this RocksDB's env.
func (r *RocksDB) WriteFile(filename string, data []byte) error {
	return statusToError(C.DBEnvWriteFile(r.rdb, goToCSlice([]byte(filename)), goToCSlice(data)))
}

// Remove deletes the file with the given filename from this RocksDB's env.
// If the file with given filename doesn't exist, return os.ErrNotExist.
func (r *RocksDB) Remove(filename string) error {
	if err := statusToError(C.DBEnvDeleteFile(r.rdb, goToCSlice([]byte(filename)))); err != nil {
		return notFoundErrOrDefault(err)
	}
	return nil
}

// RemoveAll removes path and any children it contains from this RocksDB's
// env. If the path does not exist, RemoveAll returns nil (no error).
func (r *RocksDB) RemoveAll(path string) error {
	// We don't have a reliable way of telling whether a path is a directory
	// or a file from the RocksDB Env interface. Assume it's a directory,
	// ignoring any resulting error, and delete any of its children.
	dirents, listErr := r.List(path)
	if listErr == nil {
		for _, dirent := range dirents {
			err := r.RemoveAll(filepath.Join(path, dirent))
			if err != nil {
				return err
			}
		}

		// Path should exist, point to a directory and have no children.
		return r.RemoveDir(path)
	}

	// Path might be a file, non-existent, or a directory for which List
	// errored for some other reason.
	err := r.Remove(path)
	if err == nil {
		return nil
	}
	if os.IsNotExist(err) && os.IsNotExist(listErr) {
		return nil
	}
	return listErr
}

// Link creates 'newname' as a hard link to 'oldname'. This use the Env
// responsible for the file which may handle extra logic (eg: copy encryption
// settings for EncryptedEnv).
func (r *RocksDB) Link(oldname, newname string) error {
	if err := statusToError(C.DBEnvLinkFile(r.rdb, goToCSlice([]byte(oldname)), goToCSlice([]byte(newname)))); err != nil {
		return &os.LinkError{
			Op:  "link",
			Old: oldname,
			New: newname,
			Err: err,
		}
	}
	return nil
}

// IsValidSplitKey returns whether the key is a valid split key. Certain key
// ranges cannot be split (the meta1 span and the system DB span); split keys
// chosen within any of these ranges are considered invalid. And a split key
// equal to Meta2KeyMax (\x03\xff\xff) is considered invalid.
func IsValidSplitKey(key roachpb.Key) bool {
	return bool(C.MVCCIsValidSplitKey(goToCSlice(key)))
}

// lockFile sets a lock on the specified file using RocksDB's file locking interface.
func lockFile(filename string) (C.DBFileLock, error) {
	var lock C.DBFileLock
	// C.DBLockFile mutates its argument. `lock, statusToError(...)`
	// happens to work in gc, but does not work in gccgo.
	//
	// See https://github.com/golang/go/issues/23188.
	err := statusToError(C.DBLockFile(goToCSlice([]byte(filename)), &lock))
	return lock, err
}

// unlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for the lock.
func unlockFile(lock C.DBFileLock) error {
	return statusToError(C.DBUnlockFile(lock))
}

// MVCCScanDecodeKeyValue decodes a key/value pair returned in an MVCCScan
// "batch" (this is not the RocksDB batch repr format), returning both the
// key/value and the suffix of data remaining in the batch.
func MVCCScanDecodeKeyValue(repr []byte) (key MVCCKey, value []byte, orepr []byte, err error) {
	k, ts, value, orepr, err := enginepb.ScanDecodeKeyValue(repr)
	return MVCCKey{k, ts}, value, orepr, err
}

// MVCCScanDecodeKeyValues decodes all key/value pairs returned in one or more
// MVCCScan "batches" (this is not the RocksDB batch repr format). The provided
// function is called for each key/value pair.
func MVCCScanDecodeKeyValues(repr [][]byte, fn func(key MVCCKey, rawBytes []byte) error) error {
	var k MVCCKey
	var rawBytes []byte
	var err error
	for _, data := range repr {
		for len(data) > 0 {
			k, rawBytes, data, err = MVCCScanDecodeKeyValue(data)
			if err != nil {
				return err
			}
			if err = fn(k, rawBytes); err != nil {
				return err
			}
		}
	}
	return nil
}

func notFoundErrOrDefault(err error) error {
	if err == nil {
		return nil
	}
	errStr := err.Error()
	if strings.Contains(errStr, "No such") ||
		strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "does not exist") ||
		strings.Contains(errStr, "NotFound:") ||
		strings.Contains(errStr, "cannot find") {
		return os.ErrNotExist
	}
	return err
}

// rocksdbWritableFile implements the File interface. It is used to interact with the
// DBWritableFile in the corresponding RocksDB env.
type rocksdbWritableFile struct {
	file C.DBWritableFile
	rdb  *C.DBEngine
}

var _ fs.File = &rocksdbWritableFile{}

// Write implements the File interface.
func (f *rocksdbWritableFile) Write(data []byte) (int, error) {
	err := statusToError(C.DBEnvAppendFile(f.rdb, f.file, goToCSlice(data)))
	return len(data), err
}

// Close implements the File interface.
func (f *rocksdbWritableFile) Close() error {
	return statusToError(C.DBEnvCloseFile(f.rdb, f.file))
}

// Sync implements the File interface.
func (f *rocksdbWritableFile) Sync() error {
	return statusToError(C.DBEnvSyncFile(f.rdb, f.file))
}

// Read implements the File interface.
func (f *rocksdbWritableFile) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot read file opened for writing")
}

// ReadAt implements the File interface.
func (f *rocksdbWritableFile) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("cannot read file opened for writing")
}

// rocksdbReadableFile implements the File interface. It is used to interact with the
// DBReadableFile in the corresponding RocksDB env.
type rocksdbReadableFile struct {
	file   C.DBReadableFile
	rdb    *C.DBEngine
	offset int64
}

var _ fs.File = &rocksdbReadableFile{}

// Write implements the File interface.
func (f *rocksdbReadableFile) Write(data []byte) (int, error) {
	return 0, fmt.Errorf("cannot write file opened for reading")
}

// Close implements the File interface.
func (f *rocksdbReadableFile) Close() error {
	return statusToError(C.DBEnvCloseReadableFile(f.rdb, f.file))
}

// Sync implements the File interface.
func (f *rocksdbReadableFile) Sync() error {
	return fmt.Errorf("cannot sync file opened for reading")
}

// Read implements the File interface.
func (f *rocksdbReadableFile) Read(p []byte) (n int, err error) {
	n, err = f.ReadAt(p, f.offset)
	f.offset += int64(n)
	return
}

// ReadAt implements the File interface.
func (f *rocksdbReadableFile) ReadAt(p []byte, off int64) (int, error) {
	var n C.int
	err := statusToError(C.DBEnvReadAtFile(f.rdb, f.file, goToCSlice(p), C.int64_t(off), &n))
	// The io.ReaderAt interface requires implementations to return a non-nil
	// error if fewer than len(p) bytes are read.
	if int(n) < len(p) {
		err = io.EOF
	}
	return int(n), err
}

type rocksdbDirectory struct {
	file C.DBDirectory
	rdb  *C.DBEngine
}

var _ fs.File = &rocksdbDirectory{}

// Write implements the File interface.
func (f *rocksdbDirectory) Write(data []byte) (int, error) {
	return 0, fmt.Errorf("cannot write to directory")
}

// Close implements the File interface.
func (f *rocksdbDirectory) Close() error {
	return statusToError(C.DBEnvCloseDirectory(f.rdb, f.file))
}

// Sync implements the File interface.
func (f *rocksdbDirectory) Sync() error {
	return statusToError(C.DBEnvSyncDirectory(f.rdb, f.file))
}

// Read implements the File interface.
func (f *rocksdbDirectory) Read(p []byte) (n int, err error) {
	return 0, fmt.Errorf("cannot read directory")
}

// ReadAt implements the File interface.
func (f *rocksdbDirectory) ReadAt(p []byte, off int64) (n int, err error) {
	return 0, fmt.Errorf("cannot read directory")
}

var _ fs.FS = &RocksDB{}

// Create implements the FS interface.
func (r *RocksDB) Create(name string) (fs.File, error) {
	return r.CreateWithSync(name, 0)
}

// CreateWithSync implements the FS interface.
func (r *RocksDB) CreateWithSync(name string, bytesPerSync int) (fs.File, error) {
	var file C.DBWritableFile
	if err := statusToError(C.DBEnvOpenFile(
		r.rdb, goToCSlice([]byte(name)), C.uint64_t(bytesPerSync), &file)); err != nil {
		return nil, notFoundErrOrDefault(err)
	}
	return &rocksdbWritableFile{file: file, rdb: r.rdb}, nil
}

// Open implements the FS interface.
func (r *RocksDB) Open(name string) (fs.File, error) {
	var file C.DBReadableFile
	if err := statusToError(C.DBEnvOpenReadableFile(r.rdb, goToCSlice([]byte(name)), &file)); err != nil {
		return nil, notFoundErrOrDefault(err)
	}
	return &rocksdbReadableFile{file: file, rdb: r.rdb}, nil
}

// OpenDir implements the FS interface.
func (r *RocksDB) OpenDir(name string) (fs.File, error) {
	var file C.DBDirectory
	if err := statusToError(C.DBEnvOpenDirectory(r.rdb, goToCSlice([]byte(name)), &file)); err != nil {
		return nil, notFoundErrOrDefault(err)
	}
	return &rocksdbDirectory{file: file, rdb: r.rdb}, nil
}

// Rename implements the FS interface.
func (r *RocksDB) Rename(oldname, newname string) error {
	return statusToError(C.DBEnvRenameFile(r.rdb, goToCSlice([]byte(oldname)), goToCSlice([]byte(newname))))
}

// MkdirAll implements the FS interface.
func (r *RocksDB) MkdirAll(path string) error {
	path = filepath.Clean(path)

	// Skip trailing path separators.
	for len(path) > 0 && path[len(path)-1] == filepath.Separator {
		path = path[:len(path)-1]
	}
	// The path may be empty after cleaning and trimming tailing path
	// separators.
	if path == "" {
		return nil
	}

	// Ensure the parent exists first.
	parent, _ := filepath.Split(path)
	if parent != "" {
		if err := r.MkdirAll(parent); err != nil {
			return err
		}
	}
	return statusToError(C.DBEnvCreateDir(r.rdb, goToCSlice([]byte(path))))
}

// RemoveDir implements the FS interface.
func (r *RocksDB) RemoveDir(name string) error {
	return notFoundErrOrDefault(statusToError(C.DBEnvDeleteDir(r.rdb, goToCSlice([]byte(name)))))
}

// List implements the FS interface.
func (r *RocksDB) List(name string) ([]string, error) {
	list := C.DBEnvListDir(r.rdb, goToCSlice([]byte(name)))
	n := list.n
	names := list.names
	// We can't index into names because it is a pointer, not a slice. The
	// hackery below treats the pointer as an array and then constructs
	// a slice from it.
	nameSize := unsafe.Sizeof(C.DBString{})
	nameVal := func(i int) C.DBString {
		return *(*C.DBString)(unsafe.Pointer(uintptr(unsafe.Pointer(names)) + uintptr(i)*nameSize))
	}
	err := statusToError(list.status)
	if err != nil {
		err = notFoundErrOrDefault(err)
	}

	result := make([]string, n)
	j := 0
	for i := range result {
		str := cStringToGoString(nameVal(i))
		if str == "." || str == ".." {
			continue
		}
		result[j] = str
		j++
	}
	C.free(unsafe.Pointer(names))

	result = result[:j]
	sort.Strings(result)
	return result, err
}

// Stat implements the FS interface.
func (r *RocksDB) Stat(name string) (os.FileInfo, error) {
	// The RocksDB Env doesn't expose a Stat equivalent. If we're using an
	// on-disk filesystem, circumvent the Env and return the os.Stat results.
	if r.cfg.Dir != "" {
		return os.Stat(name)
	}

	// Otherwise, we don't know whether the path names a directory or a file,
	// so try both to check for existence.  The code paths that actually hit
	// this today are only checking for existence, so we return an
	// unimplemented FileInfo implementation.
	if _, listErr := r.List(name); listErr == nil {
		return inMemRocksDBFileInfo{}, nil
	}
	f, err := r.Open(name)
	if err != nil {
		return nil, err
	}
	return inMemRocksDBFileInfo{}, f.Close()
}

type inMemRocksDBFileInfo struct{}

var _ os.FileInfo = inMemRocksDBFileInfo{}

func (fi inMemRocksDBFileInfo) Name() string       { panic("unimplemented") }
func (fi inMemRocksDBFileInfo) Size() int64        { panic("unimplemented") }
func (fi inMemRocksDBFileInfo) Mode() os.FileMode  { panic("unimplemented") }
func (fi inMemRocksDBFileInfo) ModTime() time.Time { panic("unimplemented") }
func (fi inMemRocksDBFileInfo) IsDir() bool        { panic("unimplemented") }
func (fi inMemRocksDBFileInfo) Sys() interface{}   { panic("unimplemented") }

// ThreadStacks returns the stacks for all threads. The stacks are raw
// addresses, and do not contain symbols. Use addr2line (or atos on Darwin) to
// symbolize.
func ThreadStacks() string {
	return cStringToGoString(C.DBDumpThreadStacks())
}
