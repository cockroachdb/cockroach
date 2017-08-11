// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package engine

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/gogo/protobuf/proto"
	"github.com/pkg/errors"
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// TODO(tamird): why does rocksdb not link jemalloc,snappy statically?

// #cgo CPPFLAGS: -I../../../c-deps/libroach/include
// #cgo LDFLAGS: -lroach
// #cgo LDFLAGS: -lprotobuf
// #cgo LDFLAGS: -lrocksdb
// #cgo LDFLAGS: -lsnappy
// #cgo linux LDFLAGS: -lrt -lpthread
// #cgo windows LDFLAGS: -lrpcrt4
//
// #include <stdlib.h>
// #include <libroach.h>
import "C"

//export rocksDBLog
func rocksDBLog(s *C.char, n C.int) {
	// Note that rocksdb logging is only enabled if log.V(3) is true
	// when RocksDB.Open() is called.
	log.Info(context.TODO(), C.GoStringN(s, n))
}

//export prettyPrintKey
func prettyPrintKey(cKey C.DBKey) *C.char {
	mvccKey := MVCCKey{
		Key: C.GoBytes(unsafe.Pointer(cKey.key.data), cKey.key.len),
		Timestamp: hlc.Timestamp{
			WallTime: int64(cKey.wall_time),
			Logical:  int32(cKey.logical),
		},
	}
	return C.CString(mvccKey.String())
}

const (
	// defaultBlockSize configures the size of a black. When reading a key-value
	// pair from a table file, RocksDB loads an entire block into memory. The
	// RocksDB default is 4KB. This sets it to 32KB.
	defaultBlockSize = 32 << 10

	// DefaultMaxOpenFiles is the default value for rocksDB's max_open_files
	// option.
	DefaultMaxOpenFiles = -1
	// RecommendedMaxOpenFiles is the recommended value for rocksDB's
	// max_open_files option. If more file descriptors are available than the
	// recommended number, than the default value is used.
	RecommendedMaxOpenFiles = 10000
	// MinimumMaxOpenFiles is the minimum value that rocksDB's max_open_files
	// option can be set to. While this should be set as high as possible, the
	// minimum total for a single store node must be under 2048 for Windows
	// compatibility. See:
	// https://wpdev.uservoice.com/forums/266908-command-prompt-console-bash-on-ubuntu-on-windo/suggestions/17310124-add-ability-to-change-max-number-of-open-files-for
	MinimumMaxOpenFiles = 1700
)

// SSTableInfo contains metadata about a single RocksDB sstable. This mirrors
// the C.DBSSTable struct contents.
type SSTableInfo struct {
	Level int
	Size  int64
	Start MVCCKey
	End   MVCCKey
}

// SSTableInfos is a slice of SSTableInfo structures.
type SSTableInfos []SSTableInfo

func (s SSTableInfos) Len() int {
	return len(s)
}

func (s SSTableInfos) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s SSTableInfos) Less(i, j int) bool {
	switch {
	case s[i].Level < s[j].Level:
		return true
	case s[i].Level > s[j].Level:
		return false
	case s[i].Size > s[j].Size:
		return true
	case s[i].Size < s[j].Size:
		return false
	default:
		return s[i].Start.Less(s[j].Start)
	}
}

func (s SSTableInfos) String() string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
		TB = 1 << 40
	)

	roundTo := func(val, to int64) int64 {
		return (val + to/2) / to
	}

	// We're intentionally not using humanizeutil here as we want a slightly more
	// compact representation.
	humanize := func(size int64) string {
		switch {
		case size < MB:
			return fmt.Sprintf("%dK", roundTo(size, KB))
		case size < GB:
			return fmt.Sprintf("%dM", roundTo(size, MB))
		case size < TB:
			return fmt.Sprintf("%dG", roundTo(size, GB))
		default:
			return fmt.Sprintf("%dT", roundTo(size, TB))
		}
	}

	type levelInfo struct {
		size  int64
		count int
	}

	var levels []*levelInfo
	for _, t := range s {
		for i := len(levels); i <= t.Level; i++ {
			levels = append(levels, &levelInfo{})
		}
		info := levels[t.Level]
		info.size += t.Size
		info.count++
	}

	var maxSize int
	var maxLevelCount int
	for _, info := range levels {
		size := len(humanize(info.size))
		if maxSize < size {
			maxSize = size
		}
		count := 1 + int(math.Log10(float64(info.count)))
		if maxLevelCount < count {
			maxLevelCount = count
		}
	}
	levelFormat := fmt.Sprintf("%%d [ %%%ds %%%dd ]:", maxSize, maxLevelCount)

	level := -1
	var buf bytes.Buffer
	var lastSize string
	var lastSizeCount int

	flushLastSize := func() {
		if lastSizeCount > 0 {
			fmt.Fprintf(&buf, " %s", lastSize)
			if lastSizeCount > 1 {
				fmt.Fprintf(&buf, "[%d]", lastSizeCount)
			}
			lastSizeCount = 0
		}
	}

	maybeFlush := func(newLevel, i int) {
		if level == newLevel {
			return
		}
		flushLastSize()
		if buf.Len() > 0 {
			buf.WriteString("\n")
		}
		level = newLevel
		if level >= 0 {
			info := levels[level]
			fmt.Fprintf(&buf, levelFormat, level, humanize(info.size), info.count)
		}
	}

	for i, t := range s {
		maybeFlush(t.Level, i)
		size := humanize(t.Size)
		if size == lastSize {
			lastSizeCount++
		} else {
			flushLastSize()
			lastSize = size
			lastSizeCount = 1
		}
	}

	maybeFlush(-1, 0)
	return buf.String()
}

// ReadAmplification returns RocksDB's worst case read amplification, which is
// the number of level-0 sstables plus the number of levels, other than level 0,
// with at least one sstable.
//
// This definition comes from here:
// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#level-style-compaction
func (s SSTableInfos) ReadAmplification() int {
	var readAmp int
	seenLevel := make(map[int]bool)
	for _, t := range s {
		if t.Level == 0 {
			readAmp++
		} else if !seenLevel[t.Level] {
			readAmp++
			seenLevel[t.Level] = true
		}
	}
	return readAmp
}

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
	cluster.RocksDBSettings
	Attrs roachpb.Attributes
	// Dir is the data directory for this store.
	Dir string
	// MaxSizeBytes is used for calculating free space and making rebalancing
	// decisions. Zero indicates that there is no maximum size.
	MaxSizeBytes int64
	// MaxOpenFiles controls the maximum number of file descriptors RocksDB
	// creates. If MaxOpenFiles is zero, this is set to DefaultMaxOpenFiles.
	MaxOpenFiles int
	// WarnLargeBatchThreshold controls if a log message is printed when a
	// WriteBatch takes longer than WarnLargeBatchThreshold. If it is set to
	// zero, no log messages are ever printed.
	WarnLargeBatchThreshold time.Duration
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
		pending    []*rocksDBBatch
	}

	syncer struct {
		syncutil.Mutex
		cond    sync.Cond
		closed  bool
		pending []*rocksDBBatch
	}
}

var _ Engine = &RocksDB{}

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
	if cfg.MinWALSyncInterval == nil {
		panic("MinWALSyncInterval must be set")
	}

	r := &RocksDB{
		cfg:   cfg,
		cache: cache.ref(),
	}

	if err := r.setAuxiliaryDir(filepath.Join(cfg.Dir, "auxiliary")); err != nil {
		return nil, err
	}

	if err := r.open(); err != nil {
		return nil, err
	}
	return r, nil
}

func newMemRocksDB(
	attrs roachpb.Attributes, cache RocksDBCache, MaxSizeBytes int64,
) (*RocksDB, error) {
	// FIXME(tschottdorf): should be passed in.
	st := cluster.MakeClusterSettings()
	r := &RocksDB{
		cfg: RocksDBConfig{
			RocksDBSettings: st.RocksDBSettings,
			Attrs:           attrs,
			MaxSizeBytes:    MaxSizeBytes,
		},
		// dir: empty dir == "mem" RocksDB instance.
		cache: cache.ref(),
	}

	tempDir, err := ioutil.TempDir(os.TempDir(), ".inmemrocksdb")
	if err != nil {
		return nil, err
	}
	if err := r.setAuxiliaryDir(filepath.Join(tempDir, "./auxiliary")); err != nil {
		return nil, err
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
	var ver storageVersion
	if len(r.cfg.Dir) != 0 {
		log.Infof(context.TODO(), "opening rocksdb instance at %q", r.cfg.Dir)

		// Check the version number.
		var err error
		if ver, err = getVersion(r.cfg.Dir); err != nil {
			return err
		}
		if ver < versionMinimum || ver > versionCurrent {
			// Instead of an error, we should call a migration if possible when
			// one is needed immediately following the DBOpen call.
			return fmt.Errorf("incompatible rocksdb data version, current:%d, on disk:%d, minimum:%d",
				versionCurrent, ver, versionMinimum)
		}
	} else {
		if log.V(2) {
			log.Infof(context.TODO(), "opening in memory rocksdb instance")
		}

		// In memory dbs are always current.
		ver = versionCurrent
	}

	blockSize := envutil.EnvOrDefaultBytes("COCKROACH_ROCKSDB_BLOCK_SIZE", defaultBlockSize)
	walTTL := envutil.EnvOrDefaultDuration("COCKROACH_ROCKSDB_WAL_TTL", 0).Seconds()
	maxOpenFiles := DefaultMaxOpenFiles
	if r.cfg.MaxOpenFiles != 0 {
		maxOpenFiles = r.cfg.MaxOpenFiles
	}

	status := C.DBOpen(&r.rdb, goToCSlice([]byte(r.cfg.Dir)),
		C.DBOptions{
			cache:           r.cache.cache,
			block_size:      C.uint64_t(blockSize),
			wal_ttl_seconds: C.uint64_t(walTTL),
			logging_enabled: C.bool(log.V(3)),
			num_cpu:         C.int(runtime.NumCPU()),
			max_open_files:  C.int(maxOpenFiles),
		})
	if err := statusToError(status); err != nil {
		return errors.Errorf("could not open rocksdb instance: %s", err)
	}

	// Update or add the version file if needed.
	if ver < versionCurrent {
		if err := writeVersionFile(r.cfg.Dir); err != nil {
			return err
		}
	}

	r.commit.cond.L = &r.commit.Mutex
	r.syncer.cond.L = &r.syncer.Mutex

	// NB: The sync goroutine acts as a check that the RocksDB instance was
	// properly closed as the goroutine will leak otherwise.
	go r.syncLoop()
	return nil
}

func (r *RocksDB) syncLoop() {
	// Lock the OS thread to prevent other goroutines from running on it. We
	// don't want other goroutines to run on this thread because of the
	// relatively long (multiple millisecond) Cgo calls that are
	// performed. Scheduling other goroutines on this thread would introduce
	// latency to those goroutines whenever this goroutine needs to sync the WAL.
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	s := &r.syncer
	s.Lock()
	defer s.Unlock()

	var lastSync time.Time

	for {
		for len(s.pending) == 0 && !s.closed {
			s.cond.Wait()
		}
		if s.closed {
			return
		}

		min := r.cfg.MinWALSyncInterval.Get()
		if delta := timeutil.Since(lastSync); delta < min {
			s.Unlock()
			time.Sleep(min - delta)
			s.Lock()
		}

		pending := s.pending
		s.pending = nil

		s.Unlock()

		var err error
		if r.cfg.Dir != "" {
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
		// Remove the temporary directory when the engine is in-memory.
		if err := os.RemoveAll(r.auxDir); err != nil {
			log.Warning(context.TODO(), err)
		}
	} else {
		log.Infof(context.TODO(), "closing rocksdb instance at %q", r.cfg.Dir)
	}
	if r.rdb != nil {
		C.DBClose(r.rdb)
		r.rdb = nil
	}
	r.cache.Release()
	r.syncer.Lock()
	r.syncer.closed = true
	r.syncer.cond.Signal()
	r.syncer.Unlock()
}

// Closed returns true if the engine is closed.
func (r *RocksDB) Closed() bool {
	return r.rdb == nil
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
// The key and value byte slices may be reused safely. put takes a copy of
// them before returning.
func (r *RocksDB) Put(key MVCCKey, value []byte) error {
	return dbPut(r.rdb, key, value)
}

// Merge implements the RocksDB merge operator using the function goMergeInit
// to initialize missing values and goMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goMerge and goMergeInit for details.
//
// The key and value byte slices may be reused safely. merge takes a copy
// of them before returning.
func (r *RocksDB) Merge(key MVCCKey, value []byte) error {
	return dbMerge(r.rdb, key, value)
}

// ApplyBatchRepr atomically applies a set of batched updates. Created by
// calling Repr() on a batch. Using this method is equivalent to constructing
// and committing a batch whose Repr() equals repr.
func (r *RocksDB) ApplyBatchRepr(repr []byte, sync bool) error {
	return dbApplyBatchRepr(r.rdb, repr, sync)
}

// Get returns the value for the given key.
func (r *RocksDB) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.rdb, key)
}

// GetProto fetches the value at the specified key and unmarshals it.
func (r *RocksDB) GetProto(
	key MVCCKey, msg proto.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.rdb, key, msg)
}

// Clear removes the item from the db with the given key.
func (r *RocksDB) Clear(key MVCCKey) error {
	return dbClear(r.rdb, key)
}

// ClearRange removes a set of entries, from start (inclusive) to end
// (exclusive).
func (r *RocksDB) ClearRange(start, end MVCCKey) error {
	return dbClearRange(r.rdb, start, end)
}

// ClearIterRange removes a set of entries, from start (inclusive) to end
// (exclusive).
func (r *RocksDB) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	return dbClearIterRange(r.rdb, iter, start, end)
}

// Iterate iterates from start to end keys, invoking f on each
// key/value pair. See engine.Iterate for details.
func (r *RocksDB) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return dbIterate(r.rdb, r, start, end, f)
}

// Capacity queries the underlying file system for disk capacity information.
func (r *RocksDB) Capacity() (roachpb.StoreCapacity, error) {
	fileSystemUsage := gosigar.FileSystemUsage{}
	dir := r.cfg.Dir
	if dir == "" {
		// This is an in-memory instance. Pretend we're empty since we
		// don't know better and only use this for testing. Using any
		// part of the actual file system here can throw off allocator
		// rebalancing in a hard-to-trace manner. See #7050.
		return roachpb.StoreCapacity{
			Capacity:  r.cfg.MaxSizeBytes,
			Available: r.cfg.MaxSizeBytes,
		}, nil
	}
	if err := fileSystemUsage.Get(dir); err != nil {
		return roachpb.StoreCapacity{}, err
	}

	if fileSystemUsage.Total > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Total), humanizeutil.IBytes(math.MaxInt64))
	}
	if fileSystemUsage.Avail > math.MaxInt64 {
		return roachpb.StoreCapacity{}, fmt.Errorf("unsupported disk size %s, max supported size is %s",
			humanize.IBytes(fileSystemUsage.Avail), humanizeutil.IBytes(math.MaxInt64))
	}
	fsuTotal := int64(fileSystemUsage.Total)
	fsuAvail := int64(fileSystemUsage.Avail)

	// If no size limitation have been placed on the store size or if the
	// limitation is greater than what's available, just return the actual
	// totals.
	if r.cfg.MaxSizeBytes == 0 || r.cfg.MaxSizeBytes >= fsuTotal || r.cfg.Dir == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
		}, nil
	}

	// Find the total size of all the files in the r.dir and all its
	// subdirectories.
	var totalUsedBytes int64
	if errOuter := filepath.Walk(r.cfg.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if info.Mode().IsRegular() {
			totalUsedBytes += info.Size()
		}
		return nil
	}); errOuter != nil {
		return roachpb.StoreCapacity{}, errOuter
	}

	available := r.cfg.MaxSizeBytes - totalUsedBytes
	if available > fsuAvail {
		available = fsuAvail
	}
	if available < 0 {
		available = 0
	}

	return roachpb.StoreCapacity{
		Capacity:  r.cfg.MaxSizeBytes,
		Available: available,
	}, nil
}

// Compact forces compaction on the database.
func (r *RocksDB) Compact() error {
	return statusToError(C.DBCompact(r.rdb))
}

// Destroy destroys the underlying filesystem data associated with the database.
func (r *RocksDB) Destroy() error {
	return statusToError(C.DBDestroy(goToCSlice([]byte(r.cfg.Dir))))
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (r *RocksDB) Flush() error {
	return statusToError(C.DBFlush(r.rdb))
}

// NewIterator returns an iterator over this rocksdb engine.
func (r *RocksDB) NewIterator(prefix bool) Iterator {
	return newRocksDBIterator(r.rdb, prefix, r)
}

// NewTimeBoundIterator is like NewIterator, but returns a time-bound iterator.
func (r *RocksDB) NewTimeBoundIterator(start, end hlc.Timestamp) Iterator {
	it := &rocksDBIterator{}
	it.initTimeBound(r.rdb, start, end, r)
	return it
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

func (r *rocksDBReadOnly) Get(key MVCCKey) ([]byte, error) {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	return dbGet(r.parent.rdb, key)
}

func (r *rocksDBReadOnly) GetProto(
	key MVCCKey, msg proto.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	return dbGetProto(r.parent.rdb, key, msg)
}

func (r *rocksDBReadOnly) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	return dbIterate(r.parent.rdb, r, start, end, f)
}

// NewIterator returns an iterator over the underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// rocksDBReadOnly. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same rocksDBReadOnly.
func (r *rocksDBReadOnly) NewIterator(prefix bool) Iterator {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	iter := &r.normalIter
	if prefix {
		iter = &r.prefixIter
	}
	if iter.rocksDBIterator.iter == nil {
		iter.rocksDBIterator.init(r.parent.rdb, prefix, r)
	}
	if iter.inuse {
		panic("iterator already in use")
	}
	iter.inuse = true
	return iter
}

func (r *rocksDBReadOnly) NewTimeBoundIterator(start, end hlc.Timestamp) Iterator {
	if r.isClosed {
		panic("using a closed rocksDBReadOnly")
	}
	it := &rocksDBIterator{}
	it.initTimeBound(r.parent.rdb, start, end, r)
	return it
}

// Writer methods are not implemented for rocksDBReadOnly. Ideally, the code could be refactored so that
// a Reader could be supplied to evaluateBatch

// Writer is the write interface to an engine's data.
func (r *rocksDBReadOnly) ApplyBatchRepr(repr []byte, sync bool) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) Clear(key MVCCKey) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) ClearRange(start, end MVCCKey) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) Merge(key MVCCKey, value []byte) error {
	panic("not implemented")
}

func (r *rocksDBReadOnly) Put(key MVCCKey, value []byte) error {
	panic("not implemented")
}

// NewBatch returns a new batch wrapping this rocksdb engine.
func (r *RocksDB) NewBatch() Batch {
	return newRocksDBBatch(r, false /* writeOnly */)
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

	tablesPtr := uintptr(unsafe.Pointer(tables))
	tableSize := unsafe.Sizeof(C.DBSSTable{})
	tableVal := func(i int) C.DBSSTable {
		return *(*C.DBSSTable)(unsafe.Pointer(tablesPtr + uintptr(i)*tableSize))
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

// getUserProperties fetches the user properties stored in each sstable's
// metadata.
func (r *RocksDB) getUserProperties() (enginepb.SSTUserPropertiesCollection, error) {
	buf := cStringToGoBytes(C.DBGetUserProperties(r.rdb))
	var ssts enginepb.SSTUserPropertiesCollection
	if err := ssts.Unmarshal(buf); err != nil {
		return enginepb.SSTUserPropertiesCollection{}, err
	}
	if ssts.Error != "" {
		return enginepb.SSTUserPropertiesCollection{}, errors.New(ssts.Error)
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
		BlockCacheHits:           int64(s.block_cache_hits),
		BlockCacheMisses:         int64(s.block_cache_misses),
		BlockCacheUsage:          int64(s.block_cache_usage),
		BlockCachePinnedUsage:    int64(s.block_cache_pinned_usage),
		BloomFilterPrefixChecked: int64(s.bloom_filter_prefix_checked),
		BloomFilterPrefixUseful:  int64(s.bloom_filter_prefix_useful),
		MemtableHits:             int64(s.memtable_hits),
		MemtableMisses:           int64(s.memtable_misses),
		MemtableTotalSize:        int64(s.memtable_total_size),
		Flushes:                  int64(s.flushes),
		Compactions:              int64(s.compactions),
		TableReadersMemEstimate:  int64(s.table_readers_mem_estimate),
	}, nil
}

// GetCompactionStats returns the internal RocksDB compaction stats. See
// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#rocksdb-statistics.
func (r *RocksDB) GetCompactionStats() string {
	return cStringToGoString(C.DBGetCompactionStats(r.rdb))
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

// Get returns the value for the given key, nil otherwise using
// the snapshot handle.
func (r *rocksDBSnapshot) Get(key MVCCKey) ([]byte, error) {
	return dbGet(r.handle, key)
}

func (r *rocksDBSnapshot) GetProto(
	key MVCCKey, msg proto.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	return dbGetProto(r.handle, key, msg)
}

// Iterate iterates over the keys between start inclusive and end
// exclusive, invoking f() on each key/value pair using the snapshot
// handle.
func (r *rocksDBSnapshot) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return dbIterate(r.handle, r, start, end, f)
}

// NewIterator returns a new instance of an Iterator over the
// engine using the snapshot handle.
func (r *rocksDBSnapshot) NewIterator(prefix bool) Iterator {
	return newRocksDBIterator(r.handle, prefix, r)
}

// NewTimeBoundIterator is like NewIterator, but returns a time-bound iterator.
func (r *rocksDBSnapshot) NewTimeBoundIterator(start, end hlc.Timestamp) Iterator {
	panic("not implemented")
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
func (r *distinctBatch) NewIterator(prefix bool) Iterator {
	// Used the cached iterator, creating it on first access.
	iter := &r.normalIter
	if prefix {
		iter = &r.prefixIter
	}
	if iter.rocksDBIterator.iter == nil {
		if r.writeOnly {
			iter.rocksDBIterator.init(r.parent.rdb, prefix, r)
		} else {
			r.ensureBatch()
			iter.rocksDBIterator.init(r.batch, prefix, r)
		}
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
	key MVCCKey, msg proto.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if r.writeOnly {
		return dbGetProto(r.parent.rdb, key, msg)
	}
	r.ensureBatch()
	return dbGetProto(r.batch, key, msg)
}

func (r *distinctBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	r.ensureBatch()
	return dbIterate(r.batch, r, start, end, f)
}

func (r *distinctBatch) Put(key MVCCKey, value []byte) error {
	r.builder.Put(key, value)
	return nil
}

func (r *distinctBatch) Merge(key MVCCKey, value []byte) error {
	r.builder.Merge(key, value)
	return nil
}

func (r *distinctBatch) Clear(key MVCCKey) error {
	r.builder.Clear(key)
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

func (r *distinctBatch) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearIterRange(r.batch, iter, start, end)
}

func (r *distinctBatch) close() {
	if i := &r.prefixIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
	if i := &r.normalIter.rocksDBIterator; i.iter != nil {
		i.destroy()
	}
}

// rocksDBBatchIterator wraps rocksDBIterator and allows reuse of an iterator
// for the lifetime of a batch.
type rocksDBBatchIterator struct {
	iter  rocksDBIterator
	batch *rocksDBBatch
}

func (r *rocksDBBatchIterator) Close() {
	// rocksDBBatchIterator.Close() leaves the underlying rocksdb iterator open
	// until the associated batch is closed.
	if r.batch == nil {
		panic("closing idle iterator")
	}
	r.batch = nil
}

func (r *rocksDBBatchIterator) Seek(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.Seek(key)
}

func (r *rocksDBBatchIterator) SeekReverse(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.SeekReverse(key)
}

func (r *rocksDBBatchIterator) Valid() (bool, error) {
	return r.iter.Valid()
}

func (r *rocksDBBatchIterator) Next() {
	r.batch.flushMutations()
	r.iter.Next()
}

func (r *rocksDBBatchIterator) Prev() {
	r.batch.flushMutations()
	r.iter.Prev()
}

func (r *rocksDBBatchIterator) NextKey() {
	r.batch.flushMutations()
	r.iter.NextKey()
}

func (r *rocksDBBatchIterator) PrevKey() {
	r.batch.flushMutations()
	r.iter.PrevKey()
}

func (r *rocksDBBatchIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	r.batch.flushMutations()
	return r.iter.ComputeStats(start, end, nowNanos)
}

func (r *rocksDBBatchIterator) Key() MVCCKey {
	return r.iter.Key()
}

func (r *rocksDBBatchIterator) Value() []byte {
	return r.iter.Value()
}

func (r *rocksDBBatchIterator) ValueProto(msg proto.Message) error {
	return r.iter.ValueProto(msg)
}

func (r *rocksDBBatchIterator) UnsafeKey() MVCCKey {
	return r.iter.UnsafeKey()
}

func (r *rocksDBBatchIterator) UnsafeValue() []byte {
	return r.iter.UnsafeValue()
}

func (r *rocksDBBatchIterator) Less(key MVCCKey) bool {
	return r.iter.Less(key)
}

func (r *rocksDBBatchIterator) getIter() *C.DBIterator {
	return r.iter.iter
}

type rocksDBBatch struct {
	parent             *RocksDB
	batch              *C.DBEngine
	flushes            int
	flushedCount       int
	flushedSize        int
	prefixIter         rocksDBBatchIterator
	normalIter         rocksDBBatchIterator
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

func newRocksDBBatch(parent *RocksDB, writeOnly bool) *rocksDBBatch {
	r := &rocksDBBatch{
		parent:    parent,
		writeOnly: writeOnly,
	}
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
	r.closed = true
}

// Closed returns true if the engine is closed.
func (r *rocksDBBatch) Closed() bool {
	return r.closed || r.committed
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

// ApplyBatchRepr atomically applies a set of batched updates to the current
// batch (the receiver).
func (r *rocksDBBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
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
	key MVCCKey, msg proto.Message,
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

func (r *rocksDBBatch) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	if r.writeOnly {
		panic("write-only batch")
	}
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.ensureBatch()
	return dbIterate(r.batch, r, start, end, f)
}

func (r *rocksDBBatch) Clear(key MVCCKey) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.distinctNeedsFlush = true
	r.builder.Clear(key)
	return nil
}

func (r *rocksDBBatch) ClearRange(start, end MVCCKey) error {
	if !r.writeOnly {
		panic("readable batch")
	}
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearRange(r.batch, start, end)
}

func (r *rocksDBBatch) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	if r.distinctOpen {
		panic("distinct batch open")
	}
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearIterRange(r.batch, iter, start, end)
}

// NewIterator returns an iterator over the batch and underlying engine. Note
// that the returned iterator is cached and re-used for the lifetime of the
// batch. A panic will be thrown if multiple prefix or normal (non-prefix)
// iterators are used simultaneously on the same batch.
func (r *rocksDBBatch) NewIterator(prefix bool) Iterator {
	if r.writeOnly {
		panic("write-only batch")
	}
	if r.distinctOpen {
		panic("distinct batch open")
	}
	// Used the cached iterator, creating it on first access.
	iter := &r.normalIter
	if prefix {
		iter = &r.prefixIter
	}
	if iter.iter.iter == nil {
		r.ensureBatch()
		iter.iter.init(r.batch, prefix, r)
	}
	if iter.batch != nil {
		panic("iterator already in use")
	}
	iter.batch = r
	return iter
}

// NewTimeBoundIterator is like NewIterator, but returns a time-bound iterator.
func (r *rocksDBBatch) NewTimeBoundIterator(start, end hlc.Timestamp) Iterator {
	panic("not implemented")
}

func (r *rocksDBBatch) Commit(syncCommit bool) error {
	if r.Closed() {
		panic("this batch was already committed")
	}
	r.distinctOpen = false

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
	// slice. Every batch has an associated wait group which is signalled when
	// the commit is complete.
	c.Lock()
	leader := len(c.pending) == 0
	c.pending = append(c.pending, r)

	if leader {
		// We're the leader. Wait for any running commit to finish.
		for c.committing {
			c.cond.Wait()
		}
		pending := c.pending
		c.pending = nil
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
		c.cond.Signal()
		c.Unlock()

		// Propagate the error to all of the batches involved in the commit. If a
		// batch requires syncing and the commit was successful, add it to the
		// syncing list. Note that we're reusing the pending list here for the
		// syncing list.
		syncing := pending[:0]
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
	} else if r.builder.count > 0 {
		count, size = r.builder.count, len(r.builder.repr)

		// Fast-path which avoids flushing mutations to the C++ batch. Instead, we
		// directly apply the mutations to the database.
		if err := r.parent.ApplyBatchRepr(r.builder.Finish(), sync); err != nil {
			return err
		}
		if r.batch != nil {
			C.DBClose(r.batch)
			r.batch = nil
		}
	}
	r.committed = true

	warnLargeBatches := r.parent.cfg.WarnLargeBatchThreshold > 0
	if elapsed := timeutil.Since(start); warnLargeBatches && (elapsed >= r.parent.cfg.WarnLargeBatchThreshold) {
		log.Warningf(context.TODO(), "batch [%d/%d/%d] commit took %s (>%s):\n%s",
			count, size, r.flushes, elapsed, r.parent.cfg.WarnLargeBatchThreshold, debug.Stack())
	}

	return nil
}

func (r *rocksDBBatch) Repr() []byte {
	if r.flushes == 0 {
		// We've never flushed to C++. Return the mutations only.
		return r.builder.getRepr()
	}
	r.flushMutations()
	return cSliceToGoBytes(C.DBBatchRepr(r.batch))
}

func (r *rocksDBBatch) unsafeRepr() []byte {
	if r.flushes == 0 {
		// We've never flushed to C++. Return the mutations only.
		return r.builder.getRepr()
	}
	r.flushMutations()
	return cSliceToUnsafeGoBytes(C.DBBatchRepr(r.batch))
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
	if r.builder.count == 0 {
		return
	}
	r.ensureBatch()
	r.distinctNeedsFlush = false
	r.flushes++
	r.flushedCount += r.builder.count
	r.flushedSize += len(r.builder.repr)
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
	engine Reader
	iter   *C.DBIterator
	valid  bool
	reseek bool
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
func newRocksDBIterator(rdb *C.DBEngine, prefix bool, engine Reader) Iterator {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	r := iterPool.Get().(*rocksDBIterator)
	r.init(rdb, prefix, engine)
	return r
}

func (r *rocksDBIterator) getIter() *C.DBIterator {
	return r.iter
}

func (r *rocksDBIterator) init(rdb *C.DBEngine, prefix bool, engine Reader) {
	r.iter = C.DBNewIter(rdb, C.bool(prefix))
	if r.iter == nil {
		panic("unable to create iterator")
	}
	r.engine = engine
}

func (r *rocksDBIterator) initTimeBound(rdb *C.DBEngine, start, end hlc.Timestamp, engine Reader) {
	r.iter = C.DBNewTimeBoundIter(rdb, goToCTimestamp(start), goToCTimestamp(end))
	if r.iter == nil {
		panic("unable to create iterator")
	}
	r.engine = engine
}

func (r *rocksDBIterator) checkEngineOpen() {
	if r.engine.Closed() {
		panic("iterator used after backing engine closed")
	}
}

func (r *rocksDBIterator) destroy() {
	C.DBIterDestroy(r.iter)
	*r = rocksDBIterator{}
}

// The following methods implement the Iterator interface.
func (r *rocksDBIterator) Close() {
	r.destroy()
	iterPool.Put(r)
}

func (r *rocksDBIterator) Seek(key MVCCKey) {
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

func (r *rocksDBIterator) SeekReverse(key MVCCKey) {
	r.checkEngineOpen()
	if len(key.Key) == 0 {
		r.setState(C.DBIterSeekToLast(r.iter))
	} else {
		// We can avoid seeking if we're already at the key we seek.
		if r.valid && !r.reseek && key.Equal(r.UnsafeKey()) {
			return
		}
		r.setState(C.DBIterSeek(r.iter, goToCKey(key)))
		// Maybe the key sorts after the last key in RocksDB.
		if ok, _ := r.Valid(); !ok {
			r.setState(C.DBIterSeekToLast(r.iter))
		}
		if ok, _ := r.Valid(); !ok {
			return
		}
		// Make sure the current key is <= the provided key.
		if key.Less(r.UnsafeKey()) {
			r.Prev()
		}
	}
}

func (r *rocksDBIterator) Valid() (bool, error) {
	return r.valid, r.err
}

func (r *rocksDBIterator) Next() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, false /* !skip_current_key_versions */))
}

func (r *rocksDBIterator) Prev() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, false /* !skip_current_key_versions */))
}

func (r *rocksDBIterator) NextKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, true /* skip_current_key_versions */))
}

func (r *rocksDBIterator) PrevKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, true /* skip_current_key_versions */))
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

func (r *rocksDBIterator) ValueProto(msg proto.Message) error {
	if r.value.len <= 0 {
		return nil
	}
	return proto.Unmarshal(r.UnsafeValue(), msg)
}

func (r *rocksDBIterator) UnsafeKey() MVCCKey {
	return cToUnsafeGoKey(r.key)
}

func (r *rocksDBIterator) UnsafeValue() []byte {
	return cSliceToUnsafeGoBytes(r.value)
}

func (r *rocksDBIterator) Less(key MVCCKey) bool {
	return r.UnsafeKey().Less(key)
}

func (r *rocksDBIterator) setState(state C.DBIterState) {
	r.valid = bool(state.valid)
	r.reseek = false
	r.key = state.key
	r.value = state.value
	r.err = statusToError(state.status)
}

func (r *rocksDBIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	result := C.MVCCComputeStats(r.iter, goToCKey(start), goToCKey(end), C.int64_t(nowNanos))
	stats, err := cStatsToGoStats(result, nowNanos)
	if util.RaceEnabled {
		// If we've come here via rocksDBBatchIterator, then flushMutations
		// (which forces reseek) was called just before C.MVCCComputeStats. Set
		// it here as well to match.
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

func cStatsToGoStats(stats C.MVCCStatsResult, nowNanos int64) (enginepb.MVCCStats, error) {
	ms := enginepb.MVCCStats{}
	if err := statusToError(stats.status); err != nil {
		return ms, err
	}
	ms.ContainsEstimates = false
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
		len:  C.int(len(b)),
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
	result := C.GoStringN(s.data, s.len)
	C.free(unsafe.Pointer(s.data))
	return result
}

func cStringToGoBytes(s C.DBString) []byte {
	if s.data == nil {
		return nil
	}
	result := C.GoBytes(unsafe.Pointer(s.data), s.len)
	C.free(unsafe.Pointer(s.data))
	return result
}

func cSliceToGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	return C.GoBytes(unsafe.Pointer(s.data), s.len)
}

func cSliceToUnsafeGoBytes(s C.DBSlice) []byte {
	if s.data == nil {
		return nil
	}
	// Interpret the C pointer as a pointer to a Go array, then slice.
	return (*[maxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func goToCTimestamp(ts hlc.Timestamp) C.DBTimestamp {
	return C.DBTimestamp{
		wall_time: C.int64_t(ts.WallTime),
		logical:   C.int32_t(ts.Logical),
	}
}

func statusToError(s C.DBStatus) error {
	if s.data == nil {
		return nil
	}
	return errors.New(cStringToGoString(s))
}

// goMerge takes existing and update byte slices that are expected to
// be marshalled roachpb.Values and merges the two values returning a
// marshalled roachpb.Value or an error.
func goMerge(existing, update []byte) ([]byte, error) {
	var result C.DBString
	status := C.DBMergeOne(goToCSlice(existing), goToCSlice(update), &result)
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
	rdb *C.DBEngine, key MVCCKey, msg proto.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		err = emptyKeyError()
		return
	}
	var result C.DBString
	if err = statusToError(C.DBGet(rdb, goToCKey(key), &result)); err != nil {
		return
	}
	if result.len <= 0 {
		msg.Reset()
		return
	}
	ok = true
	if msg != nil {
		// Make a byte slice that is backed by result.data. This slice
		// cannot live past the lifetime of this method, but we're only
		// using it to unmarshal the roachpb.
		data := cSliceToUnsafeGoBytes(C.DBSlice(result))
		err = proto.Unmarshal(data, msg)
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

func dbClearRange(rdb *C.DBEngine, start, end MVCCKey) error {
	return statusToError(C.DBDeleteRange(rdb, goToCKey(start), goToCKey(end)))
}

func dbClearIterRange(rdb *C.DBEngine, iter Iterator, start, end MVCCKey) error {
	getter, ok := iter.(dbIteratorGetter)
	if !ok {
		return errors.Errorf("%T is not a RocksDB iterator", iter)
	}
	return statusToError(C.DBDeleteIterRange(rdb, getter.getIter(), goToCKey(start), goToCKey(end)))
}

func dbIterate(
	rdb *C.DBEngine, engine Reader, start, end MVCCKey, f func(MVCCKeyValue) (bool, error),
) error {
	if !start.Less(end) {
		return nil
	}
	it := newRocksDBIterator(rdb, false, engine)
	defer it.Close()

	it.Seek(start)
	for ; ; it.Next() {
		if ok, err := it.Valid(); err != nil {
			return err
		} else if !ok {
			break
		}
		k := it.Key()
		if !k.Less(end) {
			break
		}
		if done, err := f(MVCCKeyValue{Key: k, Value: it.Value()}); done || err != nil {
			return err
		}
	}
	return nil
}

// TODO(dan): Rename this to RocksDBSSTFileReader and RocksDBSSTFileWriter.

// RocksDBSstFileReader allows iteration over a number of non-overlapping
// sstables exported by `RocksDBSstFileWriter`.
type RocksDBSstFileReader struct {
	rocksDB         InMem
	filenameCounter int
}

// MakeRocksDBSstFileReader creates a RocksDBSstFileReader backed by an
// in-memory RocksDB instance.
func MakeRocksDBSstFileReader() RocksDBSstFileReader {
	// cacheSize was selected because it's used for almost all other NewInMem
	// calls. It's seemed to work well so far, but there's probably more tuning
	// to be done here.
	const cacheSize = 1 << 20
	return RocksDBSstFileReader{rocksDB: NewInMem(roachpb.Attributes{}, cacheSize)}
}

// IngestExternalFile links a file with the given contents into a database. See
// the RocksDB documentation on `IngestExternalFile` for the various
// restrictions on what can be added.
func (fr *RocksDBSstFileReader) IngestExternalFile(data []byte) error {
	if fr.rocksDB.RocksDB == nil {
		return errors.New("cannot call IngestExternalFile on a closed reader")
	}

	filename := fmt.Sprintf("ingest-%d", fr.filenameCounter)
	fr.filenameCounter++
	if err := fr.rocksDB.WriteFile(filename, data); err != nil {
		return err
	}
	return statusToError(C.DBIngestExternalFile(fr.rocksDB.rdb, goToCSlice([]byte(filename)), false))
}

// Iterate iterates over the keys between start inclusive and end
// exclusive, invoking f() on each key/value pair.
func (fr *RocksDBSstFileReader) Iterate(
	start, end MVCCKey, f func(MVCCKeyValue) (bool, error),
) error {
	if fr.rocksDB.RocksDB == nil {
		return errors.New("cannot call Iterate on a closed reader")
	}
	return fr.rocksDB.Iterate(start, end, f)
}

// NewIterator returns an iterator over this sst reader.
func (fr *RocksDBSstFileReader) NewIterator(prefix bool) Iterator {
	return newRocksDBIterator(fr.rocksDB.rdb, prefix, fr.rocksDB)
}

// Close finishes the reader.
func (fr *RocksDBSstFileReader) Close() {
	if fr.rocksDB.RocksDB == nil {
		return
	}
	fr.rocksDB.RocksDB.Close()
	fr.rocksDB.RocksDB = nil
}

// RocksDBSstFileWriter creates a file suitable for importing with
// RocksDBSstFileReader.
type RocksDBSstFileWriter struct {
	fw *C.DBSstFileWriter
	// DataSize tracks the total key and value bytes added so far.
	DataSize int64
}

// MakeRocksDBSstFileWriter creates a new RocksDBSstFileWriter with the default
// configuration.
func MakeRocksDBSstFileWriter() (RocksDBSstFileWriter, error) {
	fw := C.DBSstFileWriterNew()
	err := statusToError(C.DBSstFileWriterOpen(fw))
	return RocksDBSstFileWriter{fw: fw}, err
}

// Add puts a kv entry into the sstable being built. An error is returned if it
// is not greater than any previously added entry (according to the comparator
// configured during writer creation). `Close` cannot have been called.
func (fw *RocksDBSstFileWriter) Add(kv MVCCKeyValue) error {
	if fw.fw == nil {
		return errors.New("cannot call Open on a closed writer")
	}
	fw.DataSize += int64(len(kv.Key.Key)) + int64(len(kv.Value))
	return statusToError(C.DBSstFileWriterAdd(fw.fw, goToCKey(kv.Key), goToCSlice(kv.Value)))
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

// GetAuxiliaryDir returns the auxiliary storage path for this engine.
func (r *RocksDB) GetAuxiliaryDir() string {
	return r.auxDir
}

func (r *RocksDB) setAuxiliaryDir(d string) error {
	if err := os.MkdirAll(d, 0755); err != nil {
		return err
	}
	r.auxDir = d
	return nil
}

// IngestExternalFile links a file into the RocksDB log-structured merge-tree.
func (r *RocksDB) IngestExternalFile(ctx context.Context, path string, move bool) error {
	return statusToError(C.DBIngestExternalFile(r.rdb, goToCSlice([]byte(path)), C._Bool(move)))
}

// WriteFile writes data to a file in this RocksDB's env.
func (r *RocksDB) WriteFile(filename string, data []byte) error {
	return statusToError(C.DBEnvWriteFile(r.rdb, goToCSlice([]byte(filename)), goToCSlice(data)))
}
