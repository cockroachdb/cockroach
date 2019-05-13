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
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/diskmap"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logtags"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	humanize "github.com/dustin/go-humanize"
	"github.com/elastic/gosigar"
	"github.com/pkg/errors"
)

// TODO(tamird): why does rocksdb not link jemalloc,snappy statically?

// #cgo CPPFLAGS: -I../../../c-deps/libroach/include
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

var ingestDelayL0Threshold = settings.RegisterIntSetting(
	"rocksdb.ingest_backpressure.l0_file_count_threshold",
	"number of L0 files after which to backpressure SST ingestions",
	20,
)

var ingestDelayPendingLimit = settings.RegisterByteSizeSetting(
	"rocksdb.ingest_backpressure.pending_compaction_threshold",
	"pending compaction estimate above which to backpressure SST ingestions",
	64<<30,
)

var ingestDelayTime = settings.RegisterDurationSetting(
	"rocksdb.ingest_backpressure.max_delay",
	"maximum amount of time to backpressure a single SST ingestion",
	time.Second*5,
)

// Set to true to perform expensive iterator debug leak checking. In normal
// operation, we perform inexpensive iterator leak checking but those checks do
// not indicate where the leak arose. The expensive checking tracks the stack
// traces of every iterator allocated. DO NOT ENABLE in production code.
const debugIteratorLeak = false

//export rocksDBV
func rocksDBV(sevLvl C.int, infoVerbosity C.int) bool {
	sev := log.Severity(sevLvl)
	return sev == log.Severity_INFO && log.V(int32(infoVerbosity)) ||
		sev == log.Severity_WARNING ||
		sev == log.Severity_ERROR ||
		sev == log.Severity_FATAL
}

//export rocksDBLog
func rocksDBLog(sevLvl C.int, s *C.char, n C.int) {
	ctx := logtags.AddTag(context.Background(), "rocksdb", nil)
	switch log.Severity(sevLvl) {
	case log.Severity_WARNING:
		log.Warning(ctx, C.GoStringN(s, n))
	case log.Severity_ERROR:
		log.Error(ctx, C.GoStringN(s, n))
	case log.Severity_FATAL:
		log.Fatal(ctx, C.GoStringN(s, n))
	default:
		log.Info(ctx, C.GoStringN(s, n))
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

// SSTableInfo contains metadata about a single sstable. Note this mirrors
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

// SSTableInfosByLevel maintains slices of SSTableInfo objects, one
// per level. The slice for each level contains the SSTableInfo
// objects for SSTables at that level, sorted by start key.
type SSTableInfosByLevel struct {
	// Each level is a slice of SSTableInfos.
	levels [][]SSTableInfo
}

// NewSSTableInfosByLevel returns a new SSTableInfosByLevel object
// based on the supplied SSTableInfos slice.
func NewSSTableInfosByLevel(s SSTableInfos) SSTableInfosByLevel {
	var result SSTableInfosByLevel
	for _, t := range s {
		for i := len(result.levels); i <= t.Level; i++ {
			result.levels = append(result.levels, []SSTableInfo{})
		}
		result.levels[t.Level] = append(result.levels[t.Level], t)
	}
	// Sort each level by start key.
	for _, l := range result.levels {
		sort.Slice(l, func(i, j int) bool { return l[i].Start.Less(l[j].Start) })
	}
	return result
}

// MaxLevel returns the maximum level for which there are SSTables.
func (s *SSTableInfosByLevel) MaxLevel() int {
	return len(s.levels) - 1
}

// MaxLevelSpanOverlapsContiguousSSTables returns the maximum level at
// which the specified key span overlaps either none, one, or at most
// two contiguous SSTables. Level 0 is returned if no level qualifies.
//
// This is useful when considering when to merge two compactions. In
// this case, the method is called with the "gap" between the two
// spans to be compacted. When the result is that the gap span touches
// at most two SSTables at a high level, it suggests that merging the
// two compactions is a good idea (as the up to two SSTables touched
// by the gap span, due to containing endpoints of the existing
// compactions, would be rewritten anyway).
//
// As an example, consider the following sstables in a small database:
//
// Level 0.
//  {Level: 0, Size: 20, Start: key("a"), End: key("z")},
//  {Level: 0, Size: 15, Start: key("a"), End: key("k")},
// Level 2.
//  {Level: 2, Size: 200, Start: key("a"), End: key("j")},
//  {Level: 2, Size: 100, Start: key("k"), End: key("o")},
//  {Level: 2, Size: 100, Start: key("r"), End: key("t")},
// Level 6.
//  {Level: 6, Size: 201, Start: key("a"), End: key("c")},
//  {Level: 6, Size: 200, Start: key("d"), End: key("f")},
//  {Level: 6, Size: 300, Start: key("h"), End: key("r")},
//  {Level: 6, Size: 405, Start: key("s"), End: key("z")},
//
// - The span "a"-"c" overlaps only a single SSTable at the max level
//   (L6). That's great, so we definitely want to compact that.
// - The span "s"-"t" overlaps zero SSTables at the max level (L6).
//   Again, great! That means we're going to compact the 3rd L2
//   SSTable and maybe push that directly to L6.
func (s *SSTableInfosByLevel) MaxLevelSpanOverlapsContiguousSSTables(span roachpb.Span) int {
	// Note overlapsMoreTHanTwo should not be called on level 0, where
	// the SSTables are not guaranteed disjoint.
	overlapsMoreThanTwo := func(tables []SSTableInfo) bool {
		// Search to find the first sstable which might overlap the span.
		i := sort.Search(len(tables), func(i int) bool { return span.Key.Compare(tables[i].End.Key) < 0 })
		// If no SSTable is overlapped, return false.
		if i == -1 || i == len(tables) || span.EndKey.Compare(tables[i].Start.Key) < 0 {
			return false
		}
		// Return true if the span is not subsumed by the combination of
		// this sstable and the next. This logic is complicated and is
		// covered in the unittest. There are three successive conditions
		// which together ensure the span doesn't overlap > 2 SSTables.
		//
		// - If the first overlapped SSTable is the last.
		// - If the span does not exceed the end of the next SSTable.
		// - If the span does not overlap the start of the next next SSTable.
		if i >= len(tables)-1 {
			// First overlapped SSTable is the last (right-most) SSTable.
			//    Span:   [c-----f)
			//    SSTs: [a---d)
			// or
			//    SSTs: [a-----------q)
			return false
		}
		if span.EndKey.Compare(tables[i+1].End.Key) <= 0 {
			// Span does not reach outside of this SSTable's right neighbor.
			//    Span:    [c------f)
			//    SSTs: [a---d) [e-f) ...
			return false
		}
		if i >= len(tables)-2 {
			// Span reaches outside of this SSTable's right neighbor, but
			// there are no more SSTables to the right.
			//    Span:    [c-------------x)
			//    SSTs: [a---d) [e---q)
			return false
		}
		if span.EndKey.Compare(tables[i+2].Start.Key) <= 0 {
			// There's another SSTable two to the right, but the span doesn't
			// reach into it.
			//    Span:    [c------------x)
			//    SSTs: [a---d) [e---q) [x--z) ...
			return false
		}

		// Touching at least three SSTables.
		//    Span:    [c-------------y)
		//    SSTs: [a---d) [e---q) [x--z) ...
		return true
	}
	// Note that we never consider level 0, where SSTables can overlap.
	// Level 0 is instead returned as a catch-all which means that there
	// is no level where the span overlaps only two or fewer SSTables.
	for i := len(s.levels) - 1; i > 0; i-- {
		if !overlapsMoreThanTwo(s.levels[i]) {
			return i
		}
	}
	return 0
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
	Attrs roachpb.Attributes
	// Dir is the data directory for this store.
	Dir string
	// If true, creating the instance fails if the target directory does not hold
	// an initialized RocksDB instance.
	//
	// Makes no sense for in-memory instances.
	MustExist bool
	// ReadOnly will open the database in read only mode if set to true.
	ReadOnly bool
	// MaxSizeBytes is used for calculating free space and making rebalancing
	// decisions. Zero indicates that there is no maximum size.
	MaxSizeBytes int64
	// MaxOpenFiles controls the maximum number of file descriptors RocksDB
	// creates. If MaxOpenFiles is zero, this is set to DefaultMaxOpenFiles.
	MaxOpenFiles uint64
	// WarnLargeBatchThreshold controls if a log message is printed when a
	// WriteBatch takes longer than WarnLargeBatchThreshold. If it is set to
	// zero, no log messages are ever printed.
	WarnLargeBatchThreshold time.Duration
	// Settings instance for cluster-wide knobs.
	Settings *cluster.Settings
	// UseFileRegistry is true if the file registry is needed (eg: encryption-at-rest).
	// This may force the store version to versionFileRegistry if currently lower.
	UseFileRegistry bool
	// RocksDBOptions contains RocksDB specific options using a semicolon
	// separated key-value syntax ("key1=value1; key2=value2").
	RocksDBOptions string
	// ExtraOptions is a serialized protobuf set by Go CCL code and passed through
	// to C CCL code.
	ExtraOptions []byte
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
	r := &RocksDB{
		cfg: RocksDBConfig{
			Attrs:        attrs,
			MaxSizeBytes: MaxSizeBytes,
		},
		// dir: empty dir == "mem" RocksDB instance.
		cache: cache.ref(),
	}

	auxDir, err := ioutil.TempDir(os.TempDir(), "cockroach-auxiliary")
	if err != nil {
		return nil, err
	}
	if err := r.setAuxiliaryDir(auxDir); err != nil {
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
		// Remove the temporary directory when the engine is in-memory.
		if err := os.RemoveAll(r.auxDir); err != nil {
			log.Warning(context.TODO(), err)
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

	// Find the total size of all the files in the r.dir and all its
	// subdirectories.
	var totalUsedBytes int64
	if errOuter := filepath.Walk(r.cfg.Dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			// This can happen if rocksdb removes files out from under us - just keep
			// going to get the best estimate we can.
			if os.IsNotExist(err) {
				return nil
			}
			// Special-case: if the store-dir is configured using the root of some fs,
			// e.g. "/mnt/db", we might have special fs-created files like lost+found
			// that we can't read, so just ignore them rather than crashing.
			if os.IsPermission(err) && filepath.Base(path) == "lost+found" {
				return nil
			}
			return err
		}
		if info.Mode().IsRegular() {
			totalUsedBytes += info.Size()
		}
		return nil
	}); errOuter != nil {
		return roachpb.StoreCapacity{}, errOuter
	}

	// If no size limitation have been placed on the store size or if the
	// limitation is greater than what's available, just return the actual
	// totals.
	if r.cfg.MaxSizeBytes == 0 || r.cfg.MaxSizeBytes >= fsuTotal || r.cfg.Dir == "" {
		return roachpb.StoreCapacity{
			Capacity:  fsuTotal,
			Available: fsuAvail,
			Used:      totalUsedBytes,
		}, nil
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
		Used:      totalUsedBytes,
	}, nil
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
	key MVCCKey, msg protoutil.Message,
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

// Writer methods are not implemented for rocksDBReadOnly. Ideally, the code could be refactored so that
// a Reader could be supplied to evaluateBatch

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

func (r *rocksDBReadOnly) ClearIterRange(iter Iterator, start, end MVCCKey) error {
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
		BlockCacheHits:                 int64(s.block_cache_hits),
		BlockCacheMisses:               int64(s.block_cache_misses),
		BlockCacheUsage:                int64(s.block_cache_usage),
		BlockCachePinnedUsage:          int64(s.block_cache_pinned_usage),
		BloomFilterPrefixChecked:       int64(s.bloom_filter_prefix_checked),
		BloomFilterPrefixUseful:        int64(s.bloom_filter_prefix_useful),
		MemtableTotalSize:              int64(s.memtable_total_size),
		Flushes:                        int64(s.flushes),
		Compactions:                    int64(s.compactions),
		TableReadersMemEstimate:        int64(s.table_readers_mem_estimate),
		PendingCompactionBytesEstimate: int64(s.pending_compaction_bytes_estimate),
		L0FileCount:                    int64(s.l0_file_count),
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

	tickers := (*[maxArrayLen / C.sizeof_TickerInfo]C.TickerInfo)(
		unsafe.Pointer(s.tickers))[:s.tickers_len:s.tickers_len]
	res.Tickers = make(map[string]uint64)
	for _, ticker := range tickers {
		name := cStringToGoString(ticker.name)
		value := uint64(ticker.value)
		res.Tickers[name] = value
	}
	C.free(unsafe.Pointer(s.tickers))

	res.Histograms = make(map[string]enginepb.HistogramData)
	histograms := (*[maxArrayLen / C.sizeof_HistogramInfo]C.HistogramInfo)(
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
	return cStringToGoString(C.DBGetCompactionStats(r.rdb))
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
func (r *rocksDBSnapshot) Iterate(start, end MVCCKey, f func(MVCCKeyValue) (bool, error)) error {
	return dbIterate(r.handle, r, start, end, f)
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

func (r *distinctBatch) ClearIterRange(iter Iterator, start, end MVCCKey) error {
	r.flushMutations()
	r.flushes++ // make sure that Repr() doesn't take a shortcut
	r.ensureBatch()
	return dbClearIterRange(r.batch, iter, start, end)
}

func (r *distinctBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	// No-op. Logical logging disabled.
}

func (r *distinctBatch) close() {
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

func (r *batchIterator) Seek(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.Seek(key)
}

func (r *batchIterator) SeekReverse(key MVCCKey) {
	r.batch.flushMutations()
	r.iter.SeekReverse(key)
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

func (r *batchIterator) PrevKey() {
	r.batch.flushMutations()
	r.iter.PrevKey()
}

func (r *batchIterator) ComputeStats(
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	r.batch.flushMutations()
	return r.iter.ComputeStats(start, end, nowNanos)
}

func (r *batchIterator) FindSplitKey(
	start, end, minSplitKey MVCCKey, targetSize int64,
) (MVCCKey, error) {
	r.batch.flushMutations()
	return r.iter.FindSplitKey(start, end, minSplitKey, targetSize)
}

func (r *batchIterator) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	r.batch.flushMutations()
	return r.iter.MVCCGet(key, timestamp, opts)
}

func (r *batchIterator) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData []byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	r.batch.flushMutations()
	return r.iter.MVCCScan(start, end, max, timestamp, opts)
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
	*r = rocksDBBatch{
		builder: r.builder,
		closed:  true,
	}
	batchPool.Put(r)
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

func (r *rocksDBBatch) ClearIterRange(iter Iterator, start, end MVCCKey) error {
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
	} else if len(r.builder.repr) > 0 {
		count, size = r.builder.count, len(r.builder.repr)

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
	return r.flushes == 0 && r.builder.count == 0 && !r.builder.logData
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
	parent *RocksDB
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
func newRocksDBIterator(
	rdb *C.DBEngine, opts IterOptions, engine Reader, parent *RocksDB,
) Iterator {
	// In order to prevent content displacement, caching is disabled
	// when performing scans. Any options set within the shared read
	// options field that should be carried over needs to be set here
	// as well.
	r := iterPool.Get().(*rocksDBIterator)
	r.init(rdb, opts, engine, parent)
	return r
}

func (r *rocksDBIterator) getIter() *C.DBIterator {
	return r.iter
}

func (r *rocksDBIterator) init(rdb *C.DBEngine, opts IterOptions, engine Reader, parent *RocksDB) {
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
	r.engine = engine
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
	if r.engine.Closed() {
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
		TimeBoundNumSSTs:           int(C.ulonglong(stats.timebound_num_ssts)),
		InternalDeleteSkippedCount: int(C.ulonglong(stats.internal_delete_skipped_count)),
	}
}

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
	r.setState(C.DBIterNext(r.iter, C.bool(false) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) Prev() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, C.bool(false) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) NextKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterNext(r.iter, C.bool(true) /* skip_current_key_versions */))
}

func (r *rocksDBIterator) PrevKey() {
	r.checkEngineOpen()
	r.setState(C.DBIterPrev(r.iter, C.bool(true) /* skip_current_key_versions */))
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
	if r.value.len <= 0 {
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
	start, end MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	r.clearState()
	result := C.MVCCComputeStats(r.iter, goToCKey(start), goToCKey(end), C.int64_t(nowNanos))
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
	start, end, minSplitKey MVCCKey, targetSize int64,
) (MVCCKey, error) {
	var splitKey C.DBString
	r.clearState()
	status := C.MVCCFindSplitKey(r.iter, goToCKey(start), goToCKey(end), goToCKey(minSplitKey),
		C.int64_t(targetSize), &splitKey)
	if err := statusToError(status); err != nil {
		return MVCCKey{}, err
	}
	return MVCCKey{Key: cStringToGoBytes(splitKey)}, nil
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
		C.bool(opts.Inconsistent), C.bool(opts.Tombstones), C.bool(opts.IgnoreSequence),
	)

	if err := statusToError(state.status); err != nil {
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
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData []byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	if opts.Inconsistent && opts.Txn != nil {
		return nil, 0, nil, nil, errors.Errorf("cannot allow inconsistent reads within a transaction")
	}
	if len(end) == 0 {
		return nil, 0, nil, nil, emptyKeyError()
	}
	if max == 0 {
		resumeSpan = &roachpb.Span{Key: start, EndKey: end}
		return nil, 0, resumeSpan, nil, nil
	}

	r.clearState()
	state := C.MVCCScan(
		r.iter, goToCSlice(start), goToCSlice(end),
		goToCTimestamp(timestamp), C.int64_t(max),
		goToCTxn(opts.Txn), C.bool(opts.Inconsistent),
		C.bool(opts.Reverse), C.bool(opts.Tombstones),
		C.bool(opts.IgnoreSequence),
	)

	if err := statusToError(state.status); err != nil {
		return nil, 0, nil, nil, err
	}
	if err := uncertaintyToError(timestamp, state.uncertainty_timestamp, opts.Txn); err != nil {
		return nil, 0, nil, nil, err
	}

	kvData = copyFromSliceVector(state.data.bufs, state.data.len)
	numKVs = int64(state.data.count)

	if resumeKey := cSliceToGoBytes(state.resume_key); resumeKey != nil {
		if opts.Reverse {
			resumeSpan = &roachpb.Span{Key: start, EndKey: roachpb.Key(resumeKey).Next()}
		} else {
			resumeSpan = &roachpb.Span{Key: resumeKey, EndKey: end}
		}
	}

	intents, err = buildScanIntents(cSliceToGoBytes(state.intents))
	if err != nil {
		return nil, 0, nil, nil, err
	}
	if !opts.Inconsistent && len(intents) > 0 {
		// When encountering intents during a consistent scan we still need to
		// return the resume key.
		return nil, 0, resumeSpan, nil, &roachpb.WriteIntentError{Intents: intents}
	}

	return kvData, numKVs, resumeSpan, intents, nil
}

func (r *rocksDBIterator) SetUpperBound(key roachpb.Key) {
	C.DBIterSetUpperBound(r.iter, goToCKey(MakeMVCCMetadataKey(key)))
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
	return (*[maxArrayLen]byte)(unsafe.Pointer(s.data))[:s.len:s.len]
}

func goToCTimestamp(ts hlc.Timestamp) C.DBTimestamp {
	return C.DBTimestamp{
		wall_time: C.int64_t(ts.WallTime),
		logical:   C.int32_t(ts.Logical),
	}
}

func goToCTxn(txn *roachpb.Transaction) C.DBTxn {
	var r C.DBTxn
	if txn != nil {
		r.id = goToCSlice(txn.ID.GetBytes())
		r.epoch = C.uint32_t(txn.Epoch)
		r.sequence = C.int32_t(txn.Sequence)
		r.max_timestamp = goToCTimestamp(txn.MaxTimestamp)
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
	return &RocksDBError{msg: cStringToGoString(s)}
}

func uncertaintyToError(
	readTS hlc.Timestamp, existingTS C.DBTimestamp, txn *roachpb.Transaction,
) error {
	if existingTS.wall_time != 0 || existingTS.logical != 0 {
		return roachpb.NewReadWithinUncertaintyIntervalError(
			readTS, hlc.Timestamp{
				WallTime: int64(existingTS.wall_time),
				Logical:  int32(existingTS.logical),
			},
			txn)
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
	it := newRocksDBIterator(rdb, IterOptions{UpperBound: end.Key}, engine, nil)
	defer it.Close()

	it.Seek(start)
	for ; ; it.Next() {
		ok, err := it.Valid()
		if err != nil {
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

	cPaths := make([]*C.char, 1)
	cPaths[0] = C.CString(filename)
	cPathLen := C.size_t(len(cPaths))
	defer C.free(unsafe.Pointer(cPaths[0]))

	const noMove, writeSeqNo, modify = false, false, true
	return statusToError(C.DBIngestExternalFiles(
		fr.rocksDB.rdb, &cPaths[0], cPathLen, noMove, writeSeqNo, modify,
	))
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
func (fr *RocksDBSstFileReader) NewIterator(opts IterOptions) Iterator {
	return newRocksDBIterator(fr.rocksDB.rdb, opts, fr.rocksDB, fr.rocksDB.RocksDB)
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

// Delete puts a deletion tombstone into the sstable being built. See
// the Add method for more.
func (fw *RocksDBSstFileWriter) Delete(k MVCCKey) error {
	if fw.fw == nil {
		return errors.New("cannot call Delete on a closed writer")
	}
	fw.DataSize += int64(len(k.Key))
	return statusToError(C.DBSstFileWriterDelete(fw.fw, goToCKey(k)))
}

var _ = (*RocksDBSstFileWriter).Delete

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

func (r *RocksDB) setAuxiliaryDir(d string) error {
	if err := os.MkdirAll(d, 0755); err != nil {
		return err
	}
	r.auxDir = d
	return nil
}

// PreIngestDelay may choose to block for some duration if L0 has an excessive
// number of files in it or if PendingCompactionBytesEstimate is elevated. This
// it is intended to be called before ingesting a new SST, since we'd rather
// backpressure the bulk operation adding SSTs than slow down the whole RocksDB
// instance and impact all forground traffic by adding too many files to it.
// After the number of L0 files exceeds the configured limit, it gradually
// begins delaying more for each additional file in L0 over the limit until
// hitting its configured (via settings) maximum delay. If the pending
// compaction limit is exceeded, it waits for the maximum delay.
func (r *RocksDB) PreIngestDelay(ctx context.Context) {
	if r.cfg.Settings == nil {
		return
	}
	stats, err := r.GetStats()
	if err != nil {
		log.Warningf(ctx, "failed to read stats: %+v", err)
		return
	}
	targetDelay := calculatePreIngestDelay(r.cfg, stats)

	if targetDelay == 0 {
		return
	}
	log.VEventf(ctx, 2, "delaying SST ingestion %s. %d L0 files, %db pending compaction", targetDelay, stats.L0FileCount, stats.PendingCompactionBytesEstimate)

	select {
	case <-time.After(targetDelay):
	case <-ctx.Done():
	}
}

func calculatePreIngestDelay(cfg RocksDBConfig, stats *Stats) time.Duration {
	maxDelay := ingestDelayTime.Get(&cfg.Settings.SV)
	l0Filelimit := ingestDelayL0Threshold.Get(&cfg.Settings.SV)
	compactionLimit := ingestDelayPendingLimit.Get(&cfg.Settings.SV)

	if stats.PendingCompactionBytesEstimate >= compactionLimit {
		return maxDelay
	}
	const ramp = 10
	if stats.L0FileCount > l0Filelimit {
		delayPerFile := maxDelay / time.Duration(ramp)
		targetDelay := time.Duration(stats.L0FileCount-l0Filelimit) * delayPerFile
		if targetDelay > maxDelay {
			return maxDelay
		}
		return targetDelay
	}
	return 0
}

// IngestExternalFiles atomically links a slice of files into the RocksDB
// log-structured merge-tree.
func (r *RocksDB) IngestExternalFiles(
	ctx context.Context, paths []string, skipWritingSeqNo, allowFileModifications bool,
) error {
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
		C._Bool(!skipWritingSeqNo),
		C._Bool(allowFileModifications),
	))
}

// WriteFile writes data to a file in this RocksDB's env.
func (r *RocksDB) WriteFile(filename string, data []byte) error {
	return statusToError(C.DBEnvWriteFile(r.rdb, goToCSlice([]byte(filename)), goToCSlice(data)))
}

// OpenFile opens a DBFile, which is essentially a rocksdb WritableFile
// with the given filename, in this RocksDB's env.
func (r *RocksDB) OpenFile(filename string) (DBFile, error) {
	var file C.DBWritableFile
	if err := statusToError(C.DBEnvOpenFile(r.rdb, goToCSlice([]byte(filename)), &file)); err != nil {
		return nil, notFoundErrOrDefault(err)
	}
	return &rocksdbFile{file: file, rdb: r.rdb}, nil
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

// DeleteFile deletes the file with the given filename from this RocksDB's env.
// If the file with given filename doesn't exist, return os.ErrNotExist.
func (r *RocksDB) DeleteFile(filename string) error {
	if err := statusToError(C.DBEnvDeleteFile(r.rdb, goToCSlice([]byte(filename)))); err != nil {
		return notFoundErrOrDefault(err)
	}
	return nil
}

// DeleteDirAndFiles deletes the directory and any files it contains but
// not subdirectories from this RocksDB's env. If dir does not exist,
// DeleteDirAndFiles returns nil (no error).
func (r *RocksDB) DeleteDirAndFiles(dir string) error {
	if err := statusToError(C.DBEnvDeleteDirAndFiles(r.rdb, goToCSlice([]byte(dir)))); err != nil && notFoundErrOrDefault(err) != os.ErrNotExist {
		return err
	}
	return nil
}

// LinkFile creates 'newname' as a hard link to 'oldname'. This use the Env responsible for the file
// which may handle extra logic (eg: copy encryption settings for EncryptedEnv).
func (r *RocksDB) LinkFile(oldname, newname string) error {
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

// NewSortedDiskMap implements the MapProvidingEngine interface.
func (r *RocksDB) NewSortedDiskMap() diskmap.SortedDiskMap {
	return NewRocksDBMap(r)
}

// NewSortedDiskMultiMap implements the MapProvidingEngine interface.
func (r *RocksDB) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return NewRocksDBMultiMap(r)
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

// ExportToSst exports changes to the keyrange [start.Key, end.Key) over the
// interval (start.Timestamp, end.Timestamp]. Passing exportAllRevisions exports
// every revision of a key for the interval, otherwise only the latest value
// within the interval is exported. Deletions are included if all revisions are
// requested or if the start.Timestamp is non-zero. Returns the bytes of an
// SSTable containing the exported keys, the size of exported data, or an error.
func ExportToSst(
	ctx context.Context, e Reader, start, end MVCCKey, exportAllRevisions bool, io IterOptions,
) ([]byte, int64, error) {

	var cdbEngine *C.DBEngine
	switch v := e.(type) {
	case *RocksDB:
		cdbEngine = v.rdb
	case *rocksDBReadOnly:
		cdbEngine = v.parent.rdb
	default:
		panic(errors.Errorf("Not a rocksdb or rocksdbReadOnly engine but a %T", e))
	}

	var data C.DBString
	var entries C.int64_t
	var dataSize C.int64_t
	var intentErr C.DBString

	err := statusToError(C.DBExportToSst(goToCKey(start), goToCKey(end), C.bool(exportAllRevisions),
		goToCIterOptions(io), cdbEngine, &data, &entries, &dataSize, &intentErr))

	if err != nil {
		if err.Error() == "WriteIntentError" {
			var e roachpb.WriteIntentError
			if err := protoutil.Unmarshal(cStringToGoBytes(intentErr), &e); err != nil {
				return nil, 0, errors.Wrap(err, "failed to decode write intent error")
			}

			return nil, 0, &e
		}
		return nil, 0, err
	}

	return cStringToGoBytes(data), int64(dataSize), nil
}

func notFoundErrOrDefault(err error) error {
	errStr := err.Error()
	if strings.Contains(errStr, "No such file or directory") ||
		strings.Contains(errStr, "File not found") ||
		strings.Contains(errStr, "The system cannot find the path specified") {
		return os.ErrNotExist
	}
	return err
}

// DBFile is an interface for interacting with DBWritableFile in RocksDB.
type DBFile interface {
	// Append appends data to this DBFile.
	Append(data []byte) error
	// Close closes this DBFile.
	Close() error
	// Sync synchronously flushes this DBFile's data to disk.
	Sync() error
}

// rocksdbFile implements DBFile interface. It is used to interact with the
// DBWritableFile in the corresponding RocksDB env.
type rocksdbFile struct {
	file C.DBWritableFile
	rdb  *C.DBEngine
}

// Append implements the DBFile interface.
func (f *rocksdbFile) Append(data []byte) error {
	return statusToError(C.DBEnvAppendFile(f.rdb, f.file, goToCSlice(data)))
}

// Close implements the DBFile interface.
func (f *rocksdbFile) Close() error {
	return statusToError(C.DBEnvCloseFile(f.rdb, f.file))
}

// Sync implements the DBFile interface.
func (f *rocksdbFile) Sync() error {
	return statusToError(C.DBEnvSyncFile(f.rdb, f.file))
}
