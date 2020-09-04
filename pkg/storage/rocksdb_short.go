// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//
// +build short

package storage

import (
	"context"
	"os"
	"time"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// RocksDB is a wrapper around a RocksDB database instance.
type RocksDB struct {}

var _ Engine = &RocksDB{}

// RocksDBCache is a wrapper around C.DBCache
type RocksDBCache struct {}

// NewRocksDBCache creates a new cache of the specified size. Note that the
// cache is refcounted internally and starts out with a refcount of one (i.e.
// Release() should be called after having used the cache).
func NewRocksDBCache(cacheSize int64) RocksDBCache {
	return RocksDBCache{}
}

// Release releases the cache. Note that the cache will continue to be used
// until all of the RocksDB engines it was attached to have been closed, and
// that RocksDB engines which use it auto-release when they close.
func (c RocksDBCache) Release() {
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

// NewRocksDB allocates and returns a new RocksDB stub object.
func NewRocksDB(cfg RocksDBConfig, cache RocksDBCache) (*RocksDB, error) {
	r := &RocksDB{}
	return r, nil
}

func newRocksDBInMem(attrs roachpb.Attributes, cacheSize int64) *RocksDB {
	return &RocksDB{}
}

func newMemRocksDB(attrs roachpb.Attributes, cache RocksDBCache, maxSize int64) (*RocksDB, error) {
	return &RocksDB{}, nil
}

type lockStruct struct{}

// lockFile sets a lock on the specified file using RocksDB's file locking interface.
func lockFile(filename string) (lockStruct, error) {
	return lockStruct{}, nil
}

// unlockFile unlocks the file asscoiated with the specified lock and GCs any allocated memory for the lock.
func unlockFile(lock lockStruct) error {
	return nil
}

// IsValidSplitKey returns whether the key is a valid split key. Passthrough
// to the Go version of isValidSplitKey.
//
// TODO(bilal): Replace the exported version of IsValidSplitKey with this one
// on all builds, short or otherwise.
func IsValidSplitKey(key roachpb.Key) bool {
	return isValidSplitKey(key, keys.NoSplitSpans)
}

// InitRocksDBLogger initializes the logger to use for RocksDB log messages. If
// not called, WARNING, ERROR, and FATAL logs will be output to the normal
// CockroachDB log. The caller is responsible for ensuring the
// Close() method is eventually called on the new logger.
func InitRocksDBLogger(ctx context.Context) *log.SecondaryLogger {
	return nil
}

// SetRocksDBOpenHook sets the DBOpenHook function that will be called during
// RocksDB initialization. It is intended to be called by CCL code.
func SetRocksDBOpenHook(fn unsafe.Pointer) {
	// No-op.
}

// ThreadStacks returns the stacks for all threads. The stacks are raw
// addresses, and do not contain symbols. Use addr2line (or atos on Darwin) to
// symbolize.
func ThreadStacks() string {
	return ""
}

// String formatter.
func (r *RocksDB) String() string {
	return "rocksdb-stub"
}

// Close closes the database by deallocating the underlying handle.
func (r *RocksDB) Close() {
	panic("rocksdb used in short build")
}

// CreateCheckpoint creates a RocksDB checkpoint in the given directory (which
// must not exist). This directory should be located on the same file system, or
// copies of all data are used instead of hard links, which is very expensive.
func (r *RocksDB) CreateCheckpoint(dir string) error {
	panic("rocksdb used in short build")
}

// Closed returns true if the engine is closed.
func (r *RocksDB) Closed() bool {
	panic("rocksdb used in short build")
}

// ExportToSst is part of the engine.Reader interface.
func (r *RocksDB) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	panic("rocksdb used in short build")
}

// Attrs returns the list of attributes describing this engine. This
// may include a specification of disk type (e.g. hdd, ssd, fio, etc.)
// and potentially other labels to identify important attributes of
// the engine.
func (r *RocksDB) Attrs() roachpb.Attributes {
	panic("rocksdb used in short build")
}

// Put sets the given key to the value provided.
//
// It is safe to modify the contents of the arguments after Put returns.
func (r *RocksDB) Put(key MVCCKey, value []byte) error {
	panic("rocksdb used in short build")
}

// Merge implements the RocksDB merge operator using the function goMergeInit
// to initialize missing values and goMerge to merge the old and the given
// value into a new value, which is then stored under key.
// Currently 64-bit counter logic is implemented. See the documentation of
// goMerge and goMergeInit for details.
//
// It is safe to modify the contents of the arguments after Merge returns.
func (r *RocksDB) Merge(key MVCCKey, value []byte) error {
	panic("rocksdb used in short build")
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
	panic("rocksdb used in short build")
}

// Get returns the value for the given key.
func (r *RocksDB) Get(key MVCCKey) ([]byte, error) {
	panic("rocksdb used in short build")
}

// GetProto fetches the value at the specified key and unmarshals it.
func (r *RocksDB) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	panic("rocksdb used in short build")
}

// Clear removes the item from the db with the given key.
//
// It is safe to modify the contents of the arguments after Clear returns.
func (r *RocksDB) Clear(key MVCCKey) error {
	panic("rocksdb used in short build")
}

// SingleClear removes the most recent item from the db with the given key.
//
// It is safe to modify the contents of the arguments after SingleClear returns.
func (r *RocksDB) SingleClear(key MVCCKey) error {
	panic("rocksdb used in short build")
}

// ClearRange removes a set of entries, from start (inclusive) to end
// (exclusive).
//
// It is safe to modify the contents of the arguments after ClearRange returns.
func (r *RocksDB) ClearRange(start, end MVCCKey) error {
	panic("rocksdb used in short build")
}

// ClearIterRange removes a set of entries, from start (inclusive) to end
// (exclusive).
//
// It is safe to modify the contents of the arguments after ClearIterRange
// returns.
func (r *RocksDB) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	panic("rocksdb used in short build")
}

// Iterate iterates from start to end keys, invoking f on each
// key/value pair. See engine.Iterate for details.
func (r *RocksDB) Iterate(start, end roachpb.Key, f func(MVCCKeyValue) (bool, error)) error {
	panic("rocksdb used in short build")
}

// Capacity queries the underlying file system for disk capacity information.
func (r *RocksDB) Capacity() (roachpb.StoreCapacity, error) {
	panic("rocksdb used in short build")
}

// Compact forces compaction over the entire database.
func (r *RocksDB) Compact() error {
	panic("rocksdb used in short build")
}

// CompactRange forces compaction over a specified range of keys in the database.
func (r *RocksDB) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	panic("rocksdb used in short build")
}

// ApproximateDiskBytes returns the approximate on-disk size of the specified key range.
func (r *RocksDB) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	panic("rocksdb used in short build")
}

// Flush causes RocksDB to write all in-memory data to disk immediately.
func (r *RocksDB) Flush() error {
	panic("rocksdb used in short build")
}

// NewIterator returns an iterator over this rocksdb engine.
func (r *RocksDB) NewIterator(opts IterOptions) Iterator {
	panic("rocksdb used in short build")
}

// NewSnapshot creates a snapshot handle from engine and returns a
// read-only rocksDBSnapshot engine.
func (r *RocksDB) NewSnapshot() Reader {
	panic("rocksdb used in short build")
}

// Type implements the Engine interface.
func (r *RocksDB) Type() enginepb.EngineType {
	panic("rocksdb used in short build")
}

// NewReadOnly returns a new ReadWriter wrapping this rocksdb engine.
func (r *RocksDB) NewReadOnly() ReadWriter {
	return r
}

// NewBatch returns a new batch wrapping this rocksdb engine.
func (r *RocksDB) NewBatch() Batch {
	panic("rocksdb used in short build")
}

// NewWriteOnlyBatch returns a new write-only batch wrapping this rocksdb
// engine.
func (r *RocksDB) NewWriteOnlyBatch() Batch {
	panic("rocksdb used in short build")
}

// GetSSTables retrieves metadata about this engine's live sstables.
func (r *RocksDB) GetSSTables() SSTableInfos {
	panic("rocksdb used in short build")
}

// GetUserProperties fetches the user properties stored in each sstable's
// metadata.
func (r *RocksDB) GetUserProperties() (enginepb.SSTUserPropertiesCollection, error) {
	panic("rocksdb used in short build")
}

// GetStats retrieves stats from this engine's RocksDB instance and
// returns it in a new instance of Stats.
func (r *RocksDB) GetStats() (*Stats, error) {
	panic("rocksdb used in short build")
}

// GetTickersAndHistograms retrieves maps of all RocksDB tickers and histograms.
// It differs from `GetStats` by getting _every_ ticker and histogram, and by not
// getting anything else (DB properties, for example).
func (r *RocksDB) GetTickersAndHistograms() (*enginepb.TickersAndHistograms, error) {
	panic("rocksdb used in short build")
}

// GetCompactionStats returns the internal RocksDB compaction stats. See
// https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide#rocksdb-statistics.
func (r *RocksDB) GetCompactionStats() string {
	panic("rocksdb used in short build")
}

// GetEnvStats returns stats for the RocksDB env. This may include encryption stats.
func (r *RocksDB) GetEnvStats() (*EnvStats, error) {
	panic("rocksdb used in short build")
}

// GetEncryptionRegistries returns the file and key registries when encryption is enabled
// on the store.
func (r *RocksDB) GetEncryptionRegistries() (*EncryptionRegistries, error) {
	panic("rocksdb used in short build")
}

// RunLDB runs RocksDB's ldb command-line tool. The passed
// command-line arguments should not include argv[0].
func RunLDB(args []string) {
	panic("rocksdb used in short build")
}

// RunSSTDump runs RocksDB's sst_dump command-line tool. The passed
// command-line arguments should not include argv[0].
func RunSSTDump(args []string) {
	panic("rocksdb used in short build")
}

// GetAuxiliaryDir returns the auxiliary storage path for this engine.
func (r *RocksDB) GetAuxiliaryDir() string {
	panic("rocksdb used in short build")
}

// PreIngestDelay implements the Engine interface.
func (r *RocksDB) PreIngestDelay(ctx context.Context) {
	panic("rocksdb used in short build")
}

// IngestExternalFiles atomically links a slice of files into the RocksDB
// log-structured merge-tree.
func (r *RocksDB) IngestExternalFiles(ctx context.Context, paths []string) error {
	panic("rocksdb used in short build")
}

// InMem returns true if the receiver is an in-memory engine and false
// otherwise.
func (r *RocksDB) InMem() bool {
	panic("rocksdb used in short build")
}

// ReadFile reads the content from a file with the given filename. The file
// must have been opened through Engine.OpenFile. Otherwise an error will be
// returned.
func (r *RocksDB) ReadFile(filename string) ([]byte, error) {
	panic("rocksdb used in short build")
}

// WriteFile writes data to a file in this RocksDB's env.
func (r *RocksDB) WriteFile(filename string, data []byte) error {
	panic("rocksdb used in short build")
}

// Remove deletes the file with the given filename from this RocksDB's env.
// If the file with given filename doesn't exist, return os.ErrNotExist.
func (r *RocksDB) Remove(filename string) error {
	panic("rocksdb used in short build")
}

// RemoveAll removes path and any children it contains from this RocksDB's
// env. If the path does not exist, RemoveAll returns nil (no error).
func (r *RocksDB) RemoveAll(path string) error {
	panic("rocksdb used in short build")
}

// Link creates 'newname' as a hard link to 'oldname'. This use the Env
// responsible for the file which may handle extra logic (eg: copy encryption
// settings for EncryptedEnv).
func (r *RocksDB) Link(oldname, newname string) error {
	panic("rocksdb used in short build")
}

var _ fs.FS = &RocksDB{}

// Create implements the FS interface.
func (r *RocksDB) Create(name string) (fs.File, error) {
	panic("rocksdb used in short build")
}

// CreateWithSync implements the FS interface.
func (r *RocksDB) CreateWithSync(name string, bytesPerSync int) (fs.File, error) {
	panic("rocksdb used in short build")
}

// Open implements the FS interface.
func (r *RocksDB) Open(name string) (fs.File, error) {
	panic("rocksdb used in short build")
}

// OpenDir implements the FS interface.
func (r *RocksDB) OpenDir(name string) (fs.File, error) {
	panic("rocksdb used in short build")
}

// Rename implements the FS interface.
func (r *RocksDB) Rename(oldname, newname string) error {
	panic("rocksdb used in short build")
}

// MkdirAll implements the FS interface.
func (r *RocksDB) MkdirAll(path string) error {
	panic("rocksdb used in short build")
}

// RemoveDir implements the FS interface.
func (r *RocksDB) RemoveDir(name string) error {
	panic("rocksdb used in short build")
}

// List implements the FS interface.
func (r *RocksDB) List(name string) ([]string, error) {
	panic("rocksdb used in short build")
}

// Stat implements the FS interface.
func (r *RocksDB) Stat(name string) (os.FileInfo, error) {
	panic("rocksdb used in short build")
}
