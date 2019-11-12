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
	"bytes"
	"context"
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TeePebbleRocksDB sends all write operations to both the underlying
// RocksDB and Pebble instances, and for all read operations, it verifies that
// any keys and values returned match. Implements the Engine interface.
//
// This engine is only meant to be used in testing. No performance or
// stability guarantees are made about this engine in production.
type TeePebbleRocksDB struct {
	ctx     context.Context
	pebble  *Pebble
	rocksDB *RocksDB
	inMem   bool
}

var _ Engine = &TeePebbleRocksDB{}

// NewTee creates a new instance of the TeePebbleRocksDB engine.
func NewTee(ctx context.Context, rocksdb *RocksDB, pebble *Pebble) *TeePebbleRocksDB {
	return &TeePebbleRocksDB{
		ctx:     ctx,
		pebble:  pebble,
		rocksDB: rocksdb,
	}
}

func fatalOnErrorMismatch(ctx context.Context, err error, err2 error) error {
	if err != nil && err2 != nil {
		return err
	} else if err != nil || err2 != nil {
		log.Fatalf(ctx, "error mismatch between pebble and rocksdb: %v != %v", err, err2)
	}
	return nil
}

// Close implements the Engine interface.
func (t *TeePebbleRocksDB) Close() {
	t.pebble.Close()
	t.rocksDB.Close()
}

// Closed implements the Engine interface.
func (t *TeePebbleRocksDB) Closed() bool {
	pebbleClosed := t.pebble.Closed()
	rocksDBClosed := t.rocksDB.Closed()
	if pebbleClosed && rocksDBClosed {
		return true
	} else if pebbleClosed || rocksDBClosed {
		log.Fatalf(t.ctx, "only one of pebble or rocksdb closed: %v != %v", pebbleClosed, rocksDBClosed)
	}
	return false
}

// ExportToSst implements the Engine interface.
func (t *TeePebbleRocksDB) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	pebbleSst, bulkOpSummary, err := t.pebble.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, io)
	rocksSst, _, err2 := t.rocksDB.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, io)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, bulkOpSummary, err
	}

	if !bytes.Equal(pebbleSst, rocksSst) {
		log.Fatalf(t.ctx, "mismatching SSTs returned by pebble and rocksdb: %v != %v", pebbleSst, rocksSst)
	}
	return pebbleSst, bulkOpSummary, err
}

// Get implements the Engine interface.
func (t *TeePebbleRocksDB) Get(key MVCCKey) ([]byte, error) {
	value, err := t.pebble.Get(key)
	value2, err2 := t.rocksDB.Get(key)

	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}

	if !bytes.Equal(value, value2) {
		log.Fatalf(t.ctx, "values mismatch between pebble and rocksdb: %v != %v", value, value2)
	}
	return value, nil
}

// GetProto implements the Engine interface.
func (t *TeePebbleRocksDB) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		return false, 0, 0, emptyKeyError()
	}
	// Piggyback on the value / error checks in Get.
	val, err := t.Get(key)
	if err != nil || val == nil {
		return false, 0, 0, err
	}

	err = protoutil.Unmarshal(val, msg)
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Iterate implements the Engine interface.
func (t *TeePebbleRocksDB) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(t, start, end, f)
}

// NewIterator implements the Engine interface.
func (t *TeePebbleRocksDB) NewIterator(opts IterOptions) Iterator {
	return &TeePebbleRocksDBIter{
		ctx:     t.ctx,
		pebble:  t.pebble.NewIterator(opts).(*pebbleIterator),
		rocksDB: t.rocksDB.NewIterator(opts),
	}
}

// ApplyBatchRepr implements the Engine interface.
func (t *TeePebbleRocksDB) ApplyBatchRepr(repr []byte, sync bool) error {
	err := t.pebble.ApplyBatchRepr(repr, sync)
	err2 := t.rocksDB.ApplyBatchRepr(repr, sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Clear implements the Engine interface.
func (t *TeePebbleRocksDB) Clear(key MVCCKey) error {
	err := t.pebble.Clear(key)
	err2 := t.rocksDB.Clear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// SingleClear implements the Engine interface.
func (t *TeePebbleRocksDB) SingleClear(key MVCCKey) error {
	err := t.pebble.SingleClear(key)
	err2 := t.rocksDB.SingleClear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearRange implements the Engine interface.
func (t *TeePebbleRocksDB) ClearRange(start, end MVCCKey) error {
	err := t.pebble.ClearRange(start, end)
	err2 := t.rocksDB.ClearRange(start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearIterRange implements the Engine interface.
func (t *TeePebbleRocksDB) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	err := t.pebble.ClearIterRange(iter.(*TeePebbleRocksDBIter).pebble, start, end)
	err2 := t.rocksDB.ClearIterRange(iter.(*TeePebbleRocksDBIter).rocksDB, start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Merge implements the Engine interface.
func (t *TeePebbleRocksDB) Merge(key MVCCKey, value []byte) error {
	err := t.pebble.Merge(key, value)
	err2 := t.rocksDB.Merge(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Put implements the Engine interface.
func (t *TeePebbleRocksDB) Put(key MVCCKey, value []byte) error {
	err := t.pebble.Put(key, value)
	err2 := t.rocksDB.Put(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogData implements the Engine interface.
func (t *TeePebbleRocksDB) LogData(data []byte) error {
	err := t.pebble.LogData(data)
	err2 := t.rocksDB.LogData(data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogLogicalOp implements the Engine interface.
func (t *TeePebbleRocksDB) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	t.pebble.LogLogicalOp(op, details)
	t.rocksDB.LogLogicalOp(op, details)
}

// Attrs implements the Engine interface.
func (t *TeePebbleRocksDB) Attrs() roachpb.Attributes {
	return t.pebble.Attrs()
}

// Capacity implements the Engine interface.
func (t *TeePebbleRocksDB) Capacity() (roachpb.StoreCapacity, error) {
	return t.pebble.Capacity()
}

// Compact implements the Engine interface.
func (t *TeePebbleRocksDB) Compact() error {
	err := t.pebble.Compact()
	err2 := t.rocksDB.Compact()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Flush implements the Engine interface.
func (t *TeePebbleRocksDB) Flush() error {
	err := t.pebble.Flush()
	err2 := t.rocksDB.Flush()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// GetSSTables implements the Engine interface.
func (t *TeePebbleRocksDB) GetSSTables() SSTableInfos {
	return t.pebble.GetSSTables()
}

// GetCompactionStats implements the Engine interface.
func (t *TeePebbleRocksDB) GetCompactionStats() string {
	return t.pebble.GetCompactionStats()
}

// GetStats implements the Engine interface.
func (t *TeePebbleRocksDB) GetStats() (*Stats, error) {
	return t.pebble.GetStats()
}

// GetTickersAndHistograms implements the Engine interface.
func (t *TeePebbleRocksDB) GetTickersAndHistograms() (*enginepb.TickersAndHistograms, error) {
	return t.pebble.GetTickersAndHistograms()
}

// GetEncryptionRegistries implements the Engine interface.
func (t TeePebbleRocksDB) GetEncryptionRegistries() (*EncryptionRegistries, error) {
	return t.pebble.GetEncryptionRegistries()
}

// GetEnvStats implements the Engine interface.
func (t TeePebbleRocksDB) GetEnvStats() (*EnvStats, error) {
	return t.pebble.GetEnvStats()
}

// GetAuxiliaryDir implements the Engine interface.
func (t TeePebbleRocksDB) GetAuxiliaryDir() string {
	// Treat the RocksDB path as the main aux dir, so that checkpoints are made in
	// subdirectories within it.
	return t.rocksDB.GetAuxiliaryDir()
}

// NewBatch implements the Engine interface.
func (t TeePebbleRocksDB) NewBatch() Batch {
	pebble := t.pebble.NewBatch()
	rocksDB := t.rocksDB.NewBatch()

	return &TeePebbleRocksDBBatch{
		ctx:          t.ctx,
		pebbleBatch:  pebble.(*pebbleBatch),
		rocksDBBatch: rocksDB.(*rocksDBBatch),
	}
}

// NewReadOnly implements the Engine interface.
func (t TeePebbleRocksDB) NewReadOnly() ReadWriter {
	pebble := t.pebble.NewReadOnly()
	rocksDB := t.rocksDB.NewReadOnly()

	return &TeePebbleRocksDBReadWriter{TeePebbleRocksDBReader{
		ctx:     t.ctx,
		pebble:  pebble,
		rocksDB: rocksDB,
	}}
}

// NewWriteOnlyBatch implements the Engine interface.
func (t TeePebbleRocksDB) NewWriteOnlyBatch() Batch {
	pebble := t.pebble.NewWriteOnlyBatch()
	rocksDB := t.rocksDB.NewWriteOnlyBatch()
	return &TeePebbleRocksDBBatch{
		ctx:          t.ctx,
		pebbleBatch:  pebble.(*pebbleBatch),
		rocksDBBatch: rocksDB.(*rocksDBBatch),
	}
}

// NewSnapshot implements the Engine interface.
func (t TeePebbleRocksDB) NewSnapshot() Reader {
	pebble := t.pebble.NewSnapshot()
	rocksDB := t.rocksDB.NewSnapshot()

	return &TeePebbleRocksDBReader{
		ctx:     t.ctx,
		pebble:  pebble,
		rocksDB: rocksDB,
	}
}

// Type implements the Engine interface.
func (t TeePebbleRocksDB) Type() enginepb.EngineType {
	return enginepb.EngineTypeTeePebbleRocksDB
}

// IngestExternalFiles implements the Engine interface.
func (t TeePebbleRocksDB) IngestExternalFiles(ctx context.Context, paths []string) error {
	err := t.pebble.IngestExternalFiles(ctx, paths)
	err2 := t.rocksDB.IngestExternalFiles(ctx, paths)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// PreIngestDelay implements the Engine interface.
func (t TeePebbleRocksDB) PreIngestDelay(ctx context.Context) {
	t.pebble.PreIngestDelay(ctx)
}

// ApproximateDiskBytes implements the Engine interface.
func (t TeePebbleRocksDB) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	bytes, err := t.pebble.ApproximateDiskBytes(from, to)
	bytes2, err2 := t.rocksDB.ApproximateDiskBytes(from, to)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return 0, err
	}

	return bytes + bytes2, nil
}

// CompactRange implements the Engine interface.
func (t TeePebbleRocksDB) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	err := t.pebble.CompactRange(start, end, forceBottommost)
	err2 := t.rocksDB.CompactRange(start, end, forceBottommost)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// InMem implements the Engine interface.
func (t TeePebbleRocksDB) InMem() bool {
	return t.inMem
}

// OpenFile implements the Engine interface.
func (t TeePebbleRocksDB) OpenFile(filename string) (DBFile, error) {
	pebbleFile, err := t.pebble.OpenFile(filename)
	if !t.inMem {
		// No need to write twice if the two engines share the same file system.
		return pebbleFile, err
	}
	rocksDBFile, err2 := t.rocksDB.OpenFile(filename)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}
	return &TeePebbleRocksDBFile{
		ctx:     t.ctx,
		pebble:  pebbleFile,
		rocksDB: rocksDBFile,
	}, nil
}

// ReadFile implements the Engine interface.
func (t TeePebbleRocksDB) ReadFile(filename string) ([]byte, error) {
	return t.pebble.ReadFile(filename)
}

// WriteFile implements the Engine interface.
func (t TeePebbleRocksDB) WriteFile(filename string, data []byte) error {
	err := t.pebble.WriteFile(filename, data)
	if !t.inMem {
		// No need to write twice if the two engines share the same file system.
		return err
	}
	err2 := t.rocksDB.WriteFile(filename, data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// DeleteFile implements the Engine interface.
func (t TeePebbleRocksDB) DeleteFile(filename string) error {
	err := t.pebble.DeleteFile(filename)
	if !t.inMem {
		// No need to write twice if the two engines share the same file system.
		return err
	}
	err2 := t.rocksDB.DeleteFile(filename)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// DeleteDirAndFiles implements the Engine interface.
func (t TeePebbleRocksDB) DeleteDirAndFiles(dir string) error {
	err := t.pebble.DeleteDirAndFiles(dir)
	if !t.inMem {
		// No need to write twice if the two engines share the same file system.
		return err
	}
	err2 := t.rocksDB.DeleteDirAndFiles(dir)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LinkFile implements the Engine interface.
func (t TeePebbleRocksDB) LinkFile(oldname, newname string) error {
	err := t.pebble.LinkFile(oldname, newname)
	if !t.inMem {
		// No need to write twice if the two engines share the same file system.
		return err
	}
	err2 := t.rocksDB.LinkFile(oldname, newname)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// CreateCheckpoint implements the Engine interface.
func (t TeePebbleRocksDB) CreateCheckpoint(dir string) error {
	pebblePath := filepath.Join(dir, "pebble")
	rocksDBPath := filepath.Join(dir, "rocksdb")
	err := t.pebble.CreateCheckpoint(pebblePath)
	err2 := t.rocksDB.CreateCheckpoint(rocksDBPath)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// TeePebbleRocksDBFile is a DBFile that writes to both  underlying pebble
// and rocksdb files.
type TeePebbleRocksDBFile struct {
	ctx     context.Context
	pebble  DBFile
	rocksDB DBFile
}

var _ DBFile = &TeePebbleRocksDBFile{}

// Close implements the DBFile interface.
func (t TeePebbleRocksDBFile) Close() error {
	err := t.pebble.Close()
	err2 := t.rocksDB.Close()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Sync implements the DBFile interface.
func (t TeePebbleRocksDBFile) Sync() error {
	err := t.pebble.Sync()
	err2 := t.rocksDB.Sync()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Write implements the DBFile interface.
func (t TeePebbleRocksDBFile) Write(p []byte) (int, error) {
	n, err := t.pebble.Write(p)
	n2, err2 := t.rocksDB.Write(p)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return 0, err
	}
	if n != n2 {
		log.Fatalf(t.ctx, "mismatching number of bytes written by pebble and rocksdb: %d != %d", n, n2)
	}
	return n, nil
}

// TeePebbleRocksDBReadWriter implements a generic ReadWriter. Used for
// implementing ReadOnly.
type TeePebbleRocksDBReadWriter struct {
	TeePebbleRocksDBReader
}

var _ ReadWriter = &TeePebbleRocksDBReadWriter{}

// ApplyBatchRepr implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	err := t.pebble.(ReadWriter).ApplyBatchRepr(repr, sync)
	err2 := t.rocksDB.(ReadWriter).ApplyBatchRepr(repr, sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Clear implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) Clear(key MVCCKey) error {
	err := t.pebble.(ReadWriter).Clear(key)
	err2 := t.rocksDB.(ReadWriter).Clear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// SingleClear implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) SingleClear(key MVCCKey) error {
	err := t.pebble.(ReadWriter).SingleClear(key)
	err2 := t.rocksDB.(ReadWriter).SingleClear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearRange implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) ClearRange(start, end MVCCKey) error {
	err := t.pebble.(ReadWriter).ClearRange(start, end)
	err2 := t.rocksDB.(ReadWriter).ClearRange(start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearIterRange implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	err := t.pebble.(ReadWriter).ClearIterRange(iter.(*TeePebbleRocksDBIter).pebble, start, end)
	err2 := t.rocksDB.(ReadWriter).ClearIterRange(iter.(*TeePebbleRocksDBIter).rocksDB, start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Merge implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) Merge(key MVCCKey, value []byte) error {
	err := t.pebble.(ReadWriter).Merge(key, value)
	err2 := t.rocksDB.(ReadWriter).Merge(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Put implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) Put(key MVCCKey, value []byte) error {
	err := t.pebble.(ReadWriter).Put(key, value)
	err2 := t.rocksDB.(ReadWriter).Put(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogData implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) LogData(data []byte) error {
	err := t.pebble.(ReadWriter).LogData(data)
	err2 := t.rocksDB.(ReadWriter).LogData(data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogLogicalOp implements the ReadWriter interface.
func (t *TeePebbleRocksDBReadWriter) LogLogicalOp(
	op MVCCLogicalOpType, details MVCCLogicalOpDetails,
) {
	t.pebble.(ReadWriter).LogLogicalOp(op, details)
	t.rocksDB.(ReadWriter).LogLogicalOp(op, details)
}

// TeePebbleRocksDBReader implements a generic Reader on top of two underlying
// Pebble/RocksDB readers. Used for implementing snapshots.
type TeePebbleRocksDBReader struct {
	ctx     context.Context
	pebble  Reader
	rocksDB Reader
}

var _ Reader = &TeePebbleRocksDBReader{}

// Close implements the Reader interface.
func (t *TeePebbleRocksDBReader) Close() {
	t.pebble.Close()
	t.rocksDB.Close()
}

// Closed implements the Reader interface.
func (t *TeePebbleRocksDBReader) Closed() bool {
	pebbleClosed := t.pebble.Closed()
	rocksDBClosed := t.rocksDB.Closed()
	if pebbleClosed && rocksDBClosed {
		return true
	} else if pebbleClosed || rocksDBClosed {
		log.Fatalf(t.ctx, "only one of pebble or rocksdb closed: %v != %v", pebbleClosed, rocksDBClosed)
	}
	return false
}

// ExportToSst implements the Reader interface.
func (t *TeePebbleRocksDBReader) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	pebbleSst, bulkOpSummary, err := t.pebble.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, io)
	rocksSst, _, err2 := t.rocksDB.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, io)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, bulkOpSummary, err
	}

	if !bytes.Equal(pebbleSst, rocksSst) {
		log.Fatalf(t.ctx, "mismatching SSTs returned by pebble and rocksdb: %v != %v", pebbleSst, rocksSst)
	}
	return pebbleSst, bulkOpSummary, err
}

// Get implements the Reader interface.
func (t *TeePebbleRocksDBReader) Get(key MVCCKey) ([]byte, error) {
	value, err := t.pebble.Get(key)
	value2, err2 := t.rocksDB.Get(key)

	if err != nil && err2 != nil {
		return nil, err
	} else if err != nil || err2 != nil {
		// err != nil XOR err2 != nil
		log.Fatalf(t.ctx, "error mismatch between pebble and rocksdb: %s != %s", err, err2)
	}

	if !bytes.Equal(value, value2) {
		log.Fatalf(t.ctx, "values mismatch between pebble and rocksdb: %v != %v", value, value2)
	}
	return value, nil
}

// GetProto implements the Reader interface.
func (t *TeePebbleRocksDBReader) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		return false, 0, 0, emptyKeyError()
	}
	// Piggyback on the value / error checks in Get.
	val, err := t.Get(key)
	if err != nil || val == nil {
		return false, 0, 0, err
	}

	err = protoutil.Unmarshal(val, msg)
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Iterate implements the Reader interface.
func (t *TeePebbleRocksDBReader) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(t, start, end, f)
}

// NewIterator implements the Reader interface.
func (t *TeePebbleRocksDBReader) NewIterator(opts IterOptions) Iterator {
	return &TeePebbleRocksDBIter{
		ctx:     t.ctx,
		pebble:  t.pebble.NewIterator(opts).(*pebbleIterator),
		rocksDB: t.rocksDB.NewIterator(opts),
	}
}

// TeePebbleRocksDBBatch implements a Batch on top of underlying pebble and
// rocksdb batches.
type TeePebbleRocksDBBatch struct {
	ctx          context.Context
	pebbleBatch  *pebbleBatch
	rocksDBBatch Batch
}

var _ Batch = &TeePebbleRocksDBBatch{}

// Close implements the Batch interface.
func (t TeePebbleRocksDBBatch) Close() {
	t.pebbleBatch.Close()
	t.rocksDBBatch.Close()
}

// Closed implements the Batch interface.
func (t TeePebbleRocksDBBatch) Closed() bool {
	pebbleClosed := t.pebbleBatch.Closed()
	rocksDBClosed := t.rocksDBBatch.Closed()
	if pebbleClosed && rocksDBClosed {
		return true
	} else if pebbleClosed || rocksDBClosed {
		log.Fatalf(t.ctx, "only one of pebble or rocksdb closed: %v != %v", pebbleClosed, rocksDBClosed)
	}
	return false
}

// ExportToSst implements the Batch interface.
func (t TeePebbleRocksDBBatch) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, error) {
	pebbleSst, bulkOpSummary, err := t.pebbleBatch.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, io)
	rocksSst, _, err2 := t.rocksDBBatch.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, io)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, bulkOpSummary, err
	}

	if !bytes.Equal(pebbleSst, rocksSst) {
		log.Fatalf(t.ctx, "mismatching SSTs returned by pebble and rocksdb: %v != %v", pebbleSst, rocksSst)
	}
	return pebbleSst, bulkOpSummary, err
}

// Get implements the Batch interface.
func (t TeePebbleRocksDBBatch) Get(key MVCCKey) ([]byte, error) {
	val, err := t.pebbleBatch.Get(key)
	val2, err2 := t.rocksDBBatch.Get(key)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}

	if !bytes.Equal(val, val2) {
		log.Fatalf(t.ctx, "mismatching values returned in batch.Get: %v != %v", val, val2)
	}
	return val, err
}

// GetProto implements the Batch interface.
func (t TeePebbleRocksDBBatch) GetProto(
	key MVCCKey, msg protoutil.Message,
) (ok bool, keyBytes, valBytes int64, err error) {
	if len(key.Key) == 0 {
		return false, 0, 0, emptyKeyError()
	}
	// Piggyback on the value / error checks in Get.
	val, err := t.Get(key)
	if err != nil || val == nil {
		return false, 0, 0, err
	}

	err = protoutil.Unmarshal(val, msg)
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Iterate implements the Batch interface.
func (t TeePebbleRocksDBBatch) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(t, start, end, f)
}

// NewIterator implements the Batch interface.
func (t TeePebbleRocksDBBatch) NewIterator(opts IterOptions) Iterator {
	pebbleIter := t.pebbleBatch.NewIterator(opts)
	rocksDBIter := t.rocksDBBatch.NewIterator(opts)
	return &TeePebbleRocksDBIter{
		ctx:     t.ctx,
		pebble:  pebbleIter.(*pebbleIterator),
		rocksDB: rocksDBIter,
	}
}

// ApplyBatchRepr implements the Batch interface.
func (t TeePebbleRocksDBBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	err := t.pebbleBatch.ApplyBatchRepr(repr, sync)
	err2 := t.rocksDBBatch.ApplyBatchRepr(repr, sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Clear implements the Batch interface.
func (t TeePebbleRocksDBBatch) Clear(key MVCCKey) error {
	err := t.pebbleBatch.Clear(key)
	err2 := t.rocksDBBatch.Clear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// SingleClear implements the Batch interface.
func (t TeePebbleRocksDBBatch) SingleClear(key MVCCKey) error {
	err := t.pebbleBatch.SingleClear(key)
	err2 := t.rocksDBBatch.SingleClear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearRange implements the Batch interface.
func (t TeePebbleRocksDBBatch) ClearRange(start, end MVCCKey) error {
	err := t.pebbleBatch.ClearRange(start, end)
	err2 := t.rocksDBBatch.ClearRange(start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearIterRange implements the Batch interface.
func (t TeePebbleRocksDBBatch) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	err := t.pebbleBatch.ClearIterRange(iter.(*TeePebbleRocksDBIter).pebble, start, end)
	err2 := t.rocksDBBatch.ClearIterRange(iter.(*TeePebbleRocksDBIter).rocksDB, start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Merge implements the Batch interface.
func (t TeePebbleRocksDBBatch) Merge(key MVCCKey, value []byte) error {
	err := t.pebbleBatch.Merge(key, value)
	err2 := t.rocksDBBatch.Merge(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Put implements the Batch interface.
func (t TeePebbleRocksDBBatch) Put(key MVCCKey, value []byte) error {
	err := t.pebbleBatch.Put(key, value)
	err2 := t.rocksDBBatch.Put(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogData implements the Batch interface.
func (t TeePebbleRocksDBBatch) LogData(data []byte) error {
	err := t.pebbleBatch.LogData(data)
	err2 := t.rocksDBBatch.LogData(data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogLogicalOp implements the Batch interface.
func (t TeePebbleRocksDBBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	t.pebbleBatch.LogLogicalOp(op, details)
	t.rocksDBBatch.LogLogicalOp(op, details)
}

// Commit implements the Batch interface.
func (t TeePebbleRocksDBBatch) Commit(sync bool) error {
	err := t.pebbleBatch.Commit(sync)
	err2 := t.rocksDBBatch.Commit(sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Distinct implements the Batch interface.
func (t TeePebbleRocksDBBatch) Distinct() ReadWriter {
	pebbleDistinct := t.pebbleBatch.Distinct()
	rocksDBDistinct := t.rocksDBBatch.Distinct()
	return &TeePebbleRocksDBBatch{
		ctx:          t.ctx,
		pebbleBatch:  pebbleDistinct.(*pebbleBatch),
		rocksDBBatch: rocksDBDistinct.(Batch),
	}
}

// Empty implements the Batch interface.
func (t TeePebbleRocksDBBatch) Empty() bool {
	empty := t.pebbleBatch.Empty()
	empty2 := t.rocksDBBatch.Empty()
	if empty != empty2 {
		log.Fatalf(t.ctx, "mismatching responses for batch.Empty(): %v != %v", empty, empty2)
	}
	return empty
}

// Len implements the Batch interface.
func (t TeePebbleRocksDBBatch) Len() int {
	len1 := t.pebbleBatch.Len()
	len2 := t.rocksDBBatch.Len()

	if len1 != len2 {
		log.Fatalf(t.ctx, "mismatching lengths for batch: %v != %v", len1, len2)
	}
	return len1
}

// Repr implements the Batch interface.
func (t TeePebbleRocksDBBatch) Repr() []byte {
	repr := t.pebbleBatch.Repr()
	repr2 := t.rocksDBBatch.Repr()
	if !bytes.Equal(repr, repr2) {
		log.Fatalf(t.ctx, "mismatching byte representations between pebble and rocksdb: %v != %v", repr, repr2)
	}
	return repr
}

// TeePebbleRocksDBIter is an Iterator that iterates on underlying pebble and
// rocksDB iterators in lockstep. If there's a mismatch in their states ever,
// whether validity or keys/values being pointed to, a log.Fatal is triggered.
type TeePebbleRocksDBIter struct {
	ctx     context.Context
	pebble  *pebbleIterator
	rocksDB Iterator
}

var _ MVCCIterator = &TeePebbleRocksDBIter{}

// Close implements the Iterator interface.
func (t TeePebbleRocksDBIter) Close() {
	t.pebble.Close()
	t.rocksDB.Close()
}

// check checks if the two underlying iterators have matching validity states
// and keys.
func (t TeePebbleRocksDBIter) check() {
	valid, err := t.pebble.Valid()
	valid2, err2 := t.rocksDB.Valid()
	_ = fatalOnErrorMismatch(t.ctx, err, err2)

	if !(valid && valid2) && (valid || valid2) {
		// valid XOR valid2
		log.Fatalf(t.ctx, "one of pebble or rocksdb invalid but not both: %v != %v", valid, valid2)
		return
	}

	if valid {
		pebbleKey := t.pebble.UnsafeKey()
		rocksKey := t.rocksDB.UnsafeKey()
		if !rocksKey.Equal(pebbleKey) {
			log.Fatalf(t.ctx, "pebble and rocks iterators pointing to different keys: %v != %v", pebbleKey, rocksKey)
		}
	}
}

// SeekGE implements the Iterator interface.
func (t TeePebbleRocksDBIter) SeekGE(key MVCCKey) {
	t.pebble.SeekGE(key)
	t.rocksDB.SeekGE(key)
	t.check()
}

// Valid implements the Iterator interface.
func (t TeePebbleRocksDBIter) Valid() (bool, error) {
	return t.pebble.Valid()
}

// Next implements the Iterator interface.
func (t TeePebbleRocksDBIter) Next() {
	t.pebble.Next()
	t.rocksDB.Next()
	t.check()
}

// NextKey implements the Iterator interface.
func (t TeePebbleRocksDBIter) NextKey() {
	t.pebble.NextKey()
	t.rocksDB.NextKey()
	t.check()
}

// UnsafeKey implements the Iterator interface.
func (t TeePebbleRocksDBIter) UnsafeKey() MVCCKey {
	return t.pebble.UnsafeKey()
}

// UnsafeValue implements the Iterator interface.
func (t TeePebbleRocksDBIter) UnsafeValue() []byte {
	return t.pebble.UnsafeValue()
}

// SeekLT implements the Iterator interface.
func (t TeePebbleRocksDBIter) SeekLT(key MVCCKey) {
	t.pebble.SeekLT(key)
	t.rocksDB.SeekLT(key)
	t.check()
}

// Prev implements the Iterator interface.
func (t TeePebbleRocksDBIter) Prev() {
	t.pebble.Prev()
	t.rocksDB.Prev()
	t.check()
}

// Key implements the Iterator interface.
func (t TeePebbleRocksDBIter) Key() MVCCKey {
	return t.pebble.Key()
}

func (t TeePebbleRocksDBIter) unsafeRawKey() []byte {
	return t.pebble.unsafeRawKey()
}

// Value implements the Iterator interface.
func (t TeePebbleRocksDBIter) Value() []byte {
	return t.pebble.UnsafeValue()
}

// ValueProto implements the Iterator interface.
func (t TeePebbleRocksDBIter) ValueProto(msg protoutil.Message) error {
	return t.pebble.ValueProto(msg)
}

// ComputeStats implements the Iterator interface.
func (t TeePebbleRocksDBIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	pebbleStats, err := t.pebble.ComputeStats(start, end, nowNanos)
	rocksStats, err2 := t.rocksDB.ComputeStats(start, end, nowNanos)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if !pebbleStats.Equal(rocksStats) {
		log.Fatalf(t.ctx, "mismatching stats between pebble and rocksdb: %v != %v", pebbleStats, rocksStats)
	}
	return pebbleStats, nil
}

// FindSplitKey implements the Iterator interface.
func (t TeePebbleRocksDBIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	pebbleSplitKey, err := t.pebble.FindSplitKey(start, end, minSplitKey, targetSize)
	rocksSplitKey, err2 := t.rocksDB.FindSplitKey(start, end, minSplitKey, targetSize)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return MVCCKey{}, err
	}
	if !pebbleSplitKey.Equal(rocksSplitKey) {
		log.Fatalf(t.ctx, "mismatching split keys returned from pebble and rocksdb: %v != %v", pebbleSplitKey, rocksSplitKey)
	}
	return pebbleSplitKey, err
}

// CheckForKeyCollisions implements the Iterator interface.
func (t TeePebbleRocksDBIter) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	pebbleStats, err := t.pebble.CheckForKeyCollisions(sstData, start, end)
	rocksStats, err2 := t.rocksDB.CheckForKeyCollisions(sstData, start, end)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if !pebbleStats.Equal(rocksStats) {
		log.Fatalf(t.ctx, "mismatching stats between pebble and rocksdb: %v != %v", pebbleStats, rocksStats)
	}
	return pebbleStats, nil
}

// SetUpperBound implements the Iterator interface.
func (t TeePebbleRocksDBIter) SetUpperBound(key roachpb.Key) {
	t.pebble.SetUpperBound(key)
	t.rocksDB.SetUpperBound(key)
}

// Stats implements the Iterator interface.
func (t TeePebbleRocksDBIter) Stats() IteratorStats {
	return t.pebble.Stats()
}

// MVCCOpsSpecialized implements the MVCCIterator interface.
func (t TeePebbleRocksDBIter) MVCCOpsSpecialized() bool {
	return true
}

// MVCCGet implements the MVCCIterator interface.
func (t TeePebbleRocksDBIter) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	pebbleValue, pebbleIntent, err := mvccGet(t.ctx, t.pebble, key, timestamp, opts)
	rocksValue, rocksIntent, err2 := t.rocksDB.(MVCCIterator).MVCCGet(key, timestamp, opts)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, nil, err
	}

	if !pebbleValue.Equal(rocksValue) {
		log.Fatalf(t.ctx, "mismatching values returned by pebble and rocksdb in MVCCGet: %v != %v", pebbleValue, rocksValue)
	}
	if !pebbleIntent.Equal(rocksIntent) {
		log.Fatalf(t.ctx, "mismatching intents returned by pebble and rocksdb in MVCCGet: %v != %v", pebbleIntent, rocksIntent)
	}
	return pebbleValue, pebbleIntent, err
}

// MVCCScan implements the MVCCIterator interface.
func (t TeePebbleRocksDBIter) MVCCScan(
	start, end roachpb.Key, max int64, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (kvData [][]byte, numKVs int64, resumeSpan *roachpb.Span, intents []roachpb.Intent, err error) {
	pebbleKvData, pebbleNumKVs, pebbleResumeSpan, pebbleIntents, err := mvccScanToBytes(t.ctx, t.pebble, start, end, max, timestamp, opts)
	rocksKvData, rocksNumKVs, rocksResumeSpan, rocksIntents, err2 := t.rocksDB.(MVCCIterator).MVCCScan(start, end, max, timestamp, opts)

	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, 0, nil, nil, err
	}

	if pebbleNumKVs != rocksNumKVs {
		log.Fatalf(t.ctx, "mismatching number of KVs returned from pebble and rocksdb MVCCScan: %d != %d", pebbleNumKVs, rocksNumKVs)
	}

	// rocksKvData is always going to have one slice inside it, even if it's
	// empty.
	if len(rocksKvData) != 1 {
		panic("unsupported multiple-slice result from rocksDBIterator.MVCCScan")
	}
	if len(pebbleKvData) == 0 && len(rocksKvData[0]) == 0 {
		return pebbleKvData, 0, nil, nil, nil
	}

	i := 0
	for _, pebbleSlice := range pebbleKvData {
		sliceEnd := i + len(pebbleSlice)
		if sliceEnd > len(rocksKvData[0]) {
			log.Fatalf(t.ctx, "pebble returned more data than rocksdb: %d bytes so far > %d bytes", sliceEnd, len(rocksKvData[0]))
		}
		if !bytes.Equal(pebbleSlice, rocksKvData[0][i:sliceEnd]) {
			log.Fatalf(t.ctx, "mismatching kv data returned by pebble and rocksdb: %v != %v", pebbleSlice, rocksKvData[0][i:sliceEnd])
		}
		i = sliceEnd
	}

	if !pebbleResumeSpan.Equal(rocksResumeSpan) {
		log.Fatalf(t.ctx, "mismatching resume spans returned by pebble and rocksdb: %v != %v", pebbleResumeSpan, rocksResumeSpan)
	}
	if len(pebbleIntents) != len(rocksIntents) {
		log.Fatalf(t.ctx, "mismatching number of intents returned by pebble and rocksdb: %v != %v", len(pebbleIntents), len(rocksIntents))
	}
	for i := range pebbleIntents {
		if !pebbleIntents[i].Equal(rocksIntents[i]) {
			log.Fatalf(t.ctx, "mismatching intents returned by pebble and rocksdb: %v != %v", pebbleIntents[i], rocksIntents[i])
		}
	}
	return pebbleKvData, pebbleNumKVs, pebbleResumeSpan, pebbleIntents, err
}
