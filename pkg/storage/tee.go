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
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
)

// TeeEngine sends all write operations to both the underlying
// engine instances, and for all read operations, it verifies that
// any keys and values returned match. Implements the Engine interface.
//
// This engine is only meant to be used in testing. No performance or
// stability guarantees are made about this engine in production.
type TeeEngine struct {
	ctx  context.Context
	eng1 Engine
	eng2 Engine
}

var _ Engine = &TeeEngine{}

// NewTee creates a new instance of the TeeEngine engine. Conventionally,
// the first engine is the engine that's more likely to be correct, and the
// second engine is the one that's being tested and compared with the first.
// All error messages have results from the first engine ordered before those
// from the second. For example, in:
//
// "one of engine iters invalid but not both: false != true"
//
// The iterator from engine 1 is returning false while the iterator from
// engine2 is returning true.
func NewTee(ctx context.Context, eng1 Engine, eng2 Engine) *TeeEngine {
	return &TeeEngine{
		ctx:  ctx,
		eng1: eng1,
		eng2: eng2,
	}
}

func fatalOnErrorMismatch(ctx context.Context, err error, err2 error) error {
	if err != nil && err2 != nil {
		return err
	} else if err != nil || err2 != nil {
		log.Fatalf(ctx, "error mismatch between engines: %v != %v", err, err2)
	}
	return nil
}

// Close implements the Engine interface.
func (t *TeeEngine) Close() {
	t.eng1.Close()
	t.eng2.Close()
}

// Closed implements the Engine interface.
func (t *TeeEngine) Closed() bool {
	eng1Closed := t.eng1.Closed()
	eng2Closed := t.eng2.Closed()
	if eng1Closed && eng2Closed {
		return true
	} else if eng1Closed || eng2Closed {
		log.Fatalf(t.ctx, "only one of engines closed: %v != %v", eng1Closed, eng2Closed)
	}
	return false
}

// ExportToSst implements the Engine interface.
func (t *TeeEngine) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	eng1Sst, bulkOpSummary, resume1, err := t.eng1.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
	rocksSst, _, resume2, err2 := t.eng2.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, bulkOpSummary, nil, err
	}

	if !bytes.Equal(eng1Sst, rocksSst) {
		log.Fatalf(t.ctx, "mismatching SSTs returned by engines: %v != %v", eng1Sst, rocksSst)
	}
	if !resume1.Equal(resume2) {
		log.Fatalf(t.ctx, "mismatching resume key returned by engines: %v != %v", resume1, resume2)
	}
	return eng1Sst, bulkOpSummary, resume1, err
}

// Get implements the Engine interface.
func (t *TeeEngine) Get(key MVCCKey) ([]byte, error) {
	value, err := t.eng1.Get(key)
	value2, err2 := t.eng2.Get(key)

	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}

	if !bytes.Equal(value, value2) {
		log.Fatalf(t.ctx, "values mismatch between engines: %v != %v", value, value2)
	}
	return value, nil
}

// GetProto implements the Engine interface.
func (t *TeeEngine) GetProto(
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

	if msg != nil {
		err = protoutil.Unmarshal(val, msg)
	}
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Iterate implements the Engine interface.
func (t *TeeEngine) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(t, start, end, f)
}

// NewIterator implements the Engine interface.
func (t *TeeEngine) NewIterator(opts IterOptions) Iterator {
	return &TeeEngineIter{
		ctx:   t.ctx,
		iter1: t.eng1.NewIterator(opts),
		iter2: t.eng2.NewIterator(opts),
	}
}

// ApplyBatchRepr implements the Engine interface.
func (t *TeeEngine) ApplyBatchRepr(repr []byte, sync bool) error {
	err := t.eng1.ApplyBatchRepr(repr, sync)
	err2 := t.eng2.ApplyBatchRepr(repr, sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Clear implements the Engine interface.
func (t *TeeEngine) Clear(key MVCCKey) error {
	err := t.eng1.Clear(key)
	err2 := t.eng2.Clear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// SingleClear implements the Engine interface.
func (t *TeeEngine) SingleClear(key MVCCKey) error {
	err := t.eng1.SingleClear(key)
	err2 := t.eng2.SingleClear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearRange implements the Engine interface.
func (t *TeeEngine) ClearRange(start, end MVCCKey) error {
	err := t.eng1.ClearRange(start, end)
	err2 := t.eng2.ClearRange(start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearIterRange implements the Engine interface.
func (t *TeeEngine) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	err := t.eng1.ClearIterRange(iter.(*TeeEngineIter).iter1, start, end)
	err2 := t.eng2.ClearIterRange(iter.(*TeeEngineIter).iter2, start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Merge implements the Engine interface.
func (t *TeeEngine) Merge(key MVCCKey, value []byte) error {
	err := t.eng1.Merge(key, value)
	err2 := t.eng2.Merge(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Put implements the Engine interface.
func (t *TeeEngine) Put(key MVCCKey, value []byte) error {
	err := t.eng1.Put(key, value)
	err2 := t.eng2.Put(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogData implements the Engine interface.
func (t *TeeEngine) LogData(data []byte) error {
	err := t.eng1.LogData(data)
	err2 := t.eng2.LogData(data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogLogicalOp implements the Engine interface.
func (t *TeeEngine) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	t.eng1.LogLogicalOp(op, details)
	t.eng2.LogLogicalOp(op, details)
}

// Attrs implements the Engine interface.
func (t *TeeEngine) Attrs() roachpb.Attributes {
	return t.eng1.Attrs()
}

// Capacity implements the Engine interface.
func (t *TeeEngine) Capacity() (roachpb.StoreCapacity, error) {
	return t.eng1.Capacity()
}

// Compact implements the Engine interface.
func (t *TeeEngine) Compact() error {
	err := t.eng1.Compact()
	err2 := t.eng2.Compact()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Flush implements the Engine interface.
func (t *TeeEngine) Flush() error {
	err := t.eng1.Flush()
	err2 := t.eng2.Flush()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// GetSSTables implements the Engine interface.
func (t *TeeEngine) GetSSTables() SSTableInfos {
	return t.eng1.GetSSTables()
}

// GetCompactionStats implements the Engine interface.
func (t *TeeEngine) GetCompactionStats() string {
	return t.eng1.GetCompactionStats()
}

// GetStats implements the Engine interface.
func (t *TeeEngine) GetStats() (*Stats, error) {
	return t.eng1.GetStats()
}

// GetEncryptionRegistries implements the Engine interface.
func (t *TeeEngine) GetEncryptionRegistries() (*EncryptionRegistries, error) {
	return t.eng1.GetEncryptionRegistries()
}

// GetEnvStats implements the Engine interface.
func (t *TeeEngine) GetEnvStats() (*EnvStats, error) {
	return t.eng2.GetEnvStats()
}

// GetAuxiliaryDir implements the Engine interface.
func (t *TeeEngine) GetAuxiliaryDir() string {
	// Treat the eng1 path as the main aux dir, so that checkpoints are made in
	// subdirectories within it.
	return t.eng1.GetAuxiliaryDir()
}

// NewBatch implements the Engine interface.
func (t *TeeEngine) NewBatch() Batch {
	batch1 := t.eng1.NewBatch()
	batch2 := t.eng2.NewBatch()

	return &TeeEngineBatch{
		ctx:    t.ctx,
		batch1: batch1,
		batch2: batch2,
	}
}

// NewReadOnly implements the Engine interface.
func (t TeeEngine) NewReadOnly() ReadWriter {
	reader1 := t.eng1.NewReadOnly()
	reader2 := t.eng2.NewReadOnly()

	return &TeeEngineReadWriter{TeeEngineReader{
		ctx:     t.ctx,
		reader1: reader1,
		reader2: reader2,
	}}
}

// NewWriteOnlyBatch implements the Engine interface.
func (t *TeeEngine) NewWriteOnlyBatch() Batch {
	batch1 := t.eng1.NewWriteOnlyBatch()
	batch2 := t.eng2.NewWriteOnlyBatch()
	return &TeeEngineBatch{
		ctx:    t.ctx,
		batch1: batch1,
		batch2: batch2,
	}
}

// NewSnapshot implements the Engine interface.
func (t *TeeEngine) NewSnapshot() Reader {
	snap1 := t.eng1.NewSnapshot()
	snap2 := t.eng2.NewSnapshot()

	return &TeeEngineReader{
		ctx:     t.ctx,
		reader1: snap1,
		reader2: snap2,
	}
}

// Type implements the Engine interface.
func (t *TeeEngine) Type() enginepb.EngineType {
	return enginepb.EngineTypeTeePebbleRocksDB
}

// Helper to remap a path in the first engine's aux dir, into the same path in
// the second engine's aux dir. Returns ok = true only if the provided path
// is in the first engine's aux dir.
func (t *TeeEngine) remapPath(path1 string) (path2 string, ok bool) {
	auxDir1 := t.eng1.GetAuxiliaryDir()
	if !strings.HasPrefix(path1, auxDir1) {
		// This dir isn't in the first engine's aux dir.
		return path1, false
	}
	ok = true
	path2 = filepath.Join(t.eng2.GetAuxiliaryDir(), strings.TrimPrefix(path1, auxDir1))
	return
}

// IngestExternalFiles implements the Engine interface.
func (t *TeeEngine) IngestExternalFiles(ctx context.Context, paths []string) error {
	var err, err2 error

	// The paths should be in eng1's aux directory. Map them to eng2's aux
	// directory.
	paths2 := make([]string, len(paths))
	for i, path := range paths {
		var ok bool
		paths2[i], ok = t.remapPath(path)
		if !ok {
			paths2[i] = filepath.Join(t.eng2.GetAuxiliaryDir(), "temp-ingest", filepath.Base(path))
			data, err := t.eng1.ReadFile(path)
			if err != nil {
				return err
			}
			f, err := t.eng2.Create(paths2[i])
			if err != nil {
				return err
			}
			_, _ = f.Write(data)
			_ = f.Sync()
			_ = f.Close()
		}
	}

	err = t.eng1.IngestExternalFiles(ctx, paths)
	err2 = t.eng2.IngestExternalFiles(ctx, paths2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// PreIngestDelay implements the Engine interface.
func (t *TeeEngine) PreIngestDelay(ctx context.Context) {
	t.eng1.PreIngestDelay(ctx)
}

// ApproximateDiskBytes implements the Engine interface.
func (t *TeeEngine) ApproximateDiskBytes(from, to roachpb.Key) (uint64, error) {
	bytes, err := t.eng1.ApproximateDiskBytes(from, to)
	bytes2, err2 := t.eng2.ApproximateDiskBytes(from, to)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return 0, err
	}

	return bytes + bytes2, nil
}

// CompactRange implements the Engine interface.
func (t *TeeEngine) CompactRange(start, end roachpb.Key, forceBottommost bool) error {
	err := t.eng1.CompactRange(start, end, forceBottommost)
	err2 := t.eng2.CompactRange(start, end, forceBottommost)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// InMem implements the Engine interface.
func (t *TeeEngine) InMem() bool {
	return t.eng1.InMem()
}

// Create implements the FS interface.
func (t *TeeEngine) Create(filename string) (fs.File, error) {
	_ = os.MkdirAll(filepath.Dir(filename), 0755)
	file1, err := t.eng1.Create(filename)
	filename2, ok := t.remapPath(filename)
	if !ok {
		return file1, err
	}
	_ = os.MkdirAll(filepath.Dir(filename2), 0755)
	file2, err2 := t.eng2.Create(filename2)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}
	return &TeeEngineFile{
		ctx:   t.ctx,
		file1: file1,
		file2: file2,
	}, nil
}

// CreateWithSync implements the FS interface.
func (t *TeeEngine) CreateWithSync(filename string, bytesPerSync int) (fs.File, error) {
	_ = os.MkdirAll(filepath.Dir(filename), 0755)
	file1, err := t.eng1.CreateWithSync(filename, bytesPerSync)
	filename2, ok := t.remapPath(filename)
	if !ok {
		return file1, err
	}
	_ = os.MkdirAll(filepath.Dir(filename2), 0755)
	file2, err2 := t.eng2.CreateWithSync(filename2, bytesPerSync)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}
	return &TeeEngineFile{
		ctx:   t.ctx,
		file1: file1,
		file2: file2,
	}, nil
}

// Open implements the FS interface.
func (t *TeeEngine) Open(filename string) (fs.File, error) {
	file1, err := t.eng1.Open(filename)
	filename2, ok := t.remapPath(filename)
	if !ok {
		return file1, err
	}
	file2, err2 := t.eng2.Open(filename2)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}
	return &TeeEngineFile{
		ctx:   t.ctx,
		file1: file1,
		file2: file2,
	}, nil
}

// OpenDir implements the FS interface.
func (t *TeeEngine) OpenDir(name string) (fs.File, error) {
	file1, err := t.eng1.OpenDir(name)
	name2, ok := t.remapPath(name)
	if !ok {
		return file1, err
	}
	file2, err2 := t.eng2.OpenDir(name2)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}
	return &TeeEngineFile{
		ctx:   t.ctx,
		file1: file1,
		file2: file2,
	}, nil
}

// ReadFile implements the Engine interface.
func (t *TeeEngine) ReadFile(filename string) ([]byte, error) {
	return t.eng1.ReadFile(filename)
}

// WriteFile implements the Engine interface.
func (t *TeeEngine) WriteFile(filename string, data []byte) error {
	err := t.eng1.WriteFile(filename, data)
	filename2, ok := t.remapPath(filename)
	if !ok {
		return err
	}
	_ = os.MkdirAll(filepath.Dir(filename2), 0755)
	err2 := t.eng2.WriteFile(filename2, data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Remove implements the FS interface.
func (t *TeeEngine) Remove(filename string) error {
	err := t.eng1.Remove(filename)
	filename2, ok := t.remapPath(filename)
	if !ok {
		return err
	}
	err2 := t.eng2.Remove(filename2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// RemoveAll implements the Engine interface.
func (t *TeeEngine) RemoveAll(dir string) error {
	err := t.eng1.RemoveAll(dir)
	dir2, ok := t.remapPath(dir)
	if !ok {
		return err
	}
	err2 := t.eng2.RemoveAll(dir2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Link implements the FS interface.
func (t *TeeEngine) Link(oldname, newname string) error {
	err := t.eng1.Link(oldname, newname)
	oldname2, ok := t.remapPath(oldname)
	if !ok {
		return err
	}
	newname2, _ := t.remapPath(newname)
	err2 := t.eng2.Link(oldname2, newname2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Rename implements the FS interface.
func (t *TeeEngine) Rename(oldname, newname string) error {
	err := t.eng1.Rename(oldname, newname)
	oldname2, ok := t.remapPath(oldname)
	if !ok {
		return err
	}
	newname2, _ := t.remapPath(newname)
	err2 := t.eng2.Rename(oldname2, newname2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// MkdirAll implements the FS interface.
func (t *TeeEngine) MkdirAll(name string) error {
	err := t.eng1.MkdirAll(name)
	name2, ok := t.remapPath(name)
	if !ok {
		return err
	}
	err2 := t.eng2.MkdirAll(name2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// RemoveDir implements the FS interface.
func (t *TeeEngine) RemoveDir(name string) error {
	err := t.eng1.RemoveDir(name)
	name2, ok := t.remapPath(name)
	if !ok {
		return err
	}
	err2 := t.eng2.RemoveDir(name2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// List implements the FS interface.
func (t *TeeEngine) List(name string) ([]string, error) {
	list1, err := t.eng1.List(name)
	name2, ok := t.remapPath(name)
	if !ok {
		return list1, err
	}
	_, err2 := t.eng2.List(name2)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}
	// TODO(sbhola): compare the slices.
	return list1, nil
}

// Stat implements the FS interface.
func (t *TeeEngine) Stat(name string) (os.FileInfo, error) {
	info1, err := t.eng1.Stat(name)
	name2, ok := t.remapPath(name)
	if !ok {
		return info1, err
	}
	info2, err2 := t.eng2.Stat(name2)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}
	if info1.Size() != info2.Size() {
		log.Fatalf(t.ctx, "mismatching file sizes from stat: %d != %d", info1.Size(), info2.Size())
	}
	return info1, err
}

// CreateCheckpoint implements the Engine interface.
func (t *TeeEngine) CreateCheckpoint(dir string) error {
	path1 := filepath.Join(dir, "eng1")
	path2 := filepath.Join(dir, "eng2")
	err := t.eng1.CreateCheckpoint(path1)
	err2 := t.eng2.CreateCheckpoint(path2)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

func (t TeeEngine) String() string {
	return t.eng1.(fmt.Stringer).String()
}

// TeeEngineFile is a File that forwards to both underlying eng1
// and eng2 files.
type TeeEngineFile struct {
	ctx   context.Context
	file1 fs.File
	file2 fs.File
}

var _ fs.File = &TeeEngineFile{}

// Close implements the File interface.
func (t *TeeEngineFile) Close() error {
	err := t.file1.Close()
	err2 := t.file2.Close()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Sync implements the File interface.
func (t *TeeEngineFile) Sync() error {
	err := t.file1.Sync()
	err2 := t.file2.Sync()
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Write implements the File interface.
func (t *TeeEngineFile) Write(p []byte) (int, error) {
	n, err := t.file1.Write(p)
	n2, err2 := t.file2.Write(p)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return 0, err
	}
	if n != n2 {
		log.Fatalf(t.ctx, "mismatching number of bytes written by engines: %d != %d", n, n2)
	}
	return n, nil
}

// Read implements the File interface.
func (t *TeeEngineFile) Read(p []byte) (n int, err error) {
	p2 := make([]byte, len(p))
	n, err = t.file1.Read(p)
	n2, err2 := t.file2.Read(p2)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return 0, err
	}
	if n != n2 {
		log.Fatalf(t.ctx, "mismatching number of bytes read by engines: %d != %d", n, n2)
	}
	if !bytes.Equal(p[:n], p2[:n]) {
		log.Fatalf(t.ctx, "different bytes read by engines: %s != %s", hex.EncodeToString(p[:n]), hex.EncodeToString(p2[:n]))
	}
	return n, nil
}

// ReadAt implements the File interface.
func (t *TeeEngineFile) ReadAt(p []byte, off int64) (n int, err error) {
	p2 := make([]byte, len(p))
	n, err = t.file1.ReadAt(p, off)
	n2, err2 := t.file2.ReadAt(p2, off)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return 0, err
	}
	if n != n2 {
		log.Fatalf(t.ctx, "mismatching number of bytes read by engines: %d != %d", n, n2)
	}
	if !bytes.Equal(p[:n], p2[:n]) {
		log.Fatalf(t.ctx, "different bytes read by engines: %s != %s", hex.EncodeToString(p[:n]), hex.EncodeToString(p2[:n]))
	}
	return n, nil
}

// TeeEngineReadWriter implements a generic ReadWriter. Used for
// implementing ReadOnly.
type TeeEngineReadWriter struct {
	TeeEngineReader
}

var _ ReadWriter = &TeeEngineReadWriter{}

// ApplyBatchRepr implements the ReadWriter interface.
func (t *TeeEngineReadWriter) ApplyBatchRepr(repr []byte, sync bool) error {
	err := t.reader1.(ReadWriter).ApplyBatchRepr(repr, sync)
	err2 := t.reader2.(ReadWriter).ApplyBatchRepr(repr, sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Clear implements the ReadWriter interface.
func (t *TeeEngineReadWriter) Clear(key MVCCKey) error {
	err := t.reader1.(ReadWriter).Clear(key)
	err2 := t.reader2.(ReadWriter).Clear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// SingleClear implements the ReadWriter interface.
func (t *TeeEngineReadWriter) SingleClear(key MVCCKey) error {
	err := t.reader1.(ReadWriter).SingleClear(key)
	err2 := t.reader2.(ReadWriter).SingleClear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearRange implements the ReadWriter interface.
func (t *TeeEngineReadWriter) ClearRange(start, end MVCCKey) error {
	err := t.reader1.(ReadWriter).ClearRange(start, end)
	err2 := t.reader2.(ReadWriter).ClearRange(start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearIterRange implements the ReadWriter interface.
func (t *TeeEngineReadWriter) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	err := t.reader1.(ReadWriter).ClearIterRange(iter.(*TeeEngineIter).iter1, start, end)
	err2 := t.reader2.(ReadWriter).ClearIterRange(iter.(*TeeEngineIter).iter2, start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Merge implements the ReadWriter interface.
func (t *TeeEngineReadWriter) Merge(key MVCCKey, value []byte) error {
	err := t.reader1.(ReadWriter).Merge(key, value)
	err2 := t.reader2.(ReadWriter).Merge(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Put implements the ReadWriter interface.
func (t *TeeEngineReadWriter) Put(key MVCCKey, value []byte) error {
	err := t.reader1.(ReadWriter).Put(key, value)
	err2 := t.reader2.(ReadWriter).Put(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogData implements the ReadWriter interface.
func (t *TeeEngineReadWriter) LogData(data []byte) error {
	err := t.reader1.(ReadWriter).LogData(data)
	err2 := t.reader2.(ReadWriter).LogData(data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogLogicalOp implements the ReadWriter interface.
func (t *TeeEngineReadWriter) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	t.reader1.(ReadWriter).LogLogicalOp(op, details)
	t.reader2.(ReadWriter).LogLogicalOp(op, details)
}

// TeeEngineReader implements a generic Reader on top of two underlying
// engine1/2 readers. Used for implementing snapshots.
type TeeEngineReader struct {
	ctx     context.Context
	reader1 Reader
	reader2 Reader
}

var _ Reader = &TeeEngineReader{}

// Close implements the Reader interface.
func (t *TeeEngineReader) Close() {
	t.reader1.Close()
	t.reader2.Close()
}

// Closed implements the Reader interface.
func (t *TeeEngineReader) Closed() bool {
	closed1 := t.reader1.Closed()
	closed2 := t.reader2.Closed()
	if closed1 && closed2 {
		return true
	} else if closed1 || closed2 {
		log.Fatalf(t.ctx, "only one of engines closed: %v != %v", closed1, closed2)
	}
	return false
}

// ExportToSst implements the Reader interface.
func (t *TeeEngineReader) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	sst1, bulkOpSummary, resume1, err := t.reader1.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
	sst2, _, resume2, err2 := t.reader2.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, bulkOpSummary, nil, err
	}

	if !bytes.Equal(sst1, sst2) {
		log.Fatalf(t.ctx, "mismatching SSTs returned by engines: %v != %v", sst1, sst2)
	}
	if !resume1.Equal(resume2) {
		log.Fatalf(t.ctx, "mismatching resume key returned by engines: %v != %v", resume1, resume2)
	}
	return sst1, bulkOpSummary, resume1, err
}

// Get implements the Reader interface.
func (t *TeeEngineReader) Get(key MVCCKey) ([]byte, error) {
	value, err := t.reader1.Get(key)
	value2, err2 := t.reader2.Get(key)

	if err != nil && err2 != nil {
		return nil, err
	} else if err != nil || err2 != nil {
		// err != nil XOR err2 != nil
		log.Fatalf(t.ctx, "error mismatch between engines: %s != %s", err, err2)
	}

	if !bytes.Equal(value, value2) {
		log.Fatalf(t.ctx, "values mismatch between engines: %v != %v", value, value2)
	}
	return value, nil
}

// GetProto implements the Reader interface.
func (t *TeeEngineReader) GetProto(
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

	if msg != nil {
		err = protoutil.Unmarshal(val, msg)
	}
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Iterate implements the Reader interface.
func (t *TeeEngineReader) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(t, start, end, f)
}

// NewIterator implements the Reader interface.
func (t *TeeEngineReader) NewIterator(opts IterOptions) Iterator {
	return &TeeEngineIter{
		ctx:   t.ctx,
		iter1: t.reader1.NewIterator(opts),
		iter2: t.reader2.NewIterator(opts),
	}
}

// TeeEngineBatch implements a Batch on top of underlying eng1 and
// eng2 batches.
type TeeEngineBatch struct {
	ctx    context.Context
	batch1 Batch
	batch2 Batch
}

var _ Batch = &TeeEngineBatch{}

// Close implements the Batch interface.
func (t *TeeEngineBatch) Close() {
	t.batch1.Close()
	t.batch2.Close()
}

// Closed implements the Batch interface.
func (t *TeeEngineBatch) Closed() bool {
	closed1 := t.batch1.Closed()
	closed2 := t.batch2.Closed()
	if closed1 && closed2 {
		return true
	} else if closed1 || closed2 {
		log.Fatalf(t.ctx, "only one of engines closed: %v != %v", closed1, closed2)
	}
	return false
}

// ExportToSst implements the Batch interface.
func (t *TeeEngineBatch) ExportToSst(
	startKey, endKey roachpb.Key,
	startTS, endTS hlc.Timestamp,
	exportAllRevisions bool,
	targetSize, maxSize uint64,
	io IterOptions,
) ([]byte, roachpb.BulkOpSummary, roachpb.Key, error) {
	sst1, bulkOpSummary, resume1, err := t.batch1.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
	sst2, _, resume2, err2 := t.batch2.ExportToSst(startKey, endKey, startTS, endTS, exportAllRevisions, targetSize, maxSize, io)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, bulkOpSummary, nil, err
	}

	if !bytes.Equal(sst1, sst2) {
		log.Fatalf(t.ctx, "mismatching SSTs returned by engines: %v != %v", sst1, sst2)
	}
	if !resume1.Equal(resume2) {
		log.Fatalf(t.ctx, "mismatching resume key returned by engines: %v != %v", resume1, resume2)
	}
	return sst1, bulkOpSummary, resume1, err
}

// Get implements the Batch interface.
func (t *TeeEngineBatch) Get(key MVCCKey) ([]byte, error) {
	val, err := t.batch1.Get(key)
	val2, err2 := t.batch2.Get(key)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, err
	}

	if !bytes.Equal(val, val2) {
		log.Fatalf(t.ctx, "mismatching values returned in batch.Get: %v != %v", val, val2)
	}
	return val, err
}

// GetProto implements the Batch interface.
func (t *TeeEngineBatch) GetProto(
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

	if msg != nil {
		err = protoutil.Unmarshal(val, msg)
	}
	keyBytes = int64(key.Len())
	valBytes = int64(len(val))
	return true, keyBytes, valBytes, err
}

// Iterate implements the Batch interface.
func (t *TeeEngineBatch) Iterate(
	start, end roachpb.Key, f func(MVCCKeyValue) (stop bool, err error),
) error {
	return iterateOnReader(t, start, end, f)
}

// NewIterator implements the Batch interface.
func (t *TeeEngineBatch) NewIterator(opts IterOptions) Iterator {
	iter1 := t.batch1.NewIterator(opts)
	iter2 := t.batch2.NewIterator(opts)
	return &TeeEngineIter{
		ctx:   t.ctx,
		iter1: iter1,
		iter2: iter2,
	}
}

// ApplyBatchRepr implements the Batch interface.
func (t *TeeEngineBatch) ApplyBatchRepr(repr []byte, sync bool) error {
	err := t.batch1.ApplyBatchRepr(repr, sync)
	err2 := t.batch2.ApplyBatchRepr(repr, sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Clear implements the Batch interface.
func (t *TeeEngineBatch) Clear(key MVCCKey) error {
	err := t.batch1.Clear(key)
	err2 := t.batch2.Clear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// SingleClear implements the Batch interface.
func (t *TeeEngineBatch) SingleClear(key MVCCKey) error {
	err := t.batch1.SingleClear(key)
	err2 := t.batch2.SingleClear(key)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearRange implements the Batch interface.
func (t *TeeEngineBatch) ClearRange(start, end MVCCKey) error {
	err := t.batch1.ClearRange(start, end)
	err2 := t.batch2.ClearRange(start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// ClearIterRange implements the Batch interface.
func (t *TeeEngineBatch) ClearIterRange(iter Iterator, start, end roachpb.Key) error {
	err := t.batch1.ClearIterRange(iter.(*TeeEngineIter).iter1, start, end)
	err2 := t.batch2.ClearIterRange(iter.(*TeeEngineIter).iter2, start, end)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Merge implements the Batch interface.
func (t *TeeEngineBatch) Merge(key MVCCKey, value []byte) error {
	err := t.batch1.Merge(key, value)
	err2 := t.batch2.Merge(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Put implements the Batch interface.
func (t *TeeEngineBatch) Put(key MVCCKey, value []byte) error {
	err := t.batch1.Put(key, value)
	err2 := t.batch2.Put(key, value)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogData implements the Batch interface.
func (t *TeeEngineBatch) LogData(data []byte) error {
	err := t.batch1.LogData(data)
	err2 := t.batch2.LogData(data)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// LogLogicalOp implements the Batch interface.
func (t *TeeEngineBatch) LogLogicalOp(op MVCCLogicalOpType, details MVCCLogicalOpDetails) {
	t.batch1.LogLogicalOp(op, details)
	t.batch2.LogLogicalOp(op, details)
}

// Commit implements the Batch interface.
func (t *TeeEngineBatch) Commit(sync bool) error {
	err := t.batch1.Commit(sync)
	err2 := t.batch2.Commit(sync)
	return fatalOnErrorMismatch(t.ctx, err, err2)
}

// Distinct implements the Batch interface.
func (t *TeeEngineBatch) Distinct() ReadWriter {
	distinct1 := t.batch1.Distinct()
	distinct2 := t.batch2.Distinct()
	return &TeeEngineReadWriter{TeeEngineReader{
		ctx:     t.ctx,
		reader1: distinct1,
		reader2: distinct2,
	}}
}

// Empty implements the Batch interface.
func (t *TeeEngineBatch) Empty() bool {
	empty := t.batch1.Empty()
	empty2 := t.batch2.Empty()
	if empty != empty2 {
		log.Fatalf(t.ctx, "mismatching responses for batch.Empty(): %v != %v", empty, empty2)
	}
	return empty
}

// Len implements the Batch interface.
func (t *TeeEngineBatch) Len() int {
	len1 := t.batch1.Len()
	len2 := t.batch2.Len()

	if len1 != len2 {
		log.Fatalf(t.ctx, "mismatching lengths for batch: %v != %v", len1, len2)
	}
	return len1
}

// Repr implements the Batch interface.
func (t *TeeEngineBatch) Repr() []byte {
	repr := t.batch1.Repr()
	repr2 := t.batch2.Repr()
	if !bytes.Equal(repr, repr2) {
		log.Fatalf(t.ctx, "mismatching byte representations between engines: %v != %v", repr, repr2)
	}
	return repr
}

// TeeEngineIter is an Iterator that iterates on underlying eng1 and
// eng2 iterators in lockstep. If there's a mismatch in their states ever,
// whether validity or keys/values being pointed to, a log.Fatal is triggered.
type TeeEngineIter struct {
	ctx   context.Context
	iter1 Iterator
	iter2 Iterator
}

var _ MVCCIterator = &TeeEngineIter{}

// Close implements the Iterator interface.
func (t *TeeEngineIter) Close() {
	t.iter1.Close()
	t.iter2.Close()
}

// check checks if the two underlying iterators have matching validity states
// and keys.
func (t *TeeEngineIter) check() {
	valid, err := t.iter1.Valid()
	valid2, err2 := t.iter2.Valid()
	_ = fatalOnErrorMismatch(t.ctx, err, err2)

	if !(valid && valid2) && (valid || valid2) {
		// valid XOR valid2
		log.Fatalf(t.ctx, "one of engine iters invalid but not both: %v != %v", valid, valid2)
		return
	}

	if valid {
		key1 := t.iter1.UnsafeKey()
		key2 := t.iter2.UnsafeKey()
		if !key2.Equal(key1) {
			log.Fatalf(t.ctx, "engine iterators pointing to different keys: %v != %v", key1, key2)
		}
	}
}

// SeekGE implements the Iterator interface.
func (t *TeeEngineIter) SeekGE(key MVCCKey) {
	t.iter1.SeekGE(key)
	t.iter2.SeekGE(key)
	t.check()
}

// Valid implements the Iterator interface.
func (t *TeeEngineIter) Valid() (bool, error) {
	return t.iter1.Valid()
}

// Next implements the Iterator interface.
func (t *TeeEngineIter) Next() {
	t.iter1.Next()
	t.iter2.Next()
	t.check()
}

// NextKey implements the Iterator interface.
func (t *TeeEngineIter) NextKey() {
	t.iter1.NextKey()
	t.iter2.NextKey()
	t.check()
}

// UnsafeKey implements the Iterator interface.
func (t *TeeEngineIter) UnsafeKey() MVCCKey {
	return t.iter1.UnsafeKey()
}

// UnsafeValue implements the Iterator interface.
func (t *TeeEngineIter) UnsafeValue() []byte {
	return t.iter1.UnsafeValue()
}

// SeekLT implements the Iterator interface.
func (t *TeeEngineIter) SeekLT(key MVCCKey) {
	t.iter1.SeekLT(key)
	t.iter2.SeekLT(key)
	t.check()
}

// Prev implements the Iterator interface.
func (t *TeeEngineIter) Prev() {
	t.iter1.Prev()
	t.iter2.Prev()
	t.check()
}

// Key implements the Iterator interface.
func (t *TeeEngineIter) Key() MVCCKey {
	return t.iter1.Key()
}

func (t *TeeEngineIter) unsafeRawKey() []byte {
	type unsafeRawKeyGetter interface {
		unsafeRawKey() []byte
	}
	return t.iter1.(unsafeRawKeyGetter).unsafeRawKey()
}

// Value implements the Iterator interface.
func (t *TeeEngineIter) Value() []byte {
	return t.iter1.UnsafeValue()
}

// ValueProto implements the Iterator interface.
func (t *TeeEngineIter) ValueProto(msg protoutil.Message) error {
	return t.iter1.ValueProto(msg)
}

// ComputeStats implements the Iterator interface.
func (t *TeeEngineIter) ComputeStats(
	start, end roachpb.Key, nowNanos int64,
) (enginepb.MVCCStats, error) {
	stats1, err := t.iter1.ComputeStats(start, end, nowNanos)
	stats2, err2 := t.iter2.ComputeStats(start, end, nowNanos)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if !stats1.Equal(stats2) {
		log.Fatalf(t.ctx, "mismatching stats between engines: %v != %v", stats1, stats2)
	}
	return stats1, err
}

// FindSplitKey implements the Iterator interface.
func (t *TeeEngineIter) FindSplitKey(
	start, end, minSplitKey roachpb.Key, targetSize int64,
) (MVCCKey, error) {
	splitKey1, err := t.iter1.FindSplitKey(start, end, minSplitKey, targetSize)
	splitKey2, err2 := t.iter2.FindSplitKey(start, end, minSplitKey, targetSize)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return MVCCKey{}, err
	}
	if !splitKey1.Equal(splitKey2) {
		log.Fatalf(t.ctx, "mismatching split keys returned from engines: %v != %v", splitKey1, splitKey2)
	}
	return splitKey1, err
}

// CheckForKeyCollisions implements the Iterator interface.
func (t *TeeEngineIter) CheckForKeyCollisions(
	sstData []byte, start, end roachpb.Key,
) (enginepb.MVCCStats, error) {
	stats1, err := t.iter1.CheckForKeyCollisions(sstData, start, end)
	stats2, err2 := t.iter2.CheckForKeyCollisions(sstData, start, end)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return enginepb.MVCCStats{}, err
	}
	if !stats1.Equal(stats2) {
		log.Fatalf(t.ctx, "mismatching stats between engines: %v != %v", stats1, stats2)
	}
	return stats1, nil
}

// SetUpperBound implements the Iterator interface.
func (t *TeeEngineIter) SetUpperBound(key roachpb.Key) {
	t.iter1.SetUpperBound(key)
	t.iter2.SetUpperBound(key)
}

// Stats implements the Iterator interface.
func (t *TeeEngineIter) Stats() IteratorStats {
	return t.iter1.Stats()
}

// MVCCOpsSpecialized implements the MVCCIterator interface.
func (t *TeeEngineIter) MVCCOpsSpecialized() bool {
	return true
}

// MVCCGet implements the MVCCIterator interface.
func (t *TeeEngineIter) MVCCGet(
	key roachpb.Key, timestamp hlc.Timestamp, opts MVCCGetOptions,
) (*roachpb.Value, *roachpb.Intent, error) {
	value1, intent1, err := mvccGet(t.ctx, t.iter1, key, timestamp, opts)
	value2, intent2, err2 := mvccGet(t.ctx, t.iter2, key, timestamp, opts)
	if err = fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return nil, nil, err
	}

	if !value1.Equal(value2) {
		log.Fatalf(t.ctx, "mismatching values returned by engines in MVCCGet: %v != %v", value1, value2)
	}
	if !intent1.Equal(intent2) {
		log.Fatalf(t.ctx, "mismatching intents returned by engines in MVCCGet: %v != %v", intent1, intent2)
	}
	return value1, intent1, err
}

// Helper function to check for equality in kvdata. Assumes data1 is a large
// contiguous byte slice while data2 is a slice of slices.
func kvDataEqual(ctx context.Context, data1 []byte, data2 [][]byte) bool {
	if len(data1) == 0 && len(data2) == 0 {
		return true
	}

	i := 0
	for _, subSlice := range data2 {
		sliceEnd := i + len(subSlice)
		if sliceEnd > len(data1) {
			return false
		}
		if !bytes.Equal(subSlice, data1[i:sliceEnd]) {
			return false
		}
		i = sliceEnd
	}
	return true
}

// MVCCScan implements the MVCCIterator interface.
func (t *TeeEngineIter) MVCCScan(
	start, end roachpb.Key, timestamp hlc.Timestamp, opts MVCCScanOptions,
) (MVCCScanResult, error) {
	res1, err := mvccScanToBytes(t.ctx, t.iter1, start, end, timestamp, opts)
	res2, err2 := mvccScanToBytes(t.ctx, t.iter2, start, end, timestamp, opts)

	if err := fatalOnErrorMismatch(t.ctx, err, err2); err != nil {
		return MVCCScanResult{}, err
	}

	if res1.NumKeys != res2.NumKeys {
		log.Fatalf(t.ctx, "mismatching number of KVs returned from engines MVCCScan: %d != %d", res1.NumKeys, res2.NumKeys)
	}
	if res1.NumBytes != res2.NumBytes {
		log.Fatalf(t.ctx, "mismatching NumBytes returned from engines MVCCScan: %d != %d", res1.NumBytes, res2.NumBytes)
	}

	// At least one side is expected to have only one contiguous slice inside it.
	// This lets us simplify the checking code below.
	equal := false
	if len(res1.KVData) != 1 && len(res2.KVData) != 1 {
		panic("unsupported multiple-slice result from both iterators in MVCCScan")
	} else if len(res2.KVData) == 1 {
		// Swap the two slices so that the first argument is the one with only one
		// slice inside it.
		equal = kvDataEqual(t.ctx, res2.KVData[0], res1.KVData)
	} else {
		equal = kvDataEqual(t.ctx, res1.KVData[0], res2.KVData)
	}

	if !equal {
		log.Fatalf(t.ctx, "mismatching kv data returned by engines: %v != %v", res1.KVData, res2.KVData)
	}

	if !res1.ResumeSpan.Equal(res2.ResumeSpan) {
		log.Fatalf(t.ctx, "mismatching resume spans returned by engines: %v != %v", res1.ResumeSpan, res2.ResumeSpan)
	}
	if len(res1.Intents) != len(res2.Intents) {
		log.Fatalf(t.ctx, "mismatching number of intents returned by engines: %v != %v", len(res1.Intents), len(res2.Intents))
	}
	for i := range res1.Intents {
		if !res1.Intents[i].Equal(res2.Intents[i]) {
			log.Fatalf(t.ctx, "mismatching intents returned by engines: %v != %v", res1.Intents[i], res2.Intents[i])
		}
	}
	return res1, err
}
