// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage"
	"github.com/cockroachdb/pebble/vfs"
	"golang.org/x/time/rate"
)

// SSTSnapshotStorage provides an interface to create scratches and owns the
// directory of scratches created. A scratch manages the SSTs created during a
// specific snapshot.
type SSTSnapshotStorage struct {
	engine  storage.Engine
	limiter *rate.Limiter
	dir     string
	mu      struct {
		syncutil.Mutex
		rangeRefCount map[roachpb.RangeID]int
	}
}

// NewSSTSnapshotStorage creates a new SST snapshot storage.
func NewSSTSnapshotStorage(engine storage.Engine, limiter *rate.Limiter) SSTSnapshotStorage {
	return SSTSnapshotStorage{
		engine:  engine,
		limiter: limiter,
		dir:     filepath.Join(engine.GetAuxiliaryDir(), "sstsnapshot"),
		mu: struct {
			syncutil.Mutex
			rangeRefCount map[roachpb.RangeID]int
		}{rangeRefCount: make(map[roachpb.RangeID]int)},
	}
}

// NewScratchSpace creates a new storage scratch space for SSTs for a specific
// snapshot.
func (s *SSTSnapshotStorage) NewScratchSpace(
	rangeID roachpb.RangeID, snapUUID uuid.UUID,
) *SSTSnapshotStorageScratch {
	s.mu.Lock()
	s.mu.rangeRefCount[rangeID]++
	s.mu.Unlock()
	snapDir := filepath.Join(s.dir, strconv.Itoa(int(rangeID)), snapUUID.String())
	return &SSTSnapshotStorageScratch{
		storage: s,
		rangeID: rangeID,
		snapDir: snapDir,
	}
}

// Clear removes all created directories and SSTs.
func (s *SSTSnapshotStorage) Clear() error {
	return s.engine.Env().RemoveAll(s.dir)
}

// scratchClosed is called when an SSTSnapshotStorageScratch created by this
// SSTSnapshotStorage is closed. This method handles any cleanup of range
// directories if all SSTSnapshotStorageScratches corresponding to a range
// have closed.
func (s *SSTSnapshotStorage) scratchClosed(rangeID roachpb.RangeID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	val := s.mu.rangeRefCount[rangeID]
	if val <= 0 {
		panic("inconsistent scratch ref count")
	}
	val--
	s.mu.rangeRefCount[rangeID] = val
	if val == 0 {
		delete(s.mu.rangeRefCount, rangeID)
		// Suppressing an error here is okay, as orphaned directories are at worst
		// a performance issue when we later walk directories in pebble.Capacity()
		// but not a correctness issue.
		_ = s.engine.Env().RemoveAll(filepath.Join(s.dir, strconv.Itoa(int(rangeID))))
	}
}

// SSTSnapshotStorageScratch keeps track of the SST files incrementally created
// when receiving a snapshot. Each scratch is associated with a specific
// snapshot.
type SSTSnapshotStorageScratch struct {
	storage    *SSTSnapshotStorage
	rangeID    roachpb.RangeID
	ssts       []string
	snapDir    string
	dirCreated bool
	closed     bool
}

func (s *SSTSnapshotStorageScratch) filename(id int) string {
	return filepath.Join(s.snapDir, fmt.Sprintf("%d.sst", id))
}

func (s *SSTSnapshotStorageScratch) createDir() error {
	err := s.storage.engine.Env().MkdirAll(s.snapDir, os.ModePerm)
	s.dirCreated = s.dirCreated || err == nil
	return err
}

// NewFile adds another file to SSTSnapshotStorageScratch. This file is lazily
// created when the file is written to the first time. A nonzero value for
// bytesPerSync will sync dirty data periodically as it is written. The syncing
// does not provide persistency guarantees, but is used to smooth out disk
// writes. Sync() must be called for data persistence.
func (s *SSTSnapshotStorageScratch) NewFile(
	ctx context.Context, bytesPerSync int64,
) (*SSTSnapshotStorageFile, error) {
	if s.closed {
		return nil, errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
	}
	id := len(s.ssts)
	filename := s.filename(id)
	s.ssts = append(s.ssts, filename)
	f := &SSTSnapshotStorageFile{
		scratch:      s,
		filename:     filename,
		ctx:          ctx,
		bytesPerSync: bytesPerSync,
	}
	return f, nil
}

// WriteSST writes SST data to a file. The method closes
// the provided SST when it is finished using it. If the provided SST is empty,
// then no file will be created and nothing will be written.
func (s *SSTSnapshotStorageScratch) WriteSST(ctx context.Context, data []byte) error {
	if s.closed {
		return errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
	}
	if len(data) == 0 {
		return nil
	}
	f, err := s.NewFile(ctx, 512<<10 /* 512 KB */)
	if err != nil {
		return err
	}
	if err := f.Write(data); err != nil {
		f.Abort()
		return err
	}
	return f.Finish()
}

// SSTs returns the names of the files created.
func (s *SSTSnapshotStorageScratch) SSTs() []string {
	return s.ssts
}

// Close removes the directory and SSTs created for a particular snapshot.
func (s *SSTSnapshotStorageScratch) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	defer s.storage.scratchClosed(s.rangeID)
	return s.storage.engine.Env().RemoveAll(s.snapDir)
}

// SSTSnapshotStorageFile is an SST file managed by a
// SSTSnapshotStorageScratch.
type SSTSnapshotStorageFile struct {
	scratch      *SSTSnapshotStorageScratch
	created      bool
	file         vfs.File
	filename     string
	ctx          context.Context
	bytesPerSync int64
}

var _ objstorage.Writable = (*SSTSnapshotStorageFile)(nil)

func (f *SSTSnapshotStorageFile) ensureFile() error {
	if f.created {
		if f.file == nil {
			return errors.Errorf("file has already been closed")
		}
		return nil
	}
	if !f.scratch.dirCreated {
		if err := f.scratch.createDir(); err != nil {
			return err
		}
	}
	if f.scratch.closed {
		return errors.AssertionFailedf("SSTSnapshotStorageScratch closed")
	}
	var err error
	if f.bytesPerSync > 0 {
		f.file, err = fs.CreateWithSync(f.scratch.storage.engine.Env(), f.filename, int(f.bytesPerSync), fs.RaftSnapshotWriteCategory)
	} else {
		f.file, err = f.scratch.storage.engine.Env().Create(f.filename, fs.RaftSnapshotWriteCategory)
	}
	if err != nil {
		return err
	}
	f.created = true
	return nil
}

// Write is part of objstorage.Writable; it writes contents to the file while
// respecting the limiter passed into SSTSnapshotStorageScratch. Writing empty
// contents is okay and is treated as a noop.
// Cannot be called after Finish or Abort.
func (f *SSTSnapshotStorageFile) Write(contents []byte) error {
	if len(contents) == 0 {
		return nil
	}
	if err := f.ensureFile(); err != nil {
		return err
	}
	if err := kvserverbase.LimitBulkIOWrite(f.ctx, f.scratch.storage.limiter, len(contents)); err != nil {
		return err
	}
	// Write always returns an error if it can't write all the contents.
	_, err := f.file.Write(contents)
	return err
}

// Finish is part of the objstorage.Writable interface.
func (f *SSTSnapshotStorageFile) Finish() error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	if !f.created {
		return errors.New("file is empty")
	}
	errSync := f.file.Sync()
	errClose := f.file.Close()
	f.file = nil
	if errSync != nil {
		return errSync
	}
	return errClose
}

// Abort is part of the objstorage.Writable interface.
func (f *SSTSnapshotStorageFile) Abort() {
	if f.file != nil {
		_ = f.file.Close()
		f.file = nil
	}
}
