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
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

// SSTSnapshotStorage provides an interface to create scratches and owns the
// directory of scratches created. A scratch manages the SSTs created during a
// specific snapshot.
type SSTSnapshotStorage struct {
	engine  engine.Engine
	limiter *rate.Limiter
	dir     string
}

// NewSSTSnapshotStorage creates a new SST snapshot storage.
func NewSSTSnapshotStorage(engine engine.Engine, limiter *rate.Limiter) SSTSnapshotStorage {
	return SSTSnapshotStorage{
		engine:  engine,
		limiter: limiter,
		dir:     filepath.Join(engine.GetAuxiliaryDir(), "sstsnapshot"),
	}
}

// NewScratchSpace creates a new storage scratch space for SSTs for a specific
// snapshot.
func (s *SSTSnapshotStorage) NewScratchSpace(
	rangeID roachpb.RangeID, snapUUID uuid.UUID,
) *SSTSnapshotStorageScratch {
	snapDir := filepath.Join(s.dir, strconv.Itoa(int(rangeID)), snapUUID.String())
	return &SSTSnapshotStorageScratch{
		storage: s,
		snapDir: snapDir,
	}
}

// Clear removes all created directories and SSTs.
func (s *SSTSnapshotStorage) Clear() error {
	return os.RemoveAll(s.dir)
}

// SSTSnapshotStorageScratch keeps track of the SST files incrementally created
// when receiving a snapshot. Each scratch is associated with a specific
// snapshot.
type SSTSnapshotStorageScratch struct {
	storage    *SSTSnapshotStorage
	ssts       []string
	snapDir    string
	dirCreated bool
}

func (s *SSTSnapshotStorageScratch) filename(id int) string {
	return filepath.Join(s.snapDir, fmt.Sprintf("%d.sst", id))
}

func (s *SSTSnapshotStorageScratch) createDir() error {
	// TODO(peter): The directory creation needs to be plumbed through the Engine
	// interface. Right now, this is creating a directory on disk even when the
	// Engine has an in-memory filesystem. The only reason everything still works
	// is because RocksDB MemEnvs allow the creation of files when the parent
	// directory doesn't exist.
	err := os.MkdirAll(s.snapDir, 0755)
	s.dirCreated = s.dirCreated || err == nil
	return err
}

// NewFile adds another file to SSTSnapshotStorageScratch. This file is lazily
// created when the file is written to the first time. A nonzero value for
// chunkSize buffers up writes until the buffer is greater than chunkSize.
func (s *SSTSnapshotStorageScratch) NewFile(
	ctx context.Context, chunkSize int64,
) (*SSTSnapshotStorageFile, error) {
	id := len(s.ssts)
	filename := s.filename(id)
	s.ssts = append(s.ssts, filename)
	f := &SSTSnapshotStorageFile{
		scratch:   s,
		filename:  filename,
		ctx:       ctx,
		chunkSize: chunkSize,
	}
	return f, nil
}

// WriteSST writes SST data to a file. The method closes
// the provided SST when it is finished using it. If the provided SST is empty,
// then no file will be created and nothing will be written.
func (s *SSTSnapshotStorageScratch) WriteSST(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	f, err := s.NewFile(ctx, 0)
	if err != nil {
		return err
	}
	defer func() {
		// Closing an SSTSnapshotStorageFile multiple times is idempotent. Nothing
		// actionable if closing fails.
		_ = f.Close()
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	return f.Close()
}

// SSTs returns the names of the files created.
func (s *SSTSnapshotStorageScratch) SSTs() []string {
	return s.ssts
}

// Clear removes the directory and SSTs created for a particular snapshot.
func (s *SSTSnapshotStorageScratch) Clear() error {
	return os.RemoveAll(s.snapDir)
}

// SSTSnapshotStorageFile is an SST file managed by a
// SSTSnapshotStorageScratch.
type SSTSnapshotStorageFile struct {
	scratch   *SSTSnapshotStorageScratch
	created   bool
	file      engine.File
	filename  string
	ctx       context.Context
	chunkSize int64
	buffer    []byte
}

func (f *SSTSnapshotStorageFile) openFile() error {
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
	file, err := f.scratch.storage.engine.CreateFile(f.filename)
	if err != nil {
		return err
	}
	f.file = file
	f.created = true
	return nil
}

// Write writes contents to the file while respecting the limiter passed into
// SSTSnapshotStorageScratch. Writing empty contents is okay and is treated as
// a noop. The file must have not been closed.
func (f *SSTSnapshotStorageFile) Write(contents []byte) (int, error) {
	if len(contents) == 0 {
		return 0, nil
	}
	if err := f.openFile(); err != nil {
		return 0, err
	}
	limitBulkIOWrite(f.ctx, f.scratch.storage.limiter, len(contents))
	if f.chunkSize > 0 {
		if int64(len(contents)+len(f.buffer)) < f.chunkSize {
			// Don't write to file yet - buffer write until next time.
			f.buffer = append(f.buffer, contents...)
			return len(contents), nil
		} else if len(f.buffer) > 0 {
			// Write buffered writes and then empty the buffer.
			if _, err := f.file.Write(f.buffer); err != nil {
				return 0, err
			}
			f.buffer = f.buffer[:0]
		}
	}
	if _, err := f.file.Write(contents); err != nil {
		return 0, err
	}
	return len(contents), f.file.Sync()
}

// Close closes the file. Calling this function multiple times is idempotent.
// The file must have been written to before being closed.
func (f *SSTSnapshotStorageFile) Close() error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	if !f.created {
		return errors.New("file is empty")
	}
	if f.file == nil {
		return nil
	}
	if len(f.buffer) > 0 {
		// Write out any buffered data.
		if _, err := f.file.Write(f.buffer); err != nil {
			return err
		}
		f.buffer = f.buffer[:0]
	}
	if err := f.file.Close(); err != nil {
		return err
	}
	f.file = nil
	return nil
}

// Sync syncs the file to disk. Implements writeCloseSyncer in engine.
func (f *SSTSnapshotStorageFile) Sync() error {
	return f.file.Sync()
}
