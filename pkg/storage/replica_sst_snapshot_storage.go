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

// NewSSTSnapshotStorageScratch creates a new SST snapshot storage scratch for
// a specific snapshot.
func (sss *SSTSnapshotStorage) NewSSTSnapshotStorageScratch(
	rangeID roachpb.RangeID, snapUUID uuid.UUID,
) *SSTSnapshotStorageScratch {
	snapDir := filepath.Join(sss.dir, strconv.Itoa(int(rangeID)), snapUUID.String())
	ssss := &SSTSnapshotStorageScratch{
		sss:     sss,
		snapDir: snapDir,
	}
	return ssss
}

// Clear removes all created directories and SSTs.
func (sss *SSTSnapshotStorage) Clear() error {
	return os.RemoveAll(sss.dir)
}

// SSTSnapshotStorageScratch keeps track of the SST files incrementally created
// when receiving a snapshot. Each scratch is associated with a specific
// snapshot.
type SSTSnapshotStorageScratch struct {
	sss        *SSTSnapshotStorage
	ssts       []string
	snapDir    string
	dirCreated bool
}

func (ssss *SSTSnapshotStorageScratch) filename(id int) string {
	return filepath.Join(ssss.snapDir, fmt.Sprintf("%d.sst", id))
}

func (ssss *SSTSnapshotStorageScratch) createDir() error {
	// TODO(peter): The directory creation needs to be plumbed through the Engine
	// interface. Right now, this is creating a directory on disk even when the
	// Engine has an in-memory filesystem. The only reason everything still works
	// is because RocksDB MemEnvs allow the creation of files when the parent
	// directory doesn't exist.
	err := os.MkdirAll(ssss.snapDir, 0755)
	ssss.dirCreated = ssss.dirCreated || err == nil
	return err
}

// NewFile adds another file to SSTSnapshotStorageScratch. This file is lazily
// created when the file is written to the first time. A nonzero value for
// chunkSize buffers up writes until the buffer is greater than chunkSize.
func (ssss *SSTSnapshotStorageScratch) NewFile(
	ctx context.Context, chunkSize int64,
) (*SSTSnapshotStorageFile, error) {
	id := len(ssss.ssts)
	filename := ssss.filename(id)
	ssss.ssts = append(ssss.ssts, filename)
	sssf := &SSTSnapshotStorageFile{
		ssss:      ssss,
		filename:  filename,
		ctx:       ctx,
		chunkSize: chunkSize,
	}
	return sssf, nil
}

// WriteSST writes SST data to a file. The method closes
// the provided SST when it is finished using it. If the provided SST is empty,
// then no file will be created and nothing will be written.
func (ssss *SSTSnapshotStorageScratch) WriteSST(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	sssf, err := ssss.NewFile(ctx, 0)
	if err != nil {
		return err
	}
	defer func() {
		// Closing an SSTSnapshotStorageFile multiple times is idempotent. Nothing
		// actionable if closing fails.
		_ = sssf.Close()
	}()
	if _, err := sssf.Write(data); err != nil {
		return err
	}
	return sssf.Close()
}

// SSTs returns the names of the files created.
func (ssss *SSTSnapshotStorageScratch) SSTs() []string {
	return ssss.ssts
}

// Clear removes the directory and SSTs created for a particular snapshot.
func (ssss *SSTSnapshotStorageScratch) Clear() error {
	return os.RemoveAll(ssss.snapDir)
}

// SSTSnapshotStorageFile is an SST file managed by a
// SSTSnapshotStorageScratch.
type SSTSnapshotStorageFile struct {
	ssss      *SSTSnapshotStorageScratch
	created   bool
	file      engine.DBFile
	filename  string
	ctx       context.Context
	chunkSize int64
	buffer    []byte
}

func (sssf *SSTSnapshotStorageFile) openFile() error {
	if sssf.created {
		if sssf.file == nil {
			return errors.Errorf("file has already been closed")
		}
		return nil
	}
	if !sssf.ssss.dirCreated {
		if err := sssf.ssss.createDir(); err != nil {
			return err
		}
	}
	file, err := sssf.ssss.sss.engine.OpenFile(sssf.filename)
	if err != nil {
		return err
	}
	sssf.file = file
	sssf.created = true
	return nil
}

// Write writes contents to the file while respecting the limiter passed into
// SSTSnapshotStorageScratch. Writing empty contents is okay and is treated as
// a noop. The file must have not been closed.
func (sssf *SSTSnapshotStorageFile) Write(contents []byte) (int, error) {
	if len(contents) == 0 {
		return 0, nil
	}
	if err := sssf.openFile(); err != nil {
		return 0, err
	}
	limitBulkIOWrite(sssf.ctx, sssf.ssss.sss.limiter, len(contents))
	if sssf.chunkSize > 0 {
		if int64(len(contents)+len(sssf.buffer)) < sssf.chunkSize {
			// Don't write to file yet - buffer write until next time.
			sssf.buffer = append(sssf.buffer, contents...)
			return len(contents), nil
		} else if len(sssf.buffer) > 0 {
			// Write buffered writes and then empty the buffer.
			if _, err := sssf.file.Write(sssf.buffer); err != nil {
				return 0, err
			}
			sssf.buffer = sssf.buffer[:0]
		}
	}
	if _, err := sssf.file.Write(contents); err != nil {
		return 0, err
	}
	return len(contents), sssf.file.Sync()
}

// Close closes the file. Calling this function multiple times is idempotent.
// The file must have been written to before being closed.
func (sssf *SSTSnapshotStorageFile) Close() error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	if !sssf.created {
		return errors.New("file is empty")
	}
	if sssf.file == nil {
		return nil
	}
	if len(sssf.buffer) > 0 {
		// Write out any buffered data.
		if _, err := sssf.file.Write(sssf.buffer); err != nil {
			return err
		}
		sssf.buffer = sssf.buffer[:0]
	}
	if err := sssf.file.Close(); err != nil {
		return err
	}
	sssf.file = nil
	return nil
}

// Sync syncs the file to disk. Implements writeCloseSyncer in engine.
func (sssf *SSTSnapshotStorageFile) Sync() error {
	return sssf.file.Sync()
}
