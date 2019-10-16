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
	err := os.MkdirAll(ssss.snapDir, 0755)
	ssss.dirCreated = ssss.dirCreated || err == nil
	return err
}

// NewFile adds another file to SSTSnapshotStorageScratch. This file is lazily
// created when the file is written to the first time.
func (ssss *SSTSnapshotStorageScratch) NewFile() (*SSTSnapshotStorageFile, error) {
	id := len(ssss.ssts)
	filename := ssss.filename(id)
	ssss.ssts = append(ssss.ssts, filename)
	sssf := &SSTSnapshotStorageFile{
		ssss:     ssss,
		filename: filename,
	}
	return sssf, nil
}

// WriteSST writes an entire RocksDBSstFileWriter to a file. The method closes
// the provided SST when it is finished using it. If the provided SST is empty,
// then no file will be created and nothing will be written.
func (ssss *SSTSnapshotStorageScratch) WriteSST(
	ctx context.Context, sst *engine.RocksDBSstFileWriter,
) error {
	defer sst.Close()
	if sst.DataSize == 0 {
		return nil
	}
	data, err := sst.Finish()
	if err != nil {
		return err
	}
	sssf, err := ssss.NewFile()
	if err != nil {
		return err
	}
	defer func() {
		// Closing an SSTSnapshotStorageFile multiple times is idempotent. Nothing
		// actionable if closing fails.
		_ = sssf.Close()
	}()
	if err := sssf.Write(ctx, data); err != nil {
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
	ssss     *SSTSnapshotStorageScratch
	created  bool
	file     engine.DBFile
	filename string
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
func (sssf *SSTSnapshotStorageFile) Write(ctx context.Context, contents []byte) error {
	if len(contents) == 0 {
		return nil
	}
	if err := sssf.openFile(); err != nil {
		return err
	}
	limitBulkIOWrite(ctx, sssf.ssss.sss.limiter, len(contents))
	if _, err := sssf.file.Write(contents); err != nil {
		return err
	}
	return sssf.file.Sync()
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
	if err := sssf.file.Close(); err != nil {
		return err
	}
	sssf.file = nil
	return nil
}
