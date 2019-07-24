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

// SSTSnapshotStorage keeps track of the SST files created when receiving a
// snapshot with the SST strategy.
type SSTSnapshotStorage struct {
	limiter     *rate.Limiter
	activeFiles map[int]*os.File
	ssts        []string
	rangeDir    string
	snapDir     string
	dirCreated  bool
	eng         engine.Engine
}

// NewSSTSnapshotStorage creates a new SST snapshot storage. This storage is
// used for incrementally writing SSTs when snapshotting.
func NewSSTSnapshotStorage(
	rangeID roachpb.RangeID,
	snapUUID uuid.UUID,
	baseDir string,
	limiter *rate.Limiter,
	eng engine.Engine,
) *SSTSnapshotStorage {
	rangeDir := filepath.Join(baseDir, "sstsnapshot", strconv.FormatInt(int64(rangeID), 10))
	snapDir := filepath.Join(rangeDir, snapUUID.String())
	sss := &SSTSnapshotStorage{
		activeFiles: make(map[int]*os.File),
		limiter:     limiter,
		rangeDir:    rangeDir,
		snapDir:     snapDir,
		eng:         eng,
	}
	return sss
}

func (sss *SSTSnapshotStorage) filename(id int) string {
	return filepath.Join(sss.snapDir, fmt.Sprintf("%d.sst", id))
}

func (sss *SSTSnapshotStorage) createDir() error {
	err := os.MkdirAll(sss.snapDir, 0755)
	sss.dirCreated = sss.dirCreated || err == nil
	return err
}

func (sss *SSTSnapshotStorage) createFile(id int) error {
	if !sss.dirCreated {
		if err := sss.createDir(); err != nil {
			return err
		}
	}
	filename := sss.filename(id)
	// Use 0644 since that's what RocksDB uses:
	// https://github.com/facebook/rocksdb/blob/56656e12d67d8a63f1e4c4214da9feeec2bd442b/env/env_posix.cc#L171
	currFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	sss.activeFiles[id] = currFile
	return nil
}

// NewFile adds another file to SSSSnapshotStorage. This file is lazily created
// when the file is written to the first time. There must be no active files.
func (sss *SSTSnapshotStorage) NewFile() int {
	id := len(sss.ssts)
	sss.ssts = append(sss.ssts, sss.filename(id))
	sss.activeFiles[id] = nil
	return id
}

// Write writes contents to the file with the specified file while respecting
// the limiter passed into SSTSnapshotStorage.
func (sss *SSTSnapshotStorage) Write(ctx context.Context, id int, contents []byte) error {
	currFile, ok := sss.activeFiles[id]
	if !ok {
		return errors.Errorf("file with id %d is not an active file", id)
	}
	if currFile == nil {
		if err := sss.createFile(id); err != nil {
			return err
		}
		currFile = sss.activeFiles[id]
	}
	limitBulkIOWrite(ctx, sss.limiter, len(contents))
	_, err := currFile.Write(contents)
	return err
}

// WriteAll writes an entire RocksDBSstFileWriter to a file. There must be no
// active file when calling WriteAll.
func (sss *SSTSnapshotStorage) WriteAll(
	ctx context.Context, sst *engine.RocksDBSstFileWriter,
) error {
	defer sst.Close()
	data, err := sst.Finish()
	if err != nil {
		return err
	}
	id := sss.NewFile()
	if err := sss.Write(ctx, id, data); err != nil {
		return err
	}
	return sss.Close(id)
}

// SSTs returns the names of the files created.
func (sss *SSTSnapshotStorage) SSTs() []string {
	return sss.ssts
}

// Dir returns the directory of the SSTs.
func (sss *SSTSnapshotStorage) Dir() string {
	return sss.snapDir
}

// Close closes the file with the specified id. Calling this function multiple
// times is idempotent. The file must have been written to before being closed.
func (sss *SSTSnapshotStorage) Close(id int) error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	currFile, ok := sss.activeFiles[id]
	if ok {
		if currFile == nil {
			return errors.New("closing an empty file")
		}
		err := currFile.Close()
		delete(sss.activeFiles, id)
		return err
	}
	return nil
}

// Clear removes the directory and all SST files created.
func (sss *SSTSnapshotStorage) Clear() error {
	return os.RemoveAll(sss.rangeDir)
}
