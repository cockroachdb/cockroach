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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

// SSTSnapshotStorage keeps track of the SST files created when receiving a
// snapshot with the SST strategy.
type SSTSnapshotStorage struct {
	limiter    *rate.Limiter
	activeFile bool
	currFile   *os.File
	ssts       []string
	rangeDir   string
	snapDir    string
	dirCreated bool
	eng        engine.Engine
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
	rangeDir := filepath.Join(baseDir, "sstsnapshot")
	snapDir := filepath.Join(rangeDir, snapUUID.String())
	sss := &SSTSnapshotStorage{
		limiter:  limiter,
		rangeDir: rangeDir,
		snapDir:  snapDir,
		eng:      eng,
	}
	return sss
}

func (sss *SSTSnapshotStorage) filename(index int) string {
	return filepath.Join(sss.snapDir, fmt.Sprintf("%d.sst", index))
}

func (sss *SSTSnapshotStorage) createDir() error {
	err := os.MkdirAll(sss.snapDir, 0755)
	sss.dirCreated = sss.dirCreated || err == nil
	return err
}

func (sss *SSTSnapshotStorage) createFile(index int) error {
	if !sss.dirCreated {
		if err := sss.createDir(); err != nil {
			return err
		}
	}
	filename := sss.filename(index)
	// Use 0644 since that's what RocksDB uses:
	// https://github.com/facebook/rocksdb/blob/56656e12d67d8a63f1e4c4214da9feeec2bd442b/env/env_posix.cc#L171
	currFile, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	sss.currFile = currFile
	return err
}

// NewFile adds another file to SSSSnapshotStorage. This file is lazily created
// when the file is written to the first time. There must be no active files.
func (sss *SSTSnapshotStorage) NewFile() error {
	if sss.activeFile {
		return errors.New("exists an active file that hasn't been closed")
	}
	sss.ssts = append(sss.ssts, sss.filename(len(sss.ssts)))
	sss.activeFile = true
	return nil
}

// Write writes contents to the current file while respecting the limiter
// passed into SSTSnapshotStorage.
func (sss *SSTSnapshotStorage) Write(ctx context.Context, contents []byte) error {
	if !sss.activeFile {
		return errors.New("no active file")
	}
	if sss.currFile == nil {
		if err := sss.createFile(len(sss.ssts) - 1); err != nil {
			return err
		}
	}
	// TODO(jeffreyxiao): We should limit the size of a single SST, but right now
	// we won't need this because the size of ranges aren't large enough.
	limitBulkIOWrite(ctx, sss.limiter, len(contents))
	_, err := sss.currFile.Write(contents)
	return err
}

// WriteAll writes an entire RocksDBSstFileWriter to a file. There must be no
// active file when calling WriteAll.
func (sss *SSTSnapshotStorage) WriteAll(
	ctx context.Context, sst *engine.RocksDBSstFileWriter,
) error {
	defer sst.Close()
	if err := sss.NewFile(); err != nil {
		return err
	}
	data, err := sst.Finish()
	if err != nil {
		return err
	}
	if err := sss.Write(ctx, data); err != nil {
		return err
	}
	return sss.Close()
}

// HasActiveFile returns true if there is an active file, false otherwise.
func (sss *SSTSnapshotStorage) HasActiveFile() bool {
	return sss.activeFile
}

// SSTs returns the names of the files created.
func (sss *SSTSnapshotStorage) SSTs() []string {
	return sss.ssts
}

// Dir returns the directory of the SSTs.
func (sss *SSTSnapshotStorage) Dir() string {
	return sss.snapDir
}

// Close closes the current file, if any. Calling this function multiple times
// is idempotent. The current file must have been written to before being
// closed.
func (sss *SSTSnapshotStorage) Close() error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	if sss.activeFile && sss.currFile == nil {
		return errors.New("closing an empty file")
	}
	if sss.currFile != nil {
		err := sss.currFile.Close()
		sss.currFile = nil
		sss.activeFile = false
		return err
	}
	return nil
}

// Clear removes the directory and all SST files created.
func (sss *SSTSnapshotStorage) Clear() error {
	return os.RemoveAll(sss.rangeDir)
}
