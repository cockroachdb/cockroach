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
// directory of scratches created.
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

func rangeDir(baseDir string, rangeID roachpb.RangeID) string {
	return filepath.Join(baseDir, strconv.Itoa(int(rangeID)))
}

// NewSSTSnapshotStorageScratch creates a new SST snapshot storage scratch for
// a specific snapshot.
func (sss *SSTSnapshotStorage) NewSSTSnapshotStorageScratch(
	rangeID roachpb.RangeID, snapUUID uuid.UUID,
) *SSTSnapshotStorageScratch {
	rangeDir := rangeDir(sss.dir, rangeID)
	snapDir := filepath.Join(rangeDir, snapUUID.String())
	ssss := &SSTSnapshotStorageScratch{
		sss:      sss,
		rangeDir: rangeDir,
		snapDir:  snapDir,
	}
	return ssss
}

// Clear removes the directories and SSTs created for a particular range.
func (sss *SSTSnapshotStorage) Clear(rangeID roachpb.RangeID) error {
	return os.RemoveAll(rangeDir(sss.dir, rangeID))
}

// SSTSnapshotStorageScratch keeps track of the SST files incrementally created
// when receiving a snapshot. Each scratch is associated with a specific
// snapshot.
type SSTSnapshotStorageScratch struct {
	sss        *SSTSnapshotStorage
	activeFile bool
	currFile   engine.DBFile
	ssts       []string
	rangeDir   string
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

func (ssss *SSTSnapshotStorageScratch) createFile() error {
	if !ssss.dirCreated {
		if err := ssss.createDir(); err != nil {
			return err
		}
	}
	filename := ssss.filename(len(ssss.ssts) - 1)
	currFile, err := ssss.sss.engine.OpenFile(filename)
	if err != nil {
		return err
	}
	ssss.currFile = currFile
	return nil
}

// NewFile adds another file to SSTSnapshotStorageScratch. This file is lazily
// created when the file is written to the first time. Any previous file must
// be closed before another one is created.
func (ssss *SSTSnapshotStorageScratch) NewFile() error {
	if ssss.activeFile {
		return errors.New("exists another active file")
	}
	id := len(ssss.ssts)
	ssss.ssts = append(ssss.ssts, ssss.filename(id))
	ssss.activeFile = true
	return nil
}

// Write writes contents to the active file while respecting the limiter passed
// into SSTSnapshotStorageScratch.
func (ssss *SSTSnapshotStorageScratch) Write(ctx context.Context, contents []byte) error {
	if !ssss.activeFile {
		return errors.New("no active file")
	}
	if len(contents) == 0 {
		return nil
	}
	if ssss.currFile == nil {
		if err := ssss.createFile(); err != nil {
			return err
		}
	}
	limitBulkIOWrite(ctx, ssss.sss.limiter, len(contents))
	if err := ssss.currFile.Append(contents); err != nil {
		return err
	}
	return ssss.currFile.Sync()
}

// WriteAll writes an entire RocksDBSstFileWriter to a file. There must be no
// active file when calling WriteAll. The method closes the provided SST when
// it is finished using it. If the provided SST is empty, then no file will be
// created and nothing will be written.
func (ssss *SSTSnapshotStorageScratch) WriteAll(
	ctx context.Context, sst *engine.RocksDBSstFileWriter,
) error {
	if sst.DataSize == 0 {
		sst.Close()
		return nil
	}
	if err := ssss.NewFile(); err != nil {
		return err
	}
	defer sst.Close()
	data, err := sst.Finish()
	if err != nil {
		return err
	}
	if err := ssss.Write(ctx, data); err != nil {
		return err
	}
	return ssss.Close()
}

// Close closes the active file. Calling this function multiple times is
// idempotent. The file must have been written to before being closed.
func (ssss *SSTSnapshotStorageScratch) Close() error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	if !ssss.activeFile {
		return nil
	}
	if ssss.currFile == nil {
		return errors.New("active file is empty")
	}
	if err := ssss.currFile.Close(); err != nil {
		return err
	}
	ssss.currFile = nil
	ssss.activeFile = false
	return nil
}

// SSTs returns the names of the files created.
func (ssss *SSTSnapshotStorageScratch) SSTs() []string {
	return ssss.ssts
}

// Clear removes the directories and SSTs created.
func (ssss *SSTSnapshotStorageScratch) Clear() error {
	return os.RemoveAll(ssss.snapDir)
}
