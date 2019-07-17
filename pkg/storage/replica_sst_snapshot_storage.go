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
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"golang.org/x/time/rate"
)

// SstSnapshotStorage keeps track of the SST files created when receiving a
// snapshot with the SST strategy.
type SstSnapshotStorage struct {
	st         *cluster.Settings
	limiter    *rate.Limiter
	ssts       []string
	dir        string
	dirCreated bool
	eng        engine.Engine
}

func sstSnapshotStoragePath(baseDir string, rangeID roachpb.RangeID, snapUUID uuid.UUID) string {
	return filepath.Join(baseDir, "sstsnapshot", fmt.Sprintf("r%d_%s", rangeID, snapUUID))
}

func newSstSnapshotStorage(
	st *cluster.Settings,
	rangeID roachpb.RangeID,
	snapUUID uuid.UUID,
	baseDir string,
	limiter *rate.Limiter,
	eng engine.Engine,
) (*SstSnapshotStorage, error) {
	dir := sstSnapshotStoragePath(baseDir, rangeID, snapUUID)
	var ssts []string
	matches, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	if err != nil {
		return nil, err
	}
	ssts = append(ssts, matches...)
	sss := &SstSnapshotStorage{
		st:      st,
		limiter: limiter,
		ssts:    ssts,
		dir:     dir,
		eng:     eng,
	}
	return sss, nil
}

func (sss *SstSnapshotStorage) createDir() error {
	err := os.MkdirAll(sss.dir, 0755)
	sss.dirCreated = sss.dirCreated || err == nil
	return err
}

func (sss *SstSnapshotStorage) filename(index int) string {
	return filepath.Join(sss.dir, fmt.Sprintf("%d.sst", index))
}

// CreateFile creates a new SST file. If the directory storing the SST files
// does not exist, it will also be created.
func (sss *SstSnapshotStorage) CreateFile() (*os.File, error) {
	if !sss.dirCreated {
		if err := sss.createDir(); err != nil {
			return nil, err
		}
	}
	filename := sss.filename(len(sss.ssts))
	// Use 0644 since that's what RocksDB uses:
	// https://github.com/facebook/rocksdb/blob/56656e12d67d8a63f1e4c4214da9feeec2bd442b/env/env_posix.cc#L171
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, err
	}
	sss.ssts = append(sss.ssts, filename)
	return file, nil
}

// Write writes contents to the file while respecting the limiter passed into
// SstSnapshotStorage.
func (sss *SstSnapshotStorage) Write(ctx context.Context, file *os.File, contents []byte) error {
	limitBulkIOWrite(ctx, sss.limiter, len(contents))
	_, err := file.Write(contents)
	return err
}

// Clear removes the directory and all SST files created.
func (sss *SstSnapshotStorage) Clear() error {
	return os.RemoveAll(sss.dir)
}
