// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

var _ sideloadStorage = &diskSideloadStorage{}

type diskSideloadStorage struct {
	st         *cluster.Settings
	limiter    *rate.Limiter
	dir        string
	dirCreated bool
}

func newDiskSideloadStorage(
	st *cluster.Settings,
	rangeID roachpb.RangeID,
	replicaID roachpb.ReplicaID,
	baseDir string,
	limiter *rate.Limiter,
) (sideloadStorage, error) {
	ss := &diskSideloadStorage{
		dir: filepath.Join(
			baseDir,
			"sideloading",
			fmt.Sprintf("%d", rangeID%1000), // sharding
			fmt.Sprintf("%d.%d", rangeID, replicaID),
		),
		st:      st,
		limiter: limiter,
	}
	return ss, nil
}

func (ss *diskSideloadStorage) createDir() error {
	err := os.MkdirAll(ss.dir, 0755)
	ss.dirCreated = ss.dirCreated || err == nil
	return err
}

func (ss *diskSideloadStorage) Dir() string {
	return ss.dir
}

func (ss *diskSideloadStorage) PutIfNotExists(
	ctx context.Context, index, term uint64, contents []byte,
) error {
	filename := ss.filename(ctx, index, term)
	if _, err := os.Stat(filename); err == nil {
		// File exists.
		return nil
	} else if !os.IsNotExist(err) {
		return err
	}
	// File does not exist yet. There's a chance the whole path is missing (for
	// example after Clear()), in which case handle that transparently.
	for {
		// Use 0644 since that's what RocksDB uses:
		// https://github.com/facebook/rocksdb/blob/56656e12d67d8a63f1e4c4214da9feeec2bd442b/env/env_posix.cc#L171
		if err := writeFileSyncing(ctx, filename, contents, 0644, ss.st, ss.limiter); err == nil {
			return nil
		} else if !os.IsNotExist(err) {
			return err
		}
		if err := ss.createDir(); err != nil {
			return err
		}
		continue
	}
}

func (ss *diskSideloadStorage) Get(ctx context.Context, index, term uint64) ([]byte, error) {
	filename := ss.filename(ctx, index, term)
	b, err := ioutil.ReadFile(filename)
	if os.IsNotExist(err) {
		return nil, errSideloadedFileNotFound
	}
	return b, err
}

func (ss *diskSideloadStorage) Filename(ctx context.Context, index, term uint64) (string, error) {
	return ss.filename(ctx, index, term), nil
}

func (ss *diskSideloadStorage) filename(ctx context.Context, index, term uint64) string {
	return filepath.Join(ss.dir, fmt.Sprintf("i%d.t%d", index, term))
}

func (ss *diskSideloadStorage) Purge(ctx context.Context, index, term uint64) error {
	return ss.purgeFile(ctx, ss.filename(ctx, index, term))
}

func (ss *diskSideloadStorage) purgeFile(ctx context.Context, filename string) error {
	if err := os.Remove(filename); err != nil {
		if os.IsNotExist(err) {
			return errSideloadedFileNotFound
		}
		return err
	}
	return nil
}

func (ss *diskSideloadStorage) Clear(_ context.Context) error {
	err := os.RemoveAll(ss.dir)
	ss.dirCreated = ss.dirCreated && err != nil
	return err
}

func (ss *diskSideloadStorage) TruncateTo(ctx context.Context, index uint64) error {
	matches, err := filepath.Glob(filepath.Join(ss.dir, "i*.t*"))
	if err != nil {
		return err
	}
	var deleted int
	for _, match := range matches {
		base := filepath.Base(match)
		if len(base) < 1 || base[0] != 'i' {
			continue
		}
		base = base[1:]
		upToDot := strings.SplitN(base, ".", 2)
		i, err := strconv.ParseUint(upToDot[0], 10, 64)
		if err != nil {
			return errors.Wrapf(err, "while parsing %q during TruncateTo", match)
		}
		if i >= index {
			continue
		}
		if err := ss.purgeFile(ctx, match); err != nil {
			return errors.Wrapf(err, "while purging %q", match)
		}
		deleted++
	}

	if deleted == len(matches) {
		err = os.Remove(ss.dir)
		if !os.IsNotExist(err) {
			return errors.Wrapf(err, "while purging %q", ss.dir)
		}
	}
	return nil
}
