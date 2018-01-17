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
	"path/filepath"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

type slKey struct {
	index, term uint64
}

type inMemSideloadStorage struct {
	m      map[slKey][]byte
	prefix string
}

func mustNewInMemSideloadStorage(
	rangeID roachpb.RangeID, replicaID roachpb.ReplicaID, baseDir string,
) sideloadStorage {
	ss, err := newInMemSideloadStorage(cluster.MakeTestingClusterSettings(), rangeID, replicaID, baseDir)
	if err != nil {
		panic(err)
	}
	return ss
}

func newInMemSideloadStorage(
	_ *cluster.Settings, rangeID roachpb.RangeID, replicaID roachpb.ReplicaID, baseDir string,
) (sideloadStorage, error) {
	return &inMemSideloadStorage{
		prefix: filepath.Join(baseDir, fmt.Sprintf("%d.%d", rangeID, replicaID)),
		m:      make(map[slKey][]byte),
	}, nil
}

func (ss *inMemSideloadStorage) key(index, term uint64) slKey {
	return slKey{index: index, term: term}
}

func (ss *inMemSideloadStorage) Dir() string {
	// We could return ss.prefix but real code calling this would then take the
	// result in look for it on the actual file system.
	panic("unsupported")
}

func (ss *inMemSideloadStorage) PutIfNotExists(
	_ context.Context, index, term uint64, contents []byte,
) error {
	key := ss.key(index, term)
	if _, ok := ss.m[key]; ok {
		return nil
	}
	ss.m[key] = contents
	return nil
}

func (ss *inMemSideloadStorage) Get(_ context.Context, index, term uint64) ([]byte, error) {
	key := ss.key(index, term)
	data, ok := ss.m[key]
	if !ok {
		return nil, errSideloadedFileNotFound
	}
	return data, nil
}

func (ss *inMemSideloadStorage) Filename(_ context.Context, index, term uint64) (string, error) {
	return filepath.Join(ss.prefix, fmt.Sprintf("i%d.t%d", index, term)), nil
}

func (ss *inMemSideloadStorage) Purge(_ context.Context, index, term uint64) error {
	k := ss.key(index, term)
	if _, ok := ss.m[k]; !ok {
		return errSideloadedFileNotFound
	}
	delete(ss.m, k)
	return nil
}

func (ss *inMemSideloadStorage) Clear(_ context.Context) error {
	ss.m = make(map[slKey][]byte)
	return nil
}

func (ss *inMemSideloadStorage) TruncateTo(_ context.Context, index uint64) error {
	// Not efficient, but this storage is for testing purposes only anyway.
	for k := range ss.m {
		if k.index < index {
			delete(ss.m, k)
		}
	}
	return nil
}
