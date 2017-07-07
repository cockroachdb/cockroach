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
	"fmt"
	"path/filepath"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type slKey struct {
	index, term uint64
}

type inMemSideloadStorage struct {
	m      map[slKey][]byte
	prefix string
}

func newInMemSideloadStorage(
	rangeID roachpb.RangeID, replicaID roachpb.ReplicaID, baseDir string,
) sideloadStorage {
	return &inMemSideloadStorage{
		prefix: filepath.Join(baseDir, fmt.Sprintf("%d.%d", rangeID, replicaID)),
		m:      make(map[slKey][]byte),
	}
}

func (imss *inMemSideloadStorage) key(index, term uint64) slKey {
	return slKey{index: index, term: term}
}

func (imss *inMemSideloadStorage) PutIfNotExists(
	_ context.Context, index, term uint64, contents []byte,
) error {
	key := imss.key(index, term)
	if _, ok := imss.m[key]; ok {
		return nil
	}
	imss.m[key] = contents
	return nil
}

func (imss *inMemSideloadStorage) Get(_ context.Context, index, term uint64) ([]byte, error) {
	key := imss.key(index, term)
	data, ok := imss.m[key]
	if !ok {
		return nil, errSideloadedFileNotFound
	}
	return data, nil
}

func (imss *inMemSideloadStorage) Filename(_ context.Context, index, term uint64) (string, error) {
	key := imss.key(index, term)
	_, ok := imss.m[key]
	if !ok {
		return "", errSideloadedFileNotFound
	}
	return filepath.Join(imss.prefix, fmt.Sprintf("i%d.t%d", index, term)), nil
}

func (imss *inMemSideloadStorage) Purge(_ context.Context, index, term uint64) error {
	k := imss.key(index, term)
	if _, ok := imss.m[k]; !ok {
		return errSideloadedFileNotFound
	}
	delete(imss.m, k)
	return nil
}

func (imss *inMemSideloadStorage) Clear(_ context.Context) error {
	imss.m = make(map[slKey][]byte)
	return nil
}

func (imss *inMemSideloadStorage) TruncateTo(_ context.Context, index uint64) error {
	// Not efficient, but this storage is for testing purposes only anyway.
	for k := range imss.m {
		if k.index < index {
			delete(imss.m, k)
		}
	}
	return nil
}
