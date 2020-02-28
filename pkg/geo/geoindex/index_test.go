// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geoindex

import (
	"sort"

	"github.com/cockroachdb/errors"
	"github.com/google/btree"
)

type indexEntry struct {
	key Key
	pk  int
}

func (e indexEntry) Less(than btree.Item) bool {
	k1, k2 := e.key, than.(indexEntry).key
	if k1 == k2 {
		return e.pk < than.(indexEntry).pk
	}
	return k1 < k2
}

type indexStore struct {
	bt *btree.BTree
}

func newIndexStore() *indexStore {
	return &indexStore{bt: btree.New(8)}
}

func (i *indexStore) Write(keys []Key, pk int) {
	for _, key := range keys {
		if existing := i.bt.ReplaceOrInsert(indexEntry{key: key, pk: pk}); existing != nil {
			panic(errors.AssertionFailedf(`inserted twice: (%d,%d)`, key, pk))
		}
	}
}

func (i *indexStore) Read(spans []KeySpan, buf []int) []int {
	buf = buf[:0]
	// TODO(dan): Do this without the map allocation.
	uniquePKs := make(map[int]struct{})
	for _, span := range spans {
		i.bt.AscendGreaterOrEqual(indexEntry{key: span.Start}, func(i btree.Item) bool {
			e := i.(indexEntry)
			if e.key > span.End {
				return false
			}
			uniquePKs[e.pk] = struct{}{}
			return true
		})
	}
	for pk := range uniquePKs {
		buf = append(buf, pk)
	}
	sort.Ints(buf)
	return buf
}
