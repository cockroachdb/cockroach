// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package nstree provides a data structure for storing and retrieving
// descriptors namespace entry-like data.
package nstree

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/google/btree"
)

type item interface {
	btree.Item
	put()
	value() interface{}
}

// degree is totally arbitrary, used for the btree.
const degree = 8

var btreeSyncPool = sync.Pool{
	New: func() interface{} {
		return btree.New(degree)
	},
}

func upsert(t *btree.BTree, toUpsert item) interface{} {
	if overwritten := t.ReplaceOrInsert(toUpsert); overwritten != nil {
		overwrittenItem := overwritten.(item)
		defer overwrittenItem.put()
		return overwrittenItem.value()
	}
	return nil
}

func get(t *btree.BTree, k item) interface{} {
	defer k.put()
	if got := t.Get(k); got != nil {
		return got.(item).value()
	}
	return nil
}

func remove(t *btree.BTree, k item) interface{} {
	defer k.put()
	if deleted, ok := t.Delete(k).(item); ok {
		defer deleted.put()
		return deleted.value()
	}
	return nil
}

func clear(t *btree.BTree) {
	for t.Len() > 0 {
		t.DeleteMin().(item).put()
	}
}

func ascend(t *btree.BTree, f func(k interface{}) error) (err error) {
	t.Ascend(func(i btree.Item) bool {
		err = f(i.(item).value())
		return err == nil
	})
	return iterutil.Map(err)
}

func ascendRange(
	t *btree.BTree, greaterOrEqual, lessThan btree.Item, f func(k interface{}) error,
) (err error) {
	t.AscendRange(greaterOrEqual, lessThan, func(i btree.Item) bool {
		err = f(i.(item).value())
		return err == nil
	})
	return iterutil.Map(err)
}
