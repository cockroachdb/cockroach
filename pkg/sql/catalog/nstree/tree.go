// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package nstree provides a data structure for storing and retrieving
// descriptors namespace entry-like data.
package nstree

import (
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/google/btree"
)

type item interface {
	btree.Item
	put()
	value() interface{}
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

func delete(t *btree.BTree, k item) interface{} {
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
	if iterutil.Done(err) {
		err = nil
	}
	return err
}
