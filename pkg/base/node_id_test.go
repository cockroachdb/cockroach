// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package base_test

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestNodeIDContainer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	n := &base.NodeIDContainer{}

	if val := n.Get(); val != 0 {
		t.Errorf("initial value should be 0, not %d", val)
	}
	if str := n.String(); str != "?" {
		t.Errorf("initial string should be ?, not %s", str)
	}

	for i := 0; i < 2; i++ {
		n.Set(context.Background(), 5)
		if val := n.Get(); val != 5 {
			t.Errorf("value should be 5, not %d", val)
		}
		if str := n.String(); str != "5" {
			t.Errorf("string should be 5, not %s", str)
		}
	}

	n.Reset(6)
	if val := n.Get(); val != 6 {
		t.Errorf("value should be 6, not %d", val)
	}
	if str := n.String(); str != "6" {
		t.Errorf("string should be 6, not %s", str)
	}
}

func TestStoreIDPebbleLog(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tempstore := &base.StoreIDContainer{}
	if val := tempstore.Get(); val != 0 {
		t.Errorf("store ID for temp store should be -1, not %d", val)
	}
	tempstore.Set(context.Background(), base.TempStoreID)
	if val := tempstore.Get(); val != base.TempStoreID {
		t.Errorf(
			"store ID for temp store is incorrect, expected %d, but got %d",
			base.TempStoreID, val)
	}
	if str := tempstore.String(); str != "temp" {
		t.Errorf("String method for temp store should return, temp, not %s", str)
	}

	store := &base.StoreIDContainer{}
	if val := store.Get(); val != 0 {
		t.Errorf("store ID for store should initially be 0, not %d", val)
	}
	if str := store.String(); str != "?" {
		t.Errorf("initial string should be ?, not %s", str)
	}

	store.Set(context.Background(), 5)
	if val := store.Get(); val != 5 {
		t.Errorf("value should be 5, not %d", val)
	}
	if str := store.String(); str != "5" {
		t.Errorf("string should be 5, not %s", str)
	}
}
