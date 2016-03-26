// Copyright 2014 The Cockroach Authors.
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
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package engine

import (
	"testing"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/testutils"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/stop"
)

const testCacheSize = 1 << 30 // 1 GB

func TestMinMemtableBudget(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rocksdb := NewRocksDB(roachpb.Attributes{}, ".", 0, 0, 0, stop.NewStopper())
	const expected = "memtable budget must be at least"
	if err := rocksdb.Open(); !testutils.IsError(err, expected) {
		t.Fatalf("expected %s, but got %v", expected, err)
	}
}
