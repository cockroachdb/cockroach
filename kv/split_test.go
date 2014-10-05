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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Tobias Schottdorf (tobias.schottdorf@gmail.com)

package kv

import (
	"testing"

	"github.com/cockroachdb/cockroach/storage/engine"
)

func TestRangeCoordinateSplit(t *testing.T) {
	db, _, _ := createTestDB(t)
	defer db.Close()

	store, err := db.kv.(*LocalKV).GetStore(1)
	if err != nil {
		t.Fatal(err)
	}
	rng, err := store.GetRange(1)
	if err != nil {
		t.Fatal(err)
	}
	splitKey := engine.Key("m")

	err = rng.CoordinateSplit(splitKey)
	if err != nil {
		t.Fatal(err)
	}
}
