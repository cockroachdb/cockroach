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
// implied.  See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package storage

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// TODO(spencer): move all of the non-specific engine tests from
// rocksdb_test.go and in_mem_test.go into this file.

// runWithAllEngines creates a new engine of each supported type and
// invokes the supplied test func with each instance.
func runWithAllEngines(test func(e Engine, t *testing.T), t *testing.T) {
	in_mem := NewInMem(Attributes{}, 10<<20)

	loc := fmt.Sprintf("%s/data_%d", os.TempDir(), time.Now().UnixNano())
	rocksdb, err := NewRocksDB(Attributes([]string{"ssd"}), loc)
	if err != nil {
		t.Fatalf("could not create new rocksdb db instance at %s: %v", loc, err)
	}
	defer func(t *testing.T) {
		rocksdb.close()
		if err := rocksdb.destroy(); err != nil {
			t.Errorf("could not delete rocksdb db at %s: %v", loc, err)
		}
	}(t)

	test(in_mem, t)
	test(rocksdb, t)
}

// TestEngineWriteBatch writes a batch containing 50K rows (all the
// same key) and concurrently attempts to read the value in a tight
// loop. The test verifies that either there is no value for the key
// or it contains the final value, but never a value in between.
func TestEngineWriteBatch(t *testing.T) {
	numWrites := 50000
	key := Key("a")
	finalVal := []byte(strconv.Itoa(numWrites - 1))

	runWithAllEngines(func(e Engine, t *testing.T) {
		// Start a concurrent read operation in a busy loop.
		readsBegun := make(chan struct{})
		readsDone := make(chan struct{})
		writesDone := make(chan struct{})
		go func() {
			for i := 0; ; i++ {
				select {
				case <-writesDone:
					close(readsDone)
					return
				default:
					val, err := e.get(key)
					if err != nil {
						t.Fatal(err)
					}
					if val.Bytes != nil && bytes.Compare(val.Bytes, finalVal) != 0 {
						close(readsDone)
						t.Fatalf("key value should be empty or %q; got %q", string(finalVal), string(val.Bytes))
					}
					if i == 0 {
						close(readsBegun)
					}
				}
			}
		}()
		// Wait until we've succeeded with first read.
		<-readsBegun

		// Create key/values and put them in a batch to engine.
		puts := make([]interface{}, numWrites, numWrites)
		for i := 0; i < numWrites; i++ {
			puts[i] = BatchPut{Key: key, Value: Value{Bytes: []byte(strconv.Itoa(i))}}
		}
		if err := e.writeBatch(puts); err != nil {
			t.Fatal(err)
		}
		close(writesDone)
		<-readsDone
	}, t)
}

func TestEngineBatch(t *testing.T) {
	// TODO(Tobias): add a test of writeBatch as soon as D83 merges.
}
