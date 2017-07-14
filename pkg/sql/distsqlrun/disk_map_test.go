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
//
// Author: Alfonso Subiotto Marqués (alfonso@cockroachlabs.com)

package distsqlrun

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"sort"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestRocksDBMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tempEngine, err := engine.NewTempEngine(ctx, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	diskMap, err := NewRocksDBMap(0 /* prefix */, tempEngine)
	if err != nil {
		t.Fatal(err)
	}
	defer diskMap.Close(ctx)

	batchWriter := diskMap.NewBatchWriterCapacity(64)
	defer batchWriter.Close(ctx)

	rng := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))

	numKeysToWrite := 1 << 12
	keys := make([]string, numKeysToWrite)
	for i := 0; i < numKeysToWrite; i++ {
		k := []byte(fmt.Sprintf("%d", rng.Int()))
		v := []byte(fmt.Sprintf("%d", rng.Int()))

		keys[i] = string(k)
		// Use batch on every other write.
		if i%2 == 0 {
			if err := diskMap.Put(k, v); err != nil {
				t.Fatal(err)
			}
			// Check key was inserted properly.
			if b, err := diskMap.Get(k); err != nil {
				t.Fatal(err)
			} else if !bytes.Equal(b, v) {
				t.Fatalf("expected %v for value of key %v but got %v", v, k, b)
			}
		} else {
			if err := batchWriter.Put(k, v); err != nil {
				t.Fatal(err)
			}
		}
	}

	sort.StringSlice(keys).Sort()

	if err := batchWriter.Flush(); err != nil {
		t.Fatal(err)
	}

	i := diskMap.NewIterator()
	defer i.Close()

	checkKeyAndPopFirst := func(k []byte) error {
		if !bytes.Equal([]byte(keys[0]), k) {
			return fmt.Errorf("expected %v but got %v", []byte(keys[0]), k)
		}
		keys = keys[1:]
		return nil
	}

	i.Rewind()
	if ok, err := i.Valid(); err != nil {
		t.Fatal(err)
	} else if !ok {
		t.Fatal("unexpectedly invalid")
	}
	lastKey := i.Key()
	if err := checkKeyAndPopFirst(lastKey); err != nil {
		t.Fatal(err)
	}
	i.Next()

	numKeysRead := 1
	for ; ; i.Next() {
		if ok, err := i.Valid(); err != nil {
			t.Fatal(err)
		} else if !ok {
			break
		}
		curKey := i.Key()
		if err := checkKeyAndPopFirst(curKey); err != nil {
			t.Fatal(err)
		}
		if bytes.Compare(curKey, lastKey) < 0 {
			t.Fatalf("expected keys in sorted order but %v is larger than %v", curKey, lastKey)
		}
		lastKey = curKey
		numKeysRead++
	}
	if numKeysRead != numKeysToWrite {
		t.Fatalf("expected to read %d keys but only read %d", numKeysToWrite, numKeysRead)
	}
}

// TestRocksDBMapSandbox verifies that multiple instances of a RocksDBMap
// initialized with the same RocksDB storage engine cannot read or write
// another instance's data.
func TestRocksDBMapSandbox(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	tempEngine, err := engine.NewTempEngine(ctx, base.DefaultTestStoreSpec)
	if err != nil {
		t.Fatal(err)
	}
	defer tempEngine.Close()

	if _, err := NewRocksDBMap(math.MaxUint64, tempEngine); err == nil {
		t.Fatal("expected error when creating map with prefix math.MaxUint64")
	}

	diskMaps := make([]*RocksDBMap, 3)
	for i := 0; i < len(diskMaps); i++ {
		if diskMaps[i], err = NewRocksDBMap(uint64(i) /* prefix */, tempEngine); err != nil {
			t.Fatal(err)
		}
	}

	// Put [0,10) as a key into each diskMap with the value specifying which
	// diskMap inserted this value.
	numKeys := 10
	for i := 0; i < numKeys; i++ {
		for j := 0; j < len(diskMaps); j++ {
			if err := diskMaps[j].Put([]byte{byte(i)}, []byte{byte(j)}); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Verify that an iterator created from a diskMap is constrained to the
	// diskMap's keyspace and that the keys in the keyspace were all written
	// by the expected diskMap.
	t.Run("KeyspaceSandbox", func(t *testing.T) {
		for j := 0; j < len(diskMaps); j++ {
			func() {
				i := diskMaps[j].NewIterator()
				defer i.Close()
				numRead := 0
				for i.Rewind(); ; i.Next() {
					if ok, err := i.Valid(); err != nil {
						t.Fatal(err)
					} else if !ok {
						break
					}
					numRead++
					if numRead > numKeys {
						t.Fatal("read too many keys")
					}
					if int(i.Value()[0]) != j {
						t.Fatalf(
							"key %s in %d's keyspace was clobbered by %d", i.Key(), j, i.Value()[0],
						)
					}
				}
				if numRead < numKeys {
					t.Fatalf("only read %d keys in %d's keyspace", numRead, j)
				}
			}()
		}
	})

	// Verify that a diskMap cleans up its keyspace when closed.
	t.Run("KeyspaceDelete", func(t *testing.T) {
		for j := 0; j < len(diskMaps); j++ {
			diskMaps[j].Close(ctx)
			numKeysRemaining := 0
			func() {
				i := tempEngine.NewIterator(false)
				defer i.Close()
				for i.Seek(engine.NilKey); ; i.Next() {
					if ok, err := i.Valid(); err != nil {
						t.Fatal(err)
					} else if !ok {
						break
					}
					if int(i.Value()[0]) == j {
						t.Fatalf("key %s belonging to %d was not deleted", i.Key(), j)
					}
					numKeysRemaining++
				}
				expectedKeysRemaining := (len(diskMaps) - 1 - j) * numKeys
				if numKeysRemaining != expectedKeysRemaining {
					t.Fatalf(
						"expected %d keys to remain but counted %d",
						expectedKeysRemaining,
						numKeysRemaining,
					)
				}
			}()
		}
	})
}

func BenchmarkRocksDBMapWrite(b *testing.B) {
	dir, err := ioutil.TempDir("", "BenchmarkRocksDBMapWrite")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}()
	ctx := context.Background()
	tempEngine, err := engine.NewTempEngine(ctx, base.StoreSpec{Path: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	rng := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))

	for _, inputSize := range []int{1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20} {
		b.Run(fmt.Sprintf("InputSize%d", inputSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				func() {
					diskMap, err := NewRocksDBMap(uint64(i) /* prefix */, tempEngine)
					if err != nil {
						b.Fatal(err)
					}
					defer diskMap.Close(ctx)
					batchWriter := diskMap.NewBatchWriter()
					// This Close() flushes writes.
					defer batchWriter.Close(ctx)
					for j := 0; j < inputSize; j++ {
						k := fmt.Sprintf("%d", rng.Int())
						v := fmt.Sprintf("%d", rng.Int())
						if err := batchWriter.Put([]byte(k), []byte(v)); err != nil {
							b.Fatal(err)
						}
					}
				}()
			}
		})
	}
}

func BenchmarkRocksDBMapIteration(b *testing.B) {
	dir, err := ioutil.TempDir("", "BenchmarkRocksDBMapIteration")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}()
	ctx := context.Background()
	tempEngine, err := engine.NewTempEngine(ctx, base.StoreSpec{Path: dir})
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	diskMap, err := NewRocksDBMap(0 /* prefix */, tempEngine)
	if err != nil {
		b.Fatal(err)
	}
	defer diskMap.Close(context.Background())

	rng := rand.New(rand.NewSource(int64(timeutil.Now().UnixNano())))

	for _, inputSize := range []int{1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20} {
		for i := 0; i < inputSize; i++ {
			k := fmt.Sprintf("%d", rng.Int())
			v := fmt.Sprintf("%d", rng.Int())
			if err := diskMap.Put([]byte(k), []byte(v)); err != nil {
				b.Fatal(err)
			}
		}

		b.Run(fmt.Sprintf("InputSize%d", inputSize), func(b *testing.B) {
			for j := 0; j < b.N; j++ {
				i := diskMap.NewIterator()
				for i.Rewind(); ; i.Next() {
					if ok, err := i.Valid(); err != nil {
						b.Fatal(err)
					} else if !ok {
						break
					}
					i.Key()
					i.Value()
				}
				i.Close()
			}
		})
	}
}
