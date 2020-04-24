// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
)

// put gives a quick interface for writing a KV pair to a rocksDBMap in tests.
func (r *rocksDBMap) put(k []byte, v []byte) error {
	return r.store.Put(r.makeKeyWithTimestamp(k), v)
}

// Helper function to run a datadriven test for a provided diskMap
func runTestForEngine(ctx context.Context, t *testing.T, filename string, engine diskmap.Factory) {
	diskMaps := make(map[string]diskmap.SortedDiskMap)

	datadriven.RunTest(t, filename, func(t *testing.T, d *datadriven.TestData) string {
		if d.Cmd == "raw-count" {
			var keyCount int
			// Trying to build a common interface to RocksDB and Pebble's iterator
			// creation and usage code involves too much test-specific code, so use
			// a type switch with implementation-specific code instead.
			switch e := engine.(type) {
			case *rocksDBTempEngine:
				iter := e.db.NewIterator(IterOptions{UpperBound: roachpb.KeyMax})
				defer iter.Close()

				for iter.SeekGE(NilKey); ; iter.Next() {
					valid, err := iter.Valid()
					if valid && err == nil {
						keyCount++
					} else if err != nil {
						return fmt.Sprintf("err=%v\n", err)
					} else {
						break
					}
				}
			case *pebbleTempEngine:
				iter := e.db.NewIter(&pebble.IterOptions{UpperBound: roachpb.KeyMax})

				defer func() {
					if err := iter.Close(); err != nil {
						t.Fatal(err)
					}
				}()

				for valid := iter.First(); valid; valid = iter.Next() {
					keyCount++
				}
			default:
				return "unsupported engine type"
			}
			return fmt.Sprintf("count=%d\n", keyCount)
		}

		// All other commands require a map name
		if len(d.CmdArgs) < 1 {
			return "no map name specified"
		}

		switch d.Cmd {
		case "new-map":
			duplicates := false
			for _, arg := range d.CmdArgs[1:] {
				if arg.Key == "duplicates" {
					if len(arg.Vals) < 1 || (arg.Vals[0] != "true" && arg.Vals[0] != "false") {
						return "usage: new-map <name> [duplicates={true, false}]"
					} else if arg.Vals[0] == "true" {
						duplicates = true
					}
				}
			}
			if duplicates {
				diskMaps[d.CmdArgs[0].Key] = engine.NewSortedDiskMultiMap()
			} else {
				diskMaps[d.CmdArgs[0].Key] = engine.NewSortedDiskMap()
			}

			return ""
		}

		diskMap, ok := diskMaps[d.CmdArgs[0].Key]

		if !ok {
			return fmt.Sprintf("unknown map: %s", d.CmdArgs[0].Key)
		}

		switch d.Cmd {
		case "close-map":
			diskMap.Close(ctx)
			return ""

		case "batch":
			batch := diskMap.NewBatchWriter()
			defer func() {
				if err := batch.Close(ctx); err != nil {
					t.Fatal(err)
				}
			}()

			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "put":
					if len(parts) != 3 {
						return fmt.Sprintf("put <key> <value>\n")
					}
					err := batch.Put([]byte(strings.TrimSpace(parts[1])), []byte(strings.TrimSpace(parts[2])))
					if err != nil {
						return err.Error()
					}
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
			}
			return ""

		case "iter":
			iter := diskMap.NewIterator()
			defer iter.Close()
			var b bytes.Buffer
			for _, line := range strings.Split(d.Input, "\n") {
				parts := strings.Fields(line)
				if len(parts) == 0 {
					continue
				}
				switch parts[0] {
				case "next":
					iter.Next()

				case "rewind":
					iter.Rewind()

				case "seek":
					if len(parts) != 2 {
						return fmt.Sprintf("seek <key>\n")
					}
					iter.SeekGE([]byte(strings.TrimSpace(parts[1])))
				default:
					return fmt.Sprintf("unknown op: %s", parts[0])
				}
				valid, err := iter.Valid()
				if valid && err == nil {
					fmt.Fprintf(&b, "%s:%s\n", iter.UnsafeKey(), iter.UnsafeValue())
				} else if err != nil {
					fmt.Fprintf(&b, "err=%v\n", err)
				} else {
					fmt.Fprintf(&b, ".\n")
				}
			}
			return b.String()
		default:
			// No other commands supported.
			return fmt.Sprintf("unsupported command: %s", d.Cmd)
		}
	})
}

func TestRocksDBMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	e := newRocksDBInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	runTestForEngine(ctx, t, "testdata/diskmap", &rocksDBTempEngine{db: e})
}

func TestRocksDBMultiMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	e := newRocksDBInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	runTestForEngine(ctx, t, "testdata/diskmap_duplicates", &rocksDBTempEngine{db: e})
}

func TestRocksDBMapClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	e := newRocksDBInMem(roachpb.Attributes{}, 1<<20)
	defer e.Close()

	decodeKey := func(v []byte) []byte {
		var err error
		v, _, err = encoding.DecodeUvarintAscending(v)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	getSSTables := func() string {
		ssts := e.GetSSTables()
		sort.Slice(ssts, func(i, j int) bool {
			a, b := ssts[i], ssts[j]
			if a.Level < b.Level {
				return true
			}
			if a.Level > b.Level {
				return false
			}
			return a.Start.Less(b.Start)
		})
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\n")
		for i := range ssts {
			fmt.Fprintf(&buf, "%d: %s - %s\n",
				ssts[i].Level, decodeKey(ssts[i].Start.Key), decodeKey(ssts[i].End.Key))
		}
		return buf.String()
	}

	verifySSTables := func(expected string) {
		actual := getSSTables()
		if expected != actual {
			t.Fatalf("expected%sgot%s", expected, actual)
		}
		if testing.Verbose() {
			fmt.Printf("%s", actual)
		}
	}

	diskMap := newRocksDBMap(e, false /* allowDuplicates */)

	// Put a small amount of data into the disk map.
	const letters = "abcdefghijklmnopqrstuvwxyz"
	for i := range letters {
		k := []byte{letters[i]}
		if err := diskMap.put(k, k); err != nil {
			t.Fatal(err)
		}
	}

	// Force the data to disk.
	if err := e.Flush(); err != nil {
		t.Fatal(err)
	}

	// Force it to a lower-level. This is done so as to avoid the automatic
	// compactions out of L0 that would normally occur.
	if err := e.Compact(); err != nil {
		t.Fatal(err)
	}

	// Verify we have a single sstable.
	verifySSTables(`
6: a - z
`)

	// Close the disk map. This should both delete the data, and initiate
	// compactions for the deleted data.
	diskMap.Close(ctx)

	// Wait for the data stored in the engine to disappear.
	testutils.SucceedsSoon(t, func() error {
		actual := getSSTables()
		if testing.Verbose() {
			fmt.Printf("%s", actual)
		}
		if actual != "\n" {
			return fmt.Errorf("%s", actual)
		}
		return nil
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
	tempEngine, _, err := NewRocksDBTempEngine(base.TempStorageConfig{Path: dir}, base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	for _, inputSize := range []int{1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20} {
		b.Run(fmt.Sprintf("InputSize%d", inputSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				func() {
					diskMap := tempEngine.NewSortedDiskMap()
					defer diskMap.Close(ctx)
					batchWriter := diskMap.NewBatchWriter()
					// This Close() flushes writes.
					defer func() {
						if err := batchWriter.Close(ctx); err != nil {
							b.Fatal(err)
						}
					}()
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
	if testing.Short() {
		b.Skip("short flag")
	}
	dir, err := ioutil.TempDir("", "BenchmarkRocksDBMapIteration")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}()
	tempEngine, _, err := NewRocksDBTempEngine(base.TempStorageConfig{Path: dir}, base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	diskMap := tempEngine.NewSortedDiskMap().(*rocksDBMap)
	defer diskMap.Close(context.Background())

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	for _, inputSize := range []int{1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20} {
		for i := 0; i < inputSize; i++ {
			k := fmt.Sprintf("%d", rng.Int())
			v := fmt.Sprintf("%d", rng.Int())
			if err := diskMap.put([]byte(k), []byte(v)); err != nil {
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
					i.UnsafeKey()
					i.UnsafeValue()
				}
				i.Close()
			}
		})
	}
}

func TestPebbleMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	e, _, err := NewPebbleTempEngine(ctx, base.TempStorageConfig{Path: dir}, base.StoreSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	runTestForEngine(ctx, t, "testdata/diskmap", e)

}

func TestPebbleMultiMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()

	e, _, err := NewPebbleTempEngine(ctx, base.TempStorageConfig{Path: dir}, base.StoreSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	runTestForEngine(ctx, t, "testdata/diskmap_duplicates_pebble", e)

}

func TestPebbleMapClose(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	e, _, err := newPebbleTempEngine(ctx, base.TempStorageConfig{Path: dir}, base.StoreSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

	decodeKey := func(v []byte) []byte {
		var err error
		v, _, err = encoding.DecodeUvarintAscending(v)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	getSSTables := func() string {
		var buf bytes.Buffer
		fmt.Fprintf(&buf, "\n")
		for l, ssts := range e.db.SSTables() {
			for _, sst := range ssts {
				fmt.Fprintf(&buf, "%d: %s - %s\n",
					l, decodeKey(sst.Smallest.UserKey), decodeKey(sst.Largest.UserKey))
			}
		}
		return buf.String()
	}

	verifySSTables := func(expected string) {
		actual := getSSTables()
		if expected != actual {
			t.Fatalf("expected%sgot%s", expected, actual)
		}
		if testing.Verbose() {
			fmt.Printf("%s", actual)
		}
	}

	diskMap := newPebbleMap(e.db, false /* allowDuplicates */)

	// Put a small amount of data into the disk map.
	bw := diskMap.NewBatchWriter()
	const letters = "abcdefghijklmnopqrstuvwxyz"
	for i := range letters {
		k := []byte{letters[i]}
		if err := bw.Put(k, k); err != nil {
			t.Fatal(err)
		}
	}
	if err := bw.Close(ctx); err != nil {
		t.Fatal(err)
	}

	// Force the data to disk.
	if err := e.db.Flush(); err != nil {
		t.Fatal(err)
	}

	// Force it to a lower-level. This is done so as to avoid the automatic
	// compactions out of L0 that would normally occur.
	if err := e.db.Compact(diskMap.makeKey([]byte{'a'}), diskMap.makeKey([]byte{'z'})); err != nil {
		t.Fatal(err)
	}

	// Verify we have a single sstable.
	verifySSTables(`
6: a - z
`)

	// Close the disk map. This should both delete the data, and initiate
	// compactions for the deleted data.
	diskMap.Close(ctx)

	// Wait for the data stored in the engine to disappear.
	testutils.SucceedsSoon(t, func() error {
		actual := getSSTables()
		if testing.Verbose() {
			fmt.Printf("%s", actual)
		}
		if actual != "\n" {
			return fmt.Errorf("%s", actual)
		}
		return nil
	})
}

func BenchmarkPebbleMapWrite(b *testing.B) {
	dir, err := ioutil.TempDir("", "BenchmarkPebbleMapWrite")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}()
	ctx := context.Background()
	tempEngine, _, err := NewPebbleTempEngine(ctx, base.TempStorageConfig{Path: dir}, base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	for _, inputSize := range []int{1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20} {
		b.Run(fmt.Sprintf("InputSize%d", inputSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				func() {
					diskMap := tempEngine.NewSortedDiskMap()
					defer diskMap.Close(ctx)
					batchWriter := diskMap.NewBatchWriter()
					// This Close() flushes writes.
					defer func() {
						if err := batchWriter.Close(ctx); err != nil {
							b.Fatal(err)
						}
					}()
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

func BenchmarkPebbleMapIteration(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	dir, err := ioutil.TempDir("", "BenchmarkPebbleMapIteration")
	if err != nil {
		b.Fatal(err)
	}
	defer func() {
		if err := os.RemoveAll(dir); err != nil {
			b.Fatal(err)
		}
	}()
	ctx := context.Background()
	tempEngine, _, err := NewPebbleTempEngine(ctx, base.TempStorageConfig{Path: dir}, base.DefaultTestStoreSpec)
	if err != nil {
		b.Fatal(err)
	}
	defer tempEngine.Close()

	diskMap := tempEngine.NewSortedDiskMap()
	defer diskMap.Close(ctx)

	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))

	for _, inputSize := range []int{1 << 12, 1 << 14, 1 << 16, 1 << 18, 1 << 20} {
		batchWriter := diskMap.NewBatchWriter()
		defer func() {
			if err := batchWriter.Close(ctx); err != nil {
				b.Fatal(err)
			}
		}()

		for i := 0; i < inputSize; i++ {
			k := fmt.Sprintf("%d", rng.Int())
			v := fmt.Sprintf("%d", rng.Int())
			if err := batchWriter.Put([]byte(k), []byte(v)); err != nil {
				b.Fatal(err)
			}
		}

		if err := batchWriter.Flush(); err != nil {
			b.Fatal(err)
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
					i.UnsafeKey()
					i.UnsafeValue()
				}
				i.Close()
			}
		})
	}
}
