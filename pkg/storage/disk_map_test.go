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
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/diskmap"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
)

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
						return "put <key> <value>\n"
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
						return "seek <key>\n"
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

func TestPebbleMap(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)
	ctx := context.Background()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	e, _, err := newPebbleTempEngine(ctx, base.TempStorageConfig{Path: dir}, base.StoreSpec{})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()

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
	startKey := diskMap.makeKey([]byte{'a'})
	startKeyCopy := make([]byte, len(startKey))
	copy(startKeyCopy, startKey)
	if err := e.db.Compact(startKeyCopy, diskMap.makeKey([]byte{'z'})); err != nil {
		t.Fatal(err)
	}

	// Verify we have a single sstable.
	var buf bytes.Buffer
	fileNums := map[pebble.FileNum]bool{}
	sstInfos, err := e.db.SSTables()
	if err != nil {
		t.Fatal(err)
	}
	for l, ssts := range sstInfos {
		for _, sst := range ssts {
			sm := bytes.TrimPrefix(sst.Smallest.UserKey, diskMap.prefix)
			la := bytes.TrimPrefix(sst.Largest.UserKey, diskMap.prefix)
			fmt.Fprintf(&buf, "%d: %s - %s\n", l, sm, la)
			fileNums[sst.FileNum] = true
		}
	}
	const expected = "6: a - z\n"
	actual := buf.String()
	if expected != actual {
		t.Fatalf("expected\n%sgot\n%s", expected, actual)
	}
	if testing.Verbose() {
		fmt.Printf("%s", actual)
	}

	// Close the disk map. This should both delete the data, and initiate
	// compactions for the deleted data.
	diskMap.Close(ctx)

	// Wait for the previously-observed sstables to be removed. We can't
	// assert that the LSM is completely empty because the range tombstone's
	// sstable will not necessarily be compacted.
	var lsmBuf bytes.Buffer
	var stillExistBuf bytes.Buffer
	testutils.SucceedsSoon(t, func() error {
		lsmBuf.Reset()
		stillExistBuf.Reset()
		sstInfos, err := e.db.SSTables()
		if err != nil {
			return err
		}
		for l, ssts := range sstInfos {
			if len(ssts) == 0 {
				continue
			}

			fmt.Fprintf(&lsmBuf, "L%d:\n", l)
			for _, sst := range ssts {
				if fileNums[sst.FileNum] {
					fmt.Fprintf(&stillExistBuf, "%s\n", sst.FileNum)
				}
				fmt.Fprintf(&lsmBuf, "  %s: %d bytes, %x (%s) - %x (%s)\n", sst.FileNum, sst.Size,
					sst.Smallest.UserKey, bytes.TrimPrefix(sst.Smallest.UserKey, diskMap.prefix),
					sst.Largest.UserKey, bytes.TrimPrefix(sst.Largest.UserKey, diskMap.prefix))
			}
		}

		if testing.Verbose() {
			fmt.Println(buf.String())
		}
		if stillExist := stillExistBuf.String(); stillExist != "" {
			return fmt.Errorf("%s", stillExist)
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
	skip.UnderShort(b)
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
