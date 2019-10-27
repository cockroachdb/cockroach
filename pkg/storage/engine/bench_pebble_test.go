// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
)

func newPebbleOptions(fs vfs.FS) *pebble.Options {
	return &pebble.Options{
		Cache:                       pebble.NewCache(testCacheSize),
		FS:                          fs,
		MemTableSize:                64 << 20,
		MemTableStopWritesThreshold: 4,
		MinFlushRate:                4 << 20,
		L0CompactionThreshold:       2,
		L0StopWritesThreshold:       400,
		LBaseMaxBytes:               64 << 20, // 64 MB
		Levels: []pebble.LevelOptions{{
			BlockSize: 32 << 10,
		}},
	}
}

func setupMVCCPebble(b testing.TB, dir string) Engine {
	peb, err := NewPebble(dir, newPebbleOptions(vfs.Default))
	if err != nil {
		b.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
	}
	return peb
}

func setupMVCCInMemPebble(b testing.TB, loc string) Engine {
	peb, err := NewPebble("", newPebbleOptions(vfs.NewMem()))
	if err != nil {
		b.Fatalf("could not create new in-mem pebble instance: %+v", err)
	}
	return peb
}

func BenchmarkMVCCComputeStats_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}
	ctx := context.Background()
	for _, valueSize := range []int{8, 32, 256} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCComputeStats(ctx, b, setupMVCCPebble, valueSize)
		})
	}
}

func BenchmarkMVCCFindSplitKey_Pebble(b *testing.B) {
	ctx := context.Background()
	for _, valueSize := range []int{32} {
		b.Run(fmt.Sprintf("valueSize=%d", valueSize), func(b *testing.B) {
			runMVCCFindSplitKey(ctx, b, setupMVCCPebble, valueSize)
		})
	}
}

func BenchmarkMVCCGarbageCollect_Pebble(b *testing.B) {
	if testing.Short() {
		b.Skip("short flag")
	}

	// NB: To debug #16068, test only 128-128-15000-6.
	ctx := context.Background()
	for _, keySize := range []int{128} {
		b.Run(fmt.Sprintf("keySize=%d", keySize), func(b *testing.B) {
			for _, valSize := range []int{128} {
				b.Run(fmt.Sprintf("valSize=%d", valSize), func(b *testing.B) {
					for _, numKeys := range []int{1, 1024} {
						b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
							for _, numVersions := range []int{2, 1024} {
								b.Run(fmt.Sprintf("numVersions=%d", numVersions), func(b *testing.B) {
									runMVCCGarbageCollect(ctx, b, setupMVCCInMemPebble, benchGarbageCollectOptions{
										benchDataOptions: benchDataOptions{
											numKeys:     numKeys,
											numVersions: numVersions,
											valueBytes:  valSize,
										},
										keyBytes:       keySize,
										deleteVersions: numVersions - 1,
									})
								})
							}
						})
					}
				})
			}
		})
	}
}
