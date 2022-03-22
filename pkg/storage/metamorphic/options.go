// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package metamorphic

import (
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/pebble"
)

const numStandardOptions = 18
const numRandomOptions = 10

func standardOptions(i int) *pebble.Options {
	stdOpts := []string{
		0: "", // default options
		1: `
[Options]
  cache_size=1
`,
		2: `
[Options]
  l0_compaction_threshold=1
`,
		3: `
[Options]
  l0_compaction_threshold=1
  l0_stop_writes_threshold=1
`,
		4: `
[Options]
  lbase_max_bytes=1
`,
		5: `
[Options]
  max_manifest_file_size=1
`,
		6: `
[Options]
  max_open_files=1
`,
		7: `
[Options]
  mem_table_size=2000
`,
		8: `
[Options]
  mem_table_stop_writes_threshold=2
`,
		9: `
[Options]
  wal_dir=wal
`,
		10: `
[Level "0"]
  block_restart_interval=1
`,
		11: `
[Level "0"]
  block_size=1
`,
		12: `
[Level "0"]
  compression=NoCompression
`,
		13: `
[Level "0"]
  index_block_size=1
`,
		14: `
[Level "0"]
  target_file_size=1
`,
		15: `
[Level "0"]
  filter_policy=none
`,
		// 1GB
		16: `
[Options]
  bytes_per_sync=1073741824
`,
		17: `
[Options]
  max_concurrent_compactions=2
`,
	}
	if i < 0 || i >= len(stdOpts) {
		panic("invalid index for standard option")
	}
	opts := storage.DefaultPebbleOptions()
	if err := opts.Parse(stdOpts[i], nil); err != nil {
		panic(err)
	}
	return opts
}

func randomOptions() *pebble.Options {
	opts := storage.DefaultPebbleOptions()

	rng, _ := randutil.NewTestRand()
	opts.BytesPerSync = 1 << rngIntRange(rng, 8, 30)
	opts.FlushSplitBytes = 1 << rng.Intn(20)
	opts.LBaseMaxBytes = 1 << rngIntRange(rng, 8, 30)
	opts.L0CompactionThreshold = int(rngIntRange(rng, 1, 10))
	opts.L0StopWritesThreshold = int(rngIntRange(rng, 1, 32))
	if opts.L0StopWritesThreshold < opts.L0CompactionThreshold {
		opts.L0StopWritesThreshold = opts.L0CompactionThreshold
	}
	for i := range opts.Levels {
		if i == 0 {
			opts.Levels[i].BlockRestartInterval = int(rngIntRange(rng, 1, 64))
			opts.Levels[i].BlockSize = 1 << rngIntRange(rng, 1, 20)
			opts.Levels[i].BlockSizeThreshold = int(rngIntRange(rng, 50, 100))
			opts.Levels[i].IndexBlockSize = opts.Levels[i].BlockSize
			opts.Levels[i].TargetFileSize = 1 << rngIntRange(rng, 1, 20)
		} else {
			opts.Levels[i] = opts.Levels[i-1]
			opts.Levels[i].TargetFileSize = opts.Levels[i-1].TargetFileSize * 2
		}
	}
	opts.MaxManifestFileSize = 1 << rngIntRange(rng, 1, 28)
	opts.MaxOpenFiles = int(rngIntRange(rng, 20, 2000))
	opts.MemTableSize = 1 << rngIntRange(rng, 11, 28)
	opts.MemTableStopWritesThreshold = int(rngIntRange(rng, 2, 7))
	opts.MaxConcurrentCompactions = int(rngIntRange(rng, 1, 4))

	opts.Cache = pebble.NewCache(1 << rngIntRange(rng, 1, 30))
	defer opts.Cache.Unref()

	return opts
}
