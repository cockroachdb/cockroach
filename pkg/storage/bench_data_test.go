// Copyright 2022 The Cockroach Authors.
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
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testfixtures"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

type initialState interface {
	// Base may return an initialState to extend. If Base is non-nil, Build will
	// be supplied an engine with the initial state Base. This allows initial
	// states to be layered, ensuring that lower layers are computed once and
	// cached.
	Base() initialState

	// Key returns a unique sequence of strings that uniquely identifies the
	// represented initial conditions. Key is used as the cache key for reusing
	// databases computed by previous runs, so all configuration must be fully
	// represented in Key's return value.
	Key() []string

	// ConfigOptions reutrns additional configuration options that should be
	// supplied to Open.
	ConfigOptions() []ConfigOption

	// Build is called when no cached version of the engine state exists yet.
	// Build must populate the provided engine appropriately. Build must produce
	// equivalent engine state across initialConditions with equal Key() values.
	Build(context.Context, *testing.B, Engine) error
}

type engineWithLocation struct {
	Engine
	Location
}

// TODO(jackson): Tie this to the mapping in SetMinVersion.
var latestReleaseFormatMajorVersion = pebble.FormatPrePebblev1Marked // v22.2

var latestReleaseFormatMajorVersionOpt ConfigOption = func(cfg *engineConfig) error {
	cfg.PebbleConfig.Opts.FormatMajorVersion = latestReleaseFormatMajorVersion
	return nil
}

// getInitialStateEngine constructs an Engine with an initial database state
// necessary for a benchmark. The initial states are cached on the filesystem to
// avoid expensive reconstruction when possible (see
// testfixtures.ReuseOrGenerate).
//
// The return value of initial.Key() must be unique for each unique initial database
// configuration, because the Key() value is used to key cached initial databases.
func getInitialStateEngine(
	ctx context.Context, b *testing.B, initial initialState, inMemory bool,
) engineWithLocation {
	name := strings.Join(initial.Key(), "-")
	dir := testfixtures.ReuseOrGenerate(b, name, func(dir string) {
		buildInitialState(ctx, b, initial, dir)
	})
	dataDir := filepath.Join(dir, "data")

	opts := append([]ConfigOption{
		MustExist,
		latestReleaseFormatMajorVersionOpt,
	}, initial.ConfigOptions()...)

	var loc Location
	if inMemory {
		loc = InMemory()
	} else {
		// The caller wants a durable engine; use a temp directory.
		loc = Filesystem(b.TempDir())
	}

	// We now copy the initial state to the desired FS.
	ok, err := vfs.Clone(vfs.Default, loc.fs, dataDir, loc.dir, vfs.CloneSync)
	require.NoError(b, err)
	require.True(b, ok)

	if !inMemory {
		// Load all the files into the OS buffer cache for better determinism.
		testutils.ReadAllFiles(filepath.Join(loc.dir, "*"))
	}

	e, err := Open(ctx, loc, cluster.MakeClusterSettings(), opts...)
	require.NoError(b, err)
	return engineWithLocation{Engine: e, Location: loc}
}

func buildInitialState(
	ctx context.Context, b *testing.B, initial initialState, dir string,
) (buildFS vfs.FS) {
	dataDir := filepath.Join(dir, "data")
	// The data directory might exist and be non-empty if the previous run did
	// not complete successfully. Remove it.
	require.NoError(b, vfs.Default.RemoveAll(dataDir))
	require.NoError(b, os.MkdirAll(dataDir, os.ModePerm))

	// If the initial conditions specify a base, we can compute the initial
	// conditions recursively. For example, if we have two variants of an
	// initial state, f(A) and g(A), we can compute and persist A. Then f(A) and
	// g(A) may be computed starting from A's state.
	if base := initial.Base(); base != nil {
		e := getInitialStateEngine(ctx, b, base, true /* inMemory */)
		require.NoError(b, initial.Build(ctx, b, e))
		e.Close()
		buildFS = e.Location.fs
	} else {
		opts := append([]ConfigOption{latestReleaseFormatMajorVersionOpt}, initial.ConfigOptions()...)

		// Regardless of whether the initial conditions specify an in-memory engine
		// or not, we build the conditions using an in-memory engine for
		// performance.
		buildFS = vfs.NewMem()

		var err error
		e, err := Open(ctx, Location{fs: buildFS}, cluster.MakeClusterSettings(), opts...)

		require.NoError(b, err)

		require.NoError(b, initial.Build(ctx, b, e))
		e.Close()
	}

	// Write the initial state out to disk.
	ok, err := vfs.Clone(buildFS, vfs.Default, "", dataDir, vfs.CloneSync)
	require.NoError(b, err)
	require.True(b, ok)

	return buildFS
}

type buildFunc func(ctx context.Context, b *testing.B, eng Engine) error

func extendInitialConditions(base initialState, apply buildFunc, key ...string) initialState {
	return extendedInitial{
		base:  base,
		apply: apply,
		keys:  key,
	}
}

// extendedInitial wraps another initialConditions, layering on a buildFunc.
type extendedInitial struct {
	base  initialState
	apply buildFunc
	keys  []string
}

func (e extendedInitial) Base() initialState            { return e.base }
func (e extendedInitial) Key() []string                 { return append(e.base.Key(), e.keys...) }
func (e extendedInitial) ConfigOptions() []ConfigOption { return e.base.ConfigOptions() }
func (e extendedInitial) Build(ctx context.Context, b *testing.B, eng Engine) error {
	return e.apply(ctx, b, eng)
}

func withCompactedDB(base initialState) initialState {
	return extendInitialConditions(base,
		func(ctx context.Context, b *testing.B, eng Engine) error {
			return eng.Compact()
		}, "compacted")
}

// mvccBenchData implements initialConditions, initializing a database with a
// configurable count of MVCC keys.
type mvccBenchData struct {
	numVersions       int
	numKeys           int
	valueBytes        int
	numColumnFamilies int
	numRangeKeys      int // MVCC range tombstones, 1=global

	// If garbage is enabled, point keys will be tombstones rather than values.
	// Furthermore, range keys (controlled by numRangeKeys) will be written above
	// the point keys rather than below them.
	garbage bool

	// In transactional mode, data is written by writing and later resolving
	// intents. In non-transactional mode, data is written directly, without
	// leaving intents. Transactional mode notably stresses RocksDB deletion
	// tombstones, as the metadata key is repeatedly written and deleted.
	//
	// Both modes are reflective of real workloads. Transactional mode simulates
	// data that has recently been INSERTed into a table, while non-transactional
	// mode simulates data that has been RESTOREd or is old enough to have been
	// fully compacted.
	transactional bool
}

var _ initialState = mvccBenchData{}

func (d mvccBenchData) Key() []string {
	key := []string{
		"mvcc",
		fmt.Sprintf("fmtver_%d", latestReleaseFormatMajorVersion),
		fmt.Sprintf("numKeys_%d", d.numKeys),
		fmt.Sprintf("numVersions_%d", d.numVersions),
		fmt.Sprintf("valueBytes_%d", d.valueBytes),
		fmt.Sprintf("numColFams_%d", d.numColumnFamilies),
		fmt.Sprintf("numRangeKeys_%d", d.numRangeKeys),
	}
	if d.garbage {
		key = append(key, fmt.Sprintf("garbage_%t", d.garbage))
	}
	if d.transactional {
		key = append(key, fmt.Sprintf("transactional_%t", d.transactional))
	}
	return key
}
func (d mvccBenchData) ConfigOptions() []ConfigOption { return nil }
func (d mvccBenchData) Base() initialState            { return nil }
func (d mvccBenchData) Build(ctx context.Context, b *testing.B, eng Engine) error {
	// Writes up to numVersions values at each of numKeys keys. The number of
	// versions written for each key is chosen randomly according to a uniform
	// distribution. Each successive version is written starting at 5ns and then
	// in 5ns increments. This allows scans at various times, starting at t=5ns,
	// and continuing to t=5ns*(numVersions+1). A version for each key will be
	// read on every such scan, but the dynamics of the scan will change
	// depending on the historical timestamp. Earlier timestamps mean scans
	// which must skip more historical versions; later timestamps mean scans
	// which skip fewer.
	//
	// MVCC range keys are written below all point keys unless garbage=true.
	// They're always written with random start/end bounds, at increasing
	// logical timestamps (WallTime 0).
	//
	// The creation of the database is time consuming, especially for larger
	// numbers of versions. The database is persisted between runs and stored in
	// a directory with the path elements returned by Key().

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	// Write MVCC range keys. If garbage is enabled, they will be written on top
	// of the point keys, otherwise they will be written below them.
	writeRangeKeys := func(b testing.TB, wallTime int) {
		batch := eng.NewBatch()
		defer batch.Close()
		for i := 0; i < d.numRangeKeys; i++ {
			ts := hlc.Timestamp{WallTime: int64(wallTime), Logical: int32(i + 1)}
			start := rng.Intn(d.numKeys)
			end := start + rng.Intn(d.numKeys-start) + 1
			// As a special case, if we're only writing one range key, write it across
			// the entire span.
			if d.numRangeKeys == 1 {
				start = 0
				end = d.numKeys + 1
			}
			startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(start)))
			endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(end)))
			require.NoError(b, MVCCDeleteRangeUsingTombstone(
				ctx, batch, nil, startKey, endKey, ts, hlc.ClockTimestamp{}, nil, nil, false, 0, nil))
		}
		require.NoError(b, batch.Commit(false /* sync */))
	}
	if !d.garbage {
		writeRangeKeys(b, 0 /* wallTime */)
	}

	// Generate point keys.
	keySlice := make([]roachpb.Key, d.numKeys)
	var order []int
	var cf uint32
	for i := 0; i < d.numKeys; i++ {
		if d.numColumnFamilies > 0 {
			keySlice[i] = makeBenchRowKey(b, nil, i/d.numColumnFamilies, cf)
			cf = (cf + 1) % uint32(d.numColumnFamilies)
		} else {
			keySlice[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
		}
		keyVersions := rng.Intn(d.numVersions) + 1
		for j := 0; j < keyVersions; j++ {
			order = append(order, i)
		}
	}

	// Randomize the order in which the keys are written.
	for i, n := 0, len(order); i < n-1; i++ {
		j := i + rng.Intn(n-i)
		order[i], order[j] = order[j], order[i]
	}

	counts := make([]int, d.numKeys)

	var txn *roachpb.Transaction
	if d.transactional {
		txnCopy := *txn1Commit
		txn = &txnCopy
	}

	writeKey := func(batch Batch, idx int) {
		key := keySlice[idx]
		var value roachpb.Value
		if !d.garbage {
			value = roachpb.MakeValueFromBytes(randutil.RandBytes(rng, d.valueBytes))
			value.InitChecksum(key)
		}
		counts[idx]++
		ts := hlc.Timestamp{WallTime: int64(counts[idx] * 5)}
		if txn != nil {
			txn.ReadTimestamp = ts
			txn.WriteTimestamp = ts
		}
		require.NoError(b, MVCCPut(ctx, batch, nil, key, ts, hlc.ClockTimestamp{}, value, txn))
	}

	resolveLastIntent := func(batch Batch, idx int) {
		key := keySlice[idx]
		txnMeta := txn.TxnMeta
		txnMeta.WriteTimestamp = hlc.Timestamp{WallTime: int64(counts[idx]) * 5}
		if _, _, _, err := MVCCResolveWriteIntent(ctx, batch, nil /* ms */, roachpb.LockUpdate{
			Span:   roachpb.Span{Key: key},
			Status: roachpb.COMMITTED,
			Txn:    txnMeta,
		}, MVCCResolveWriteIntentOptions{}); err != nil {
			b.Fatal(err)
		}
	}

	batch := eng.NewBatch()
	for i, idx := range order {
		// Output the keys in ~20 batches. If we used a single batch to output all
		// of the keys rocksdb would create a single sstable. We want multiple
		// sstables in order to exercise filtering of which sstables are examined
		// during iterator seeking. We fix the number of batches we output so that
		// optimizations which change the data size result in the same number of
		// sstables.
		if scaled := len(order) / 20; i > 0 && (i%scaled) == 0 {
			log.Infof(ctx, "committing (%d/~%d)", i/scaled, 20)
			if err := batch.Commit(false /* sync */); err != nil {
				return err
			}
			batch.Close()
			batch = eng.NewBatch()
			if err := eng.Flush(); err != nil {
				return err
			}
		}

		if d.transactional {
			// If we've previously written this key transactionally, we need to
			// resolve the intent we left. We don't do this immediately after writing
			// the key to introduce the possibility that the intent's resolution ends
			// up in a different batch than writing the intent itself. Note that the
			// first time through this loop for any given key we'll attempt to resolve
			// a non-existent intent, but that's OK.
			resolveLastIntent(batch, idx)
		}
		writeKey(batch, idx)
	}
	if d.transactional {
		// If we were writing transactionally, we need to do one last round of
		// intent resolution. Just stuff it all into the last batch.
		for idx := range keySlice {
			resolveLastIntent(batch, idx)
		}
	}
	if err := batch.Commit(false /* sync */); err != nil {
		return err
	}
	batch.Close()

	// If we're writing garbage, write MVCC range tombstones on top of the
	// point keys.
	if d.garbage {
		writeRangeKeys(b, 10*d.numVersions)
	}

	if err := eng.Flush(); err != nil {
		return err
	}

	return nil
}
