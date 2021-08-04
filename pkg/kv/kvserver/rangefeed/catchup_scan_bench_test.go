package rangefeed_test

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors/oserror"
	"github.com/cockroachdb/pebble/vfs"
)

func runCatchupBenchmark(b *testing.B, emk engineMaker, opts benchDataOptions) {
	eng, _ := setupAppendOnlyData(context.Background(), b, emk, opts)
	defer eng.Close()

	startKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(0)))
	endKey := roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(opts.numKeys)))
	span := roachpb.Span{
		Key:    startKey,
		EndKey: endKey,
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		iter := rangefeed.NewCatchupIterator(eng, &roachpb.RangeFeedRequest{
			Header: roachpb.Header{
				Timestamp: opts.ts,
			},
			WithDiff: opts.withDiff,
			Span:     span,
		}, opts.useTBI, func() {})
		counter := 0
		err := iter.CatchupScan(storage.MakeMVCCMetadataKey(startKey), storage.MakeMVCCMetadataKey(endKey), opts.ts, opts.withDiff, func(*roachpb.RangeFeedEvent) error {
			counter++
			return nil
		})
		if err != nil {
			b.Fatalf("failed catchup scan: %+v", err)
		}
		if counter < 1 {
			b.Fatalf("didn't emit any events!")
		}
		b.Logf("Emitted %d events", counter)
	}
}

func BenchmarkCatchupScan(b *testing.B) {
	numKeys := 1_000_000
	valueBytes := 512

	for _, useTBI := range []bool{true, false} {
		b.Run(fmt.Sprintf("useTBI=%v", useTBI), func(b *testing.B) {
			for _, withDiff := range []bool{true, false} {
				b.Run(fmt.Sprintf("withDiff=%v", withDiff), func(b *testing.B) {
					for _, tsExcludePercent := range []float64{0.999} {
						wallTime := int64((5 * (float64(numKeys)*tsExcludePercent + 1)))
						ts := hlc.Timestamp{WallTime: wallTime}
						b.Run(fmt.Sprintf("ts=%d", ts.WallTime), func(b *testing.B) {
							runCatchupBenchmark(b, setupMVCCPebble, benchDataOptions{
								numKeys:    numKeys,
								valueBytes: valueBytes,

								ts:       ts,
								useTBI:   useTBI,
								withDiff: withDiff,
							})
						})
					}
				})
			}
		})
	}
}

type benchDataOptions struct {
	numVersions int
	numKeys     int
	valueBytes  int

	ts       hlc.Timestamp
	useTBI   bool
	withDiff bool
}

//
// The following code was copied and then modified from the testing
// code in pkg/storage.
//

type engineMaker func(testing.TB, string) storage.Engine

// const testCacheSize = 1 << 30 // 1 GB

func setupMVCCPebble(b testing.TB, dir string) storage.Engine {
	opts := storage.DefaultPebbleOptions()
	opts.FS = vfs.Default
	// opts.Cache = pebble.NewCache(testCacheSize)
	// defer opts.Cache.Unref()

	peb, err := storage.NewPebble(
		context.Background(),
		storage.PebbleConfig{
			StorageConfig: base.StorageConfig{
				Dir: dir,
				Settings: makeSettingsForSeparatedIntents(
					false /* oldClusterVersion */, true /* enabled */),
			},
			Opts: opts,
		})
	if err != nil {
		b.Fatalf("could not create new pebble instance at %s: %+v", dir, err)
	}
	return peb
}

func makeSettingsForSeparatedIntents(oldClusterVersion bool, enabled bool) *cluster.Settings {
	version := clusterversion.ByKey(clusterversion.SeparatedIntents)
	if oldClusterVersion {
		version = clusterversion.ByKey(clusterversion.V20_2)
	}
	settings := cluster.MakeTestingClusterSettingsWithVersions(version, version, true)
	storage.SeparatedIntentsEnabled.Override(context.TODO(), &settings.SV, enabled)
	return settings
}

// setupAppendOnly data writes numKeys keys. One version of each key
// is written. The write timestamp starts at 5ns and then in 5ns
// increments. This allows scans at various times, starting at t=5ns,
// and continuing to t=5ns*(numVersions+1). The goal of this is to
// approximate an append-only type workload.
//
// The creation of the database is time consuming, especially for larger
// numbers of versions. The database is persisted between runs and stored in
// the current directory as "mvcc_append_data_<keys>_<valueBytes>" (which
// is also returned).
func setupAppendOnlyData(
	ctx context.Context, b *testing.B, emk engineMaker, opts benchDataOptions,
) (storage.Engine, string) {
	loc := fmt.Sprintf("mvcc_append_data_%d_%d", opts.numKeys, opts.valueBytes)
	exists := true
	if _, err := os.Stat(loc); oserror.IsNotExist(err) {
		exists = false
	} else if err != nil {
		b.Fatal(err)
	}

	eng := emk(b, loc)

	if exists {
		testutils.ReadAllFiles(filepath.Join(loc, "*"))
		return eng, loc
	}

	log.Infof(ctx, "creating mvcc append-only data: %s", loc)

	// Generate the same data every time.
	rng := rand.New(rand.NewSource(1449168817))

	keys := make([]roachpb.Key, opts.numKeys)
	order := make([]int, 0, opts.numKeys)
	for i := 0; i < opts.numKeys; i++ {
		keys[i] = roachpb.Key(encoding.EncodeUvarintAscending([]byte("key-"), uint64(i)))
		order = append(order, i)
	}

	// Randomize the order in which the keys are written.
	for i, n := 0, len(order); i < n-1; i++ {
		j := i + rng.Intn(n-i)
		order[i], order[j] = order[j], order[i]
	}

	var txn *roachpb.Transaction

	writeKey := func(batch storage.Batch, idx int, pos int) {
		key := keys[idx]
		value := roachpb.MakeValueFromBytes(randutil.RandBytes(rng, opts.valueBytes))
		value.InitChecksum(key)
		ts := hlc.Timestamp{WallTime: int64((pos + 1) * 5)}
		if txn != nil {
			txn.ReadTimestamp = ts
			txn.WriteTimestamp = ts
		}
		if err := storage.MVCCPut(ctx, batch, nil /* ms */, key, ts, value, txn); err != nil {
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
			log.Infof(ctx, "committing (%d/~%d) (%d/%d)", i/scaled, 20, i, len(order))
			if err := batch.Commit(false /* sync */); err != nil {
				b.Fatal(err)
			}
			batch.Close()
			batch = eng.NewBatch()
			if err := eng.Flush(); err != nil {
				b.Fatal(err)
			}
		}
		writeKey(batch, idx, i)
	}
	if err := batch.Commit(false /* sync */); err != nil {
		b.Fatal(err)
	}
	batch.Close()
	if err := eng.Flush(); err != nil {
		b.Fatal(err)
	}

	return eng, loc
}
