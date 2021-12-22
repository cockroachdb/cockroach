// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/blobs"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/cloud"
	_ "github.com/cockroachdb/cockroach/pkg/cloud/impl" // register cloud storage providers
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/stretchr/testify/require"
)

func makeTopic(name string) tableDescriptorTopic {
	desc := tabledesc.NewBuilder(&descpb.TableDescriptor{Name: name}).BuildImmutableTable()
	return tableDescriptorTopic{desc}
}

func TestCloudStorageSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

	gzipDecompress := func(t *testing.T, compressed []byte) []byte {
		r, err := gzip.NewReader(bytes.NewReader(compressed))
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			require.NoError(t, r.Close())
		}()

		decompressed, err := ioutil.ReadAll(r)
		if err != nil {
			t.Fatal(err)
		}
		return decompressed
	}

	listLeafDirectories := func(root string) []string {
		absRoot := filepath.Join(dir, root)

		var folders []string

		hasChildDirs := func(path string) bool {
			files, err := ioutil.ReadDir(path)
			if err != nil {
				return false
			}
			for _, file := range files {
				if file.IsDir() {
					return true
				}
			}
			return false
		}

		walkFn := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path == absRoot {
				return nil
			}
			if info.IsDir() && !hasChildDirs(path) {
				relPath, _ := filepath.Rel(absRoot, path)
				folders = append(folders, relPath)
			}
			return nil
		}

		require.NoError(t, filepath.Walk(absRoot, walkFn))
		return folders
	}

	// slurpDir returns the contents of every file under root (relative to the
	// temp dir created above), sorted by the name of the file.
	slurpDir := func(t *testing.T, root string) []string {
		var files []string
		walkFn := func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if info.IsDir() {
				return nil
			}
			file, err := ioutil.ReadFile(path)
			if err != nil {
				return err
			}
			if strings.HasSuffix(path, ".gz") {
				file = gzipDecompress(t, file)
			}
			files = append(files, string(file))
			return nil
		}
		absRoot := filepath.Join(dir, root)
		require.NoError(t, os.MkdirAll(absRoot, 0755))
		require.NoError(t, filepath.Walk(absRoot, walkFn))
		return files
	}

	const unlimitedFileSize int64 = math.MaxInt64
	var noKey []byte
	settings := cluster.MakeTestingClusterSettings()
	settings.ExternalIODir = dir
	opts := map[string]string{
		changefeedbase.OptFormat:      string(changefeedbase.OptFormatJSON),
		changefeedbase.OptEnvelope:    string(changefeedbase.OptEnvelopeWrapped),
		changefeedbase.OptKeyInValue:  ``,
		changefeedbase.OptCompression: ``, // NB: overridden in single-node subtest.
	}
	ts := func(i int64) hlc.Timestamp { return hlc.Timestamp{WallTime: i} }
	e, err := makeJSONEncoder(opts, []jobspb.ChangefeedTargetSpecification{})
	require.NoError(t, err)

	clientFactory := blobs.TestBlobServiceClient(settings.ExternalIODir)
	externalStorageFromURI := func(ctx context.Context, uri string, user security.SQLUsername) (cloud.ExternalStorage,
		error) {
		return cloud.ExternalStorageFromURI(ctx, uri, base.ExternalIODirConfig{}, settings,
			clientFactory, user, nil, nil)
	}

	user := security.RootUserName()

	sinkURI := func(dir string, maxFileSize int64) sinkURL {
		uri := `nodelocal://0/` + dir
		if maxFileSize != unlimitedFileSize {
			uri += fmt.Sprintf("?%s=%d", changefeedbase.SinkParamFileSize, maxFileSize)
		}
		u, err := url.Parse(uri)
		require.NoError(t, err)
		return sinkURL{URL: u}
	}

	t.Run(`golden`, func(t *testing.T) {
		t1 := makeTopic(`t1`)
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf, err := span.MakeFrontier(testSpan)
		require.NoError(t, err)
		timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}
		sinkDir := `golden`
		s, err := makeCloudStorageSink(
			ctx, sinkURI(sinkDir, unlimitedFileSize), 1, settings,
			opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s.Flush(ctx))

		require.Equal(t, []string{
			"v1\n",
		}, slurpDir(t, sinkDir))

		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(5)))
		resolvedFile, err := ioutil.ReadFile(filepath.Join(
			dir, sinkDir, `1970-01-01`, `197001010000000000000050000000000.RESOLVED`))
		require.NoError(t, err)
		require.Equal(t, `{"resolved":"5.0000000000"}`, string(resolvedFile))
	})

	forwardFrontier := func(f *span.Frontier, s roachpb.Span, wall int64) bool {
		forwarded, err := f.Forward(s, ts(wall))
		require.NoError(t, err)
		return forwarded
	}

	t.Run(`single-node`, func(t *testing.T) {
		before := opts[changefeedbase.OptCompression]
		// Compression codecs include buffering that interferes with other tests,
		// e.g. the bucketing test that configures very small flush sizes.
		defer func() {
			opts[changefeedbase.OptCompression] = before
		}()
		for _, compression := range []string{"", "gzip"} {
			opts[changefeedbase.OptCompression] = compression
			t.Run("compress="+compression, func(t *testing.T) {
				t1 := makeTopic(`t1`)
				t2 := makeTopic(`t2`)

				testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
				sf, err := span.MakeFrontier(testSpan)
				require.NoError(t, err)
				timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}
				dir := `single-node` + compression
				s, err := makeCloudStorageSink(
					ctx, sinkURI(dir, unlimitedFileSize), 1, settings,
					opts, timestampOracle, externalStorageFromURI, user, nil,
				)
				require.NoError(t, err)
				defer func() { require.NoError(t, s.Close()) }()
				s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

				// Empty flush emits no files.
				require.NoError(t, s.Flush(ctx))
				require.Equal(t, []string(nil), slurpDir(t, dir))

				// Emitting rows and flushing should write them out in one file per table. Note
				// the ordering among these two files is non deterministic as either of them could
				// be flushed first (and thus be assigned fileID 0).
				var pool testAllocPool
				require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1), ts(1), pool.alloc()))
				require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v2`), ts(1), ts(1), pool.alloc()))
				require.NoError(t, s.EmitRow(ctx, t2, noKey, []byte(`w1`), ts(3), ts(3), pool.alloc()))
				require.NoError(t, s.Flush(ctx))
				require.EqualValues(t, 0, pool.used())
				expected := []string{
					"v1\nv2\n",
					"w1\n",
				}
				actual := slurpDir(t, dir)
				sort.Strings(actual)
				require.Equal(t, expected, actual)

				// Flushing with no new emits writes nothing new.
				require.NoError(t, s.Flush(ctx))
				actual = slurpDir(t, dir)
				sort.Strings(actual)
				require.Equal(t, expected, actual)

				// Without a flush, nothing new shows up.
				require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v3`), ts(3), ts(3), zeroAlloc))
				actual = slurpDir(t, dir)
				sort.Strings(actual)
				require.Equal(t, expected, actual)

				// Note that since we haven't forwarded `testSpan` yet, all files initiated until
				// this point must have the same `frontier` timestamp. Since fileID increases
				// monotonically, the last file emitted should be ordered as such.
				require.NoError(t, s.Flush(ctx))
				require.Equal(t, []string{
					"v3\n",
				}, slurpDir(t, dir)[2:])

				// Data from different versions of a table is put in different files, so that we
				// can guarantee that all rows in any given file have the same schema.
				// We also advance `testSpan` and `Flush` to make sure these new rows are read
				// after the rows emitted above.
				require.True(t, forwardFrontier(sf, testSpan, 4))
				require.NoError(t, s.Flush(ctx))
				require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v4`), ts(4), ts(4), zeroAlloc))
				t1.TableDesc().Version = 2
				require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v5`), ts(5), ts(5), zeroAlloc))
				require.NoError(t, s.Flush(ctx))
				expected = []string{
					"v4\n",
					"v5\n",
				}
				actual = slurpDir(t, dir)
				actual = actual[len(actual)-2:]
				sort.Strings(actual)
				require.Equal(t, expected, actual)
			})
		}
	})

	t.Run(`multi-node`, func(t *testing.T) {
		t1 := makeTopic(`t1`)

		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf, err := span.MakeFrontier(testSpan)
		require.NoError(t, err)
		timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}
		dir := `multi-node`
		s1, err := makeCloudStorageSink(
			ctx, sinkURI(dir, unlimitedFileSize), 1,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s1.Close()) }()
		s2, err := makeCloudStorageSink(
			ctx, sinkURI(dir, unlimitedFileSize), 2,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		defer func() { require.NoError(t, s2.Close()) }()
		require.NoError(t, err)
		// Hack into the sinks to pretend each is the first sink created on two
		// different nodes, which is the worst case for them conflicting.
		s1.(*cloudStorageSink).sinkID = 0
		s2.(*cloudStorageSink).sinkID = 0

		// Force deterministic job session IDs to force ordering of output files.
		s1.(*cloudStorageSink).jobSessionID = "a"
		s2.(*cloudStorageSink).jobSessionID = "b"

		// Each node writes some data at the same timestamp. When this data is
		// written out, the files have different names and don't conflict because
		// the sinks have different job session IDs.
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s2.EmitRow(ctx, t1, noKey, []byte(`w1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s1.Flush(ctx))
		require.NoError(t, s2.Flush(ctx))
		require.Equal(t, []string{
			"v1\n",
			"w1\n",
		}, slurpDir(t, dir))

		// If a node restarts then the entire distsql flow has to restart. If
		// this happens before checkpointing, some data is written again but
		// this is unavoidable.
		s1R, err := makeCloudStorageSink(
			ctx, sinkURI(dir, unbuffered), 1,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s1R.Close()) }()
		s2R, err := makeCloudStorageSink(
			ctx, sinkURI(dir, unbuffered), 2,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s2R.Close()) }()
		// Nodes restart. s1 gets the same sink id it had last time but s2
		// doesn't.
		s1R.(*cloudStorageSink).sinkID = 0
		s2R.(*cloudStorageSink).sinkID = 7

		// Again, force deterministic job session IDs to force ordering of output
		// files. Note that making s1R have the same job session ID as s1 should make
		// its output overwrite s1's output.
		s1R.(*cloudStorageSink).jobSessionID = "a"
		s2R.(*cloudStorageSink).jobSessionID = "b"
		// Each resends the data it did before.
		require.NoError(t, s1R.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s2R.EmitRow(ctx, t1, noKey, []byte(`w1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s1R.Flush(ctx))
		require.NoError(t, s2R.Flush(ctx))
		// s1 data ends up being overwritten, s2 data ends up duplicated.
		require.Equal(t, []string{
			"v1\n",
			"w1\n",
			"w1\n",
		}, slurpDir(t, dir))
	})

	// The jobs system can't always clean up perfectly after itself and so there
	// are situations where it will leave a zombie job coordinator for a bit.
	// Make sure the zombie isn't writing the same filenames so that it can't
	// overwrite good data with partial data.
	//
	// This test is also sufficient for verifying the behavior of a multi-node
	// changefeed using this sink. Ditto job restarts.
	t.Run(`zombie`, func(t *testing.T) {
		t1 := makeTopic(`t1`)
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf, err := span.MakeFrontier(testSpan)
		require.NoError(t, err)
		timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}
		dir := `zombie`
		s1, err := makeCloudStorageSink(
			ctx, sinkURI(dir, unlimitedFileSize), 1,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s1.Close()) }()
		s1.(*cloudStorageSink).sinkID = 7         // Force a deterministic sinkID.
		s1.(*cloudStorageSink).jobSessionID = "a" // Force deterministic job session ID.
		s2, err := makeCloudStorageSink(
			ctx, sinkURI(dir, unlimitedFileSize), 1,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s2.Close()) }()
		s2.(*cloudStorageSink).sinkID = 8         // Force a deterministic sinkID.
		s2.(*cloudStorageSink).jobSessionID = "b" // Force deterministic job session ID.

		// Good job writes
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v2`), ts(2), ts(2), zeroAlloc))
		require.NoError(t, s1.Flush(ctx))

		// Zombie job writes partial duplicate data
		require.NoError(t, s2.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s2.Flush(ctx))

		// Good job continues. There are duplicates in the data but nothing was
		// lost.
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v3`), ts(3), ts(3), zeroAlloc))
		require.NoError(t, s1.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"v3\n",
			"v1\n",
		}, slurpDir(t, dir))
	})

	t.Run(`bucketing`, func(t *testing.T) {
		t1 := makeTopic(`t1`)
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf, err := span.MakeFrontier(testSpan)
		require.NoError(t, err)
		timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}
		dir := `bucketing`
		const targetMaxFileSize = 6
		s, err := makeCloudStorageSink(
			ctx, sinkURI(dir, targetMaxFileSize), 1,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		// Writing more than the max file size chunks the file up and flushes it
		// out as necessary.
		for i := int64(1); i <= 5; i++ {
			require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(fmt.Sprintf(`v%d`, i)), ts(i), ts(i), zeroAlloc))
		}
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
		}, slurpDir(t, dir))

		// Flush then writes the rest.
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
			"v4\nv5\n",
		}, slurpDir(t, dir))

		// Forward the SpanFrontier here and trigger an empty flush to update
		// the sink's `inclusiveLowerBoundTs`
		_, err = sf.Forward(testSpan, ts(5))
		require.NoError(t, err)
		require.NoError(t, s.Flush(ctx))

		// Some more data is written. Some of it flushed out because of the max
		// file size.
		for i := int64(6); i < 10; i++ {
			require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(fmt.Sprintf(`v%d`, i)), ts(i), ts(i), zeroAlloc))
		}
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
			"v4\nv5\n",
			"v6\nv7\nv8\n",
		}, slurpDir(t, dir))

		// Resolved timestamps are periodically written. This happens
		// asynchronously from a different node and can be given an earlier
		// timestamp than what's been handed to EmitRow, but the system
		// guarantees that Flush been called (and returned without error) with a
		// ts at >= this one before this call starts.
		//
		// The resolved timestamp file should precede the data files that were
		// started after the SpanFrontier was forwarded to ts(5).
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(5)))
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
			"v4\nv5\n",
			`{"resolved":"5.0000000000"}`,
			"v6\nv7\nv8\n",
		}, slurpDir(t, dir))

		// Flush then writes the rest. Since we use the time of the EmitRow
		// or EmitResolvedTimestamp calls to order files, the resolved timestamp
		// file should precede the last couple files since they started buffering
		// after the SpanFrontier was forwarded to ts(5).
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
			"v4\nv5\n",
			`{"resolved":"5.0000000000"}`,
			"v6\nv7\nv8\n",
			"v9\n",
		}, slurpDir(t, dir))

		// A resolved timestamp emitted with ts > 5 should follow everything
		// emitted thus far.
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(6)))
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
			"v4\nv5\n",
			`{"resolved":"5.0000000000"}`,
			"v6\nv7\nv8\n",
			"v9\n",
			`{"resolved":"6.0000000000"}`,
		}, slurpDir(t, dir))
	})

	t.Run(`partition-formatting`, func(t *testing.T) {
		t1 := makeTopic(`t1`)
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		const targetMaxFileSize = 6

		opts := opts

		timestamps := []time.Time{
			time.Date(2000, time.January, 1, 1, 1, 1, 0, time.UTC),
			time.Date(2000, time.January, 1, 1, 2, 1, 0, time.UTC),
			time.Date(2000, time.January, 1, 2, 1, 1, 0, time.UTC),
			time.Date(2000, time.January, 2, 1, 1, 1, 0, time.UTC),
			time.Date(2000, time.January, 2, 6, 1, 1, 0, time.UTC),
		}

		for i, tc := range []struct {
			format          string
			expectedFolders []string
		}{
			{
				"hourly",
				[]string{
					"2000-01-01/01",
					"2000-01-01/02",
					"2000-01-02/01",
					"2000-01-02/06",
				},
			},
			{
				"daily",
				[]string{
					"2000-01-01",
					"2000-01-02",
				},
			},
			{
				"flat",
				[]string{},
			},
			{
				"", // should fall back to default
				[]string{
					"2000-01-01",
					"2000-01-02",
				},
			},
		} {
			t.Run(tc.format, func(t *testing.T) {
				sf, err := span.MakeFrontier(testSpan)
				require.NoError(t, err)
				timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}

				dir := fmt.Sprintf(`partition-formatting-%d`, i)

				sinkURIWithParam := sinkURI(dir, targetMaxFileSize)
				sinkURIWithParam.addParam(changefeedbase.SinkParamPartitionFormat, tc.format)
				s, err := makeCloudStorageSink(
					ctx, sinkURIWithParam, 1,
					settings, opts, timestampOracle, externalStorageFromURI, user, nil,
				)

				require.NoError(t, err)
				defer func() { require.NoError(t, s.Close()) }()
				s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

				for i, timestamp := range timestamps {
					hlcTime := ts(timestamp.UnixNano())

					// Move the frontier and flush to update the dataFilePartition value
					_, err = sf.Forward(testSpan, hlcTime)
					require.NoError(t, err)
					require.NoError(t, s.Flush(ctx))

					require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(fmt.Sprintf(`v%d`, i)), hlcTime, hlcTime, zeroAlloc))
				}

				require.NoError(t, s.Flush(ctx)) // Flush the last file
				require.ElementsMatch(t, tc.expectedFolders, listLeafDirectories(dir))
				require.Equal(t, []string{"v0\n", "v1\n", "v2\n", "v3\n", "v4\n"}, slurpDir(t, dir))
			})
		}
	})

	t.Run(`file-ordering`, func(t *testing.T) {
		t1 := makeTopic(`t1`)
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf, err := span.MakeFrontier(testSpan)
		require.NoError(t, err)
		timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}
		dir := `file-ordering`
		s, err := makeCloudStorageSink(
			ctx, sinkURI(dir, unlimitedFileSize), 1,
			settings, opts, timestampOracle, externalStorageFromURI, user, nil,
		)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		// Simulate initial scan, which emits data at a timestamp, then an equal
		// resolved timestamp.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`is1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`is2`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(1)))

		// Test some edge cases.

		// Forward the testSpan and trigger an empty `Flush` to have new rows
		// be after the resolved timestamp emitted above.
		require.True(t, forwardFrontier(sf, testSpan, 2))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e2`), ts(2), ts(2), zeroAlloc))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3prev`), ts(3).Prev(), ts(3).Prev(), zeroAlloc))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3`), ts(3), ts(3), zeroAlloc))
		require.True(t, forwardFrontier(sf, testSpan, 3))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(3)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3next`), ts(3).Next(), ts(3).Next(), zeroAlloc))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(4)))

		require.Equal(t, []string{
			"is1\nis2\n",
			`{"resolved":"1.0000000000"}`,
			"e2\ne3prev\ne3\n",
			`{"resolved":"3.0000000000"}`,
			"e3next\n",
			`{"resolved":"4.0000000000"}`,
		}, slurpDir(t, dir))

		// Test that files with timestamp lower than the least resolved timestamp
		// as of file creation time are ignored.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`noemit`), ts(1).Next(), ts(1).Next(), zeroAlloc))
		require.Equal(t, []string{
			"is1\nis2\n",
			`{"resolved":"1.0000000000"}`,
			"e2\ne3prev\ne3\n",
			`{"resolved":"3.0000000000"}`,
			"e3next\n",
			`{"resolved":"4.0000000000"}`,
		}, slurpDir(t, dir))
	})

	t.Run(`ordering-among-schema-versions`, func(t *testing.T) {
		t1 := makeTopic(`t1`)
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf, err := span.MakeFrontier(testSpan)
		require.NoError(t, err)
		timestampOracle := &changeAggregatorLowerBoundOracle{sf: sf}
		dir := `ordering-among-schema-versions`
		var targetMaxFileSize int64 = 10
		s, err := makeCloudStorageSink(
			ctx, sinkURI(dir, targetMaxFileSize), 1, settings,
			opts, timestampOracle, externalStorageFromURI, user, nil)
		require.NoError(t, err)
		defer func() { require.NoError(t, s.Close()) }()

		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1), ts(1), zeroAlloc))
		t1.TableDesc().Version = 1
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v3`), ts(1), ts(1), zeroAlloc))
		// Make the first file exceed its file size threshold. This should trigger a flush
		// for the first file but not the second one.
		t1.TableDesc().Version = 0
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`trigger-flush-v1`), ts(1), ts(1), zeroAlloc))
		require.Equal(t, []string{
			"v1\ntrigger-flush-v1\n",
		}, slurpDir(t, dir))

		// Now make the file with the newer schema exceed its file size threshold and ensure
		// that the file with the older schema is flushed (and ordered) before.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v2`), ts(1), ts(1), zeroAlloc))
		t1.TableDesc().Version = 1
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`trigger-flush-v3`), ts(1), ts(1), zeroAlloc))
		require.Equal(t, []string{
			"v1\ntrigger-flush-v1\n",
			"v2\n",
			"v3\ntrigger-flush-v3\n",
		}, slurpDir(t, dir))

		// Calling `Flush()` on the sink should emit files in the order of their schema IDs.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`w1`), ts(1), ts(1), zeroAlloc))
		t1.TableDesc().Version = 0
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`x1`), ts(1), ts(1), zeroAlloc))
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\ntrigger-flush-v1\n",
			"v2\n",
			"v3\ntrigger-flush-v3\n",
			"x1\n",
			"w1\n",
		}, slurpDir(t, dir))
	})
}
