// Copyright 2019 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"fmt"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

func TestCloudStorageSink(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()

	dir, dirCleanupFn := testutils.TempDir(t)
	defer dirCleanupFn()

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
			files = append(files, string(file))
			return nil
		}
		absRoot := filepath.Join(dir, root)
		require.NoError(t, os.MkdirAll(absRoot, 0755))
		require.NoError(t, filepath.Walk(absRoot, walkFn))
		return files
	}

	const unlimitedFileSize = math.MaxInt64
	var noKey []byte
	settings := cluster.MakeTestingClusterSettings()
	settings.ExternalIODir = dir
	opts := map[string]string{
		optFormat:     string(optFormatJSON),
		optEnvelope:   string(optEnvelopeWrapped),
		optKeyInValue: ``,
	}
	ts := func(i int64) hlc.Timestamp { return hlc.Timestamp{WallTime: i} }
	highWater := ts(0)
	sessionID := uuid.FastMakeV4().String()
	e, err := makeJSONEncoder(opts)
	require.NoError(t, err)

	t.Run(`golden`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf := makeSpanFrontier(testSpan)
		sinkDir := `golden`
		s, err := makeCloudStorageSink(`nodelocal:///`+sinkDir, 1, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1)))
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
	t.Run(`single-node`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}
		t2 := &sqlbase.TableDescriptor{Name: `t2`}

		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf := makeSpanFrontier(testSpan)
		dir := `single-node`
		s, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		// Empty flush emits no files.
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string(nil), slurpDir(t, dir))

		// Emitting rows and flushing should write them out in one file per table.
		var rows = []struct {
			tableDescriptor *sqlbase.TableDescriptor
			data            []byte
			timestamp       hlc.Timestamp
		}{
			{t1, []byte(`v1`), ts(1)},
			{t1, []byte(`v2`), ts(1)},
			{t2, []byte(`w1`), ts(1)},
		}
		for _, row := range rows {
			require.NoError(t, s.EmitRow(ctx, row.tableDescriptor, noKey, row.data, row.timestamp))
		}
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"w1\n",
		}, slurpDir(t, dir))

		// Flushing with no new emits writes nothing new.
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"w1\n",
		}, slurpDir(t, dir))

		// Without a flush, nothing new shows up.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v3`), ts(3)))
		require.Equal(t, []string{
			"v1\nv2\n",
			"w1\n",
		}, slurpDir(t, dir))

		// Flush and now it does.
		// Note that rows with table descriptor t2 should be read after rows
		// with table descriptor t1.
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"v3\n",
			"w1\n",
		}, slurpDir(t, dir))

		// Data from different versions of a table is put in different files, so
		// that we can guarantee that all rows in any given file have the same
		// schema.

		// We also advance the spanFrontier to make sure these new rows are read
		// after the rows emitted above.
		require.True(t, sf.Forward(testSpan, ts(4)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v4`), ts(4)))
		t1.Version = 2
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v5`), ts(5)))
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"v3\n",
			"w1\n",
			"v4\n",
			"v5\n",
		}, slurpDir(t, dir))
	})

	t.Run(`multi-node`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}

		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf := makeSpanFrontier(testSpan)
		dir := `multi-node`
		s1, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s2, err := makeCloudStorageSink(`nodelocal:///`+dir, 2, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		// Hack into the sinks to pretend each is the first sink created on two
		// different nodes, which is the worst case for them conflicting.
		s1.(*cloudStorageSink).sinkID = 0
		s2.(*cloudStorageSink).sinkID = 0

		// Each node writes some data at the same timestamp. When this data is
		// written out, the files have different names and don't conflict.
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1)))
		require.NoError(t, s2.EmitRow(ctx, t1, noKey, []byte(`w1`), ts(1)))
		require.NoError(t, s1.Flush(ctx))
		require.NoError(t, s2.Flush(ctx))
		require.Equal(t, []string{
			"v1\n",
			"w1\n",
		}, slurpDir(t, dir))

		// If a node restarts then the entire distsql flow has to restart. If
		// this happens before checkpointing, some data is written again but
		// this is unavoidable.
		s1R, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s2R, err := makeCloudStorageSink(`nodelocal:///`+dir, 2, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		// Nodes restart. s1 gets the same sink id it had last time but s2
		// doesn't.
		s1R.(*cloudStorageSink).sinkID = 0
		s2R.(*cloudStorageSink).sinkID = 7
		// Each resends the data it did before.
		require.NoError(t, s1R.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1)))
		require.NoError(t, s2R.EmitRow(ctx, t1, noKey, []byte(`w1`), ts(1)))
		require.NoError(t, s1R.Flush(ctx))
		require.NoError(t, s2R.Flush(ctx))
		// s1 ends up overwriting old data, s2 data ends up being duplicated.
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
		t1 := &sqlbase.TableDescriptor{Name: `t1`}
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf := makeSpanFrontier(testSpan)
		dir := `zombie`
		s1, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s1.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.
		s2, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s2.(*cloudStorageSink).sinkID = 8 // Force a deterministic sinkID.

		// Good job writes
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1)))
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v2`), ts(2)))
		require.NoError(t, s1.Flush(ctx))

		// Zombie job writes partial duplicate data
		require.NoError(t, s2.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1)))
		require.NoError(t, s2.Flush(ctx))

		// Good job continues. There are duplicates in the data but nothing was
		// lost.
		require.NoError(t, s1.EmitRow(ctx, t1, noKey, []byte(`v3`), ts(3)))
		require.NoError(t, s1.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"v3\n",
			"v1\n",
		}, slurpDir(t, dir))
	})

	t.Run(`bucketing`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf := makeSpanFrontier(testSpan)
		dir := `bucketing`
		const targetMaxFileSize = 6
		s, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, sessionID, targetMaxFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		// Writing more than the max file size chunks the file up and flushes it
		// out as necessary.
		for i := int64(1); i <= 5; i++ {
			require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(fmt.Sprintf(`v%d`, i)), ts(i)))
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

		// Forward the spanFrontier here before triggering another flush
		sf.Forward(testSpan, ts(5))

		// Some more data is written. Some of it flushed out because of the max
		// file size.
		for i := int64(6); i < 10; i++ {
			require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(fmt.Sprintf(`v%d`, i)), ts(i)))
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
		// started after the spanFrontier was forwarded to ts(5).
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
		// after the spanFrontier was forwarded to ts(5).
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

	t.Run(`file-ordering`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}
		testSpan := roachpb.Span{Key: []byte("a"), EndKey: []byte("b")}
		sf := makeSpanFrontier(testSpan)
		dir := `file-ordering`
		s, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, sessionID, unlimitedFileSize, settings, opts, sf, highWater)
		require.NoError(t, err)
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		// Simulate initial scan, which emits data at a timestamp, then an equal
		// resolved timestamp.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`is1`), ts(1)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`is2`), ts(1)))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(1)))

		// Test some edge cases.
		require.True(t, sf.Forward(testSpan, ts(2)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e2`), ts(2)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3prev`), ts(3).Prev()))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3`), ts(3)))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(3)))
		require.True(t, sf.Forward(testSpan, ts(3)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3next`), ts(3).Next()))
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
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`noemit`), ts(1).Next()))
		require.Equal(t, []string{
			"is1\nis2\n",
			`{"resolved":"1.0000000000"}`,
			"e2\ne3prev\ne3\n",
			`{"resolved":"3.0000000000"}`,
			"e3next\n",
			`{"resolved":"4.0000000000"}`,
		}, slurpDir(t, dir))
	})
}
