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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
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
	e, err := makeJSONEncoder(opts)
	require.NoError(t, err)

	t.Run(`golden`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}

		sinkDir := `golden`
		s, err := makeCloudStorageSink(`nodelocal:///`+sinkDir, 1, unlimitedFileSize, settings, opts)
		require.NoError(t, err)
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1)))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(5)))

		dataFile, err := ioutil.ReadFile(filepath.Join(
			dir, sinkDir, `1970-01-01`, `197001010000000000000010000000000-t1-0-1-7-0.ndjson`))
		require.NoError(t, err)
		require.Equal(t, "v1\n", string(dataFile))

		resolvedFile, err := ioutil.ReadFile(filepath.Join(
			dir, sinkDir, `1970-01-01`, `197001010000000000000050000000000.RESOLVED`))
		require.NoError(t, err)
		require.Equal(t, `{"resolved":"5.0000000000"}`, string(resolvedFile))
	})
	t.Run(`single-node`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}
		t2 := &sqlbase.TableDescriptor{Name: `t2`}

		dir := `single-node`
		s, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, unlimitedFileSize, settings, opts)
		require.NoError(t, err)
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		// Empty flush emits no files.
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string(nil), slurpDir(t, dir))

		// Emitting rows and flushing should write them out in one file per table.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v1`), ts(1)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v2`), ts(1)))
		require.NoError(t, s.EmitRow(ctx, t2, noKey, []byte(`w1`), ts(1)))
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
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"w1\n",
			"v3\n",
		}, slurpDir(t, dir))

		// Data from different versions of a table is put in different files, so
		// that we can guarantee that all rows in any given file have the same
		// schema.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v4`), ts(4)))
		t1.Version = 2
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`v5`), ts(5)))
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\n",
			"w1\n",
			"v3\n",
			"v4\n",
			"v5\n",
		}, slurpDir(t, dir))
	})

	t.Run(`multi-node`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}

		dir := `multi-node`
		s1, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, unlimitedFileSize, settings, opts)
		require.NoError(t, err)
		s2, err := makeCloudStorageSink(`nodelocal:///`+dir, 2, unlimitedFileSize, settings, opts)
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
		// this is unavoidable. It may overwrite the old data if the sink id and
		// file id line up just so, but it's much more likely that they don't.
		// Either way is fine.
		s1R, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, unlimitedFileSize, settings, opts)
		require.NoError(t, err)
		s2R, err := makeCloudStorageSink(`nodelocal:///`+dir, 2, unlimitedFileSize, settings, opts)
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
		// The s1 data overwrites the old file, the s2 data ends up duplicated.
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

		dir := `zombie`
		s1, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, unlimitedFileSize, settings, opts)
		require.NoError(t, err)
		s1.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.
		s2, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, unlimitedFileSize, settings, opts)
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
			"v1\n",
			"v3\n",
		}, slurpDir(t, dir))
	})

	t.Run(`bucketing`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}

		dir := `bucketing`
		const targetMaxFileSize = 6
		s, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, targetMaxFileSize, settings, opts)
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
		// The resolved timestamp file sorts after all data with that timestamp.
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(5)))
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
			"v4\nv5\n",
			`{"resolved":"5.0000000000"}`,
			"v6\nv7\nv8\n",
		}, slurpDir(t, dir))

		// Flush then writes the rest.
		require.NoError(t, s.Flush(ctx))
		require.Equal(t, []string{
			"v1\nv2\nv3\n",
			"v4\nv5\n",
			`{"resolved":"5.0000000000"}`,
			"v6\nv7\nv8\n",
			"v9\n",
		}, slurpDir(t, dir))
	})

	t.Run(`file-ordering`, func(t *testing.T) {
		t1 := &sqlbase.TableDescriptor{Name: `t1`}

		dir := `file-ordering`
		s, err := makeCloudStorageSink(`nodelocal:///`+dir, 1, unlimitedFileSize, settings, opts)
		require.NoError(t, err)
		s.(*cloudStorageSink).sinkID = 7 // Force a deterministic sinkID.

		// Simulate initial scan, which emits data at a timestamp, then an equal
		// resolved timestamp.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`is1`), ts(1)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`is2`), ts(1)))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(1)))

		// Test some edge cases.
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e2`), ts(2)))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3prev`), ts(3).Prev()))
		require.NoError(t, s.EmitRow(ctx, t1, noKey, []byte(`e3`), ts(3)))
		require.NoError(t, s.Flush(ctx))
		require.NoError(t, s.EmitResolvedTimestamp(ctx, e, ts(3)))
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
	})
}
