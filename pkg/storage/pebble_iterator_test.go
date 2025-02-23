// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	stdfs "io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestPebbleIterator_Corruption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a Pebble DB that can be used to back a pebbleIterator.
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	p, err := Open(context.Background(), fs.MustInitPhysicalTestingEnv(dataDir), cluster.MakeClusterSettings())
	require.NoError(t, err)
	defer p.Close()

	// Insert some data into the DB and flush to create an SST.
	ek := engineKey("foo", 0)
	require.NoError(t, p.PutEngineKey(ek, nil))
	require.NoError(t, p.Flush())

	// Corrupt the SSTs in the DB.
	err = filepath.WalkDir(dataDir, func(path string, d stdfs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(d.Name(), ".sst") {
			return nil
		}
		file, err := os.OpenFile(path, os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = file.WriteAt([]byte("uh oh"), 0)
		if err != nil {
			return err
		}
		_ = file.Close()
		return nil
	})
	require.NoError(t, err)

	// Construct a pebbleIterator over the DB.
	iterOpts := IterOptions{
		LowerBound: []byte("a"),
		UpperBound: []byte("z"),
	}
	iter, err := newPebbleIterator(context.Background(), p.db, iterOpts, StandardDurability, p)
	require.NoError(t, err)

	// Seeking into the table catches the corruption.
	ok, err := iter.SeekEngineKeyGE(ek)
	require.False(t, ok)
	require.True(t, errors.Is(err, pebble.ErrCorruption))

	// Closing the iter results in a panic due to the corruption.
	require.Panics(t, func() { iter.Close() })

	// Should have laid down marker file to prevent startup.
	_, err = p.Env().Stat(base.PreventedStartupFile(p.GetAuxiliaryDir()))
	require.NoError(t, err)
}

func randStr(fill []byte, rng *rand.Rand) {
	const letters = "abcdefghijklmnopqrstuvwxyz"
	const lettersLen = len(letters)
	for i := 0; i < len(fill); i++ {
		fill[i] = letters[rng.Intn(lettersLen)]
	}
}

func TestPebbleIterator_ExternalCorruption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	st := cluster.MakeTestingClusterSettings()
	ctx := context.Background()
	rng := rand.New(rand.NewSource(timeutil.Now().UnixNano()))
	var f bytes.Buffer
	w := MakeTransportSSTWriter(ctx, st, &f)

	// Create an example sstable.
	var rawValue [64]byte
	for i := 0; i < 26; i++ {
		const numVersions = 10
		for j := 0; j < numVersions; j++ {
			randStr(rawValue[:], rng)
			v, err := EncodeMVCCValue(MVCCValue{Value: roachpb.MakeValueFromBytes(rawValue[:])})
			require.NoError(t, err)
			require.NoError(t, w.Put(pointKey(string(rune('a'+i)), numVersions-j), v))
		}
	}
	require.NoError(t, w.Finish())

	// Trash a random byte.
	b := f.Bytes()

	// If we mess with the format byte, we will get an unexpected error.
	// See https://github.com/cockroachdb/cockroach/issues/141477 and
	// TODO(radu): This can be removed if Pebble checksums the footer:
	// https://github.com/cockroachdb/pebble/issues/4344
	const nAvoidLastBytes = 12
	//b[rng.Intn(len(b)-nAvoidLastBytes)]++
	b[len(b)-60+rng.Intn(60-nAvoidLastBytes)]++

	it, err := NewSSTIterator([][]sstable.ReadableFile{{vfs.NewMemFile(b)}},
		IterOptions{UpperBound: roachpb.KeyMax})

	// We may error early, while opening the iterator.
	if err != nil {
		require.True(t, errors.Is(err, pebble.ErrCorruption))
		return
	}

	it.SeekGE(NilKey)
	valid, err := it.Valid()
	for valid {
		it.Next()
		valid, err = it.Valid()
	}
	// Or we may error during iteration.
	if err != nil {
		require.True(t, errors.Is(err, pebble.ErrCorruption))
	}
	it.Close()
}

func TestPebbleIterator_SkipPointIfOutsideTimeBounds(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var iter pebbleIterator
	var sb strings.Builder
	datadriven.RunTest(t, datapathutils.TestDataPath(t, "skip_point_if_outside_time_bounds"), func(t *testing.T, d *datadriven.TestData) string {
		sb.Reset()
		var minStr, maxStr string
		d.ScanArgs(t, "min", &minStr)
		d.ScanArgs(t, "max", &maxStr)
		min, err := hlc.ParseTimestamp(minStr)
		require.NoError(t, err)
		max, err := hlc.ParseTimestamp(maxStr)
		require.NoError(t, err)

		iter.setOptions(context.Background(), IterOptions{
			LowerBound:   []byte{0x00}, // so setOptions doesn't complain
			MinTimestamp: min,
			MaxTimestamp: max,
		}, StandardDurability)
		fmt.Fprintf(&sb, "min: 0x%x\nmax: 0x%x\n", iter.minTimestamp, iter.maxTimestamp)
		for _, line := range strings.Split(strings.TrimSpace(d.Input), "\n") {
			if i := strings.IndexByte(line, '#'); i >= 0 {
				line = line[:i]
			}
			if line == "" {
				continue
			}
			var key []byte
			switch {
			case strings.HasPrefix(line, "0x:"):
				line = line[len("0x:"):]
				line = strings.Replace(line, " ", "", -1)
				var err error
				key, err = hex.DecodeString(line)
				if err != nil {
					return err.Error()
				}
			default:
				return fmt.Sprintf("unrecognized key format %q", line)
			}
			fmt.Fprintf(&sb, "%s : ", line)
			if iter.skipPointIfOutsideTimeBounds(key) {
				fmt.Fprintln(&sb, "skip")
			} else {
				fmt.Fprintln(&sb, "don't skip")
			}
		}
		return sb.String()
	})
}
