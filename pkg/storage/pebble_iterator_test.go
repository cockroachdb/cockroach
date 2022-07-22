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
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func TestPebbleIterator_Corruption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Create a Pebble DB that can be used to back a pebbleIterator.
	dir := t.TempDir()
	dataDir := filepath.Join(dir, "data")
	p, err := Open(context.Background(), Filesystem(dataDir))
	require.NoError(t, err)
	defer p.Close()

	// Insert some data into the DB and flush to create an SST.
	ek := engineKey("foo", 0)
	require.NoError(t, p.PutEngineKey(ek, nil))
	require.NoError(t, p.Flush())

	// Corrupt the SSTs in the DB.
	err = filepath.Walk(dataDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !strings.HasSuffix(info.Name(), ".sst") {
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
	iter := newPebbleIterator(p.db, iterOpts, StandardDurability, false /* range keys */)

	// Seeking into the table catches the corruption.
	ok, err := iter.SeekEngineKeyGE(ek)
	require.False(t, ok)
	require.True(t, errors.Is(err, pebble.ErrCorruption))

	// Closing the iter results in a panic due to the corruption.
	require.Panics(t, func() { iter.Close() })
}
