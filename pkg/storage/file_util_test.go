// Copyright 2021 The Cockroach Authors.
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
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestSafeWriteToFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use an in-memory FS that strictly enforces syncs.
	mem := vfs.NewStrictMem()
	syncDir := func(dir string) {
		fdir, err := mem.OpenDir(dir)
		require.NoError(t, err)
		require.NoError(t, fdir.Sync())
		require.NoError(t, fdir.Close())
	}
	readFile := func(filename string) []byte {
		f, err := mem.Open("foo/bar")
		require.NoError(t, err)
		b, err := ioutil.ReadAll(f)
		require.NoError(t, err)
		require.NoError(t, f.Close())
		return b
	}

	require.NoError(t, mem.MkdirAll("foo", os.ModePerm))
	syncDir("")
	f, err := mem.Create("foo/bar")
	require.NoError(t, err)
	_, err = io.WriteString(f, "Hello world")
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())
	syncDir("foo")

	// Discard any unsynced writes to make sure we set up the test
	// preconditions correctly.
	mem.ResetToSyncedState()
	require.Equal(t, []byte("Hello world"), readFile("foo/bar"))

	// Use SafeWriteToFile to atomically, durably change the contents of the
	// file.
	require.NoError(t, SafeWriteToFile(mem, "foo", "foo/bar", []byte("Hello everyone")))

	// Discard any unsynced writes.
	mem.ResetToSyncedState()
	require.Equal(t, []byte("Hello everyone"), readFile("foo/bar"))
}
