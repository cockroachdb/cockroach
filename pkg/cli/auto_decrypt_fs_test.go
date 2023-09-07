// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package cli

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestAutoDecryptFS(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if runtime.GOOS == "windows" {
		skip.IgnoreLint(t, "expected output uses unix paths")
	}
	dir, err := os.MkdirTemp("", "auto-decrypt-test")
	require.NoError(t, err)
	defer func() { _ = os.RemoveAll(dir) }()

	path1 := filepath.Join(dir, "path1")
	path2 := filepath.Join(dir, "foo", "path2")

	var buf bytes.Buffer
	resolveFn := func(dir string) (vfs.FS, error) {
		if dir != path1 && dir != path2 {
			t.Fatalf("unexpected dir %s", dir)
		}
		fs := vfs.NewMem()
		require.NoError(t, fs.MkdirAll(dir, 0755))
		return vfs.WithLogging(fs, func(format string, args ...interface{}) {
			fmt.Fprintf(&buf, dir+": "+format+"\n", args...)
		}), nil
	}

	var fs autoDecryptFS
	fs.Init([]string{path1, path2}, resolveFn)

	create := func(pathElems ...string) {
		file, err := fs.Create(filepath.Join(pathElems...))
		require.NoError(t, err)
		file.Close()
	}

	create(dir, "foo")
	create(path1, "bar")
	create(path2, "baz")
	require.NoError(t, fs.MkdirAll(filepath.Join(path2, "a", "b"), 0755))
	create(path2, "a", "b", "xx")

	// Check that operations inside the two paths happen using the resolved FSes.
	output := strings.ReplaceAll(buf.String(), dir, "$TMPDIR")
	expected :=
		`$TMPDIR/path1: create: $TMPDIR/path1/bar
$TMPDIR/path1: close: $TMPDIR/path1/bar
$TMPDIR/foo/path2: create: $TMPDIR/foo/path2/baz
$TMPDIR/foo/path2: close: $TMPDIR/foo/path2/baz
$TMPDIR/foo/path2: mkdir-all: $TMPDIR/foo/path2/a/b 0755
$TMPDIR/foo/path2: create: $TMPDIR/foo/path2/a/b/xx
$TMPDIR/foo/path2: close: $TMPDIR/foo/path2/a/b/xx
`
	require.Equal(t, expected, output)
}
