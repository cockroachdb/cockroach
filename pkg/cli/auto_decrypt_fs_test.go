// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/storage/fs"
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
	resolveFn := func(dir string) (*fs.Env, error) {
		if dir != path1 && dir != path2 {
			t.Fatalf("unexpected dir %s", dir)
		}
		memFS := vfs.WithLogging(vfs.NewMem(), func(format string, args ...interface{}) {
			fmt.Fprintf(&buf, dir+": "+format+"\n", args...)
		})
		env, err := fs.InitEnv(context.Background(), memFS, "" /* dir */, fs.EnvConfig{}, nil /* statsCollector */)
		require.NoError(t, err)
		require.NoError(t, env.MkdirAll(dir, 0755))
		return env, nil
	}

	var decryptFS autoDecryptFS
	decryptFS.Init([]string{path1, path2}, resolveFn)
	defer func() {
		require.NoError(t, decryptFS.Close())
	}()

	create := func(pathElems ...string) {
		file, err := decryptFS.Create(filepath.Join(pathElems...), fs.UnspecifiedWriteCategory)
		require.NoError(t, err)
		file.Close()
	}

	create(dir, "foo")
	create(path1, "bar")
	create(path2, "baz")
	require.NoError(t, decryptFS.MkdirAll(filepath.Join(path2, "a", "b"), 0755))
	create(path2, "a", "b", "xx")

	// Check that operations inside the two paths happen using the resolved FSes.
	output := strings.TrimSpace(strings.ReplaceAll(buf.String(), dir, "$TMPDIR"))
	expected := `
$TMPDIR/path1: mkdir-all:  0777
$TMPDIR/path1: lock: LOCK
$TMPDIR/path1: mkdir-all: $TMPDIR/path1 0755
$TMPDIR/path1: create: $TMPDIR/path1/bar
$TMPDIR/path1: close: $TMPDIR/path1/bar
$TMPDIR/foo/path2: mkdir-all:  0777
$TMPDIR/foo/path2: lock: LOCK
$TMPDIR/foo/path2: mkdir-all: $TMPDIR/foo/path2 0755
$TMPDIR/foo/path2: create: $TMPDIR/foo/path2/baz
$TMPDIR/foo/path2: close: $TMPDIR/foo/path2/baz
$TMPDIR/foo/path2: mkdir-all: $TMPDIR/foo/path2/a/b 0755
$TMPDIR/foo/path2: create: $TMPDIR/foo/path2/a/b/xx
$TMPDIR/foo/path2: close: $TMPDIR/foo/path2/a/b/xx
`
	require.Equal(t, strings.TrimSpace(expected), output)
}
