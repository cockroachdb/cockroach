// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !windows
// +build !windows

package sdnotify

import (
	"os"
	"testing"

	_ "github.com/cockroachdb/cockroach/pkg/util/log" // for flags
	"github.com/stretchr/testify/require"
)

func TestSDNotify(t *testing.T) {
	tmpDir := os.TempDir()
	// On BSD, binding to a socket is limited to a path length of 104 characters
	// (including the NUL terminator). In glibc, this limit is 108 characters.
	// macOS also has a tendency to produce very long temporary directory names.
	if len(tmpDir) >= 104-1-len("sdnotify/notify.sock")-10 {
		// Perhaps running inside a sandbox?
		t.Logf("default temp dir name is too long: %s", tmpDir)
		t.Logf("forcing use of /tmp instead")
		// Note: Using /tmp may fail on some systems; this is why we
		// prefer os.TempDir() by default.
		tmpDir = "/tmp"
	}

	l, err := listen(tmpDir)
	require.NoError(t, err)
	defer func() { _ = l.close() }()

	ch := make(chan error)
	go func() {
		ch <- l.wait()
	}()

	if err := notify(l.Path, readyMsg); err != nil {
		t.Fatal(err)
	}
	if err := <-ch; err != nil {
		t.Fatal(err)
	}
}
