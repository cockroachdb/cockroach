// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colcontainer

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/pebble/vfs"
)

const inMemDirName = "testing"

// NewTestingDiskQueueCfg returns a DiskQueueCfg and a non-nil cleanup function.
func NewTestingDiskQueueCfg(t testing.TB, inMem bool) (DiskQueueCfg, func(), error) {
	t.Helper()

	var (
		cfg     DiskQueueCfg
		cleanup func()
		fs      vfs.FS
		path    string
	)

	if inMem {
		fs = vfs.NewMem()
		if err := fs.MkdirAll(inMemDirName, 0755); err != nil {
			t.Fatal(err)
		}
		path = inMemDirName
	} else {
		fs = vfs.Default
		path, cleanup = testutils.TempDir(t)
	}
	cfg.FS = fs
	cfg.Path = path

	if cleanup == nil {
		cleanup = func() {}
	}
	if err := cfg.EnsureDefaults(); err != nil {
		return cfg, cleanup, err
	}

	return cfg, cleanup, nil
}
