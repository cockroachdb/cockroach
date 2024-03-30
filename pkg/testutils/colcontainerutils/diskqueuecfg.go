// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colcontainerutils

import (
	"context"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/colcontainer"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

const inMemDirName = "testing"

// NewTestingDiskQueueCfg returns a DiskQueueCfg and a non-nil cleanup function.
func NewTestingDiskQueueCfg(t testing.TB, inMem bool) (colcontainer.DiskQueueCfg, func()) {
	t.Helper()

	var (
		cfg     colcontainer.DiskQueueCfg
		cleanup []func()
		path    string
		fsEnv   *fs.Env
	)

	if inMem {
		fsEnv = storage.InMemory()
		path = inMemDirName
	} else {
		var cleanupFunc func()
		path, cleanupFunc = testutils.TempDir(t)
		fsEnv = fs.MustInitPhysicalTestingEnv(path)
		cleanup = append(cleanup, cleanupFunc)
	}

	if inMem {
		if err := fsEnv.MkdirAll(inMemDirName, os.ModePerm); err != nil {
			t.Fatal(err)
		}
	}

	cleanup = append(cleanup, fsEnv.Close)
	cfg.FS = fsEnv
	cfg.GetPather = colcontainer.GetPatherFunc(func(context.Context) string { return path })
	if err := cfg.EnsureDefaults(); err != nil {
		t.Fatal(err)
	}
	return cfg, func() {
		for _, f := range cleanup {
			f()
		}
	}
}
