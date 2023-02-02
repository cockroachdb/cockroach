// Copyright 2015 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/pebble/vfs"
)

// InMemFromFS allocates and returns new, opened in-memory engine. Engine
// uses provided in mem file system and base directory to store data. The
// caller must call obtained engine's Close method when engine is no longer
// needed.
func InMemFromFS(
	ctx context.Context, fs vfs.FS, dir string, settings *cluster.Settings, opts ...ConfigOption,
) Engine {
	// TODO(jackson): Replace this function with a special Location
	// constructor that allows both specifying a directory and supplying your
	// own VFS?
	eng, err := Open(ctx, Location{dir: dir, fs: fs}, settings, opts...)
	if err != nil {
		panic(err)
	}
	return eng
}

// The ForTesting functions randomize the settings for separated intents. This
// is a bit peculiar for tests outside the storage package, since they usually
// have higher level cluster constructs, including creating a cluster.Settings
// as part of the StoreConfig. We are ignoring what may be produced there, and
// injecting a different cluster.Settings here. Plumbing that through for all
// the different higher level testing constructs seems painful, and the only
// places that actively change their behavior for separated intents will use
// the cluster.Settings we inject here, which is used for no other purpose
// other than configuring separated intents. So the fact that we have two
// inconsistent cluster.Settings is harmless.

// NewDefaultInMemForTesting allocates and returns a new, opened in-memory
// engine with the default configuration. The caller must call the engine's
// Close method when the engine is no longer needed. This method randomizes
// whether separated intents are written.
func NewDefaultInMemForTesting(opts ...ConfigOption) Engine {
	eng, err := Open(
		context.Background(), InMemory(), cluster.MakeTestingClusterSettings(),
		ForTesting, MaxSize(1<<20), CombineOptions(opts...),
	)
	if err != nil {
		panic(err)
	}
	return eng
}
