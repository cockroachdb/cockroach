// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package migration WIP
package migration

import (
	"context"
	gosql "database/sql"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Hook WIP
type Hook struct {
	Version fflag.VersionKey

	// RunFn is an idempotent closure that (if non-nil) must run without error
	// before advancing the cluster onto this version.
	RunFn func(context.Context, *client.DB, *gosql.DB) error
}

var registry struct {
	mu struct {
		syncutil.Mutex
		hooks []Hook
	}
}

// Register is called in an init func to add a hook implementation for use with
// RunHookOnEveryNode.
func Register(h Hook) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	registry.mu.hooks = append(registry.mu.hooks, h)
	sort.Slice(registry.mu.hooks, func(i, j int) bool {
		return registry.mu.hooks[i].Version < registry.mu.hooks[j].Version
	})
}

func init() {
	Register(Hook{
		Version: fflag.VersionNoPreemptiveSnapshotsOnDisk,
		RunFn: func(context.Context, *client.DB, *gosql.DB) error {
			// Before this hook is called, we're guaranteed by the new system that
			// `IsActive(fflag.VersionNoNewPreemptiveSnapshotsStarted)` will never
			// return false on any node in this cluster ever again. If that check is
			// what is used to determine whether a preemptive or a learner snapshot is
			// sent, then the cleanup becomes much easier. First we have to wait for
			// any in-flight replica changes to finish (or abort them). Then we use an
			// everynode.Hook to clean out the ones on disk.
			panic(`WIP`)
		},
	})
	Register(Hook{
		Version: fflag.VersionFillInTableDescriptor,
		RunFn: func(context.Context, *client.DB, *gosql.DB) error {
			tableDescsSpan := roachpb.Span{Key: keys.MakeTablePrefix(keys.DescriptorTableID)}
			tableDescsSpan.EndKey = tableDescsSpan.Key.PrefixEnd()
			// This hook pages through every record in tableDescsSpan, runs
			// MaybeFillInDescriptor on each table descriptor and writes it back to
			// kv. This allows us to get rid of MaybeFillInDescriptor entirely in the
			// next major version.
		},
	})
}
