// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package heapprofiler

import (
	"context"
	"os"
	"runtime/pprof"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// HeapProfiler is used to take Go heap profiles.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time Go heap allocated bytes exceeds the previous high-water mark. The
// recorded high-water mark is also reset periodically, so that we take some
// profiles periodically.
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type HeapProfiler struct {
	profiler
}

// HeapFileNamePrefix is the prefix of files containing pprof data.
const HeapFileNamePrefix = "memprof"

// HeapFileNameSuffix is the suffix of files containing pprof data.
const HeapFileNameSuffix = ".pprof"

// NewHeapProfiler creates a HeapProfiler. dir is the directory in which
// profiles are to be stored.
func NewHeapProfiler(ctx context.Context, dir string, st *cluster.Settings) (*HeapProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewHeapProfiler")
	}

	log.Infof(ctx, "writing go heap profiles to %s at least every %s", dir, resetHighWaterMarkInterval)

	dumpStore := dumpstore.NewStore(dir, maxCombinedFileSize, st)

	hp := &HeapProfiler{
		profiler{
			store: newProfileStore(dumpStore, HeapFileNamePrefix, HeapFileNameSuffix, st),
		},
	}
	return hp, nil
}

// MaybeTakeProfile takes a heap profile if the heap is big enough.
func (o *HeapProfiler) MaybeTakeProfile(ctx context.Context, curHeap int64) {
	o.maybeTakeProfile(ctx, curHeap, takeHeapProfile)
}

// takeHeapProfile returns true if and only if the profile dump was
// taken successfully.
func takeHeapProfile(ctx context.Context, path string) (success bool) {
	// Try writing a go heap profile.
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating go heap profile %s: %v", path, err)
		return false
	}
	defer f.Close()
	if err = pprof.WriteHeapProfile(f); err != nil {
		log.Warningf(ctx, "error writing go heap profile %s: %v", path, err)
		return false
	}
	return true
}
