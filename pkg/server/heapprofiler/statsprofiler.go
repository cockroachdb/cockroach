// Copyright 2020 The Cockroach Authors.
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
	"encoding/json"
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/server/status"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// StatsProfiler is used to take snapshots of the overall memory statistics
// to break down overall RSS memory usage.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time RSS bytes exceeds the previous high-water mark. The
// recorded high-water mark is also reset periodically, so that we take some
// profiles periodically.
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type StatsProfiler struct {
	profiler
}

// StatsFileNamePrefix is the prefix of memory stats dumps.
const StatsFileNamePrefix = "memstats"

// StatsFileNameSuffix is the suffix of memory stats dumps.
const StatsFileNameSuffix = ".txt"

// NewStatsProfiler creates a StatsProfiler. dir is the
// directory in which profiles are to be stored.
func NewStatsProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*StatsProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewStatsProfiler")
	}

	log.Infof(ctx, "writing memory stats to %s at last every %s", dir, resetHighWaterMarkInterval)

	dumpStore := dumpstore.NewStore(dir, maxCombinedFileSize, st)

	hp := &StatsProfiler{
		profiler{
			store: newProfileStore(dumpStore, StatsFileNamePrefix, StatsFileNameSuffix, st),
		},
	}
	return hp, nil
}

// MaybeTakeProfile takes a profile if the non-go size is big enough.
func (o *StatsProfiler) MaybeTakeProfile(
	ctx context.Context, curRSS int64, ms *status.GoMemStats, cs *status.CGoMemStats,
) {
	o.maybeTakeProfile(ctx, curRSS, func(ctx context.Context, path string) bool { return saveStats(ctx, path, ms, cs) })
}

func saveStats(
	ctx context.Context, path string, ms *status.GoMemStats, cs *status.CGoMemStats,
) bool {
	f, err := os.Create(path)
	if err != nil {
		log.Warningf(ctx, "error creating stats profile %s: %v", path, err)
		return false
	}
	defer f.Close()
	msJ, err := json.MarshalIndent(&ms.MemStats, "", "  ")
	if err != nil {
		log.Warningf(ctx, "error marshaling stats profile %s: %v", path, err)
		return false
	}
	csJ, err := json.MarshalIndent(cs, "", "  ")
	if err != nil {
		log.Warningf(ctx, "error marshaling stats profile %s: %v", path, err)
		return false
	}
	_, err = fmt.Fprintf(f, "Go memory stats:\n%s\n----\nNon-Go stats:\n%s\n", msJ, csJ)
	if err != nil {
		log.Warningf(ctx, "error writing stats profile %s: %v", path, err)
		return false
	}
	return true
}
