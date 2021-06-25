// Copyright 2021 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

var (
	prevRSS = int64(0)
	// varianceBytes is subtracted from the maxRSS value, in order to take
	// a more liberal strategy when determining whether or not we should query
	// dump. Chosen arbitrarily.
	varianceBytes = int64(64 * 1024 * 1024)
)

// QueryProfiler is used to take profiles of current active queries across all
// sessions within a sql.SessionRegistry.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time the rate of change of RSS bytes from the previous run indicates
// that an OOM is probable before the next call, given the process cgroup's
// memory limit.
//
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type QueryProfiler struct {
	profiler
	maxRSS int64
}

// QueryFileNamePrefix is the prefix of files containing pprof data.
const QueryFileNamePrefix = "queryprof"

// QueryFileNameSuffix is the suffix of files containing query data.
const QueryFileNameSuffix = ".csv"

// NewQueryProfiler creates a NewQueryProfiler. dir is the directory in which
// profiles are to be stored.
func NewQueryProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*QueryProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewQueryProfiler")
	}

	dumpStore := dumpstore.NewStore(dir, maxCombinedFileSize, st)

	maxMem, warn, err := cgroups.GetMemoryLimit()
	if err != nil {
		return nil, err
	}
	if warn != "" {
		log.Warningf(ctx, "warning when reading cgroup memory limit: %s", warn)
	}

	log.Infof(ctx, "writing go query profiles to %s", dir)
	qp := &QueryProfiler{
		profiler: profiler{
			store: newProfileStore(dumpStore, QueryFileNamePrefix, QueryFileNameSuffix, st),
		},
		maxRSS: maxMem,
	}
	return qp, nil
}

// MaybeTakeProfile takes a query profile if the rate of change between curRSS
// and prevRSS indicates that we are close to the system memory limit, implying
// OOM is probable.
func (o *QueryProfiler) MaybeTakeProfile(
	ctx context.Context, curRSS int64, registry *sql.SessionRegistry,
) {
	defer func() {
		prevRSS = curRSS
	}()
	if prevRSS == 0 || curRSS <= prevRSS || o.knobs.dontWriteProfiles {
		return
	}
	now := o.now()
	diff := curRSS - prevRSS
	if curRSS+diff >= (o.maxRSS-varianceBytes) &&
		o.takeQueryProfile(ctx, registry, now, curRSS) {
		// We only remove old files if the current dump was
		// successful. Otherwise, the GC may remove "interesting" files
		// from a previous crash.
		o.store.gcProfiles(ctx, now)
	}
}

func (o *QueryProfiler) takeQueryProfile(
	ctx context.Context, registry *sql.SessionRegistry, now time.Time, curRSS int64,
) bool {
	path := o.store.makeNewFileName(now, curRSS)

	f, err := os.Create(path)
	if err != nil {
		log.Errorf(ctx, "error creating query profile %s: %v", path, err)
		return false
	}
	defer f.Close()

	writer := debug.NewQueriesWriter(registry.SerializeAll(), f)
	err = writer.Write()
	if err != nil {
		log.Errorf(ctx, "error writing active queries to profile: %v", err)
		return false
	}
	return true
}
