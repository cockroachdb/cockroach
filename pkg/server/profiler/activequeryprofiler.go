// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"
	"os"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/debug"
	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/cgroups"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/logcrash"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

var (
	// varianceBytes is subtracted from the maxRSS value, in order to take
	// a more liberal strategy when determining whether or not we should query
	// dump. Chosen arbitrarily.
	varianceBytes          = int64(64 * 1024 * 1024)
	memLimitFn             = cgroups.GetMemoryLimit
	memUsageFn             = cgroups.GetMemoryUsage
	memInactiveFileUsageFn = cgroups.GetMemoryInactiveFileUsage
)

// ActiveQueryProfiler is used to take profiles of current active queries across all
// sessions within a sql.SessionRegistry.
//
// MaybeDumpQueries() is meant to be called periodically. A profile is taken
// every time the rate of change of memory usage (defined as the memory cgroup's
// measure of current memory usage, minus file-backed memory on inactive LRU
// list) from the previous run indicates that an OOM is probable before the next
// call, given the process cgroup's memory limit.
//
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type ActiveQueryProfiler struct {
	profiler
	cgroupMemLimit int64
	mu             struct {
		syncutil.Mutex
		prevMemUsage int64
	}
}

const (
	// QueryFileNamePrefix is the prefix of files containing pprof data.
	QueryFileNamePrefix = "activequeryprof"
	// QueryFileNameSuffix is the suffix of files containing query data.
	QueryFileNameSuffix = ".csv"
)

var activeQueryCombinedFileSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"server.active_query.total_dump_size_limit",
	"maximum combined disk size of preserved active query profiles",
	64<<20, // 64MiB
)

// NewActiveQueryProfiler creates a NewQueryProfiler. dir is the directory in which
// profiles are to be stored.
func NewActiveQueryProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*ActiveQueryProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewQueryProfiler")
	}

	dumpStore := dumpstore.NewStore(dir, activeQueryCombinedFileSize, st)

	maxMem, warn, err := memLimitFn()
	if err != nil {
		return nil, errors.Wrap(err, "failed to detect cgroup memory limit")
	}
	if warn != "" {
		log.Warningf(ctx, "warning when reading cgroup memory limit: %s", log.SafeManaged(warn))
	}

	log.Infof(ctx, "writing go query profiles to %s", log.SafeManaged(dir))
	qp := &ActiveQueryProfiler{
		profiler: makeProfiler(
			newProfileStore(dumpStore, QueryFileNamePrefix, QueryFileNameSuffix, st),
			zeroFloor,
			envMemprofInterval,
		),
		cgroupMemLimit: maxMem,
	}
	return qp, nil
}

// MaybeDumpQueries takes a query profile if the rate of change between curRSS
// and prevRSS indicates that we are close to the system memory limit, implying
// OOM is probable.
func (o *ActiveQueryProfiler) MaybeDumpQueries(
	ctx context.Context, registry *sql.SessionRegistry, st *cluster.Settings,
) {
	defer func() {
		if p := recover(); p != nil {
			logcrash.ReportPanic(ctx, &st.SV, p, 1)
		}
	}()
	shouldDump, memUsage := o.shouldDump(ctx, st)
	now := o.now()
	if shouldDump && o.takeQueryProfile(ctx, registry, now, memUsage) {
		// We only remove old files if the current dump was
		// successful. Otherwise, the GC may remove "interesting" files
		// from a previous crash.
		o.store.gcProfiles(ctx, now)
	}
}

// shouldDump indicates whether or not a query dump should occur, based on the
// heuristics involving runtime stats such as memory usage. The current memory
// usage is also returned for use when generating a new filename for the
// heapprofiler store.
//
// Always returns false if the ActiveQueryDumpsEnabled cluster setting is
// disabled.
func (o *ActiveQueryProfiler) shouldDump(ctx context.Context, st *cluster.Settings) (bool, int64) {
	if !ActiveQueryDumpsEnabled.Get(&st.SV) {
		return false, 0
	}
	cgMemUsage, _, err := memUsageFn()
	if err != nil {
		log.Errorf(ctx, "failed to fetch cgroup memory usage: %v", err)
		return false, 0
	}
	cgInactiveFileUsage, _, err := memInactiveFileUsageFn()
	if err != nil {
		log.Errorf(ctx, "failed to fetch cgroup memory inactive file usage: %v", err)
		return false, 0
	}
	curMemUsage := cgMemUsage - cgInactiveFileUsage

	defer func() {
		o.mu.Lock()
		defer o.mu.Unlock()
		o.mu.prevMemUsage = curMemUsage
	}()

	o.mu.Lock()
	defer o.mu.Unlock()
	if o.mu.prevMemUsage == 0 ||
		curMemUsage <= o.mu.prevMemUsage ||
		o.knobs.dontWriteProfiles {
		return false, curMemUsage
	}
	diff := curMemUsage - o.mu.prevMemUsage

	return curMemUsage+diff >= o.cgroupMemLimit-varianceBytes, curMemUsage
}

func (o *ActiveQueryProfiler) takeQueryProfile(
	ctx context.Context, registry *sql.SessionRegistry, now time.Time, curMemUsage int64,
) bool {
	path := o.store.makeNewFileName(now, curMemUsage)

	f, err := os.Create(path)
	if err != nil {
		log.Errorf(ctx, "error creating query profile %s: %v", path, err)
		return false
	}
	defer f.Close()

	writer := debug.NewActiveQueriesWriter(registry.SerializeAll(), f)
	err = writer.Write()
	if err != nil {
		log.Errorf(ctx, "error writing active queries to profile: %v", err)
		return false
	}
	return true
}
