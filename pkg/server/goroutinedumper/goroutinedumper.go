// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package goroutinedumper

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	goroutineDumpPrefix = "goroutine_dump"
	timeFormat          = "2006-01-02T15_04_05.000"
)

func dumpFilename(now time.Time, reason string, goroutines int) string {
	return fmt.Sprintf("%s.%s.%s.%09d", goroutineDumpPrefix, now.Format(timeFormat), reason, goroutines)
}

var (
	numGoroutinesThreshold = settings.RegisterIntSetting(
		settings.ApplicationLevel,
		"server.goroutine_dump.num_goroutines_threshold",
		"a threshold beyond which if number of goroutines increases, "+
			"then goroutine dump can be triggered",
		1000,
	)
	totalDumpSizeLimit = settings.RegisterByteSizeSetting(
		settings.ApplicationLevel,
		"server.goroutine_dump.total_dump_size_limit",
		"total size of goroutine dumps to be kept. "+
			"Dumps are GC'ed in the order of creation time. The latest dump is "+
			"always kept even if its size exceeds the limit.",
		500<<20, // 500MiB
	)
	onDemandMinInterval = settings.RegisterDurationSettingWithExplicitUnit(
		settings.ApplicationLevel,
		"server.goroutine_dump.on_demand.min_interval",
		"minimum amount of time that must pass between two on-demand goroutine dumps (0 disables rate limiting)",
		10*time.Second,
		settings.DurationWithMinimum(0),
	)
)

// heuristic represents whether goroutine dump is triggered. It is true when
// we think a goroutine dump is helpful in debugging OOM issues.
type heuristic struct {
	name   string
	isTrue func(s *GoroutineDumper) bool
}

var doubleSinceLastDumpHeuristic = heuristic{
	name: "double_since_last_dump",
	isTrue: func(gd *GoroutineDumper) bool {
		return gd.goroutines > gd.goroutinesThreshold &&
			gd.goroutines >= 2*gd.maxGoroutinesDumped
	},
}

// GoroutineDumper stores relevant functions and stats to take goroutine dumps
// if an abnormal change in number of goroutines is detected.
type GoroutineDumper struct {
	mu struct {
		syncutil.Mutex
		lastOnDemandDumpTime time.Time
	}

	goroutines          int64
	goroutinesThreshold int64
	maxGoroutinesDumped int64
	heuristics          []heuristic
	currentTime         func() time.Time
	takeGoroutineDump   func(path string) error
	store               *dumpstore.DumpStore
	st                  *cluster.Settings
}

// MaybeDump takes a goroutine dump only when at least one heuristic in
// GoroutineDumper is true.
// At most one dump is taken in a call of this function.
func (gd *GoroutineDumper) MaybeDump(ctx context.Context, st *cluster.Settings, goroutines int64) {
	gd.goroutines = goroutines
	if gd.goroutinesThreshold != numGoroutinesThreshold.Get(&st.SV) {
		gd.goroutinesThreshold = numGoroutinesThreshold.Get(&st.SV)
		gd.maxGoroutinesDumped = 0
	}
	for _, h := range gd.heuristics {
		if h.isTrue(gd) {
			now := gd.currentTime()
			path := gd.store.GetFullPath(dumpFilename(now, h.name, int(goroutines)))
			if err := gd.takeGoroutineDump(path); err != nil {
				log.Dev.Warningf(ctx, "error dumping goroutines: %s", err)
				continue
			}
			gd.maxGoroutinesDumped = goroutines
			gd.gcDumps(ctx, now)
			break
		}
	}
}

// DumpNow requests a goroutine dump on demand.
// nolint:deferunlockcheck
func (gd *GoroutineDumper) DumpNow(
	ctx context.Context, reason redact.RedactableString,
) (didDump bool, _ error) {
	minInterval := onDemandMinInterval.Get(&gd.st.SV)
	now := gd.currentTime()

	gd.mu.Lock()
	rateLimited := minInterval > 0 && now.Sub(gd.mu.lastOnDemandDumpTime) < minInterval
	if !rateLimited {
		gd.mu.lastOnDemandDumpTime = now
	}
	gd.mu.Unlock()

	if rateLimited {
		log.Ops.VEventfDepth(ctx, 1, 1, "on-demand goroutine dump suppressed by rate limit: %s", reason)
		return false, nil
	}

	filename := dumpFilename(now, "on_demand", runtime.NumGoroutine())
	log.Ops.Infof(ctx, "taking on-demand goroutine dump: %s [%s]", reason, filename)
	path := gd.store.GetFullPath(filename)
	if err := gd.takeGoroutineDump(path); err != nil {
		return false, err
	}

	gd.gcDumps(ctx, now)
	return true, nil
}

// NewGoroutineDumper returns a GoroutineDumper which enables
// doubleSinceLastDumpHeuristic.
// dir is the directory in which dumps are stored.
func NewGoroutineDumper(
	ctx context.Context, dir string, st *cluster.Settings,
) (*GoroutineDumper, error) {
	if dir == "" {
		return nil, errors.New("directory to store dumps could not be determined")
	}

	log.Dev.Infof(ctx, "writing goroutine dumps to %s", log.SafeManaged(dir))

	gd := &GoroutineDumper{
		heuristics: []heuristic{
			doubleSinceLastDumpHeuristic,
		},
		goroutinesThreshold: 0,
		maxGoroutinesDumped: 0,
		currentTime:         timeutil.Now,
		takeGoroutineDump:   takeGoroutineDump,
		store:               dumpstore.NewStore(dir, totalDumpSizeLimit, st),
		st:                  st,
	}
	return gd, nil
}

func (gd *GoroutineDumper) gcDumps(ctx context.Context, now time.Time) {
	gd.store.GC(ctx, now, gd)
}

// PreFilter is part of the dumpstore.Dumper interface.
//
// GC is intentionally heuristic-agnostic: all dumps share the same size budget
// and the most recent dump is preserved regardless of which heuristic created it.
func (gd *GoroutineDumper) PreFilter(
	ctx context.Context, files []os.DirEntry, cleanupFn func(fileName string) error,
) (preserved map[int]bool, _ error) {
	preserved = make(map[int]bool)
	for i := len(files) - 1; i >= 0; i-- {
		if gd.CheckOwnsFile(ctx, files[i]) {
			preserved[i] = true
			break
		}
	}
	return
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (gd *GoroutineDumper) CheckOwnsFile(_ context.Context, fi os.DirEntry) bool {
	return strings.HasPrefix(fi.Name(), goroutineDumpPrefix)
}

func takeGoroutineDump(path string) error {
	path += ".pb.gz"
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "error creating file %s for goroutine dump", path)
	}
	defer f.Close()
	if err = pprof.Lookup("goroutine").WriteTo(f, 3); err != nil {
		return errors.Wrapf(err, "error writing goroutine dump to %s", path)
	}
	// Return f.Close() too so that we don't miss a potential error if everything
	// else succeeded.
	return f.Close()
}
