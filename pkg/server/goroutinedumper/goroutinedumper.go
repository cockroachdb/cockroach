// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goroutinedumper

import (
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"runtime/pprof"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

const (
	goroutineDumpPrefix = "goroutine_dump"
	timeFormat          = "2006-01-02T15_04_05.000"
)

var (
	numGoroutinesThreshold = settings.RegisterIntSetting(
		"server.goroutine_dump.num_goroutines_threshold",
		"a threshold beyond which if number of goroutines increases, "+
			"then goroutine dump can be triggered",
		1000,
	)
	totalDumpSizeLimit = settings.RegisterByteSizeSetting(
		"server.goroutine_dump.total_dump_size_limit",
		"total size of goroutine dumps to be kept. "+
			"Dumps are GC'ed in the order of creation time. The latest dump is "+
			"always kept even if its size exceeds the limit.",
		500<<20, // 500MiB
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
			filename := fmt.Sprintf(
				"%s.%s.%s.%09d",
				goroutineDumpPrefix,
				now.Format(timeFormat),
				h.name,
				goroutines,
			)
			path := gd.store.GetFullPath(filename)
			if err := gd.takeGoroutineDump(path); err != nil {
				log.Warningf(ctx, "error dumping goroutines: %s", err)
				continue
			}
			gd.maxGoroutinesDumped = goroutines
			gd.gcDumps(ctx, now)
			break
		}
	}
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

	log.Infof(ctx, "writing goroutine dumps to %s", dir)

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
func (gd *GoroutineDumper) PreFilter(
	ctx context.Context, files []os.FileInfo, cleanupFn func(fileName string) error,
) (preserved map[int]bool, _ error) {
	preserved = make(map[int]bool)
	for i := len(files) - 1; i >= 0; i-- {
		// Always preserve the last dump in chronological order.
		if gd.CheckOwnsFile(ctx, files[i]) {
			preserved[i] = true
			break
		}
	}
	return
}

// CheckOwnsFile is part of the dumpstore.Dumper interface.
func (gd *GoroutineDumper) CheckOwnsFile(_ context.Context, fi os.FileInfo) bool {
	return strings.HasPrefix(fi.Name(), goroutineDumpPrefix)
}

func takeGoroutineDump(path string) error {
	path += ".txt.gz"
	f, err := os.Create(path)
	if err != nil {
		return errors.Wrapf(err, "error creating file %s for goroutine dump", path)
	}
	defer f.Close()
	w := gzip.NewWriter(f)
	if err = pprof.Lookup("goroutine").WriteTo(w, 2); err != nil {
		return errors.Wrapf(err, "error writing goroutine dump to %s", path)
	}
	// Flush and write the gzip header. It doesn't close the underlying writer.
	if err := w.Close(); err != nil {
		return errors.Wrapf(err, "error closing gzip writer for %s", path)
	}
	// Return f.Close() too so that we don't miss a potential error if everything
	// else succeeded.
	return f.Close()
}
