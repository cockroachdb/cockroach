// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package profiler

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/dumpstore"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// NonGoAllocProfiler is used to take heap profiles for allocations
// performed outside of Go.
//
// MaybeTakeProfile() is supposed to be called periodically. A profile is taken
// every time Non-Go allocated bytes exceeds the previous high-water mark. The
// recorded high-water mark is also reset periodically, so that we take some
// profiles periodically.
// Profiles are also GCed periodically. The latest is always kept, and a couple
// of the ones with the largest heap are also kept.
type NonGoAllocProfiler struct {
	profiler
}

// jemallocFileNamePrefix is the prefix of jemalloc profile dumps.
const jemallocFileNamePrefix = "jeprof"

// jemallocFileNameSuffix is the file name extension of jemalloc profile dumps.
const jemallocFileNameSuffix = ".jeprof"

var jemallCombinedFileSize = settings.RegisterByteSizeSetting(
	settings.ApplicationLevel,
	"server.jemalloc.total_dump_size_limit",
	"maximum combined disk size of preserved jemalloc profiles",
	32<<20, // 32MiB
)

// NewNonGoAllocProfiler creates a NonGoAllocProfiler. dir is the
// directory in which profiles are to be stored.
func NewNonGoAllocProfiler(
	ctx context.Context, dir string, st *cluster.Settings,
) (*NonGoAllocProfiler, error) {
	if dir == "" {
		return nil, errors.AssertionFailedf("need to specify dir for NewHeapProfiler")
	}

	dumpStore := dumpstore.NewStore(dir, jemallCombinedFileSize, st)

	hp := &NonGoAllocProfiler{
		profiler: makeProfiler(
			newProfileStore(dumpStore, jemallocFileNamePrefix, jemallocFileNameSuffix, st),
			zeroFloor,
			envMemprofInterval,
		),
	}

	if jemallocHeapDump != nil {
		log.Infof(ctx, "writing jemalloc profiles to %s at least every %s", dir, hp.resetInterval())
	} else {
		log.Infof(ctx, `to enable jmalloc profiling: "export MALLOC_CONF=prof:true" or "ln -s prof:true /etc/malloc.conf"`)
	}

	return hp, nil
}

// MaybeTakeProfile takes a profile if the non-go size is big enough.
func (o *NonGoAllocProfiler) MaybeTakeProfile(ctx context.Context, curNonGoAlloc int64) {
	o.maybeTakeProfile(ctx, curNonGoAlloc, takeJemallocProfile)
}

// takeJemallocProfile returns true if and only if the jemalloc dump was taken
// successfully or jemalloc was not enabled.
func takeJemallocProfile(ctx context.Context, path string, _ ...interface{}) (success bool) {
	if jemallocHeapDump == nil {
		return true
	}
	if err := jemallocHeapDump(path); err != nil {
		log.Warningf(ctx, "error writing jemalloc heap %s: %v", path, err)
		return false
	}
	return true
}

// jemallocHeapDump is an optional function to be called at heap dump time.
// This will be non-nil when jemalloc is linked in with profiling enabled.
// The function takes a filename to write the profile to.
var jemallocHeapDump func(string) error

// SetJemallocHeapDumpFn is used by the CLI package to inject the
// jemalloc heap collection function as a dependency. This is done
// here and not here so as to not impair the ability to run `go test`
// on the server package and sub-packages (Jemalloc needs custom link
// flags).
func SetJemallocHeapDumpFn(fn func(filename string) error) {
	if jemallocHeapDump != nil {
		panic("jemallocHeapDump is already set")
	}
	jemallocHeapDump = fn
}
