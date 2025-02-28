// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !stdmalloc

package status

import (
	"context"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/redact"
	"github.com/dustin/go-humanize"
)

// #cgo CPPFLAGS: -DJEMALLOC_NO_DEMANGLE
// #cgo LDFLAGS: -ljemalloc
// #cgo dragonfly freebsd LDFLAGS: -lm
// #cgo linux LDFLAGS: -lrt -lm -lpthread
//
// #include <jemalloc/jemalloc.h>
// #include <stdbool.h>
// #include <stdio.h>
// #include <stdlib.h>
// #include <string.h>
// // https://github.com/jemalloc/jemalloc/wiki/Use-Case:-Introspection-Via-mallctl*()
// // https://github.com/jemalloc/jemalloc/blob/4.5.0/src/stats.c#L960:L969
//
// typedef struct {
//   size_t Allocated;
//   size_t Active;
//   size_t Metadata;
//   size_t Resident;
//   size_t Mapped;
//   size_t Retained;
// } JemallocStats;
//
// int jemalloc_get_stats(JemallocStats *stats) {
//   // Update the statistics cached by je_mallctl.
//   uint64_t epoch = 1;
//   size_t sz = sizeof(epoch);
//   je_mallctl("epoch", &epoch, &sz, &epoch, sz);
//
//   int err;
//
//   sz = sizeof(stats->Allocated);
//   err = je_mallctl("stats.allocated", &stats->Allocated, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(stats->Active);
//   err = je_mallctl("stats.active", &stats->Active, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(stats->Metadata);
//   err = je_mallctl("stats.metadata", &stats->Metadata, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(stats->Resident);
//   err = je_mallctl("stats.resident", &stats->Resident, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(stats->Mapped);
//   err = je_mallctl("stats.mapped", &stats->Mapped, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(stats->Retained);
//   err = je_mallctl("stats.retained", &stats->Retained, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   return 0;
// }
//
// #define STRINGIFY_HELPER(x) #x
// #define STRINGIFY(x) STRINGIFY_HELPER(x)
//
// int jemalloc_purge() {
//  return je_mallctl("arena." STRINGIFY(MALLCTL_ARENAS_ALL) ".purge", NULL, 0, NULL, 0);
// }
//
//  typedef struct {
//     bool seen_per_arenas_stats;
// } stats_context_t;
// static void truncate_before_arenas_cb(void *opaque, const char *msg) {
//     stats_context_t *ctx = (stats_context_t *)opaque;
//     if (ctx->seen_per_arenas_stats) {
//         return;
//     }
//     const char *arena_pos = strstr(msg, "arenas[");
//     if (arena_pos != NULL) {
//         size_t len_to_print = (size_t)(arena_pos - msg);
//         fwrite(msg, 1, len_to_print, stderr);
//         ctx->seen_per_arenas_stats = true;
//     } else {
//         fputs(msg, stderr);
//     }
// }
// void jemalloc_stats_print_abbreviated(void) {
//     stats_context_t ctx = {.seen_per_arenas_stats = false};
//     fputs("jemalloc stats (abbreviated):\n", stderr);
//     je_malloc_stats_print(truncate_before_arenas_cb, &ctx, NULL);
// }
import "C"

func init() {
	if getCgoMemStats != nil {
		panic("getCgoMemStats is already set")
	}
	getCgoMemStats = getJemallocStats
	if cgoMemMaybePurge != nil {
		panic("cgoMemMaybePurge is already set")
	}
	cgoMemMaybePurge = jemallocMaybePurge
}

func getJemallocStats(ctx context.Context) (cgoAlloc uint, cgoTotal uint, _ error) {
	var js C.JemallocStats
	// TODO(marc): should we panic here? Failure on fetching the stats may be a problem.
	if _, err := C.jemalloc_get_stats(&js); err != nil {
		return 0, 0, err
	}

	if log.V(2) {
		// Summary of jemalloc stats:
		v := reflect.ValueOf(js)
		t := v.Type()
		stats := make([]string, v.NumField())
		for i := 0; i < v.NumField(); i++ {
			stats[i] = t.Field(i).Name + ": " + humanize.IBytes(uint64(v.Field(i).Interface().(C.size_t)))
		}
		log.Infof(ctx, "jemalloc stats: %s", redact.Safe(strings.Join(stats, " ")))
	}

	// NB: the `!V(MaxInt32)` condition is a workaround to not spew this to the
	// logs whenever log interception (log spy) is active. If we refactored
	// je_malloc_stats_print to return a string that we can `log.Infof` instead,
	// we wouldn't need this.
	if log.V(1) && !log.V(math.MaxInt32) {
		if log.V(3) {
			// Detailed jemalloc stats (very verbose, includes per-arena stats).
			C.je_malloc_stats_print(nil, nil, nil)
		} else {
			// Abbreviated jemalloc stats (does not include per-arena stats).
			C.jemalloc_stats_print_abbreviated()
		}
	}

	// js.Allocated corresponds to stats.allocated, which is effectively the sum
	// of outstanding allocations times the size class; thus it includes internal
	// fragmentation.
	//
	// js.Resident corresponds to stats.resident, which is documented as follows:
	//   Maximum number of bytes in physically resident data pages mapped by the
	//   allocator, comprising all pages dedicated to allocator metadata, pages
	//   backing active allocations, and unused dirty pages. This is a maximum
	//   rather than precise because pages may not actually be physically resident
	//   if they correspond to demand-zeroed virtual memory that has not yet been
	//   touched. This is a multiple of the page size, and is larger than
	//   stats.active.
	return uint(js.Allocated), uint(js.Resident), nil
}

// Used to force allocation in tests. 'import "C"' is not supported in tests.
func allocateMemory() {
	// Empirically, 8KiB is not enough, but 16KiB is except for ppc64le, where 256KiB is needed.
	C.malloc(256 << 10)
}

var lastPurgeTime crtime.AtomicMono

// jemallocMaybePurge checks if the current jemalloc overhead (relative to
// cgoAllocMem or cgoTargetMem, whichever is higher) is above overheadPercent;
// if it is, a purge of all arenas is performed. We perform at most a purge per
// minPeriod.
func jemallocMaybePurge(
	ctx context.Context,
	cgoAllocMem, cgoTotalMem, cgoTargetMem uint64,
	overheadPercent int,
	minPeriod time.Duration,
) {
	// We take the max between cgoAllocMem and cgoTargetMem because we only care about
	// the overhead compared to the target peak usage.
	//
	// We don't use just cgoAllocMem because when a lot of memory is freed
	// (because of an eviction), the overhead spikes up high momentarily
	// (goAllocMem goes down and goTotalMem takes a while to go down). We only
	// care about overhead above the target usage.
	//
	// We don't use just cgoTargetMem because we don't want to purge unnecessarily
	// if we actually allocate more memory than our target (e.g. because of
	// internal fragmentation).
	target := max(cgoAllocMem, cgoTargetMem)
	if cgoTotalMem*100 <= target*uint64(100+overheadPercent) {
		return
	}
	lastPurge := lastPurgeTime.Load()
	thisPurge := crtime.NowMono()
	if lastPurge != 0 && thisPurge.Sub(lastPurge) < minPeriod {
		// We purged too recently.
		return
	}
	if !lastPurgeTime.CompareAndSwap(lastPurge, thisPurge) {
		// Another goroutine just purged. This should only happen in tests where we
		// have multiple servers running in the same process.
		return
	}

	C.jemalloc_stats_print_abbreviated()
	res, err := C.jemalloc_purge()
	if err != nil || res != 0 {
		log.Warningf(ctx, "jemalloc purging failed: %v (res=%d)", err, int(res))
	} else {
		log.Infof(ctx, "jemalloc arenas purged (took %s)", thisPurge.Elapsed())
	}
}
