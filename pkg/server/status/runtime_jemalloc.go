// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !stdmalloc

package status

// #cgo CPPFLAGS: -DJEMALLOC_NO_DEMANGLE
// #cgo LDFLAGS: -ljemalloc
// #cgo dragonfly freebsd LDFLAGS: -lm
// #cgo linux LDFLAGS: -lrt -lm -lpthread
//
// #include <jemalloc/jemalloc.h>
//
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
//   sz = sizeof(&stats->Allocated);
//   err = je_mallctl("stats.allocated", &stats->Allocated, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Active);
//   err = je_mallctl("stats.active", &stats->Active, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Metadata);
//   err = je_mallctl("stats.metadata", &stats->Metadata, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Resident);
//   err = je_mallctl("stats.resident", &stats->Resident, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Mapped);
//   err = je_mallctl("stats.mapped", &stats->Mapped, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(&stats->Retained);
//   err = je_mallctl("stats.retained", &stats->Retained, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   return err;
// }
import "C"

import (
	"context"
	"math"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/dustin/go-humanize"
)

func init() {
	if getCgoMemStats != nil {
		panic("getCgoMemStats is already set")
	}
	getCgoMemStats = getJemallocStats
}

func getJemallocStats(ctx context.Context) (uint, uint, error) {
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
		log.Infof(ctx, "jemalloc stats: %s", strings.Join(stats, " "))
	}

	// NB: the `!V(MaxInt32)` condition is a workaround to not spew this to the
	// logs whenever log interception (log spy) is active. If we refactored
	// je_malloc_stats_print to return a string that we can `log.Infof` instead,
	// we wouldn't need this.
	if log.V(3) && !log.V(math.MaxInt32) {
		// Detailed jemalloc stats (very verbose, includes per-arena stats).
		C.je_malloc_stats_print(nil, nil, nil)
	}

	return uint(js.Allocated), uint(js.Resident), nil
}

// Used to force allocation in tests. 'import "C"' is not supported in tests.
func allocateMemory() {
	// Empirically, 8KiB is not enough, but 16KiB is except for ppc64le, where 256KiB is needed.
	C.malloc(256 << 10)
}
