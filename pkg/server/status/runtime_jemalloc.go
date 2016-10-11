// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Marc Berhault (marc@cockroachlabs.com)

// +build !stdmalloc

package status

// #cgo darwin CPPFLAGS: -I../../../c-jemalloc/darwin_includes/internal/include
// #cgo freebsd CPPFLAGS: -I../../../c-jemalloc/freebsd_includes/internal/include
// #cgo linux CPPFLAGS: -I../../../c-jemalloc/linux_includes/internal/include
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
//
// #include <jemalloc/jemalloc.h>
//
// // See field definitions at:
// // http://www.canonware.com/download/jemalloc/jemalloc-latest/doc/jemalloc.html#stats.allocated
// typedef struct {
//   size_t allocated;
//   size_t active;
//   size_t metadata;
//   size_t resident;
//   size_t mapped;
//   size_t retained;
// } JemallocStats;
//
// int jemalloc_get_stats(JemallocStats *stats) {
//   // Update the statistics cached by mallctl.
//   uint64_t epoch = 1;
//   size_t sz = sizeof(epoch);
//   mallctl("epoch", &epoch, &sz, &epoch, sz);
//
//   sz = sizeof(size_t);
//   int err = mallctl("stats.allocated", &stats->allocated, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   err = mallctl("stats.active", &stats->active, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   err = mallctl("stats.metadata", &stats->metadata, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   err = mallctl("stats.resident", &stats->resident, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   err = mallctl("stats.mapped", &stats->mapped, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   // stats.retained is introduced in 4.2.0.
//   // err = mallctl("stats.retained", &stats->retained, &sz, NULL, 0);
//   return err;
// }
import "C"

import (
	"fmt"

	"golang.org/x/net/context"

	// This is explicit because this Go library does not export any Go symbols.
	_ "github.com/cockroachdb/c-jemalloc"

	"github.com/cockroachdb/cockroach/util/log"

	"github.com/dustin/go-humanize"
)

func init() {
	if getCgoMemStats != nil {
		panic("getCgoMemStats is already set")
	}
	getCgoMemStats = getJemallocStats
}

func getJemallocStats() (uint64, uint64, error) {
	var js C.JemallocStats
	// TODO(marc): should we panic here? Failure on fetching the stats may be a problem.
	if errCode := C.jemalloc_get_stats(&js); errCode != 0 {
		return 0, 0, fmt.Errorf("error code %d", errCode)
	}

	if log.V(2) {
		// Summary of jemalloc stats:
		log.Infof(context.TODO(), "jemalloc stats: allocated: %s, active: %s, metadata: %s, resident: %s, mapped: %s",
			humanize.IBytes(uint64(js.allocated)), humanize.IBytes(uint64(js.active)),
			humanize.IBytes(uint64(js.metadata)), humanize.IBytes(uint64(js.resident)),
			humanize.IBytes(uint64(js.mapped)))
	}

	if log.V(3) {
		// Detailed jemalloc stats (very verbose, includes per-arena stats).
		C.malloc_stats_print(nil, nil, nil)
	}

	return uint64(js.allocated), uint64(js.resident), nil
}
