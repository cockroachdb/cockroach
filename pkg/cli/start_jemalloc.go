// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build !stdmalloc

package cli

import (
	"fmt"
	"unsafe"

	"github.com/cockroachdb/cockroach/pkg/server/profiler"
)

// #cgo CPPFLAGS: -DJEMALLOC_NO_DEMANGLE
// #cgo LDFLAGS: -ljemalloc
// #cgo dragonfly freebsd LDFLAGS: -lm
// #cgo linux LDFLAGS: -lrt -lm -lpthread
//
// #include <jemalloc/jemalloc.h>
// #include <stddef.h>
// #include <stdio.h>
//
// // These default options can still be overriden via /etc/malloc.conf and the
// // MALLOC_CONF env var. From the jemalloc man page:
// //   The string specified via --with-malloc-conf, the string pointed to by
// //   the global variable malloc_conf, the “name” of the file referenced by
// //   the symbolic link named /etc/malloc.conf, and the value of the
// //   environment variable MALLOC_CONF, will be interpreted, in that order,
// //   from left to right as options. Note that malloc_conf may be read before
// //   main() is entered, so the declaration of malloc_conf should specify an
// //   initializer that contains the final value to be read by jemalloc.
// //   --with-malloc-conf and malloc_conf are compile-time mechanisms, whereas
// //   /etc/malloc.conf and MALLOC_CONF can be safely set any time prior to
// //   program invocation.
// #ifdef __linux__
// const char *je_malloc_conf = "background_thread:true,thp:never,metadata_thp:disabled,dirty_decay_ms:2000,muzzy_decay_ms:0";
// #endif
//
// // Checks whether jemalloc profiling is enabled and active.
// // Returns true if profiling is enabled and active.
// // Returns false on any mallctl errors.
// bool is_profiling_enabled() {
//   bool enabled = false;
//   size_t enabledSize = sizeof(enabled);
//
//   // Check profiling flag.
//   if (je_mallctl("opt.prof", &enabled, &enabledSize, NULL, 0) != 0) {
//     return false;
//   }
//   if (!enabled) {
//     return false;
//   }
//
//   // Check prof_active flag.
//   if (je_mallctl("opt.prof_active", &enabled, &enabledSize, NULL, 0) != 0) {
//     return false;
//   }
//   return enabled;
// }
//
// typedef struct {
//   bool prof;
//   bool prof_active;
//   bool background_thread;
//   const char *thp;
//   const char *metadata_thp;
//   unsigned narenas;
//   size_t dirty_decay_ms;
//   size_t muzzy_decay_ms;
// } JemallocOpts;
//
// // Prints a string showing the values of some of the jemalloc options into
// // the given buffer of the given size.
// int jemalloc_get_opts(JemallocOpts *opts) {
//   size_t sz;
//   int err;
//
//   sz = sizeof(opts->prof);
//   err = je_mallctl("opt.prof", &opts->prof, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(opts->prof_active);
//   err = je_mallctl("opt.prof_active", &opts->prof_active, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(opts->thp);
//   err = je_mallctl("opt.thp", &opts->thp, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(opts->metadata_thp);
//   err = je_mallctl("opt.metadata_thp", &opts->metadata_thp, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(opts->background_thread);
//   err = je_mallctl("opt.background_thread", &opts->background_thread, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(opts->narenas);
//   err = je_mallctl("opt.narenas", &opts->narenas, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(opts->dirty_decay_ms);
//   err = je_mallctl("opt.dirty_decay_ms", &opts->dirty_decay_ms, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   sz = sizeof(opts->muzzy_decay_ms);
//   err = je_mallctl("opt.muzzy_decay_ms", &opts->muzzy_decay_ms, &sz, NULL, 0);
//   if (err != 0) {
//     return err;
//   }
//   return 0;
// }
//
// // Write a heap profile to "filename". Returns true on success, false on error.
// int dump_heap_profile(const char *filename) {
//   return je_mallctl("prof.dump", NULL, NULL, &filename, sizeof(const char *));
// }
import "C"

func init() {
	if C.is_profiling_enabled() {
		profiler.SetJemallocHeapDumpFn(writeJemallocProfile)
	}

	var opts C.JemallocOpts
	if res, _ := C.jemalloc_get_opts(&opts); res != 0 {
		cgoAllocConfStr = fmt.Sprintf("unknown (error %d)", int(res))
	} else {
		cgoAllocConfStr = fmt.Sprintf("prof:%t,prof_active:%t,background_thread:%t,thp:%s,metadata_thp:%s,narenas:%d,dirty_decay_ms:%d,muzzy_decay_ms:%d",
			bool(opts.prof),
			bool(opts.prof_active),
			bool(opts.background_thread),
			C.GoString(opts.thp),
			C.GoString(opts.metadata_thp),
			int(opts.narenas),
			int(opts.dirty_decay_ms),
			int(opts.muzzy_decay_ms),
		)
	}
}

// writeJemallocProfile tells jemalloc to write a heap profile to 'filename'.
// It assumes that profiling is enabled and active.
func writeJemallocProfile(filename string) error {
	cpath := C.CString(filename)
	defer C.free(unsafe.Pointer(cpath))

	if errCode := C.dump_heap_profile(cpath); errCode != 0 {
		return fmt.Errorf("error code %d", errCode)
	}
	return nil
}
