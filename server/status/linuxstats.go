// Copyright 2015 The Cockroach Authors.
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
// Author: Cuong Do (cdo@cockroachlabs.com)
//
// +build linux

package status

// #include <malloc.h>
// #include <stdio.h>
import "C"

import (
	"unsafe"

	"github.com/dustin/go-humanize"

	"github.com/cockroachdb/cockroach/util/log"
)

func logLinuxStats() {
	if !log.V(1) {
		return
	}

	// We don't know which fields in struct mallinfo are most relevant to us yet,
	// so log it all for now.
	//
	// A major caveat is that mallinfo() returns stats only for the main arena.
	// glibc uses multiple allocation arenas to increase malloc performance for
	// multithreaded processes, so mallinfo may not report on significant parts
	// of the heap.
	mi := C.mallinfo()
	log.Infof("mallinfo stats: ordblks=%s, smblks=%s, hblks=%s, hblkhd=%s, usmblks=%s, fsmblks=%s, "+
		"uordblks=%s, fordblks=%s, keepcost=%s",
		humanize.IBytes(uint64(mi.ordblks)),
		humanize.IBytes(uint64(mi.smblks)),
		humanize.IBytes(uint64(mi.hblks)),
		humanize.IBytes(uint64(mi.hblkhd)),
		humanize.IBytes(uint64(mi.usmblks)),
		humanize.IBytes(uint64(mi.fsmblks)),
		humanize.IBytes(uint64(mi.uordblks)),
		humanize.IBytes(uint64(mi.fordblks)),
		humanize.IBytes(uint64(mi.fsmblks)))

	// malloc_info() emits a *lot* of XML, partly because it generates stats for
	// all arenas, unlike mallinfo().
	//
	// TODO(cdo): extract the useful bits and record to time-series DB.
	if !log.V(2) {
		return
	}

	// Create a memstream and make malloc_info() output to it.
	var buf *C.char
	var bufSize C.size_t
	memstream := C.open_memstream(&buf, &bufSize)
	if memstream == nil {
		log.Warning("couldn't open memstream")
		return
	}
	defer func() {
		C.fclose(memstream)
		C.free(unsafe.Pointer(buf))
	}()
	if rc := C.malloc_info(0, memstream); rc != 0 {
		log.Warningf("malloc_info returned %d", rc)
		return
	}
	if rc := C.fflush(memstream); rc != 0 {
		log.Warningf("fflush returned %d", rc)
		return
	}
	log.Infof("malloc_info: %s", C.GoString(buf))
}

func init() {
	if logOSStats != nil {
		panic("logOSStats is already set")
	}
	logOSStats = logLinuxStats
}
