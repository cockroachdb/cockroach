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
// #cgo linux CPPFLAGS: -I../../../c-jemalloc/linux_includes/internal/include
// #cgo darwin LDFLAGS: -Wl,-undefined -Wl,dynamic_lookup
// #cgo linux LDFLAGS: -Wl,-unresolved-symbols=ignore-all
//
// #include <jemalloc/jemalloc.h>
import "C"

import (
	"github.com/cockroachdb/cockroach/util/log"
)

func init() {
	if logBuildStats != nil {
		panic("logBuildStats is already set")
	}
	logBuildStats = logJemallocStats
}

func logJemallocStats() {
	if log.V(3) {
		C.malloc_stats_print(nil, nil, nil)
	}
}
