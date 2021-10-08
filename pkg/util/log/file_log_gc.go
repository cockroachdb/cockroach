// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
)

// gcDaemon runs the GC loop for the given logger.
func (l *fileSink) gcDaemon(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-l.gcNotify:
		}

		logging.mu.Lock()
		doGC := !logging.mu.disableDaemons
		logging.mu.Unlock()

		if doGC {
			l.gcOldFiles()
		}
	}
}

// gcOldFiles removes the "old" files that do not match
// the configured size and number threshold.
func (l *fileSink) gcOldFiles() {
	// This only lists the log files for the current logger (sharing the
	// prefix).
	dir, allFiles, err := l.listLogFiles()
	if err != nil {
		fmt.Fprintf(OrigStderr, "unable to GC log files: %s\n", err)
		return
	}

	if len(allFiles) == 0 {
		// Nothing to do.
		return
	}

	logFilesCombinedMaxSize := atomic.LoadInt64(&l.logFilesCombinedMaxSize)
	if logFilesCombinedMaxSize == 0 {
		// Nothing to do.
		return
	}

	files := selectFilesInGroup(allFiles, math.MaxInt64)
	if len(files) == 0 {
		return
	}
	// files is sorted with the newest log files first (which we want
	// to keep). Note that we always keep the most recent log file.
	sum := files[0].SizeBytes
	for _, f := range files[1:] {
		sum += f.SizeBytes
		if sum < logFilesCombinedMaxSize {
			continue
		}
		path := filepath.Join(dir, f.Name)
		if err := os.Remove(path); err != nil {
			fmt.Fprintln(OrigStderr, err)
		}
	}
}
