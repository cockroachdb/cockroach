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
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
)

func init() {
	mainLog.gcNotify = make(chan struct{}, 1)
}

// StartGCDaemon starts the log file GC -- this must be called after
// command-line parsing has completed so that no data is lost when the
// user configures larger max sizes than the defaults.
//
// The logger's GC daemon stops when the provided context is canceled.
//
// Note that secondary logger get their GC daemon started when
// they are allocated (NewSecondaryLogger). This assumes that
// secondary loggers are only allocated after command line parsing
// has completed too.
func StartGCDaemon(ctx context.Context) {
	go mainLog.gcDaemon(ctx)
}

// gcDaemon runs the GC loop for the given logger.
func (l *loggerT) gcDaemon(ctx context.Context) {
	l.gcOldFiles()
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
func (l *loggerT) gcOldFiles() {
	dir, err := l.logDir.get()
	if err != nil {
		// No log directory configured. Nothing to do.
		return
	}

	// This only lists the log files for the current logger (sharing the
	// prefix).
	allFiles, err := l.listLogFiles()
	if err != nil {
		fmt.Fprintf(OrigStderr, "unable to GC log files: %s\n", err)
		return
	}

	logFilesCombinedMaxSize := atomic.LoadInt64(&LogFilesCombinedMaxSize)
	files := selectFiles(allFiles, math.MaxInt64)
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

		maybeArchiveFile(path)

		if err := os.Remove(path); err != nil {
			fmt.Fprintln(OrigStderr, err)
		}
	}
}

// maybeArchiveFile archive log file using gzip when `LogFileArchive` is set to true
func maybeArchiveFile(path string) {
	if LogFileArchive {

		// create gzip file used by gzip writer
		tf, err := os.Create(path + ".gz")
		if err != nil {
			fmt.Fprintln(OrigStderr, err)
			return
		}
		defer func() {
			if err := tf.Close(); err != nil {
				fmt.Fprintln(OrigStderr, err)
			}
		}()

		// writer for gzip with default compression
		gz, err := gzip.NewWriterLevel(tf, gzip.DefaultCompression)
		if err != nil {
			fmt.Fprintln(OrigStderr, err)
			return
		}
		defer func() {
			if err := gz.Close(); err != nil {
				fmt.Fprintln(OrigStderr, err)
			}
		}()

		// open file and write to Writer
		fs, err := os.Open(path)
		if err != nil {
			fmt.Fprintln(OrigStderr, err)
			return
		}
		defer func() {
			if err := fs.Close(); err != nil {
				fmt.Fprintln(OrigStderr, err)
			}
		}()

		// compress log file to gzip file
		if _, err := io.Copy(gz, fs); err != nil {
			fmt.Fprintln(OrigStderr, err)
			return
		}
	}
}
