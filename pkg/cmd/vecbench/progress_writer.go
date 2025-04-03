// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ProgressWriter tracks download progress.
type ProgressWriter struct {
	startTime  time.Time
	writer     io.Writer
	total      int64
	downloaded int64
}

func makeProgressWriter(writer io.Writer, total int64) ProgressWriter {
	return ProgressWriter{
		startTime: timeutil.Now(),
		writer:    writer,
		total:     total,
	}
}

func (pw *ProgressWriter) Write(p []byte) (int, error) {
	n, err := pw.writer.Write(p)
	pw.downloaded += int64(n)
	pw.printProgress()
	return n, err
}

func (pw *ProgressWriter) printProgress() {
	elapsed := timeutil.Since(pw.startTime)
	fmt.Printf(Cyan+"\rDownloaded %s / %s (%.2f%%) in %v          "+Reset,
		formatSize(pw.downloaded), formatSize(pw.total),
		(float64(pw.downloaded)/float64(pw.total))*100, elapsed.Truncate(time.Second))
}

func roundDuration(duration time.Duration) time.Duration {
	return duration.Truncate(time.Millisecond)
}

func formatSize(size int64) string {
	const (
		_           = iota // ignore first value by assigning to blank identifier
		KiB float64 = 1 << (10 * iota)
		MiB
		GiB
		TiB
		PiB
	)

	switch {
	case size < int64(KiB):
		return fmt.Sprintf("%d B", size)
	case size < int64(MiB):
		return fmt.Sprintf("%.2f KiB", float64(size)/KiB)
	case size < int64(GiB):
		return fmt.Sprintf("%.2f MiB", float64(size)/MiB)
	case size < int64(TiB):
		return fmt.Sprintf("%.2f GiB", float64(size)/GiB)
	case size < int64(PiB):
		return fmt.Sprintf("%.2f TiB", float64(size)/TiB)
	default:
		return fmt.Sprintf("%.2f PiB", float64(size)/PiB)
	}
}
