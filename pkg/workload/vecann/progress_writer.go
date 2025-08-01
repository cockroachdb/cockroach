// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vecann

import (
	"io"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ProgressWriter tracks download progress.
type ProgressWriter struct {
	// OnProgress, if set, will be called on a granular basis with download
	// progress information.
	OnProgress func(downloaded, total int64, elapsed time.Duration)

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
	if pw.OnProgress != nil {
		elapsed := timeutil.Since(pw.startTime)
		pw.OnProgress(pw.downloaded, pw.total, elapsed)
	}
	return n, err
}

func roundDuration(duration time.Duration) time.Duration {
	return duration.Truncate(time.Millisecond)
}
