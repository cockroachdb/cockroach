// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package taskset

type taskSpan struct {
	start TaskID
	end   TaskID
}

func (t *taskSpan) size() int64 {
	return int64(t.end) - int64(t.start)
}
