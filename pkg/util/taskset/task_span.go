// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package taskset

type taskSpan struct {
	start TaskId
	end   TaskId
}

func (t *taskSpan) size() int {
	return int(t.end) - int(t.start)
}

func (t taskSpan) split() (taskSpan, taskSpan) {
	mid := (t.start + t.end) / 2
	return taskSpan{start: t.start, end: mid}, taskSpan{start: mid, end: t.end}
}
