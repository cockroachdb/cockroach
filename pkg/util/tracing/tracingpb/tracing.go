// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracingpb

// Empty returns true if t does not have any tracing info in it.
func (t *TraceInfo) Empty() bool {
	return t == nil || t.TraceID == 0
}
