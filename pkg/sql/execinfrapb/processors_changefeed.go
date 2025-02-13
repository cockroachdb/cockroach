// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

// IsEmpty returns whether the checkpoint is empty.
func (m *ChangeAggregatorSpec_Checkpoint) IsEmpty() bool {
	return m == nil || (len(m.Spans) == 0 && m.Timestamp.IsEmpty())
}
