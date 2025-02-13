// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

func (m *ChangeAggregatorSpec_Checkpoint) IsZero() bool {
	return len(m.Spans) == 0 && m.Timestamp.IsEmpty()
}
