// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserverpb

import "github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"

// SafeValue implements the redact.SafeValue interface.
func (SnapshotRequest_Type) SafeValue() {}

type ResponseWithTracingSpans interface {
	GetRecordedSpans() tracingpb.Recording
}

var _ ResponseWithTracingSpans = (*SnapshotResponse)(nil)
var _ ResponseWithTracingSpans = (*DelegateSnapshotResponse)(nil)

func (m *SnapshotResponse) GetRecordedSpans() tracingpb.Recording {
	return m.CollectedSpans
}

func (m *DelegateSnapshotResponse) GetRecordedSpans() tracingpb.Recording {
	return m.CollectedSpans
}
