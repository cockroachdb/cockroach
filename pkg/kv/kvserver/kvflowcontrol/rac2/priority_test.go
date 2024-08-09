// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rac2

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/stretchr/testify/require"
)

func TestPriority(t *testing.T) {
	var lastRaftPriority raftpb.Priority
	lastConvertedBackPriority := admissionpb.LowPri
	for i := int(admissionpb.LowPri); i < admissionpb.OneAboveHighPri; i++ {
		raftPri := AdmissionToRaftPriority(admissionpb.WorkPriority(i))
		// Monotonically non-decreasing raftpb.Priorities.
		require.LessOrEqual(t, lastRaftPriority, raftPri)
		lastRaftPriority = raftPri
		convertedBackPri := RaftToAdmissionPriority(raftPri)
		// Monotonically non-decreasing convertedBackPriorities.
		require.LessOrEqual(t, lastConvertedBackPriority, convertedBackPri)
		lastConvertedBackPriority = convertedBackPri
		// WorkClassFromRaftPriority is sane.
		require.Equal(t, admissionpb.WorkClassFromPri(convertedBackPri), WorkClassFromRaftPriority(raftPri))
		require.Equal(t, admissionpb.WorkClassFromPri(admissionpb.WorkPriority(i)),
			WorkClassFromRaftPriority(raftPri))
	}
}
