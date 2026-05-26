// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func TestPriority(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

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
