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
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// AdmissionPriorityToRaftPriority maps the larger set of values in
// admissionpb.WorkPriority to the smaller set of raftpb.Priority.
func AdmissionPriorityToRaftPriority(pri admissionpb.WorkPriority) raftpb.Priority {
	if pri < admissionpb.NormalPri {
		return raftpb.LowPri
	} else if pri < admissionpb.LockingNormalPri {
		return raftpb.NormalPri
	} else if pri < admissionpb.UserHighPri {
		return raftpb.AboveNormalPri
	} else if pri <= admissionpb.HighPri {
		return raftpb.HighPri
	} else {
		panic("unknown priority")
	}
}

// RaftPriorityToAdmissionPriority maps a raftpb.Priority to the lowest
// admissionpb.WorkPriority that could map to it. This is needed before
// calling into the admission package, since it is possible for a mix of RACv2
// entries and other entries to be competing in the same admission WorkQueue.
func RaftPriorityToAdmissionPriority(rp raftpb.Priority) admissionpb.WorkPriority {
	if rp < raftpb.NormalPri {
		return admissionpb.LowPri
	} else if rp < raftpb.AboveNormalPri {
		return admissionpb.NormalPri
	} else if rp < raftpb.HighPri {
		return admissionpb.UserHighPri
	} else if rp < raftpb.NumPriorities {
		return admissionpb.HighPri
	} else {
		panic("unknown priority")
	}
}

// WorkClassFromRaftPriority maps a raftpb.Priority to the kinds of flow
// tokens needed. The result here should be equivalent to
// admissionpb.WorkClassFromPri(RaftPriorityToAdmissionPriority(pri)).
func WorkClassFromRaftPriority(pri raftpb.Priority) admissionpb.WorkClass {
	switch pri {
	case raftpb.LowPri:
		return admissionpb.ElasticWorkClass
	case raftpb.NormalPri, raftpb.AboveNormalPri, raftpb.HighPri:
		return admissionpb.RegularWorkClass
	default:
		panic("")
	}
}
