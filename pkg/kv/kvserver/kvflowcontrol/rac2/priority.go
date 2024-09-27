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
	"github.com/cockroachdb/cockroach/pkg/raft/rafttype"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// AdmissionToRaftPriority maps the larger set of values in
// admissionpb.WorkPriority to the smaller set of rafttype.Priority.
func AdmissionToRaftPriority(pri admissionpb.WorkPriority) rafttype.Priority {
	if pri < admissionpb.NormalPri {
		return rafttype.LowPri
	} else if pri < admissionpb.LockingNormalPri {
		return rafttype.NormalPri
	} else if pri < admissionpb.UserHighPri {
		return rafttype.AboveNormalPri
	} else if pri <= admissionpb.HighPri {
		return rafttype.HighPri
	} else {
		panic("unknown priority")
	}
}

// RaftToAdmissionPriority maps a rafttype.Priority to the lowest
// admissionpb.WorkPriority that could map to it. This is needed before
// calling into the admission package, since it is possible for a mix of RACv2
// entries and other entries to be competing in the same admission WorkQueue.
func RaftToAdmissionPriority(rp rafttype.Priority) admissionpb.WorkPriority {
	if rp < rafttype.NormalPri {
		return admissionpb.LowPri
	} else if rp < rafttype.AboveNormalPri {
		return admissionpb.NormalPri
	} else if rp < rafttype.HighPri {
		return admissionpb.UserHighPri
	} else if rp < rafttype.NumPriorities {
		return admissionpb.HighPri
	} else {
		panic("unknown priority")
	}
}

// WorkClassFromRaftPriority maps a rafttype.Priority to the kinds of flow
// tokens needed. The result here should be equivalent to
// admissionpb.WorkClassFromPri(RaftToAdmissionPriority(pri)).
func WorkClassFromRaftPriority(pri rafttype.Priority) admissionpb.WorkClass {
	switch pri {
	case rafttype.LowPri:
		return admissionpb.ElasticWorkClass
	case rafttype.NormalPri, rafttype.AboveNormalPri, rafttype.HighPri:
		return admissionpb.RegularWorkClass
	default:
		panic("")
	}
}
