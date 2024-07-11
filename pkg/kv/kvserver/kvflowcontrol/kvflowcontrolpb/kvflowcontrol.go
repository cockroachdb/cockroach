// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowcontrolpb

import (
	fmt "fmt"
	math "math"

	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/redact"
)

func (p RaftLogPosition) String() string {
	return redact.StringWithoutMarkers(p)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (p RaftLogPosition) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("log-position=%d/%d", p.Term, p.Index)
}

// Equal returns whether the two raft log positions are identical.
func (p *RaftLogPosition) Equal(o RaftLogPosition) bool {
	return p.Term == o.Term && p.Index == o.Index
}

// Less returns whether the one raft log position is less than the other. Those
// with lower terms sort first, and barring that, those with lower indexes.
func (p *RaftLogPosition) Less(o RaftLogPosition) bool {
	if p.Term != o.Term {
		return p.Term < o.Term
	}
	return p.Index < o.Index
}

// LessEq returns whether one raft log position is less than or equal to the
// other
func (p *RaftLogPosition) LessEq(o RaftLogPosition) bool {
	return p.Less(o) || p.Equal(o)
}

func (a AdmittedRaftLogEntries) String() string {
	return redact.StringWithoutMarkers(a)
}

// SafeFormat implements the redact.SafeFormatter interface.
func (a AdmittedRaftLogEntries) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("admitted-entries (r%s s%s pri=%s up-to-%s)",
		a.RangeID, a.StoreID, admissionpb.WorkPriority(a.AdmissionPriority), a.UpToRaftLogPosition)
}

func (a AdmittedForRangeRACv2) String() string {
	return redact.StringWithoutMarkers(a)
}

func (a AdmittedForRangeRACv2) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("admitted-entries (r%s %+v)", a.RangeID, a.Msg)
}

type RaftPriority uint8

const (
	RaftLowPri RaftPriority = iota
	RaftNormalPri
	RaftAboveNormalPri
	RaftHighPri
	NumRaftPriorities

	// The following are not real priorities, but will be encoded in
	// RaftMessageRequest.InheritedRaftPriority. This should actually be a
	// separate enum in that proto instead of trying to overload the priority.
	// The reason we were trying to fit this into the same byte earlier was that
	// we were planning to reencode this override into the Entry, but we are no
	// longer doing that. This is ok-ish for the sake of the prototype.
	//
	// TODO: move these elsewhere.

	NotSubjectToACForFlowControl       RaftPriority = math.MaxUint8 - 2
	PriorityNotInheritedForFlowControl RaftPriority = math.MaxUint8 - 1
)

func (p RaftPriority) String() string {
	switch p {
	case RaftLowPri:
		return "LowPri"
	case RaftNormalPri:
		return "NormalPri"
	case RaftAboveNormalPri:
		return "AboveNormalPri"
	case RaftHighPri:
		return "HighPri"
	default:
		panic("invalid raft priority")
	}
}

func RaftPriorityConversionForUnusedZero(pri RaftPriority) uint8 {
	if RaftPriority(pri) > RaftHighPri && RaftPriority(pri) < NotSubjectToACForFlowControl {
		panic(fmt.Sprintf("invalid raft priority: %d", pri))
	}
	return uint8(pri + 1)
}

// UndoRaftPriorityConversionForUnusedZero ...
// REQUIRES: pri > 0
func UndoRaftPriorityConversionForUnusedZero(pri uint8) RaftPriority {
	if RaftPriority(pri) > RaftHighPri && RaftPriority(pri) < NotSubjectToACForFlowControl {
		panic(fmt.Sprintf("invalid raft priority: %d", pri))
	}
	return RaftPriority(pri - 1)
}

// AdmissionPriorityToRaftPriority maps the larger set of values in admissionpb.WorkPriority
// to the smaller set of raft priorities.
func AdmissionPriorityToRaftPriority(pri admissionpb.WorkPriority) (rp RaftPriority) {
	if pri < admissionpb.NormalPri {
		return RaftLowPri
	} else if pri < admissionpb.LockingNormalPri {
		return RaftNormalPri
	} else if pri < admissionpb.UserHighPri {
		return RaftAboveNormalPri
	} else if pri <= admissionpb.HighPri {
		return RaftHighPri
	} else {
		panic("unknown priority")
	}
}

// RaftPriorityToAdmissionPriority maps a RaftPriority to the highest
// admissionpb.WorkPriority that could map to it. This is needed before
// calling into the admission package, since it is possible for a mix of RACv2
// entries and other entries to be competing in the same admission WorkQueue.
func RaftPriorityToAdmissionPriority(rp RaftPriority) (pri admissionpb.WorkPriority) {
	if rp < RaftNormalPri {
		return admissionpb.LowPri
	} else if rp < RaftAboveNormalPri {
		return admissionpb.NormalPri
	} else if rp < RaftHighPri {
		return admissionpb.UserHighPri
	} else if rp < NumRaftPriorities {
		return admissionpb.HighPri
	} else {
		panic("unknown priority")
	}
}

// Used for deciding what kind of flow tokens are needed. The result here
// should be equivalent to
// admissionpb.WorkClassFromPri(admissionpb.RaftPriorityToAdmissionPriority(pri)),
// though that is not necessary for correctness since this computation is used
// only locally in the leader and within the RACv2 sub-system.
func WorkClassFromRaftPriority(pri RaftPriority) admissionpb.WorkClass {
	switch pri {
	case RaftLowPri:
		return admissionpb.ElasticWorkClass
	case RaftNormalPri, RaftAboveNormalPri, RaftHighPri:
		return admissionpb.RegularWorkClass
	default:
		panic("")
	}
}
