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

func (a PiggybackedAdmittedState) String() string {
	return redact.StringWithoutMarkers(a)
}

func (a PiggybackedAdmittedState) SafeFormat(w redact.SafePrinter, _ rune) {
	w.Printf("admitted-state (r%s from-replica %d to s%s replica %d %s)",
		a.RangeID, a.FromReplicaID, a.ToStoreID, a.ToReplicaID, a.Admitted.String())
}

func (a AdmittedState) String() string {
	return redact.StringWithoutMarkers(a)
}

func (a AdmittedState) SafeFormat(w redact.SafePrinter, _ rune) {
	if len(a.Admitted) > 0 {
		w.Printf("admitted [%d %d %d %d]", a.Admitted[0], a.Admitted[1], a.Admitted[2], a.Admitted[3])
	}
}
