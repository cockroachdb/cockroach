// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

type admittedLogEntryAdaptor struct {
	dispatchWriter kvflowcontrol.DispatchWriter
}

var _ admission.OnLogEntryAdmitted = &admittedLogEntryAdaptor{}

func newAdmittedLogEntryAdaptor(
	dispatchWriter kvflowcontrol.DispatchWriter,
) *admittedLogEntryAdaptor {
	return &admittedLogEntryAdaptor{
		dispatchWriter: dispatchWriter,
	}
}

// AdmittedLogEntry implements the admission.OnLogEntryAdmitted interface.
func (a *admittedLogEntryAdaptor) AdmittedLogEntry(
	origin roachpb.NodeID,
	pri admissionpb.WorkPriority,
	storeID roachpb.StoreID,
	rangeID roachpb.RangeID,
	pos admission.LogPosition,
) {
	a.dispatchWriter.Dispatch(origin, kvflowcontrolpb.AdmittedRaftLogEntries{
		RangeID:           rangeID,
		AdmissionPriority: int32(pri),
		UpToRaftLogPosition: kvflowcontrolpb.RaftLogPosition{
			Term:  pos.Term,
			Index: pos.Index,
		},
		StoreID: storeID,
	})
}
