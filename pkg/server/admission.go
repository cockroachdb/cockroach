// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

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
	ctx context.Context,
	origin roachpb.NodeID,
	pri admissionpb.WorkPriority,
	storeID roachpb.StoreID,
	rangeID roachpb.RangeID,
	pos admission.LogPosition,
) {
	// TODO(irfansharif,aaditya): This contributes to a high count of
	// inuse_objects. Look to address it as part of #104154.
	a.dispatchWriter.Dispatch(ctx, origin, kvflowcontrolpb.AdmittedRaftLogEntries{
		RangeID:           rangeID,
		AdmissionPriority: int32(pri),
		UpToRaftLogPosition: kvflowcontrolpb.RaftLogPosition{
			Term:  pos.Term,
			Index: pos.Index,
		},
		StoreID: storeID,
	})
}
