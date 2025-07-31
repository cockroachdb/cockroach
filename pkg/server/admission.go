// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission"
)

type admittedLogEntryAdaptor struct {
	dispatchWriterV1 kvflowcontrol.DispatchWriter
	callbackV2       admission.OnLogEntryAdmitted
}

var _ admission.OnLogEntryAdmitted = &admittedLogEntryAdaptor{}

func newAdmittedLogEntryAdaptor(
	dispatchWriterV1 kvflowcontrol.DispatchWriter, callbackV2 admission.OnLogEntryAdmitted,
) *admittedLogEntryAdaptor {
	return &admittedLogEntryAdaptor{
		dispatchWriterV1: dispatchWriterV1,
		callbackV2:       callbackV2,
	}
}

// AdmittedLogEntry implements the admission.OnLogEntryAdmitted interface.
func (a *admittedLogEntryAdaptor) AdmittedLogEntry(
	ctx context.Context, cbState admission.LogEntryAdmittedCallbackState,
) {
	if cbState.IsV2Protocol {
		a.callbackV2.AdmittedLogEntry(ctx, cbState)
		return
	}
	// TODO(irfansharif,aaditya): This contributes to a high count of
	// inuse_objects. Look to address it as part of #104154.
	a.dispatchWriterV1.Dispatch(ctx, cbState.Origin, kvflowcontrolpb.AdmittedRaftLogEntries{
		RangeID:           cbState.RangeID,
		AdmissionPriority: int32(cbState.Pri),
		UpToRaftLogPosition: kvflowcontrolpb.RaftLogPosition{
			Term:  cbState.Pos.Term,
			Index: cbState.Pos.Index,
		},
		StoreID: cbState.StoreID,
	})
}
