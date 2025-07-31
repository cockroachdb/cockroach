// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvflowdispatch

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type dummy struct{}

var _ kvflowcontrol.Dispatch = &dummy{}

// NewDummyDispatch returns a dummy implementation of kvflowcontrol.Dispatch,
// for use in tests.
func NewDummyDispatch() kvflowcontrol.Dispatch {
	return &dummy{}
}

func (d *dummy) Dispatch(context.Context, roachpb.NodeID, kvflowcontrolpb.AdmittedRaftLogEntries) {
}

func (d *dummy) PendingDispatch() []roachpb.NodeID {
	return nil
}

func (d *dummy) PendingDispatchFor(
	nodeID roachpb.NodeID, maxBytes int64,
) ([]kvflowcontrolpb.AdmittedRaftLogEntries, int) {
	return nil, 0
}
