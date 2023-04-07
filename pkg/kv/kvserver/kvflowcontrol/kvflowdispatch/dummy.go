// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

func (d *dummy) PendingDispatchFor(id roachpb.NodeID) []kvflowcontrolpb.AdmittedRaftLogEntries {
	return nil
}
