// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowhandle

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowinspectpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// Noop is a no-op implementation of the kvflowcontrol.Handle interface.
type Noop struct{}

var _ kvflowcontrol.Handle = Noop{}

// Admit is part of the kvflowcontrol.Handle interface.
func (n Noop) Admit(
	ctx context.Context, priority admissionpb.WorkPriority, time time.Time,
) (bool, error) {
	return false, nil
}

// DeductTokensFor is part of the kvflowcontrol.Handle interface.
func (n Noop) DeductTokensFor(
	ctx context.Context,
	priority admissionpb.WorkPriority,
	position kvflowcontrolpb.RaftLogPosition,
	tokens kvflowcontrol.Tokens,
) {
}

// ReturnTokensUpto is part of the kvflowcontrol.Handle interface.
func (n Noop) ReturnTokensUpto(
	ctx context.Context,
	priority admissionpb.WorkPriority,
	position kvflowcontrolpb.RaftLogPosition,
	stream kvflowcontrol.Stream,
) {
}

// ConnectStream is part of the kvflowcontrol.Handle interface.
func (n Noop) ConnectStream(
	ctx context.Context, position kvflowcontrolpb.RaftLogPosition, stream kvflowcontrol.Stream,
) {
}

// DisconnectStream is part of the kvflowcontrol.Handle interface.
func (n Noop) DisconnectStream(ctx context.Context, stream kvflowcontrol.Stream) {}

// ResetStreams is part of the kvflowcontrol.Handle interface.
func (n Noop) ResetStreams(ctx context.Context) {}

// Inspect is part of the kvflowcontrol.Handle interface.
func (n Noop) Inspect(ctx context.Context) kvflowinspectpb.Handle {
	return kvflowinspectpb.Handle{}
}

// Close is part of the kvflowcontrol.Handle interface.
func (n Noop) Close(ctx context.Context) {}
