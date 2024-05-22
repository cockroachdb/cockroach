// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvflowconnectedstream

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// StoreStreamsTokenCounter is one per node.
//
// TODO: modify kvflowcontroller.Controller to implement this.
type StoreStreamsTokenCounter interface {
	EvalTokenCounterForStream(kvflowcontrol.Stream) EvalTokenCounter
	SendTokenCounterForStream(kvflowcontrol.Stream) SendTokenCounter
}

// EvalTokenCounter will be implemented by kvflowcontroller.bucket.
//
// TODO: rename "bucket" -- it is not a token bucket.
type EvalTokenCounter interface {
	// TokensAvailable returns true if tokens are available. If false, it
	// returns a handle to use for waiting using
	// kvflowcontroller.WaitForHandlesAndChannels. This is for waiting
	// pre-evaluation.
	TokensAvailable(admissionpb.WorkClass) (available bool, handle interface{})
	// Deduct deducts (without blocking) flow tokens for the given priority.
	Deduct(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
	// Return returns flow tokens for the given priority.
	Return(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
}

// SendTokenCounter will be implemented by kvflowcontroller.bucket.
type SendTokenCounter interface {
	TryDeduct(
		context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens) kvflowcontrol.Tokens
	Deduct(
		context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
	Return(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)

	// TokensAvailable is needed for the implementation of
	TokensAvailable(admissionpb.WorkClass) (available bool, handle interface{})
}

// StoreStreamSendTokensWatcher implements a watcher interface that will use
// at most one goroutine per SendTokenCounter and WorkClass that has no send
// tokens. Replicas (from different ranges) waiting for those tokens will call
// NotifyWhenAvailable to queue up for those send tokens.
type StoreStreamSendTokensWatcher interface {
	NotifyWhenAvailable(
		stc SendTokenCounter,
		bytesInQueue int64,
		wc admissionpb.WorkClass,
		tokensGrantedNotification TokenGrantedNotification,
	) (handle struct{})
	UpdateHandle(handle struct{}, wc admissionpb.WorkClass)
	CancelHandle(handle struct{})
}

// TokenGrantedNotification ...
type TokenGrantedNotification interface {
	Granted(tokens int64)
}
