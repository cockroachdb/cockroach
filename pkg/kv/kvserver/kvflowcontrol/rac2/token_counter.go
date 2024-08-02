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
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
)

// TokenCounter is the interface for a token counter that can be used to deduct
// and return flow control tokens. Additionally, it can be used to wait for
// tokens to become available, and to check if tokens are available without
// blocking.
type TokenCounter interface {
	// TokensAvailable returns true if tokens are available. If false, it returns
	// a handle that may be used for waiting for tokens to become available.
	TokensAvailable(admissionpb.WorkClass) (available bool, tokenWaitingHandle TokenWaitingHandle)
	// TryDeduct attempts to deduct flow tokens for the given work class. If
	// there are no tokens available, 0 tokens are returned. When less than the
	// requested token count is available, partial tokens are returned
	// corresponding to this partial amount.
	TryDeduct(
		context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens) kvflowcontrol.Tokens
	// Deduct deducts (without blocking) flow tokens for the given work class. If
	// there are not enough available tokens, the token counter will go into debt
	// (negative available count) and still issue the requested number of tokens.
	Deduct(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
	// Return returns flow tokens for the given work class.
	Return(context.Context, admissionpb.WorkClass, kvflowcontrol.Tokens)
}

// TokenWaitingHandle is the interface for waiting for positive tokens from a
// token counter.
type TokenWaitingHandle interface {
	// WaitChannel is the channel that will be signaled if tokens are possibly
	// available. If signaled, the caller must call
	// ConfirmHaveTokensAndUnblockNextWaiter.
	WaitChannel() <-chan struct{}
	// ConfirmHaveTokensAndUnblockNextWaiter is called to confirm tokens are
	// available. True is returned if tokens are available, false otherwise. If
	// no tokens are available, the caller can resume waiting using WaitChannel.
	ConfirmHaveTokensAndUnblockNextWaiter() bool
}
