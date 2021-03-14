// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package blathers

import "context"

func WithRequestID(ctx context.Context, requestID string) context.Context {
	return context.WithValue(ctx, "request_id", requestID)
}

func RequestID(ctx context.Context) string {
	ret := ctx.Value("request_id")
	if ret == nil {
		return "no_provided_request_id"
	}
	if ret == "" {
		return "blank_request_id"
	}
	return ret.(string)
}

func WithDebuggingPrefix(ctx context.Context, text string) context.Context {
	t := text + ": "
	prev := ctx.Value("debugging_prefix")
	if prev != nil {
		t = prev.(string) + t
	}
	return context.WithValue(ctx, "debugging_prefix", t)
}

func DebuggingPrefix(ctx context.Context) string {
	ret := ctx.Value("debugging_prefix")
	if ret == nil {
		return ""
	}
	return ret.(string)
}
