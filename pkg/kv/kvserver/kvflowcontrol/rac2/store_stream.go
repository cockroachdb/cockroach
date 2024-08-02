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

import "github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol"

// StreamTokenCounterProvider is the interface for retrieving token counters
// for a given stream.
type StreamTokenCounterProvider interface {
	// EvalTokenCounterForStream returns the evaluation token counter for the
	// given stream.
	EvalTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
	// SendTokenCounterForStream returns the send token counter for the given
	// stream.
	SendTokenCounterForStream(kvflowcontrol.Stream) TokenCounter
}
