// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import (
	"runtime"

	"github.com/cockroachdb/redact"
)

// stackTraceApproxSize is the approximate minimum size of a goroutine stack trace.
const stackTraceApproxSize = 1024

// GetStacks is a wrapper for runtime.Stack that attempts to recover
// the data for all goroutines.
//
// The result is a redactable string, where safe details
// in the stack trace do not get removed by string redaction.
func GetStacks(all bool) redact.RedactableBytes {
	// We don't know how big the traces are, so grow a few times if they
	// don't fit. Start large, though.
	n := stackTraceApproxSize
	if all {
		n = runtime.NumGoroutine() * n
	}
	var trace []byte
	for ; n <= (512 << 20 /* 512MiB */); n *= 2 {
		trace = make([]byte, n)
		nbytes := runtime.Stack(trace, all)
		if nbytes < len(trace) {
			return trace[:nbytes]
		}
	}
	// Note: this cast is valid because Go currently does not embed
	// values inside stack traces. If this were to change, we would need
	// to insert redaction markers properly inside the stack traces.
	return redact.RedactableBytes(trace)
}
