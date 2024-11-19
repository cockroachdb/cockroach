// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package log

import "context"

// StructuredLogProcessor defines the interface used to process structured logs, logged via
// Structured, categorized by their EventType.
//
// A StructuredLogProcessor is expected to be thread-safe.
type StructuredLogProcessor interface {
	Process(ctx context.Context, eventType EventType, event any)
}
