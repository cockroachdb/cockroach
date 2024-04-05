// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package log

import "context"

// StructuredLogProcessor defines the interface used to process structured logs, logged via
// Structured, categorized by their EventType.
//
// A StructuredLogProcessor is expected to be thread-safe.
type StructuredLogProcessor interface {
	Process(ctx context.Context, eventType EventType, event any)
}
