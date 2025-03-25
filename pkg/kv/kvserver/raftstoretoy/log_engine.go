// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftstoretoy

import "context"

// LogEngine represents the interface to the engine used for write-ahead
// logging. All operations on this engine are immediately durable.
type LogEngine interface {
	// Append appends a LogEntry. The WAGIndex returned is equal to the previously
	// assigned one unless the LogEntry contains a Split or Merge.
	Append(ctx context.Context, id FullLogID, entry LogEntry) error

	// CreateRequest initializes a new log via a snapshot. The assigned LogID and
	// WAGIndex are returned.
	Create(ctx context.Context, req CreateRequest) (FullLogID, WAGIndex, error)

	// Destroy marks a replica for destruction.
	Destroy(ctx context.Context, id FullLogID, req Destroy) (WAGIndex, error)
}
