// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package streampb

// StreamID is the ID of a replication stream.
type StreamID int64

// SafeValue implements the redact.SafeValue interface.
func (j StreamID) SafeValue() {}

// InvalidStreamID is the zero value for StreamID corresponding to no stream.
const InvalidStreamID StreamID = 0
