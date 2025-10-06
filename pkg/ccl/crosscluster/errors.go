// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package crosscluster

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
)

// StreamStatusErr is an error that encapsulate a replication stream's inactive status.
type StreamStatusErr struct {
	StreamID     streampb.StreamID
	StreamStatus streampb.StreamReplicationStatus_StreamStatus
}

// NewStreamStatusErr creates a new StreamStatusErr.
func NewStreamStatusErr(
	streamID streampb.StreamID, streamStatus streampb.StreamReplicationStatus_StreamStatus,
) StreamStatusErr {
	return StreamStatusErr{
		StreamID:     streamID,
		StreamStatus: streamStatus,
	}
}

// Error implements the error interface.
func (e StreamStatusErr) Error() string {
	return fmt.Sprintf("replication stream %d is not running, status is %s", e.StreamID, e.StreamStatus)
}
