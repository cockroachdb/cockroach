// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package streamingccl

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/ccl/streamingccl/streampb"
	"github.com/cockroachdb/cockroach/pkg/streaming"
)

// StreamStatusErr is error that encapsulate a replication stream's bad status.
type StreamStatusErr struct {
	StreamID     streaming.StreamID
	StreamStatus streampb.StreamReplicationStatus_StreamStatus
}

// Error implements the error interface.
func (e StreamStatusErr) Error() string {
	return fmt.Sprintf("Replication stream %d is not running, status is %s", e.StreamID, e.StreamStatus.String())
}
