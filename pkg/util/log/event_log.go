// Copyright 2020 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
)

// ClusterEvent emits a structured event to the debug log.
//
// TODO(knz): Change this to log to different channels depending
// on event type.
func ClusterEvent(ctx context.Context, event eventpb.EventPayload) {
	InfofDepth(ctx, 1, "Event: %+v", event)
}
