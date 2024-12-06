// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"os"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotification"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

var dummyNotificationListens = make(map[string]struct{})

func (p *planner) Notify(ctx context.Context, n *tree.Notify) (planNode, error) {
	// This is a dummy implementation.
	if _, ok := dummyNotificationListens[n.ChannelName.String()]; !ok {
		return newZeroNode(nil), nil
	}
	p.BufferClientNotification(ctx, pgnotification.Notification{
		Channel: n.ChannelName.String(),
		Payload: n.Payload.String(),
		PID:     int32(os.Getpid()),
	})

	return newZeroNode(nil), nil
}
