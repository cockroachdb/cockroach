// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) Unlisten(ctx context.Context, n *tree.Unlisten) (planNode, error) {
	delete(dummyNotificationListens, n.ChannelName.String())
	return newZeroNode(nil /* columns */), nil
}
