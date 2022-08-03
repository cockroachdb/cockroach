// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import "context"

// TryClearGossipInfo implements the tree.GossipOperator interface.
func (p *planner) TryClearGossipInfo(ctx context.Context, key string) (bool, error) {
	g, err := p.ExecCfg().Gossip.OptionalErr(0 /* issue */)
	if err != nil {
		return false, err
	}
	if err := p.RequireAdminRole(ctx, "try clear gossip info"); err != nil {
		return false, err
	}
	return g.TryClearInfo(key)
}
