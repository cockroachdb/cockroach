// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
)

// TryClearGossipInfo implements the tree.GossipOperator interface.
func (p *planner) TryClearGossipInfo(ctx context.Context, key string) (bool, error) {
	g, err := p.ExecCfg().Gossip.OptionalErr(0 /* issue */)
	if err != nil {
		return false, err
	}
	if err := p.CheckPrivilege(ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER); err != nil {
		return false, err
	}
	return g.TryClearInfo(key)
}
