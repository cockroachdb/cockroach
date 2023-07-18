// Copyright 2023 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsn"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/lsnutil"
	"github.com/cockroachdb/cockroach/pkg/sql/pgrepl/pgrepltree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type identifySystemNode struct {
	optColumnsSlot
	clusterID string
	database  string
	lsn       lsn.LSN
	shown     bool
}

func (s *identifySystemNode) startExec(params runParams) error {
	return nil
}

func (s *identifySystemNode) Next(params runParams) (bool, error) {
	if s.shown {
		return false, nil
	}
	s.shown = true
	return true, nil
}

func (s *identifySystemNode) Values() tree.Datums {
	db := tree.DNull
	if s.database != "" {
		db = tree.NewDString(s.database)
	}
	return tree.Datums{
		tree.NewDString(s.clusterID),
		tree.NewDInt(1), // timeline
		tree.NewDString(s.lsn.String()),
		db,
	}
}

func (s *identifySystemNode) Close(ctx context.Context) {}

func (p *planner) IdentifySystem(
	ctx context.Context, n *pgrepltree.IdentifySystem,
) (planNode, error) {
	return &identifySystemNode{
		// TODO(#105130): correctly populate this field.
		lsn:       lsnutil.HLCToLSN(p.Txn().ReadTimestamp()),
		clusterID: p.ExecCfg().NodeInfo.LogicalClusterID().String(),
		database:  p.SessionData().Database,
	}, nil
}
