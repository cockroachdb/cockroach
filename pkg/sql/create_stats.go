// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type createStatsNode struct {
	tree.CreateStats
	tableDesc *sqlbase.TableDescriptor
}

func (p *planner) CreateStatistics(ctx context.Context, n *tree.CreateStats) (planNode, error) {
	tn, err := p.QualifyWithDatabase(ctx, &n.Table)
	if err != nil {
		return nil, err
	}

	// Is this perhaps a name for a virtual table?
	if _, foundVirtual, err := p.getVirtualDataSource(ctx, tn); err != nil {
		return nil, err
	} else if foundVirtual {
		return nil, errors.Errorf("cannot create statistics on virtual tables")
	}

	tableDesc, err := MustGetTableDesc(ctx, p.txn, p.getVirtualTabler(), tn, false /* allowAdding */)
	if err != nil {
		return nil, err
	}

	if err := p.CheckPrivilege(tableDesc, privilege.SELECT); err != nil {
		return nil, err
	}

	return &createStatsNode{
		CreateStats: *n,
		tableDesc:   tableDesc,
	}, nil
}

func (*createStatsNode) Start(runParams) error {
	return errors.Errorf("statistics can only be created via DistSQL")
}

func (*createStatsNode) Next(runParams) (bool, error) { panic("not implemented") }
func (*createStatsNode) Close(context.Context)        {}
func (*createStatsNode) Values() tree.Datums          { panic("not implemented") }
