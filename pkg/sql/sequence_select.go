// Copyright 2018 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/pkg/errors"
)

type sequenceSelectNode struct {
	optColumnsSlot

	desc *sqlbase.TableDescriptor

	val  int64
	done bool
}

var _ planNode = &sequenceSelectNode{}

func (p *planner) SequenceSelectNode(desc *sqlbase.TableDescriptor) (planNode, error) {
	if desc.SequenceOpts == nil {
		return nil, errors.New("descriptor is not a sequence")
	}
	return &sequenceSelectNode{
		desc: desc,
	}, nil
}

func (ss *sequenceSelectNode) Next(params runParams) (bool, error) {
	if ss.done {
		return false, nil
	}
	val, err := params.p.GetSequenceValue(params.ctx, ss.desc)
	if err != nil {
		return false, err
	}
	ss.val = val
	ss.done = true
	return true, nil
}

func (ss *sequenceSelectNode) Values() tree.Datums {
	valDatum := tree.DInt(ss.val)
	cntDatum := tree.DInt(0)
	calledDatum := tree.DBoolTrue
	return []tree.Datum{
		&valDatum,
		&cntDatum,
		calledDatum,
	}
}

func (ss *sequenceSelectNode) Close(ctx context.Context) {}

var sequenceSelectColumns = sqlbase.ResultColumns{
	{
		Name: `last_value`,
		Typ:  types.Int,
	},
	{
		Name: `log_cnt`,
		Typ:  types.Int,
	},
	{
		Name: `is_called`,
		Typ:  types.Bool,
	},
}
