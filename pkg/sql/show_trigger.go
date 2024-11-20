// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var showTriggersColumns = colinfo.ResultColumns{
	{Name: "trigger_name", Typ: types.String},
	{Name: "enabled", Typ: types.Bool},
}

// ShowTriggers returns a SHOW TRIGGERS statement. The user must have any
// privilege on the table.
func (p *planner) ShowTriggers(ctx context.Context, n *tree.ShowTriggers) (planNode, error) {
	// We avoid the cache so that we can observe the trigger without taking a
	// lease, like other SHOW commands.
	tableDesc, err := p.ResolveUncachedTableDescriptorEx(
		ctx, n.Table, true /* required */, tree.ResolveRequireTableOrViewDesc,
	)
	if err != nil {
		return nil, err
	}
	if err = p.CheckAnyPrivilege(ctx, tableDesc); err != nil {
		return nil, err
	}
	return &delayedNode{
		name:    fmt.Sprintf("SHOW TRIGGERS FROM %v", n.Table),
		columns: showTriggersColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			triggers := tableDesc.GetTriggers()
			v := p.newContainerValuesNode(showTriggersColumns, len(triggers))
			for _, trigger := range triggers {
				triggerName := tree.Name(trigger.Name)
				row := tree.Datums{
					tree.NewDString(triggerName.String()),
					tree.MakeDBool(tree.DBool(trigger.Enabled)),
				}
				if _, err = v.rows.AddRow(ctx, row); err != nil {
					v.Close(ctx)
					return nil, err
				}
			}
			return v, nil
		},
	}, nil
}
