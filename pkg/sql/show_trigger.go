// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
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

var showCreateTriggerColumns = colinfo.ResultColumns{
	{Name: "trigger_name", Typ: types.String},
	{Name: "create_statement", Typ: types.String},
}

// ShowCreateTrigger returns a SHOW CREATE TRIGGER statement. The user must have
// any privilege on the table.
func (p *planner) ShowCreateTrigger(
	ctx context.Context, n *tree.ShowCreateTrigger,
) (planNode, error) {
	// We avoid the cache so that we can observe the trigger without taking a
	// lease, like other SHOW commands.
	tableDesc, err := p.ResolveUncachedTableDescriptorEx(
		ctx, n.TableName, true /* required */, tree.ResolveRequireTableOrViewDesc,
	)
	if err != nil {
		return nil, err
	}
	if err = p.CheckAnyPrivilege(ctx, tableDesc); err != nil {
		return nil, err
	}
	// Resolve the table's implicit record type to be used in parsing and
	// displaying the WHEN expression.
	tableTyp, err := p.ResolveTypeByOID(ctx, typedesc.TableIDToImplicitTypeOID(tableDesc.GetID()))
	if err != nil {
		return nil, err
	}
	return &delayedNode{
		name:    fmt.Sprintf("SHOW CREATE TRIGGER %v ON %v", n.Name, n.TableName),
		columns: showCreateTriggerColumns,
		constructor: func(ctx context.Context, p *planner) (planNode, error) {
			// Attempt to locate the trigger by name.
			var trigger *descpb.TriggerDescriptor
			triggers := tableDesc.GetTriggers()
			triggerNameString := n.Name.String()
			for i := range triggers {
				if triggers[i].Name == triggerNameString {
					trigger = &triggers[i]
					break
				}
			}
			if trigger == nil {
				return nil, pgerror.Newf(
					pgcode.UndefinedObject, "trigger %v for table %v does not exist", n.Name, n.TableName,
				)
			}

			// Resolve the fully-qualified names of the trigger function and table.
			funcName, err := p.GetQualifiedFunctionNameByID(ctx, int64(trigger.FuncID))
			if err != nil {
				return nil, err
			}
			tableName, err := p.getQualifiedTableName(ctx, tableDesc)
			if err != nil {
				return nil, err
			}

			// Use the trigger descriptor to decompile a CreateTrigger statement that
			// will be used to produce the SHOW CREATE TRIGGER output.
			events := make([]*tree.TriggerEvent, len(trigger.Events))
			for j := range events {
				descEvent := trigger.Events[j]
				events[j] = &tree.TriggerEvent{
					EventType: tree.TriggerEventTypeToTree[descEvent.Type],
					Columns:   make(tree.NameList, 0, len(descEvent.ColumnNames)),
				}
				for _, colName := range descEvent.ColumnNames {
					events[j].Columns = append(events[j].Columns, tree.Name(colName))
				}
			}
			var transitions []*tree.TriggerTransition
			if trigger.NewTransitionAlias != "" {
				transitions = append(transitions, &tree.TriggerTransition{
					IsNew: true,
					Name:  tree.Name(trigger.NewTransitionAlias),
				})
			}
			if trigger.OldTransitionAlias != "" {
				transitions = append(transitions, &tree.TriggerTransition{
					IsNew: false,
					Name:  tree.Name(trigger.OldTransitionAlias),
				})
			}
			forEach := tree.TriggerForEachStatement
			if trigger.ForEachRow {
				forEach = tree.TriggerForEachRow
			}
			var whenExpr tree.Expr
			if trigger.WhenExpr != "" {
				whenExpr, err = schemaexpr.ParseTriggerWhenExprForDisplay(
					ctx, tableTyp, trigger.WhenExpr, p.EvalContext(), p.SemaCtx(), tree.FmtParsable,
				)
				if err != nil {
					return nil, err
				}
			}
			createTrigger := &tree.CreateTrigger{
				Replace:     false,
				Name:        tree.Name(trigger.Name),
				ActionTime:  tree.TriggerActionTimeToTree[trigger.ActionTime],
				Events:      events,
				TableName:   tableName.ToUnresolvedObjectName(),
				Transitions: transitions,
				ForEach:     forEach,
				When:        whenExpr,
				FuncName:    funcName.ToUnresolvedObjectName().ToUnresolvedName(),
				FuncArgs:    trigger.FuncArgs,
			}
			f := tree.NewFmtCtx(
				tree.FmtParsable,
				tree.FmtDataConversionConfig(p.SessionData().DataConversionConfig),
				tree.FmtLocation(p.SessionData().Location),
			)
			f.FormatNode(createTrigger)

			v := p.newContainerValuesNode(showCreateTriggerColumns, 1 /* capacity */)
			row := tree.Datums{
				tree.NewDString(createTrigger.Name.String()),
				tree.NewDString(f.CloseAndGetString()),
			}
			if _, err = v.rows.AddRow(ctx, row); err != nil {
				v.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}
