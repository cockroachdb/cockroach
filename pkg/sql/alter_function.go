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

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type alterFunctionOptionsNode struct {
	n *tree.AlterFunctionOptions
}

type alterFunctionRenameNode struct {
	n *tree.AlterFunctionRename
}

type alterFunctionSetOwnerNode struct {
	n *tree.AlterFunctionSetOwner
}

type alterFunctionSetSchemaNode struct {
	n *tree.AlterFunctionSetSchema
}

type alterFunctionDepExtensionNode struct {
	n *tree.AlterFunctionDepExtension
}

// AlterFunctionOptions alters a function's options.
func (p *planner) AlterFunctionOptions(
	ctx context.Context, n *tree.AlterFunctionOptions,
) (planNode, error) {
	return &alterFunctionOptionsNode{n: n}, nil
}

func (n *alterFunctionOptionsNode) startExec(params runParams) error {
	fnDesc, err := params.p.mustGetMutableFunctionForAlter(params.ctx, &n.n.Function)
	if err != nil {
		return err
	}
	// TODO(chengxiong): add validation that a function can not be altered if it's
	// referenced by other objects. This is needed when want to allow function
	// references. Need to think about in what condition a function can be altered
	// or not.
	options := make(map[string]struct{})
	for _, option := range n.n.Options {
		optTypeName := reflect.TypeOf(option).Name()
		if _, ok := options[optTypeName]; ok {
			return pgerror.New(pgcode.Syntax, "conflicting or redundant options")
		}
		// Note that language and function body cannot be altered, and it's blocked
		// from parser level with "common_func_opt_item" syntax.
		err := setFuncOption(params, fnDesc, option)
		if err != nil {
			return err
		}
		options[optTypeName] = struct{}{}
	}

	return params.p.writeFuncSchemaChange(params.ctx, fnDesc)
}

func (n *alterFunctionOptionsNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionOptionsNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionOptionsNode) Close(ctx context.Context)           {}

// AlterFunctionRename renames a function.
func (p *planner) AlterFunctionRename(
	ctx context.Context, n *tree.AlterFunctionRename,
) (planNode, error) {
	return &alterFunctionRenameNode{n: n}, nil
}

func (n *alterFunctionRenameNode) startExec(params runParams) error {
	// TODO(chengxiong): add validation that a function can not be altered if it's
	// referenced by other objects. This is needed when want to allow function
	// references.
	fnDesc, err := params.p.mustGetMutableFunctionForAlter(params.ctx, &n.n.Function)
	if err != nil {
		return err
	}

	scDesc, err := params.p.Descriptors().GetMutableSchemaByID(
		params.ctx, params.p.txn, fnDesc.GetParentSchemaID(), tree.SchemaLookupFlags{Required: true},
	)
	if err != nil {
		return err
	}

	maybeExistingFuncObj := fnDesc.ToFuncObj()
	maybeExistingFuncObj.FuncName.ObjectName = n.n.NewName
	existing, err := params.p.matchUDF(params.ctx, &maybeExistingFuncObj, false /* required */)
	if err != nil {
		return err
	}

	if existing != nil {
		return pgerror.Newf(
			pgcode.DuplicateFunction, "function %s already exists in schema %q",
			tree.AsString(maybeExistingFuncObj), scDesc.GetName(),
		)
	}

	scDesc.RemoveFunction(fnDesc.GetName(), fnDesc.GetID())
	fnDesc.SetName(string(n.n.NewName))
	scDesc.AddFunction(fnDesc.GetName(), toSchemaOverloadSignature(fnDesc))
	if err := params.p.writeFuncSchemaChange(params.ctx, fnDesc); err != nil {
		return err
	}

	return params.p.writeSchemaDescChange(params.ctx, scDesc, "alter function name")
}

func (n *alterFunctionRenameNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionRenameNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionRenameNode) Close(ctx context.Context)           {}

// AlterFunctionSetOwner sets a function's owner.
func (p *planner) AlterFunctionSetOwner(
	ctx context.Context, n *tree.AlterFunctionSetOwner,
) (planNode, error) {
	return &alterFunctionSetOwnerNode{n: n}, nil
}

func (n *alterFunctionSetOwnerNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function owner to not supported")
}

func (n *alterFunctionSetOwnerNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionSetOwnerNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionSetOwnerNode) Close(ctx context.Context)           {}

// AlterFunctionSetSchema moves a function to another schema.
func (p *planner) AlterFunctionSetSchema(
	ctx context.Context, n *tree.AlterFunctionSetSchema,
) (planNode, error) {
	return &alterFunctionSetSchemaNode{n: n}, nil
}

func (n *alterFunctionSetSchemaNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function set schema not supported")
}

func (n *alterFunctionSetSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionSetSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionSetSchemaNode) Close(ctx context.Context)           {}

// AlterFunctionDepExtension alters a function dependency on an extension.
func (p *planner) AlterFunctionDepExtension(
	ctx context.Context, n *tree.AlterFunctionDepExtension,
) (planNode, error) {
	return &alterFunctionDepExtensionNode{n: n}, nil
}

func (n *alterFunctionDepExtensionNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function depends on extension not supported")
}

func (n *alterFunctionDepExtensionNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionDepExtensionNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionDepExtensionNode) Close(ctx context.Context)           {}

func (p *planner) mustGetMutableFunctionForAlter(
	ctx context.Context, funcObj *tree.FuncObj,
) (*funcdesc.Mutable, error) {
	ol, err := p.matchUDF(ctx, funcObj, true /*required*/)
	if err != nil {
		return nil, err
	}
	fnID, err := funcdesc.UserDefinedFunctionOIDToID(ol.Oid)
	if err != nil {
		return nil, err
	}
	mut, err := p.checkPrivilegesForDropFunction(ctx, fnID)
	if err != nil {
		return nil, err
	}
	return mut, nil
}

func toSchemaOverloadSignature(fnDesc *funcdesc.Mutable) descpb.SchemaDescriptor_FunctionOverload {
	ret := descpb.SchemaDescriptor_FunctionOverload{
		ID:         fnDesc.GetID(),
		ArgTypes:   make([]*types.T, len(fnDesc.GetArgs())),
		ReturnType: fnDesc.ReturnType.Type,
		ReturnSet:  fnDesc.ReturnType.ReturnSet,
	}
	for i := range fnDesc.Args {
		ret.ArgTypes[i] = fnDesc.Args[i].Type
	}
	return ret
}
