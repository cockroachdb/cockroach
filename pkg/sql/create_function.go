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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/lib/pq/oid"
)

type createFunctionNode struct {
	n *tree.CreateFunction
}

func (p *planner) CreateFunction(ctx context.Context, n *tree.CreateFunction) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"CREATE FUNCTION",
	); err != nil {
		return nil, err
	}

	if n.FuncName.ExplicitCatalog && n.FuncName.Catalog() != p.CurrentDatabase() {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "cross-database function references are not supported")
	}

	// TODO (Chengxiong): UDF input validation
	return &createFunctionNode{n: n}, nil
}

func (n *createFunctionNode) startExec(params runParams) error {
	dbDesc, err := params.p.Descriptors().GetImmutableDatabaseByName(
		params.ctx, params.p.Txn(), params.p.CurrentDatabase(), tree.DatabaseLookupFlags{Required: true, AvoidLeased: true},
	)
	if err != nil {
		return err
	}

	schemaName := tree.PublicSchemaName
	if n.n.FuncName.ExplicitSchema {
		schemaName = n.n.FuncName.SchemaName
	}
	scDesc, err := params.p.Descriptors().GetMutableSchemaByName(
		params.ctx, params.p.Txn(), dbDesc, string(schemaName), tree.SchemaLookupFlags{Required: true, AvoidLeased: true},
	)

	id, err := descidgen.GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB, params.p.ExecCfg().Codec)
	if err != nil {
		return err
	}

	returnType, err := tree.ResolveType(params.ctx, n.n.ReturnType.Type, params.p)
	if err != nil {
		return err
	}

	privileges := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		dbDesc.GetDefaultPrivilegeDescriptor(),
		scDesc.GetDefaultPrivilegeDescriptor(),
		dbDesc.GetID(),
		params.SessionData().User(),
		tree.Functions,
		dbDesc.GetPrivileges(),
	)

	newUdfDesc := funcdesc.InitFunctionDescriptor(
		id,
		dbDesc.GetID(),
		scDesc.GetID(),
		string(n.n.FuncName.ObjectName),
		len(n.n.Args),
		returnType,
		n.n.ReturnType.IsSet,
		privileges,
	)

	// TODO (Chengxiong): UDF track function/schema references in function body
	// TODO (Chengxiong): UDF track type references in function body and signature

	for _, argInput := range n.n.Args {
		arg := descpb.FunctionDescriptor_Argument{
			Name: string(argInput.Name),
		}

		if v, err := toPbFunctionArgClass(argInput.Class); err != nil {
			return err
		} else {
			arg.Class = v
		}

		arg.Type, err = tree.ResolveType(params.ctx, argInput.Type, params.p)
		if err != nil {
			return err
		}

		if argInput.DefaultVal != nil {
			defaultExpr, err := schemaexpr.SanitizeVarFreeExpr(
				params.ctx, argInput.DefaultVal, arg.Type, "Function Arg Default", params.p.SemaCtx(), volatility.Volatile,
			)
			if err != nil {
				return err
			}
			if defaultExpr != tree.DNull {
				s := tree.Serialize(defaultExpr)
				arg.DefaultExpr = &s
			}
		}
		newUdfDesc.AddArgument(arg)
	}

	for _, option := range n.n.Options {
		switch t := option.(type) {
		case tree.FunctionVolatility:
			if v, err := toPbFunctionVolatility(t); err != nil {
				return err
			} else {
				newUdfDesc.SetVolatility(v)
			}
		case tree.FunctionLeakProof:
			newUdfDesc.SetLeakProof(bool(t))
		case tree.FunctionNullInputBehavior:
			if v, err := toPbFunctionNullInputBehavior(t); err != nil {
				return err
			} else {
				newUdfDesc.SetNullInputBehavior(v)
			}
		case tree.FunctionLanguage:
			if v, err := toPbFunctionLang(t); err != nil {
				return err
			} else {
				newUdfDesc.SetLang(v)
			}
		case tree.FunctionBodyStr:
			newUdfDesc.SetFuncBody(string(t))
		}
	}

	err = params.p.createDescriptorWithID(
		params.ctx,
		catalogkeys.MakeObjectNameKey(params.ExecCfg().Codec, dbDesc.GetID(), scDesc.GetID(), newUdfDesc.Name),
		id,
		&newUdfDesc,
		tree.AsStringWithFQNames(n.n, params.Ann()),
	)
	if err != nil {
		return err
	}

	// TODO (Chengxiong): UDF we can only to `IN`, `INOUT`, `VARIADIC` arguments
	// to the function signature used in Schema. We should keep arg class
	// information in descriptor, because show creat function is gonna need them.
	typeOIDs := make([]oid.Oid, 0, len(newUdfDesc.Args))
	for _, arg := range newUdfDesc.Args {
		typeOIDs = append(typeOIDs, arg.Type.Oid())
	}
	scDesc.(*schemadesc.Mutable).AddFunction(
		newUdfDesc.GetName(),
		descpb.SchemaDescriptor_FunctionOverload{
			ID:       newUdfDesc.GetID(),
			ArgTypes: typeOIDs,
		},
	)
	if err := params.p.writeSchemaDescChange(params.ctx, scDesc.(*schemadesc.Mutable), "Create Function"); err != nil {
		return err
	}
	// TODO (Chengxiong): Add function signature to schema
	// TODO (Chengxiong): UDF add descriptor validation.
	// TODO (Chengxiong): UDF add event logging
	return nil
}

func toPbFunctionVolatility(
	v tree.FunctionVolatility,
) (descpb.FunctionDescriptor_Volatility, error) {
	switch v {
	case tree.FunctionImmutable:
		return descpb.FunctionDescriptor_Immutable, nil
	case tree.FunctionStable:
		return descpb.FunctionDescriptor_Stable, nil
	case tree.FunctionVolatile:
		return descpb.FunctionDescriptor_Volatile, nil
	}

	return -1, pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function volatility %q", v)
}

func toPbFunctionNullInputBehavior(
	v tree.FunctionNullInputBehavior,
) (descpb.FunctionDescriptor_NullInputBehavior, error) {
	switch v {
	case tree.FunctionCalledOnNullInput:
		return descpb.FunctionDescriptor_CalledOnNullInput, nil
	case tree.FunctionReturnsNullOnNullInput:
		return descpb.FunctionDescriptor_ReturnsNullOnNullInput, nil
	case tree.FunctionStrict:
		return descpb.FunctionDescriptor_Strict, nil
	}

	return -1, pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function null input behavior %q", v)
}

func toPbFunctionLang(v tree.FunctionLanguage) (descpb.FunctionDescriptor_Language, error) {
	switch v {
	case tree.FunctionLangSQL:
		return descpb.FunctionDescriptor_Sql, nil
	}

	return -1, pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function language %q", v)
}

func toPbFunctionArgClass(v tree.FuncArgClass) (descpb.FunctionDescriptor_Argument_Class, error) {
	switch v {
	case tree.FunctionArgIn:
		return descpb.FunctionDescriptor_Argument_In, nil
	case tree.FunctionArgOut:
		return descpb.FunctionDescriptor_Argument_Out, nil
	case tree.FunctionArgInOut:
		return descpb.FunctionDescriptor_Argument_InOut, nil
	case tree.FunctionArgVariadic:
		return descpb.FunctionDescriptor_Argument_Variadic, nil
	}

	return -1, pgerror.Newf(pgcode.InvalidParameterValue, "unknown function argument class %q", v)
}
func (*createFunctionNode) Next(runParams) (bool, error) { return false, nil }
func (*createFunctionNode) Values() tree.Datums          { return tree.Datums{} }
func (*createFunctionNode) Close(context.Context)        {}
