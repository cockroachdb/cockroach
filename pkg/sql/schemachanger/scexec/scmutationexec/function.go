// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scmutationexec

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scop"
)

func (i *immediateVisitor) CreateFunctionDescriptor(
	ctx context.Context, op scop.CreateFunctionDescriptor,
) error {
	params := make([]descpb.FunctionDescriptor_Parameter, len(op.Function.Params))
	for i, param := range op.Function.Params {

		params[i] = descpb.FunctionDescriptor_Parameter{
			Class: param.Class.Class,
			Name:  param.Name,
			Type:  param.Type.Type,
		}
		if param.DefaultExpr != "" {
			params[i].DefaultExpr = &param.DefaultExpr
		}
	}

	mut := funcdesc.NewMutableFunctionDescriptor(
		op.Function.FunctionID,
		descpb.InvalidID,
		descpb.InvalidID,
		"",
		params,
		op.Function.ReturnType.Type,
		op.Function.ReturnSet,
		op.Function.IsProcedure,
		&catpb.PrivilegeDescriptor{Version: catpb.Version23_2},
	)
	mut.State = descpb.DescriptorState_ADD
	i.CreateDescriptor(&mut)
	return nil
}

func (i *immediateVisitor) SetFunctionName(ctx context.Context, op scop.SetFunctionName) error {
	fn, err := i.checkOutFunction(ctx, op.FunctionID)
	if err != nil {
		return err
	}
	fn.SetName(op.Name)
	return nil
}

func (i *immediateVisitor) SetFunctionVolatility(
	ctx context.Context, op scop.SetFunctionVolatility,
) error {
	fn, err := i.checkOutFunction(ctx, op.FunctionID)
	if err != nil {
		return err
	}
	fn.SetVolatility(op.Volatility)
	return nil
}

func (i *immediateVisitor) SetFunctionLeakProof(
	ctx context.Context, op scop.SetFunctionLeakProof,
) error {
	fn, err := i.checkOutFunction(ctx, op.FunctionID)
	if err != nil {
		return err
	}
	fn.SetLeakProof(op.LeakProof)
	return nil
}

func (i *immediateVisitor) SetFunctionNullInputBehavior(
	ctx context.Context, op scop.SetFunctionNullInputBehavior,
) error {
	fn, err := i.checkOutFunction(ctx, op.FunctionID)
	if err != nil {
		return err
	}
	fn.SetNullInputBehavior(op.NullInputBehavior)
	return nil
}

func (i *immediateVisitor) SetFunctionBody(ctx context.Context, op scop.SetFunctionBody) error {
	fn, err := i.checkOutFunction(ctx, op.Body.FunctionID)
	if err != nil {
		return err
	}
	fn.SetFuncBody(op.Body.Body)
	fn.SetLang(op.Body.Lang.Lang)

	return nil
}

func (i *immediateVisitor) SetFunctionSecurity(
	ctx context.Context, op scop.SetFunctionSecurity,
) error {
	fn, err := i.checkOutFunction(ctx, op.FunctionID)
	if err != nil {
		return err
	}
	fn.SetSecurity(op.Security)
	return nil
}
