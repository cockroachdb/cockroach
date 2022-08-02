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
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

type createFunctionNode struct {
	cf *tree.CreateFunction

	dbDesc   catalog.DatabaseDescriptor
	scDesc   catalog.SchemaDescriptor
	planDeps planDependencies
	typeDeps typeDependencies
}

func (n *createFunctionNode) ReadingOwnWrites() {}

func (n *createFunctionNode) startExec(params runParams) error {
	if n.cf.RoutineBody != nil {
		return unimplemented.NewWithIssue(85144, "CREATE FUNCTION...sql_body unimplemented")
	}

	scDesc := n.scDesc.NewBuilder().BuildExistingMutable().(*schemadesc.Mutable)
	udfMutableDesc, err := n.getMutableFuncDesc(params)
	if err != nil {
		return err
	}

	for _, arg := range n.cf.Args {
		pbArg, err := makeFunctionArg(params.ctx, arg, params.p)
		if err != nil {
			return err
		}
		udfMutableDesc.AddArgument(pbArg)
	}

	// Set default values before applying options. This simplifies
	// the replacing logic.
	resetFuncOption(udfMutableDesc)
	for _, option := range n.cf.Options {
		err := setFuncOption(params, udfMutableDesc, option)
		if err != nil {
			return err
		}
	}
	if udfMutableDesc.LeakProof && udfMutableDesc.Volatility != catpb.Function_IMMUTABLE {
		return pgerror.Newf(
			pgcode.InvalidFunctionDefinition,
			"cannot create leakproof function with non-immutable volatility: %s",
			udfMutableDesc.Volatility.String(),
		)
	}

	// Get all table IDs for which we need to update back references, including
	// tables used directly in function body or as implicit types.
	backrefTblIDs := make([]descpb.ID, 0, len(n.planDeps)+len(n.typeDeps))
	implicitTypeTblIDs := make(map[descpb.ID]struct{}, len(n.typeDeps))
	for id := range n.planDeps {
		backrefTblIDs = append(backrefTblIDs, id)
	}
	for id := range n.typeDeps {
		if isTable, err := params.p.descIsTable(params.ctx, id); err != nil {
			return err
		} else if isTable {
			backrefTblIDs = append(backrefTblIDs, id)
			implicitTypeTblIDs[id] = struct{}{}
		}
	}

	// Read all referenced tables and update their dependencies.
	backRefMutables := make(map[descpb.ID]*tabledesc.Mutable, len(backrefTblIDs))
	for _, id := range backrefTblIDs {
		if _, ok := backRefMutables[id]; ok {
			continue
		}
		backRefMutable, err := params.p.Descriptors().GetMutableTableByID(
			params.ctx, params.p.txn, id, tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		if backRefMutable.Temporary {
			// Looks like postgres allows this, but function would be broken when
			// called from a different session.
			return pgerror.New(pgcode.InvalidFunctionDefinition, "cannot create function using temp tables")
		}
		backRefMutables[id] = backRefMutable
	}

	for id, updated := range n.planDeps {
		backRefMutable := backRefMutables[id]
		for _, dep := range updated.deps {
			dep.ID = udfMutableDesc.ID
			dep.ByID = updated.desc.IsSequence()
			backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, dep)
		}

		if err := params.p.writeSchemaChange(
			params.ctx,
			backRefMutable,
			descpb.InvalidMutationID,
			fmt.Sprintf("updating udf reference %q in table %s(%d)",
				n.cf.FuncName.String(), updated.desc.GetName(), updated.desc.GetID(),
			),
		); err != nil {
			return err
		}
	}
	for id := range implicitTypeTblIDs {
		backRefMutable := backRefMutables[id]
		backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, descpb.TableDescriptor_Reference{ID: udfMutableDesc.ID})
		if err := params.p.writeSchemaChange(
			params.ctx,
			backRefMutable,
			descpb.InvalidMutationID,
			fmt.Sprintf("updating udf reference %q in table %s(%d)",
				n.cf.FuncName.String(), backRefMutable.GetName(), backRefMutable.GetID(),
			),
		); err != nil {
			return err
		}
	}

	// Add type back references. Skip table implicit types (we update table back
	// references above).
	for id := range n.typeDeps {
		if _, ok := implicitTypeTblIDs[id]; ok {
			continue
		}
		jobDesc := fmt.Sprintf("updating type back reference %d for function %d", id, udfMutableDesc.ID)
		if err := params.p.addTypeBackReference(params.ctx, id, udfMutableDesc.ID, jobDesc); err != nil {
			return err
		}
	}

	// Add forward references to UDF descriptor.
	udfMutableDesc.DependsOn = make([]descpb.ID, 0, len(backrefTblIDs))
	udfMutableDesc.DependsOn = append(udfMutableDesc.DependsOn, backrefTblIDs...)

	sort.Sort(descpb.IDs(udfMutableDesc.DependsOn))

	udfMutableDesc.DependsOnTypes = []descpb.ID{}
	for id := range n.typeDeps {
		if _, ok := implicitTypeTblIDs[id]; ok {
			continue
		}
		udfMutableDesc.DependsOnTypes = append(udfMutableDesc.DependsOnTypes, id)
	}
	sort.Sort(descpb.IDs(udfMutableDesc.DependsOnTypes))

	err = params.p.createDescriptorWithID(
		params.ctx,
		roachpb.Key{}, // UDF does not have namespace entry.
		udfMutableDesc.GetID(),
		udfMutableDesc,
		tree.AsStringWithFQNames(&n.cf.FuncName, params.Ann()),
	)
	if err != nil {
		return err
	}

	returnType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return err
	}
	argTypes := make([]*types.T, len(udfMutableDesc.Args))
	for i, arg := range udfMutableDesc.Args {
		argTypes[i] = arg.Type
	}
	scDesc.AddFunction(
		udfMutableDesc.GetName(),
		descpb.SchemaDescriptor_FunctionOverload{
			ID:         udfMutableDesc.GetID(),
			ArgTypes:   argTypes,
			ReturnType: returnType,
			ReturnSet:  udfMutableDesc.ReturnType.ReturnSet,
		},
	)
	if err := params.p.writeSchemaDescChange(params.ctx, scDesc, "Create Function"); err != nil {
		return err
	}

	return nil
}

func (*createFunctionNode) Next(params runParams) (bool, error) { return false, nil }
func (*createFunctionNode) Values() tree.Datums                 { return tree.Datums{} }
func (*createFunctionNode) Close(ctx context.Context)           {}

func (n *createFunctionNode) getMutableFuncDesc(params runParams) (*funcdesc.Mutable, error) {
	if n.cf.Replace {
		return nil, unimplemented.New("CREATE OR REPLACE FUNCTION", "replacing function")
	}
	// TODO (Chengxiong) add function resolution and check if it's a Replace.
	// Also:
	// (1) add validation that return type can't be change.
	// (2) add validation that argument names can't be change.
	// (3) add validation that if existing function is referenced then it cannot be replace.
	// (4) add `if` branch so that we only create new descriptor when it's not a replace.

	funcDescID, err := params.EvalContext().DescIDGenerator.GenerateUniqueDescID(params.ctx)
	if err != nil {
		return nil, err
	}

	returnType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return nil, err
	}

	privileges := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		n.dbDesc.GetDefaultPrivilegeDescriptor(),
		n.scDesc.GetDefaultPrivilegeDescriptor(),
		n.dbDesc.GetID(),
		params.SessionData().User(),
		privilege.Functions,
		n.dbDesc.GetPrivileges(),
	)

	newUdfDesc := funcdesc.NewMutableFunctionDescriptor(
		funcDescID,
		n.dbDesc.GetID(),
		n.scDesc.GetID(),
		string(n.cf.FuncName.ObjectName),
		len(n.cf.Args),
		returnType,
		n.cf.ReturnType.IsSet,
		privileges,
	)

	return &newUdfDesc, nil
}

func setFuncOption(params runParams, udfDesc *funcdesc.Mutable, option tree.FunctionOption) error {
	switch t := option.(type) {
	case tree.FunctionVolatility:
		v, err := funcdesc.VolatilityToProto(t)
		if err != nil {
			return err
		}
		udfDesc.SetVolatility(v)
	case tree.FunctionLeakproof:
		udfDesc.SetLeakProof(bool(t))
	case tree.FunctionNullInputBehavior:
		v, err := funcdesc.NullInputBehaviorToProto(t)
		if err != nil {
			return err
		}
		udfDesc.SetNullInputBehavior(v)
	case tree.FunctionLanguage:
		v, err := funcdesc.FunctionLangToProto(t)
		if err != nil {
			return err
		}
		udfDesc.SetLang(v)
	case tree.FunctionBodyStr:
		// Replace any sequence names in the function body with IDs.
		seqReplacedFuncBody, err := replaceSeqNamesWithIDs(params.ctx, params.p, string(t), true)
		if err != nil {
			return err
		}
		udfDesc.SetFuncBody(seqReplacedFuncBody)
	default:
		return pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function option %q", t)
	}

	return nil
}

// resetFuncOption sets all function options to default values.
func resetFuncOption(udfDesc *funcdesc.Mutable) {
	udfDesc.SetVolatility(catpb.Function_VOLATILE)
	udfDesc.SetNullInputBehavior(catpb.Function_CALLED_ON_NULL_INPUT)
	udfDesc.SetLeakProof(false)
}

func makeFunctionArg(
	ctx context.Context, arg tree.FuncArg, typeResolver tree.TypeReferenceResolver,
) (descpb.FunctionDescriptor_Argument, error) {
	pbArg := descpb.FunctionDescriptor_Argument{
		Name: string(arg.Name),
	}
	var err error
	pbArg.Class, err = funcdesc.ArgClassToProto(arg.Class)
	if err != nil {
		return descpb.FunctionDescriptor_Argument{}, err
	}

	pbArg.Type, err = tree.ResolveType(ctx, arg.Type, typeResolver)
	if err != nil {
		return descpb.FunctionDescriptor_Argument{}, err
	}

	if arg.DefaultVal != nil {
		return descpb.FunctionDescriptor_Argument{}, unimplemented.New("CREATE FUNCTION argument", "default value")
	}

	return pbArg, nil
}

func (p *planner) descIsTable(ctx context.Context, id descpb.ID) (bool, error) {
	desc, err := p.Descriptors().GetImmutableDescriptorByID(
		ctx, p.Txn(), id, tree.ObjectLookupFlagsWithRequired().CommonLookupFlags,
	)
	if err != nil {
		return false, err
	}
	return desc.DescriptorType() == catalog.Table, nil
}
