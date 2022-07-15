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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descidgen"
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
	funcName    *tree.FunctionName
	replace     bool
	args        tree.FuncArgs
	returnType  tree.FuncReturnType
	options     tree.FunctionOptions
	funcBody    tree.FunctionBodyStr
	routineBody *tree.RoutineBody

	dbDesc   catalog.DatabaseDescriptor
	scDesc   catalog.SchemaDescriptor
	planDeps planDependencies
	typeDeps typeDependencies
}

func (n *createFunctionNode) ReadingOwnWrites() {}

func (n *createFunctionNode) startExec(params runParams) error {
	if n.routineBody != nil {
		return unimplemented.New("create function sql_body", "CREATE FUNCTION...sql_body unimplemented")
	}

	scDesc := n.scDesc.NewBuilder().BuildExistingMutable().(*schemadesc.Mutable)
	udfMutableDesc, err := n.getMutableFuncDesc(params)
	if err != nil {
		return err
	}

	// If it's replacing, there's no need to touch argument types.
	if !n.replace {
		for _, arg := range n.args {
			pbArg, err := toPbFunctionArg(params.ctx, arg, params.p)
			if err != nil {
				return err
			}
			udfMutableDesc.AddArgument(pbArg)
		}
	}

	// Set options to default value before applying specific options.
	// This just simplify the replacing logic.
	resetFuncOption(udfMutableDesc)
	for _, option := range n.options {
		err := setFuncOption(udfMutableDesc, option)
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

	// Replace sequence names with IDs in any expressions.
	seqReplacedFuncBody, err := replaceSeqNamesWithIDs(params.ctx, params.p, string(n.funcBody), true)
	if err != nil {
		return err
	}
	udfMutableDesc.SetFuncBody(seqReplacedFuncBody)

	// If replacing, remove all back references from dependencies before setting
	// new back references.
	if n.replace {
		for _, id := range udfMutableDesc.DependsOn {
			backRefMutable, err := params.p.Descriptors().GetMutableTableByID(
				params.ctx, params.p.txn, id, tree.ObjectLookupFlagsWithRequired(),
			)
			if err != nil {
				return err
			}
			backRefMutable.DependedOnBy = removeMatchingReferences(
				backRefMutable.DependedOnBy,
				udfMutableDesc.GetID(),
			)
		}

		jobDesc := fmt.Sprintf("updating type back reference %d for function %d", udfMutableDesc.DependsOnTypes, udfMutableDesc.ID)
		err = params.p.removeTypeBackReferences(
			params.ctx,
			udfMutableDesc.DependsOnTypes,
			udfMutableDesc.ID,
			jobDesc,
		)
		if err != nil {
			return err
		}
	}

	// Read all referenced tables and update their dependencies.
	backRefMutables := make(map[descpb.ID]*tabledesc.Mutable, len(n.planDeps))
	for id := range n.planDeps {
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
				n.funcName, updated.desc.GetName(), updated.desc.GetID(),
			),
		); err != nil {
			return err
		}
	}

	// Add type back references.
	for id := range n.typeDeps {
		jobDesc := fmt.Sprintf("updating type back reference %d for function %d", id, udfMutableDesc.ID)
		if err := params.p.addTypeBackReference(params.ctx, id, udfMutableDesc.ID, jobDesc); err != nil {
			return err
		}
	}

	// Add forward references to UDF descriptor.
	udfMutableDesc.DependsOn = []descpb.ID{}
	for backrefID := range n.planDeps {
		udfMutableDesc.DependsOn = append(udfMutableDesc.DependsOn, backrefID)
	}
	sort.Slice(udfMutableDesc.DependsOn, func(i, j int) bool {
		return udfMutableDesc.DependsOn[i] < udfMutableDesc.DependsOn[j]
	})

	udfMutableDesc.DependsOnTypes = []descpb.ID{}
	for backrefID := range n.typeDeps {
		if _, err := params.p.Descriptors().GetImmutableTableByID(
			params.ctx, params.p.Txn(), backrefID, tree.ObjectLookupFlagsWithRequired(),
		); err == nil {
			// If it's a table implicit type, we don't add it as a type dependency.
			continue
		}
		udfMutableDesc.DependsOnTypes = append(udfMutableDesc.DependsOnTypes, backrefID)
	}
	sort.Slice(udfMutableDesc.DependsOnTypes, func(i, j int) bool {
		return udfMutableDesc.DependsOnTypes[i] < udfMutableDesc.DependsOnTypes[j]
	})

	if n.replace {
		err = params.p.writeFuncSchemaChange(params.ctx, udfMutableDesc)
	} else {
		err = params.p.createDescriptorWithID(
			params.ctx,
			roachpb.Key{}, // UDF does not have namespace entry.
			udfMutableDesc.GetID(),
			udfMutableDesc,
			tree.AsStringWithFQNames(n.funcName, params.Ann()),
		)
	}
	if err != nil {
		return err
	}

	// If not replacing, add the new function to schema.
	if !n.replace {
		returnType, err := tree.ResolveType(params.ctx, n.returnType.Type, params.p)
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

		if err := validateDescriptor(params.ctx, params.p, udfMutableDesc); err != nil {
			return err
		}
	}
	return nil
}

func (*createFunctionNode) Next(params runParams) (bool, error) { return false, nil }
func (*createFunctionNode) Values() tree.Datums                 { return tree.Datums{} }
func (*createFunctionNode) Close(ctx context.Context)           {}

func (n *createFunctionNode) getMutableFuncDesc(params runParams) (*funcdesc.Mutable, error) {
	if n.replace {
		return nil, unimplemented.New("CREATE OR REPLACE FUNCTION", "replacing function")
	}
	// TODO (Chengxiong) add function resolution and check if it's a Replace.
	// Also:
	// (1) add validation that return type can't be change.
	// (2) add validation that argument types can't be change.
	// (3) add validation that if existing function is referenced then it cannot be replace.
	// (4) add `if` branch so that we only create new descriptor when it's not a replace.

	funcDescID, err := descidgen.GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB, params.p.ExecCfg().Codec)
	if err != nil {
		return nil, err
	}

	returnType, err := tree.ResolveType(params.ctx, n.returnType.Type, params.p)
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
		string(n.funcName.ObjectName),
		len(n.args),
		returnType,
		n.returnType.IsSet,
		privileges,
	)

	return &newUdfDesc, nil
}

func setFuncOption(udfDesc *funcdesc.Mutable, option tree.FunctionOption) error {
	switch t := option.(type) {
	case tree.FunctionVolatility:
		v, err := funcdesc.ToPbFunctionVolatility(t)
		if err != nil {
			return err
		}
		udfDesc.SetVolatility(v)
	case tree.FunctionLeakproof:
		udfDesc.SetLeakProof(bool(t))
	case tree.FunctionNullInputBehavior:
		v, err := funcdesc.ToPbFunctionNullInputBehavior(t)
		if err != nil {
			return err
		}
		udfDesc.SetNullInputBehavior(v)
	case tree.FunctionLanguage:
		v, err := funcdesc.ToPbFunctionLang(t)
		if err != nil {
			return err
		}
		udfDesc.SetLang(v)
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

func toPbFunctionArg(
	ctx context.Context, arg tree.FuncArg, typeResolver tree.TypeReferenceResolver,
) (descpb.FunctionDescriptor_Argument, error) {
	pbArg := descpb.FunctionDescriptor_Argument{
		Name: string(arg.Name),
	}
	var err error
	pbArg.Class, err = funcdesc.ToPbFunctionArgClass(arg.Class)
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

func (p *planner) writeFuncDesc(ctx context.Context, funcDesc *funcdesc.Mutable) error {
	b := p.txn.NewBatch()
	if err := p.Descriptors().WriteDescToBatch(
		ctx, p.extendedEvalCtx.Tracing.KVTracingEnabled(), funcDesc, b,
	); err != nil {
		return err
	}
	return p.txn.Run(ctx, b)
}

func (p *planner) writeFuncSchemaChange(ctx context.Context, funcDesc *funcdesc.Mutable) error {
	return p.writeFuncDesc(ctx, funcDesc)
}
