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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
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
	if !params.EvalContext().Settings.Version.IsActive(
		params.ctx,
		clusterversion.SchemaChangeSupportsCreateFunction,
	) {
		// TODO(chengxiong): remove this version gate in 23.1.
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"cannot run CREATE FUNCTION before system is fully upgraded to v22.2",
		)
	}

	if n.cf.RoutineBody != nil {
		return unimplemented.NewWithIssue(85144, "CREATE FUNCTION...sql_body unimplemented")
	}

	for _, dep := range n.planDeps {
		if dbID := dep.desc.GetParentID(); dbID != n.dbDesc.GetID() && dbID != keys.SystemDatabaseID {
			return pgerror.Newf(pgcode.FeatureNotSupported, "the function cannot refer to other databases")
		}
	}

	scDesc, err := params.p.descCollection.GetMutableSchemaByName(
		params.ctx, params.p.Txn(), n.dbDesc, n.scDesc.GetName(),
		tree.SchemaLookupFlags{Required: true, RequireMutable: true},
	)
	if err != nil {
		return err
	}
	mutScDesc := scDesc.(*schemadesc.Mutable)

	var retErr error
	params.p.runWithOptions(resolveFlags{contextDatabaseID: n.dbDesc.GetID()}, func() {
		retErr = func() error {
			udfMutableDesc, isNew, err := n.getMutableFuncDesc(mutScDesc, params)
			if err != nil {
				return err
			}

			if isNew {
				return n.createNewFunction(udfMutableDesc, mutScDesc, params)
			}
			return n.replaceFunction(udfMutableDesc, params)
		}()
	})

	return retErr
}

func (*createFunctionNode) Next(params runParams) (bool, error) { return false, nil }
func (*createFunctionNode) Values() tree.Datums                 { return tree.Datums{} }
func (*createFunctionNode) Close(ctx context.Context)           {}

func (n *createFunctionNode) createNewFunction(
	udfDesc *funcdesc.Mutable, scDesc *schemadesc.Mutable, params runParams,
) error {
	for _, option := range n.cf.Options {
		err := setFuncOption(params, udfDesc, option)
		if err != nil {
			return err
		}
	}
	if err := funcdesc.CheckLeakProofVolatility(udfDesc); err != nil {
		return err
	}

	if err := n.addUDFReferences(udfDesc, params); err != nil {
		return err
	}

	err := params.p.createDescriptorWithID(
		params.ctx,
		roachpb.Key{}, // UDF does not have namespace entry.
		udfDesc.GetID(),
		udfDesc,
		tree.AsStringWithFQNames(&n.cf.FuncName, params.Ann()),
	)
	if err != nil {
		return err
	}

	returnType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return err
	}
	argTypes := make([]*types.T, len(udfDesc.Args))
	for i, arg := range udfDesc.Args {
		argTypes[i] = arg.Type
	}
	scDesc.AddFunction(
		udfDesc.GetName(),
		descpb.SchemaDescriptor_FunctionOverload{
			ID:         udfDesc.GetID(),
			ArgTypes:   argTypes,
			ReturnType: returnType,
			ReturnSet:  udfDesc.ReturnType.ReturnSet,
		},
	)
	if err := params.p.writeSchemaDescChange(params.ctx, scDesc, "Create Function"); err != nil {
		return err
	}

	return nil
}

func (n *createFunctionNode) replaceFunction(udfDesc *funcdesc.Mutable, params runParams) error {
	// TODO(chengxiong): add validation that the function is not referenced. This
	// is needed when we start allowing function references from other objects.

	// Make sure argument names are not changed.
	for i := range n.cf.Args {
		if string(n.cf.Args[i].Name) != udfDesc.Args[i].Name {
			return pgerror.Newf(
				pgcode.InvalidFunctionDefinition, "cannot change name of input parameter %q", udfDesc.Args[i].Name,
			)
		}
	}

	// Make sure return type is the same.
	retType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return err
	}
	if n.cf.ReturnType.IsSet != udfDesc.ReturnType.ReturnSet || !retType.Equal(udfDesc.ReturnType.Type) {
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "cannot change return type of existing function")
	}

	resetFuncOption(udfDesc)
	for _, option := range n.cf.Options {
		err := setFuncOption(params, udfDesc, option)
		if err != nil {
			return err
		}
	}

	if err := funcdesc.CheckLeakProofVolatility(udfDesc); err != nil {
		return err
	}

	// Removing all existing references before adding new references.
	for _, id := range udfDesc.DependsOn {
		backRefMutable, err := params.p.Descriptors().GetMutableTableByID(
			params.ctx, params.p.txn, id, tree.ObjectLookupFlagsWithRequired(),
		)
		if err != nil {
			return err
		}
		backRefMutable.DependedOnBy = removeMatchingReferences(backRefMutable.DependedOnBy, udfDesc.ID)
		jobDesc := fmt.Sprintf(
			"removing udf reference %s(%d) in table %s(%d)",
			udfDesc.Name, udfDesc.ID, backRefMutable.Name, backRefMutable.ID,
		)
		if err := params.p.writeSchemaChange(params.ctx, backRefMutable, descpb.InvalidMutationID, jobDesc); err != nil {
			return err
		}
	}
	jobDesc := fmt.Sprintf("updating type back reference %d for function %d", udfDesc.DependsOnTypes, udfDesc.ID)
	if err := params.p.removeTypeBackReferences(params.ctx, udfDesc.DependsOnTypes, udfDesc.ID, jobDesc); err != nil {
		return err
	}
	// Add all new references.
	if err := n.addUDFReferences(udfDesc, params); err != nil {
		return err
	}

	return params.p.writeFuncSchemaChange(params.ctx, udfDesc)
}

func (n *createFunctionNode) getMutableFuncDesc(
	scDesc catalog.SchemaDescriptor, params runParams,
) (fnDesc *funcdesc.Mutable, isNew bool, err error) {
	// Resolve argument types.
	argTypes := make([]*types.T, len(n.cf.Args))
	pbArgs := make([]descpb.FunctionDescriptor_Argument, len(n.cf.Args))
	argNameSeen := make(map[tree.Name]struct{})
	for i, arg := range n.cf.Args {
		if arg.Name != "" {
			if _, ok := argNameSeen[arg.Name]; ok {
				// Argument names cannot be used more than once.
				return nil, false, pgerror.Newf(
					pgcode.InvalidFunctionDefinition, "parameter name %q used more than once", arg.Name,
				)
			}
			argNameSeen[arg.Name] = struct{}{}
		}
		pbArg, err := makeFunctionArg(params.ctx, arg, params.p)
		if err != nil {
			return nil, false, err
		}
		pbArgs[i] = pbArg
		argTypes[i] = pbArg.Type
	}

	// Try to look up an existing function.
	fuObj := tree.FuncObj{
		FuncName: n.cf.FuncName,
		Args:     n.cf.Args,
	}
	existing, err := params.p.matchUDF(params.ctx, &fuObj, false /* required */)
	if err != nil {
		return nil, false, err
	}

	if existing != nil {
		// Return an error if there is an existing match but not a replacement.
		if !n.cf.Replace {
			return nil, false, pgerror.Newf(
				pgcode.DuplicateFunction,
				"function %q already exists with same argument types",
				n.cf.FuncName.Object(),
			)
		}
		fnID, err := funcdesc.UserDefinedFunctionOIDToID(existing.Oid)
		if err != nil {
			return nil, false, err
		}
		fnDesc, err = params.p.checkPrivilegesForDropFunction(params.ctx, fnID)
		if err != nil {
			return nil, false, err
		}
		return fnDesc, false, nil
	}

	funcDescID, err := params.EvalContext().DescIDGenerator.GenerateUniqueDescID(params.ctx)
	if err != nil {
		return nil, false, err
	}

	returnType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return nil, false, err
	}

	privileges := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		n.dbDesc.GetDefaultPrivilegeDescriptor(),
		scDesc.GetDefaultPrivilegeDescriptor(),
		n.dbDesc.GetID(),
		params.SessionData().User(),
		privilege.Functions,
		n.dbDesc.GetPrivileges(),
	)

	newUdfDesc := funcdesc.NewMutableFunctionDescriptor(
		funcDescID,
		n.dbDesc.GetID(),
		scDesc.GetID(),
		string(n.cf.FuncName.ObjectName),
		pbArgs,
		returnType,
		n.cf.ReturnType.IsSet,
		privileges,
	)

	return &newUdfDesc, true, nil
}

func (n *createFunctionNode) addUDFReferences(udfDesc *funcdesc.Mutable, params runParams) error {
	// Get all table IDs for which we need to update back references, including
	// tables used directly in function body or as implicit types.
	backrefTblIDs := catalog.DescriptorIDSet{}
	implicitTypeTblIDs := catalog.DescriptorIDSet{}
	for id := range n.planDeps {
		backrefTblIDs.Add(id)
	}
	for id := range n.typeDeps {
		if isTable, err := params.p.descIsTable(params.ctx, id); err != nil {
			return err
		} else if isTable {
			backrefTblIDs.Add(id)
			implicitTypeTblIDs.Add(id)
		}
	}

	// Read all referenced tables and update their dependencies.
	backRefMutables := make(map[descpb.ID]*tabledesc.Mutable)
	for _, id := range backrefTblIDs.Ordered() {
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
			dep.ID = udfDesc.ID
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
	for _, id := range implicitTypeTblIDs.Ordered() {
		backRefMutable := backRefMutables[id]
		backRefMutable.DependedOnBy = append(backRefMutable.DependedOnBy, descpb.TableDescriptor_Reference{ID: udfDesc.ID})
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
		if implicitTypeTblIDs.Contains(id) {
			continue
		}
		jobDesc := fmt.Sprintf("updating type back reference %d for function %d", id, udfDesc.ID)
		if err := params.p.addTypeBackReference(params.ctx, id, udfDesc.ID, jobDesc); err != nil {
			return err
		}
	}

	// Add forward references to UDF descriptor.
	udfDesc.DependsOn = backrefTblIDs.Ordered()

	typeDepIDs := catalog.DescriptorIDSet{}
	for id := range n.typeDeps {
		typeDepIDs.Add(id)
	}
	udfDesc.DependsOnTypes = typeDepIDs.Difference(implicitTypeTblIDs).Ordered()
	return nil
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
		typeReplacedFuncBody, err := serializeUserDefinedTypes(params.ctx, params.p.SemaCtx(), seqReplacedFuncBody, true /* multiStmt */)
		if err != nil {
			return err
		}
		udfDesc.SetFuncBody(typeReplacedFuncBody)
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
