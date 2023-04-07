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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
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

	if err := params.p.canCreateOnSchema(
		params.ctx, n.scDesc.GetID(), n.dbDesc.GetID(), params.p.User(), skipCheckPublicSchema,
	); err != nil {
		return err
	}

	for _, dep := range n.planDeps {
		if dbID := dep.desc.GetParentID(); dbID != n.dbDesc.GetID() && dbID != keys.SystemDatabaseID {
			return pgerror.Newf(pgcode.FeatureNotSupported, "the function cannot refer to other databases")
		}
	}

	mutScDesc, err := params.p.descCollection.MutableByName(params.p.Txn()).Schema(params.ctx, n.dbDesc, n.scDesc.GetName())
	if err != nil {
		return err
	}

	var retErr error
	params.p.runWithOptions(resolveFlags{contextDatabaseID: n.dbDesc.GetID()}, func() {
		retErr = func() error {
			udfMutableDesc, isNew, err := n.getMutableFuncDesc(mutScDesc, params)
			if err != nil {
				return err
			}

			fnName := tree.MakeQualifiedFunctionName(n.dbDesc.GetName(), n.scDesc.GetName(), n.cf.FuncName.String())
			event := eventpb.CreateFunction{
				FunctionName: fnName.FQString(),
				IsReplace:    !isNew,
			}
			if isNew {
				err = n.createNewFunction(udfMutableDesc, mutScDesc, params)
			} else {
				err = n.replaceFunction(udfMutableDesc, params)
			}
			if err != nil {
				return err
			}
			return params.p.logEvent(params.ctx, udfMutableDesc.GetID(), &event)
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
	if err := validateVolatilityInOptions(n.cf.Options, udfDesc); err != nil {
		return err
	}

	for _, option := range n.cf.Options {
		err := setFuncOption(params, udfDesc, option)
		if err != nil {
			return err
		}
	}

	if err := n.addUDFReferences(udfDesc, params); err != nil {
		return err
	}

	err := params.p.createDescriptor(
		params.ctx,
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
	paramTypes := make([]*types.T, len(udfDesc.Params))
	for i, param := range udfDesc.Params {
		paramTypes[i] = param.Type
	}
	scDesc.AddFunction(
		udfDesc.GetName(),
		descpb.SchemaDescriptor_FunctionSignature{
			ID:         udfDesc.GetID(),
			ArgTypes:   paramTypes,
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

	// Make sure parameter names are not changed.
	for i := range n.cf.Params {
		if string(n.cf.Params[i].Name) != udfDesc.Params[i].Name {
			return pgerror.Newf(
				pgcode.InvalidFunctionDefinition, "cannot change name of input parameter %q", udfDesc.Params[i].Name,
			)
		}
	}

	// Make sure return type is the same. The signature of user-defined types may
	// change, as long as the same type is referenced. If this is the case, we
	// must update the return type.
	retType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return err
	}
	isSameUDT := types.IsOIDUserDefinedType(retType.Oid()) && retType.Oid() ==
		udfDesc.ReturnType.Type.Oid()
	if n.cf.ReturnType.IsSet != udfDesc.ReturnType.ReturnSet || (!retType.Equal(udfDesc.ReturnType.Type) && !isSameUDT) {
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "cannot change return type of existing function")
	}
	if isSameUDT {
		udfDesc.ReturnType.Type = retType
	}

	resetFuncOption(udfDesc)
	if err := validateVolatilityInOptions(n.cf.Options, udfDesc); err != nil {
		return err
	}
	for _, option := range n.cf.Options {
		err := setFuncOption(params, udfDesc, option)
		if err != nil {
			return err
		}
	}

	// Removing all existing references before adding new references.
	for _, id := range udfDesc.DependsOn {
		backRefMutable, err := params.p.Descriptors().MutableByID(params.p.txn).Table(params.ctx, id)
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
	// Resolve parameter types.
	paramTypes := make([]*types.T, len(n.cf.Params))
	pbParams := make([]descpb.FunctionDescriptor_Parameter, len(n.cf.Params))
	paramNameSeen := make(map[tree.Name]struct{})
	for i, param := range n.cf.Params {
		if param.Name != "" {
			if _, ok := paramNameSeen[param.Name]; ok {
				// Argument names cannot be used more than once.
				return nil, false, pgerror.Newf(
					pgcode.InvalidFunctionDefinition, "parameter name %q used more than once", param.Name,
				)
			}
			paramNameSeen[param.Name] = struct{}{}
		}
		pbParam, err := makeFunctionParam(params.ctx, param, params.p)
		if err != nil {
			return nil, false, err
		}
		pbParams[i] = pbParam
		paramTypes[i] = pbParam.Type
	}

	// Try to look up an existing function.
	fuObj := tree.FuncObj{
		FuncName: n.cf.FuncName,
		Params:   n.cf.Params,
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
		fnID := funcdesc.UserDefinedFunctionOIDToID(existing.Oid)
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

	privileges, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		n.dbDesc.GetDefaultPrivilegeDescriptor(),
		scDesc.GetDefaultPrivilegeDescriptor(),
		n.dbDesc.GetID(),
		params.SessionData().User(),
		privilege.Functions,
	)
	if err != nil {
		return nil, false, err
	}

	newUdfDesc := funcdesc.NewMutableFunctionDescriptor(
		funcDescID,
		n.dbDesc.GetID(),
		scDesc.GetID(),
		string(n.cf.FuncName.ObjectName),
		pbParams,
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
		backRefMutable, err := params.p.Descriptors().MutableByID(params.p.txn).Table(params.ctx, id)
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
		v, err := funcinfo.VolatilityToProto(t)
		if err != nil {
			return err
		}
		udfDesc.SetVolatility(v)
	case tree.FunctionLeakproof:
		udfDesc.SetLeakProof(bool(t))
	case tree.FunctionNullInputBehavior:
		v, err := funcinfo.NullInputBehaviorToProto(t)
		if err != nil {
			return err
		}
		udfDesc.SetNullInputBehavior(v)
	case tree.FunctionLanguage:
		v, err := funcinfo.FunctionLangToProto(t)
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

func makeFunctionParam(
	ctx context.Context, param tree.FuncParam, typeResolver tree.TypeReferenceResolver,
) (descpb.FunctionDescriptor_Parameter, error) {
	pbParam := descpb.FunctionDescriptor_Parameter{
		Name: string(param.Name),
	}
	var err error
	pbParam.Class, err = funcinfo.ParamClassToProto(param.Class)
	if err != nil {
		return descpb.FunctionDescriptor_Parameter{}, err
	}

	pbParam.Type, err = tree.ResolveType(ctx, param.Type, typeResolver)
	if err != nil {
		return descpb.FunctionDescriptor_Parameter{}, err
	}

	if param.DefaultVal != nil {
		return descpb.FunctionDescriptor_Parameter{}, unimplemented.NewWithIssue(100962, "default value")
	}

	return pbParam, nil
}

func (p *planner) descIsTable(ctx context.Context, id descpb.ID) (bool, error) {
	desc, err := p.Descriptors().ByIDWithLeased(p.Txn()).WithoutNonPublic().Get().Desc(ctx, id)
	if err != nil {
		return false, err
	}
	return desc.DescriptorType() == catalog.Table, nil
}

// validateVolatilityInOptions checks if the volatility values in the given list
// of function options, if any, can be applied to the function descriptor.
func validateVolatilityInOptions(
	options tree.FunctionOptions, fnDesc catalog.FunctionDescriptor,
) error {
	vp := funcinfo.MakeVolatilityProperties(fnDesc.GetVolatility(), fnDesc.GetLeakProof())
	if err := vp.Apply(options); err != nil {
		return err
	}
	if err := vp.Validate(); err != nil {
		return sqlerrors.NewInvalidVolatilityError(err)
	}
	return nil
}
