// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type createFunctionNode struct {
	cf *tree.CreateRoutine

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

	scDesc, err := params.p.descCollection.ByName(params.p.Txn()).Get().Schema(params.ctx, n.dbDesc, n.scDesc.GetName())
	if err != nil {
		return err
	}
	if scDesc.SchemaKind() == catalog.SchemaTemporary {
		return unimplemented.NewWithIssue(104687, "cannot create UDFs under a temporary schema")
	}

	telemetry.Inc(sqltelemetry.SchemaChangeCreateCounter("function"))

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

			fnName := tree.MakeQualifiedRoutineName(n.dbDesc.GetName(), n.scDesc.GetName(), n.cf.Name.String())
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

	if err := setFuncOptions(params, udfDesc, n.cf.Options); err != nil {
		return err
	}

	if err := n.addUDFReferences(udfDesc, params); err != nil {
		return err
	}

	err := params.p.createDescriptor(
		params.ctx,
		udfDesc,
		tree.AsStringWithFQNames(&n.cf.Name, params.Ann()),
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
			ID:          udfDesc.GetID(),
			ArgTypes:    paramTypes,
			ReturnType:  returnType,
			ReturnSet:   udfDesc.ReturnType.ReturnSet,
			IsProcedure: udfDesc.IsProcedure(),
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

	if n.cf.IsProcedure && !udfDesc.IsProcedure() {
		return errors.WithDetailf(
			pgerror.Newf(pgcode.WrongObjectType, "cannot change routine kind"),
			"%q is a function",
			udfDesc.Name,
		)
	}

	if !n.cf.IsProcedure && udfDesc.IsProcedure() {
		return errors.WithDetailf(
			pgerror.Newf(pgcode.WrongObjectType, "cannot change routine kind"),
			"%q is a procedure",
			udfDesc.Name,
		)
	}

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
	if n.cf.ReturnType.SetOf != udfDesc.ReturnType.ReturnSet || (!retType.Equal(udfDesc.ReturnType.Type) && !isSameUDT) {
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "cannot change return type of existing function")
	}
	if isSameUDT {
		udfDesc.ReturnType.Type = retType
	}

	resetFuncOption(udfDesc)
	if err := validateVolatilityInOptions(n.cf.Options, udfDesc); err != nil {
		return err
	}
	if err := setFuncOptions(params, udfDesc, n.cf.Options); err != nil {
		return err
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
	routineObj := tree.RoutineObj{
		FuncName: n.cf.Name,
		Params:   n.cf.Params,
	}
	existing, err := params.p.matchRoutine(params.ctx, &routineObj,
		false /* required */, tree.UDFRoutine|tree.ProcedureRoutine)
	if err != nil {
		return nil, false, err
	}

	if existing != nil {
		// Return an error if there is an existing match but not a replacement.
		if !n.cf.Replace {
			return nil, false, pgerror.Newf(
				pgcode.DuplicateFunction,
				"function %q already exists with same argument types",
				n.cf.Name.Object(),
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
		privilege.Routines,
	)
	if err != nil {
		return nil, false, err
	}

	newUdfDesc := funcdesc.NewMutableFunctionDescriptor(
		funcDescID,
		n.dbDesc.GetID(),
		scDesc.GetID(),
		string(n.cf.Name.ObjectName),
		pbParams,
		returnType,
		n.cf.ReturnType.SetOf,
		n.cf.IsProcedure,
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
				n.cf.Name.String(), updated.desc.GetName(), updated.desc.GetID(),
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
				n.cf.Name.String(), backRefMutable.GetName(), backRefMutable.GetID(),
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

func setFuncOptions(
	params runParams, udfDesc *funcdesc.Mutable, options tree.RoutineOptions,
) error {
	var err error
	var body string
	var lang catpb.Function_Language
	for _, option := range options {
		switch t := option.(type) {
		case tree.RoutineVolatility:
			vol, err := funcinfo.VolatilityToProto(t)
			if err != nil {
				return err
			}
			udfDesc.SetVolatility(vol)
		case tree.RoutineLeakproof:
			udfDesc.SetLeakProof(bool(t))
		case tree.RoutineNullInputBehavior:
			v, err := funcinfo.NullInputBehaviorToProto(t)
			if err != nil {
				return err
			}
			udfDesc.SetNullInputBehavior(v)
		case tree.RoutineLanguage:
			lang, err = funcinfo.FunctionLangToProto(t)
			if err != nil {
				return err
			}
			udfDesc.SetLang(lang)
		case tree.RoutineBodyStr:
			// Handle the body after the loop, since we don't yet know what language
			// it is.
			body = string(t)
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function option %q", t)
		}
	}

	if lang != catpb.Function_UNKNOWN_LANGUAGE && body != "" {
		// Replace any sequence names in the function body with IDs.
		seqReplacedFuncBody, err := replaceSeqNamesWithIDsLang(params.ctx, params.p, body, true, lang)
		if err != nil {
			return err
		}
		typeReplacedFuncBody, err := serializeUserDefinedTypesLang(
			params.ctx, params.p.SemaCtx(), seqReplacedFuncBody, true /* multiStmt */, "UDFs", lang)
		if err != nil {
			return err
		}
		udfDesc.SetFuncBody(typeReplacedFuncBody)
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
	ctx context.Context, param tree.RoutineParam, typeResolver tree.TypeReferenceResolver,
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
	options tree.RoutineOptions, fnDesc catalog.FunctionDescriptor,
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
