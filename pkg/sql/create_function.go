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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type functionDependencies map[catid.DescID]struct{}

type createFunctionNode struct {
	zeroInputPlanNode
	cf *tree.CreateRoutine

	dbDesc       catalog.DatabaseDescriptor
	scDesc       catalog.SchemaDescriptor
	planDeps     planDependencies
	typeDeps     typeDependencies
	functionDeps functionDependencies
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
			return pgerror.Newf(pgcode.FeatureNotSupported, "dependent relation %s cannot be from another database",
				dep.desc.GetName())
		}
	}

	for funcRef := range n.functionDeps {
		funcDesc, err := params.p.Descriptors().ByIDWithLeased(params.p.Txn()).Get().Function(params.ctx, funcRef)
		if err != nil {
			return err
		}
		if dbID := funcDesc.GetParentID(); dbID != n.dbDesc.GetID() && dbID != keys.SystemDatabaseID {
			return pgerror.Newf(pgcode.FeatureNotSupported, "dependent function %s cannot be from another database",
				funcDesc.GetName())
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
			udfMutableDesc, existing, err := n.getMutableFuncDesc(mutScDesc, params)
			if err != nil {
				return err
			}

			fnName := tree.MakeQualifiedRoutineName(n.dbDesc.GetName(), n.scDesc.GetName(), n.cf.Name.String())
			event := eventpb.CreateFunction{
				FunctionName: fnName.FQString(),
				IsReplace:    existing != nil,
			}
			if existing == nil {
				err = n.createNewFunction(udfMutableDesc, mutScDesc, params)
			} else {
				err = n.replaceFunction(udfMutableDesc, mutScDesc, params, existing)
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
	signatureTypes := make([]*types.T, 0, len(udfDesc.Params))
	var outParamOrdinals []int32
	var outParamTypes []*types.T
	var defaultExprs []string
	for paramIdx, param := range udfDesc.Params {
		class := funcdesc.ToTreeRoutineParamClass(param.Class)
		if tree.IsInParamClass(class) {
			signatureTypes = append(signatureTypes, param.Type)
		}
		if class == tree.RoutineParamOut {
			outParamOrdinals = append(outParamOrdinals, int32(paramIdx))
			outParamTypes = append(outParamTypes, param.Type)
		}
		if param.DefaultExpr != nil {
			defaultExprs = append(defaultExprs, *param.DefaultExpr)
		}
	}
	scDesc.AddFunction(
		udfDesc.GetName(),
		descpb.SchemaDescriptor_FunctionSignature{
			ID:               udfDesc.GetID(),
			ArgTypes:         signatureTypes,
			ReturnType:       returnType,
			ReturnSet:        udfDesc.ReturnType.ReturnSet,
			IsProcedure:      udfDesc.IsProcedure(),
			OutParamOrdinals: outParamOrdinals,
			OutParamTypes:    outParamTypes,
			DefaultExprs:     defaultExprs,
		},
	)
	if err := params.p.writeSchemaDescChange(params.ctx, scDesc, "Create Function"); err != nil {
		return err
	}

	return nil
}

func (n *createFunctionNode) replaceFunction(
	udfDesc *funcdesc.Mutable,
	scDesc *schemadesc.Mutable,
	params runParams,
	existing *tree.QualifiedOverload,
) error {

	if n.cf.IsProcedure != udfDesc.IsProcedure() {
		formatStr := "%q is a function"
		if udfDesc.IsProcedure() {
			formatStr = "%q is a procedure"
		}
		return errors.WithDetailf(
			pgerror.Newf(pgcode.WrongObjectType, "cannot change routine kind"),
			formatStr,
			udfDesc.Name,
		)
	}

	// Make sure return type is the same. The signature of user-defined types
	// may change, as long as the same type is referenced. If this is the case,
	// we must update the return type.
	retType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return err
	}
	isSameUDT := types.IsOIDUserDefinedType(retType.Oid()) && retType.Oid() ==
		udfDesc.ReturnType.Type.Oid()
	if n.cf.ReturnType.SetOf != udfDesc.ReturnType.ReturnSet || (!retType.Equal(udfDesc.ReturnType.Type) && !isSameUDT) {
		if udfDesc.IsProcedure() && (retType.Family() == types.VoidFamily || udfDesc.ReturnType.Type.Family() == types.VoidFamily) {
			return pgerror.Newf(pgcode.InvalidFunctionDefinition, "cannot change whether a procedure has output parameters")
		}
		return pgerror.Newf(pgcode.InvalidFunctionDefinition, "cannot change return type of existing function")
	}
	if isSameUDT {
		udfDesc.ReturnType.Type = retType
	}

	// Make sure that a trigger function is not replaced.
	if retType.Identical(types.Trigger) && len(udfDesc.DependedOnBy) > 0 {
		return errors.WithHint(
			unimplemented.NewWithIssue(134555,
				"cannot replace a trigger function with an active trigger"),
			"consider dropping and recreating the trigger",
		)
	}

	// Verify whether changes, if any, to the parameter names and classes are
	// allowed. This needs to happen after the return type has already been
	// checked.
	if err = n.validateParameters(udfDesc); err != nil {
		return err
	}
	// All parameter changes, if any, are allowed, so update the descriptor
	// accordingly.
	if cap(udfDesc.Params) >= len(n.cf.Params) {
		udfDesc.Params = udfDesc.Params[:len(n.cf.Params)]
	} else {
		udfDesc.Params = make([]descpb.FunctionDescriptor_Parameter, len(n.cf.Params))
	}
	var outParamOrdinals []int32
	var outParamTypes []*types.T
	var defaultExprs []string
	for i, p := range n.cf.Params {
		udfDesc.Params[i], err = makeFunctionParam(params.ctx, params.p.SemaCtx(), p, params.p)
		if err != nil {
			return err
		}
		if p.Class == tree.RoutineParamOut {
			outParamOrdinals = append(outParamOrdinals, int32(i))
			outParamTypes = append(outParamTypes, udfDesc.Params[i].Type)
		}
		if udfDesc.Params[i].DefaultExpr != nil {
			defaultExprs = append(defaultExprs, *udfDesc.Params[i].DefaultExpr)
		}
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
	for _, id := range udfDesc.DependsOnFunctions {
		backRefMutable, err := params.p.Descriptors().MutableByID(params.p.txn).Function(params.ctx, id)
		if err != nil {
			return err
		}
		if err := backRefMutable.RemoveFunctionReference(udfDesc.ID); err != nil {
			return err
		}
		if err := params.p.writeFuncSchemaChange(params.ctx, backRefMutable); err != nil {
			return err
		}
	}
	// Add all new references.
	if err := n.addUDFReferences(udfDesc, params); err != nil {
		return err
	}

	signatureChanged := len(existing.OutParamOrdinals) != len(outParamOrdinals) ||
		len(existing.DefaultExprs) != len(defaultExprs)
	for i := 0; !signatureChanged && i < len(outParamOrdinals); i++ {
		signatureChanged = existing.OutParamOrdinals[i] != outParamOrdinals[i] ||
			!existing.OutParamTypes.GetAt(i).Equivalent(outParamTypes[i])
	}
	for i := 0; !signatureChanged && i < len(defaultExprs); i++ {
		typ := existing.Types.GetAt(i + existing.Types.Length() - len(defaultExprs))
		// Update the overload to store the type-checked expression since this
		// is what is stored in the FunctionSignature proto.
		existing.DefaultExprs[i], err = tree.TypeCheck(params.ctx, existing.DefaultExprs[i], params.p.SemaCtx(), typ)
		if err != nil {
			return err
		}
		signatureChanged = tree.Serialize(existing.DefaultExprs[i]) != defaultExprs[i]
	}
	if signatureChanged {
		if err = scDesc.ReplaceOverload(
			udfDesc.GetName(),
			existing,
			descpb.SchemaDescriptor_FunctionSignature{
				ID:               udfDesc.GetID(),
				ArgTypes:         existing.Types.Types(),
				ReturnType:       retType,
				ReturnSet:        udfDesc.ReturnType.ReturnSet,
				IsProcedure:      n.cf.IsProcedure,
				OutParamOrdinals: outParamOrdinals,
				OutParamTypes:    outParamTypes,
				DefaultExprs:     defaultExprs,
			},
		); err != nil {
			return err
		}
		if err = params.p.writeSchemaDescChange(params.ctx, scDesc, "Replace Function"); err != nil {
			return err
		}
	}

	return params.p.writeFuncSchemaChange(params.ctx, udfDesc)
}

func (n *createFunctionNode) getMutableFuncDesc(
	scDesc catalog.SchemaDescriptor, params runParams,
) (fnDesc *funcdesc.Mutable, existing *tree.QualifiedOverload, err error) {
	pbParams := make([]descpb.FunctionDescriptor_Parameter, len(n.cf.Params))
	for i, param := range n.cf.Params {
		pbParam, err := makeFunctionParam(params.ctx, params.p.SemaCtx(), param, params.p)
		if err != nil {
			return nil, nil, err
		}
		pbParams[i] = pbParam
	}

	// Try to look up an existing function.
	routineObj := tree.RoutineObj{
		FuncName: n.cf.Name,
		Params:   n.cf.Params,
	}
	existing, err = params.p.matchRoutine(
		params.ctx, &routineObj, false, /* required */
		tree.UDFRoutine|tree.ProcedureRoutine, false, /* inDropContext */
	)
	if err != nil {
		return nil, nil, err
	}

	if existing != nil {
		// Return an error if there is an existing match but not a replacement.
		if !n.cf.Replace {
			return nil, nil, pgerror.Newf(
				pgcode.DuplicateFunction,
				"function %q already exists with same argument types",
				n.cf.Name.Object(),
			)
		}
		fnID := funcdesc.UserDefinedFunctionOIDToID(existing.Oid)
		fnDesc, err = params.p.checkPrivilegesForDropFunction(params.ctx, fnID)
		if err != nil {
			return nil, nil, err
		}
		return fnDesc, existing, nil
	}

	funcDescID, err := params.EvalContext().DescIDGenerator.GenerateUniqueDescID(params.ctx)
	if err != nil {
		return nil, nil, err
	}

	returnType, err := tree.ResolveType(params.ctx, n.cf.ReturnType.Type, params.p)
	if err != nil {
		return nil, nil, err
	}

	privileges, err := catprivilege.CreatePrivilegesFromDefaultPrivileges(
		n.dbDesc.GetDefaultPrivilegeDescriptor(),
		scDesc.GetDefaultPrivilegeDescriptor(),
		n.dbDesc.GetID(),
		params.SessionData().User(),
		privilege.Routines,
	)
	if err != nil {
		return nil, nil, err
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

	return &newUdfDesc, nil, nil
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

	udfDesc.DependsOnFunctions = make([]descpb.ID, 0, len(n.functionDeps))
	for id := range n.functionDeps {
		// Add a reference to the dependency in here. Note that we need to add
		// this dep before updating the back reference in case it's a
		// self-dependent function (so that we get a proper error from
		// AddFunctionReference).
		udfDesc.DependsOnFunctions = append(udfDesc.DependsOnFunctions, id)
		// Add a back reference.
		backRefDesc, err := params.p.Descriptors().MutableByID(params.p.Txn()).Function(params.ctx, id)
		if err != nil {
			return err
		}
		if err := backRefDesc.AddFunctionReference(udfDesc.ID); err != nil {
			return err
		}
		if err := params.p.writeFuncSchemaChange(params.ctx, backRefDesc); err != nil {
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
		case tree.RoutineSecurity:
			sec, err := funcinfo.SecurityToProto(t)
			if err != nil {
				return err
			}
			udfDesc.SetSecurity(sec)
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "Unknown function option %q", t)
		}
	}

	if lang != catpb.Function_UNKNOWN_LANGUAGE && body != "" {
		// Trigger functions do not analyze SQL statements beyond parsing, so type
		// and sequence names should not be replaced during trigger-function
		// creation.
		returnType := udfDesc.ReturnType.Type
		lazilyEvalSQL := returnType != nil && returnType.Identical(types.Trigger)
		if !lazilyEvalSQL {
			// Replace any sequence names in the function body with IDs.
			body, err = replaceSeqNamesWithIDsLang(params.ctx, params.p, body, true, lang)
			if err != nil {
				return err
			}
			// Replace any UDT names in the function body with IDs.
			body, err = serializeUserDefinedTypesLang(
				params.ctx, params.p.SemaCtx(), body, true /* multiStmt */, "UDFs", lang)
			if err != nil {
				return err
			}
		}
		udfDesc.SetFuncBody(body)
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
	ctx context.Context,
	semaCtx *tree.SemaContext,
	param tree.RoutineParam,
	typeResolver tree.TypeReferenceResolver,
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
		texpr, err := tree.TypeCheck(ctx, param.DefaultVal, semaCtx, pbParam.Type)
		if err != nil {
			return descpb.FunctionDescriptor_Parameter{}, err
		}
		s := tree.Serialize(texpr)
		pbParam.DefaultExpr = &s
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

// validateParameters checks that changes to the parameters, if any, are
// allowed. This method expects that the return type equality has already been
// checked.
func (n *createFunctionNode) validateParameters(udfDesc *funcdesc.Mutable) error {
	// Note that parameter ordering between different "namespaces" (i.e. IN vs
	// OUT) can change (i.e. going from (IN INT, OUT INT) to (OUT INT, IN INT)
	// is allowed), so we need to process each "namespace" separately.
	var origInParams, origOutParams []descpb.FunctionDescriptor_Parameter
	for _, p := range udfDesc.Params {
		class := funcdesc.ToTreeRoutineParamClass(p.Class)
		if tree.IsInParamClass(class) {
			origInParams = append(origInParams, p)
		}
		if tree.IsOutParamClass(class) {
			origOutParams = append(origOutParams, p)
		}
	}
	var newInParams, newOutParams tree.RoutineParams
	for _, p := range n.cf.Params {
		if p.IsInParam() {
			newInParams = append(newInParams, p)
		}
		if p.IsOutParam() {
			newOutParams = append(newOutParams, p)
		}
	}
	// We expect that the number of IN parameters didn't change (this would be
	// a bug in function resolution).
	if len(origInParams) != len(newInParams) {
		return errors.AssertionFailedf(
			"different number of IN parameters: old %d, new %d", len(origInParams), len(newInParams),
		)
	}
	// Verify that the names of IN parameters are not changed.
	for i := range origInParams {
		if origInParams[i].Name != string(newInParams[i].Name) && origInParams[i].Name != "" {
			// The only allowed change is if the original parameter name was
			// omitted.
			return pgerror.Newf(
				pgcode.InvalidFunctionDefinition, "cannot change name of input parameter %q", origInParams[i].Name,
			)
		}
	}
	// The number of OUT parameters can only differ if a single OUT parameter is
	// added or omitted (if we have multiple OUT parameters, then the return
	// type is a RECORD, so parameter names become contents of the return type,
	// which isn't allowed to change - this should have been caught via the
	// equality check of the return types).
	if len(origOutParams) > 1 || len(newOutParams) > 1 {
		// When we have at most one OUT parameter on each side, we don't need to
		// check anything. Consider each possible case:
		// - len(origOutParams) == 0 && len(newOutParams) == 0:
		//     Nothing to check / rename.
		// - len(origOutParams) == 0 && len(newOutParams) == 1:
		//     Introducing a single OUT parameter effectively gives the name to
		//     the RETURNS-based output.
		// - len(origOutParams) == 1 && len(newOutParams) == 0:
		//     Removing the single OUT parameter effectively gives
		//     function-based name to the output column.
		// - len(origOutParams) == 1 && len(newOutParams) == 1:
		//     This is a special case - renaming single OUT parameter is allowed
		//     without restrictions.
		//
		// With multiple OUT parameters on at least one side we expect that
		// there are no differences.
		mismatch := len(origOutParams) != len(newOutParams)
		if !mismatch {
			for i := range origOutParams {
				if origOutParams[i].Name != string(newOutParams[i].Name) {
					// There are a couple of allowed exceptions:
					// - originally the OUT name was omitted, or
					// - it becomes omitted now.
					// Note that the non-empty name must match the default name
					// ("column" || i) and it was already checked when verifying
					// the return type.
					if origOutParams[i].Name != "" && newOutParams[i].Name != "" {
						mismatch = true
						break
					}
				}
			}
		}
		if mismatch {
			return errors.AssertionFailedf(
				"different return types should've been caught earlier: old %v, new %v",
				origOutParams, newOutParams,
			)
		}
	}
	// Verify that no DEFAULT expressions have been removed (adding new ones is
	// ok). Note that other validity checks (like that the expressions are
	// coercible to the parameter types and that all "default" parameters form
	// contiguous parameter "suffix") have already been performed in the
	// optbuilder.
	for i := range origInParams {
		if origInParams[i].DefaultExpr != nil && newInParams[i].DefaultVal == nil {
			return pgerror.Newf(
				pgcode.InvalidFunctionDefinition, "cannot remove parameter defaults from existing function",
			)
		}
	}
	return nil
}
