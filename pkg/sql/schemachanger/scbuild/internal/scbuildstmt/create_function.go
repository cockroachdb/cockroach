// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func CreateFunction(b BuildCtx, n *tree.CreateRoutine) {
	b.IncrementSchemaChangeCreateCounter("function")

	var dbElts, scElts ElementResultSet
	if resolveTemporaryStatus(n.Name.ObjectNamePrefix, tree.PersistencePermanent) {
		dbElts, scElts = MaybeCreateOrResolveTemporarySchema(b)
	} else {
		dbElts, scElts = b.ResolveTargetObject(n.Name.ToUnresolvedObjectName(), privilege.CREATE)
	}
	_, _, sc := scpb.FindSchema(scElts)
	_, _, db := scpb.FindDatabase(dbElts)
	_, _, scName := scpb.FindNamespace(scElts)
	_, _, dbname := scpb.FindNamespace(dbElts)

	if sc.IsTemporary {
		panic(unimplemented.NewWithIssue(104687, "cannot create user-defined functions under a temporary schema"))
	}

	n.Name.SchemaName = tree.Name(scName.Name)
	n.Name.CatalogName = tree.Name(dbname.Name)

	existingFn := b.ResolveRoutine(
		&tree.RoutineObj{
			FuncName: n.Name,
			Params:   n.Params,
		},
		ResolveParams{
			IsExistenceOptional: true,
			RequireOwnership:    true,
		},
		tree.UDFRoutine|tree.ProcedureRoutine,
	)

	if existingFn != nil && n.Replace {
		if !b.EvalCtx().Settings.Version.ActiveVersion(b).IsActive(clusterversion.V26_2) {
			panic(scerrors.NotImplementedError(n))
		}
		replaceFunction(b, n, existingFn, db, sc)
		return
	}
	if existingFn != nil {
		panic(pgerror.Newf(
			pgcode.DuplicateFunction,
			"function %q already exists with same argument types",
			n.Name.Object(),
		))
	}

	typ := tree.ResolvableTypeReference(types.Void)
	setof := false
	if n.IsProcedure {
		if n.ReturnType != nil {
			returnType := b.ResolveTypeRef(n.ReturnType.Type)
			if returnType.Type.Family() != types.VoidFamily && returnType.Type.Oid() != oid.T_record {
				panic(errors.AssertionFailedf(
					"CreateRoutine.ReturnType is expected to be empty, VOID, or RECORD for procedures",
				))
			}
		}
		// For procedures, if specified, output parameters form the return type.
		outParamTypes, outParamNames := getOutputParameters(b, n.Params)
		if len(outParamTypes) > 0 {
			typ = types.MakeLabeledTuple(outParamTypes, outParamNames)
		}
	} else if n.ReturnType != nil {
		typ = n.ReturnType.Type
		if returnType := b.ResolveTypeRef(typ); returnType.Type.Oid() == oid.T_record {
			// If the function returns a RECORD type, then we need to check
			// whether its OUT parameters specify labels for the return type.
			outParamTypes, outParamNames := getOutputParameters(b, n.Params)
			if len(outParamTypes) == 1 {
				panic(errors.AssertionFailedf(
					"we shouldn't get the RECORD return type with a single OUT parameter, expected %s",
					outParamTypes[0].SQLStringForError(),
				))
			} else if len(outParamTypes) > 1 {
				typ = types.MakeLabeledTuple(outParamTypes, outParamNames)
			}
		}
		setof = n.ReturnType.SetOf
	}
	fnID := b.GenerateUniqueDescID()
	fn := scpb.Function{
		FunctionID:  fnID,
		ReturnSet:   setof,
		ReturnType:  b.ResolveTypeRef(typ),
		IsProcedure: n.IsProcedure,
	}
	fn.Params = make([]scpb.Function_Parameter, len(n.Params))
	for i, param := range n.Params {
		paramCls, err := funcinfo.ParamClassToProto(param.Class)
		if err != nil {
			panic(err)
		}
		fn.Params[i] = scpb.Function_Parameter{
			Name:  string(param.Name),
			Class: catpb.FunctionParamClass{Class: paramCls},
			Type:  b.ResolveTypeRef(param.Type),
		}
		if param.DefaultVal != nil {
			// Type-check the expression so that we get the right type
			// annotation when serializing it below.
			texpr, err := tree.TypeCheck(b, param.DefaultVal, b.SemaCtx(), fn.Params[i].Type.Type)
			if err != nil {
				panic(err)
			}
			fn.Params[i].DefaultExpr = tree.Serialize(texpr)
			n.Params[i].DefaultVal = texpr
		}
	}

	// Add function element.
	b.Add(&fn)
	b.Add(&scpb.SchemaChild{
		ChildObjectID: fnID,
		SchemaID:      sc.SchemaID,
	})
	b.Add(&scpb.FunctionName{
		FunctionID: fnID,
		Name:       n.Name.Object(),
	})

	validateFunctionLeakProof(n.Options, funcinfo.MakeDefaultVolatilityProperties())
	var lang catpb.Function_Language
	var fnBodyStr string
	for _, option := range n.Options {
		switch t := option.(type) {
		case tree.RoutineVolatility:
			v, err := funcinfo.VolatilityToProto(t)
			if err != nil {
				panic(err)
			}
			b.Add(&scpb.FunctionVolatility{
				FunctionID: fnID,
				Volatility: catpb.FunctionVolatility{Volatility: v},
			})
		case tree.RoutineLeakproof:
			b.Add(&scpb.FunctionLeakProof{
				FunctionID: fnID,
				LeakProof:  bool(t),
			})
		case tree.RoutineNullInputBehavior:
			v, err := funcinfo.NullInputBehaviorToProto(t)
			if err != nil {
				panic(err)
			}
			b.Add(&scpb.FunctionNullInputBehavior{
				FunctionID:        fnID,
				NullInputBehavior: catpb.FunctionNullInputBehavior{NullInputBehavior: v},
			})
		case tree.RoutineLanguage:
			v, err := funcinfo.FunctionLangToProto(t)
			if err != nil {
				panic(err)
			}
			lang = v
		case tree.RoutineBodyStr:
			fnBodyStr = string(t)
		case tree.RoutineSecurity:
			s, err := funcinfo.SecurityToProto(t)
			if err != nil {
				panic(err)
			}
			b.Add(&scpb.FunctionSecurity{
				FunctionID: fnID,
				Security:   catpb.FunctionSecurity{Security: s},
			})
		}
	}
	owner, ups := b.BuildUserPrivilegesFromDefaultPrivileges(
		db,
		sc,
		fnID,
		privilege.Routines,
		b.CurrentUser(),
	)
	b.Add(owner)
	for _, up := range ups {
		b.Add(up)
	}
	refProvider := b.BuildReferenceProvider(n)
	validateTypeReferences(b, refProvider, db.DatabaseID)
	validateFunctionRelationReferences(b, refProvider, db.DatabaseID)
	validateFunctionToFunctionReferences(b, refProvider, db.DatabaseID)
	b.Add(b.WrapFunctionBody(fnID, fnBodyStr, lang, typ, refProvider))
	if b.EvalCtx().Settings.Version.ActiveVersion(b).IsActive(clusterversion.V26_2) {
		b.Add(&scpb.FunctionParams{
			FunctionID: fnID,
			Params:     fn.Params,
		})
	}
	b.LogEventForExistingTarget(&fn)
}

// replaceFunction implements the CREATE OR REPLACE path. It reuses the existing
// function descriptor (same ID) and replaces the mutable sub-elements
// (FunctionBody, FunctionVolatility, FunctionLeakProof, FunctionNullInputBehavior,
// FunctionSecurity) while leaving Function, SchemaChild, FunctionName, Owner,
// and UserPrivileges untouched.
func replaceFunction(
	b BuildCtx,
	n *tree.CreateRoutine,
	existingFnElts ElementResultSet,
	db *scpb.Database,
	sc *scpb.Schema,
) {
	_, _, existingFnElem := scpb.FindFunction(existingFnElts)
	fnID := existingFnElem.FunctionID

	// Validate compatibility: routine kind must match.
	if n.IsProcedure != existingFnElem.IsProcedure {
		formatStr := "%q is a function"
		if existingFnElem.IsProcedure {
			formatStr = "%q is a procedure"
		}
		panic(errors.WithDetailf(
			pgerror.Newf(pgcode.WrongObjectType, "cannot change routine kind"),
			formatStr,
			n.Name.Object(),
		))
	}

	// Validate compatibility: return type must match.
	// Derive the return type from OUT parameters when needed, matching the
	// logic in the CREATE path.
	var newReturnType scpb.TypeT
	setof := false
	if n.IsProcedure {
		typ := tree.ResolvableTypeReference(types.Void)
		outParamTypes, outParamNames := getOutputParameters(b, n.Params)
		if len(outParamTypes) > 0 {
			typ = types.MakeLabeledTuple(outParamTypes, outParamNames)
		}
		newReturnType = b.ResolveTypeRef(typ)
	} else if n.ReturnType != nil {
		typ := n.ReturnType.Type
		if returnType := b.ResolveTypeRef(typ); returnType.Type.Oid() == oid.T_record {
			outParamTypes, outParamNames := getOutputParameters(b, n.Params)
			if len(outParamTypes) > 1 {
				typ = types.MakeLabeledTuple(outParamTypes, outParamNames)
			}
		}
		newReturnType = b.ResolveTypeRef(typ)
		setof = n.ReturnType.SetOf
	} else {
		newReturnType = b.ResolveTypeRef(types.Void)
	}
	isSameUDT := types.IsOIDUserDefinedType(newReturnType.Type.Oid()) &&
		newReturnType.Type.Oid() == existingFnElem.ReturnType.Type.Oid()
	if setof != existingFnElem.ReturnSet ||
		(!newReturnType.Type.Equal(existingFnElem.ReturnType.Type) && !isSameUDT) {
		if existingFnElem.IsProcedure &&
			(newReturnType.Type.Family() == types.VoidFamily ||
				existingFnElem.ReturnType.Type.Family() == types.VoidFamily) {
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition,
				"cannot change whether a procedure has output parameters",
			))
		}
		panic(pgerror.Newf(
			pgcode.InvalidFunctionDefinition,
			"cannot change return type of existing function",
		))
	}

	// Build new params.
	newParams := make([]scpb.Function_Parameter, len(n.Params))
	for i, param := range n.Params {
		paramCls, err := funcinfo.ParamClassToProto(param.Class)
		if err != nil {
			panic(err)
		}
		newParams[i] = scpb.Function_Parameter{
			Name:  string(param.Name),
			Class: catpb.FunctionParamClass{Class: paramCls},
			Type:  b.ResolveTypeRef(param.Type),
		}
		if param.DefaultVal != nil {
			texpr, err := tree.TypeCheck(b, param.DefaultVal, b.SemaCtx(), newParams[i].Type.Type)
			if err != nil {
				panic(err)
			}
			newParams[i].DefaultExpr = tree.Serialize(texpr)
			n.Params[i].DefaultVal = texpr
		}
	}

	// Validate parameter changes.
	validateReplaceParams(existingFnElem.Params, newParams, n.Params)

	// Determine the return type for FunctionBody construction.
	typ := tree.ResolvableTypeReference(types.Void)
	if n.IsProcedure {
		outParamTypes, outParamNames := getOutputParameters(b, n.Params)
		if len(outParamTypes) > 0 {
			typ = types.MakeLabeledTuple(outParamTypes, outParamNames)
		}
	} else if n.ReturnType != nil {
		typ = n.ReturnType.Type
		if returnType := b.ResolveTypeRef(typ); returnType.Type.Oid() == oid.T_record {
			outParamTypes, outParamNames := getOutputParameters(b, n.Params)
			if len(outParamTypes) == 1 {
				panic(errors.AssertionFailedf(
					"we shouldn't get the RECORD return type with a single OUT parameter, expected %s",
					outParamTypes[0].SQLStringForError(),
				))
			} else if len(outParamTypes) > 1 {
				typ = types.MakeLabeledTuple(outParamTypes, outParamNames)
			}
		}
	}

	// Replace all mutable sub-elements. Options not explicitly specified in the
	// CREATE OR REPLACE statement must be reset to their defaults (matching
	// legacy behavior). This mirrors the legacy resetFuncOption + setFuncOptions
	// pattern.
	validateFunctionLeakProof(n.Options, funcinfo.MakeDefaultVolatilityProperties())

	// Start with defaults for all options.
	var lang catpb.Function_Language
	var fnBodyStr string
	volatility := catpb.Function_VOLATILE
	leakProof := false
	nullInputBehavior := catpb.Function_CALLED_ON_NULL_INPUT
	security := catpb.Function_INVOKER

	for _, option := range n.Options {
		switch t := option.(type) {
		case tree.RoutineVolatility:
			v, err := funcinfo.VolatilityToProto(t)
			if err != nil {
				panic(err)
			}
			volatility = v
		case tree.RoutineLeakproof:
			leakProof = bool(t)
		case tree.RoutineNullInputBehavior:
			v, err := funcinfo.NullInputBehaviorToProto(t)
			if err != nil {
				panic(err)
			}
			nullInputBehavior = v
		case tree.RoutineLanguage:
			v, err := funcinfo.FunctionLangToProto(t)
			if err != nil {
				panic(err)
			}
			lang = v
		case tree.RoutineBodyStr:
			fnBodyStr = string(t)
		case tree.RoutineSecurity:
			s, err := funcinfo.SecurityToProto(t)
			if err != nil {
				panic(err)
			}
			security = s
		}
	}

	// Always replace all option sub-elements, even if using defaults.
	b.Replace(&scpb.FunctionVolatility{
		FunctionID: fnID,
		Volatility: catpb.FunctionVolatility{Volatility: volatility},
	})
	b.Replace(&scpb.FunctionLeakProof{
		FunctionID: fnID,
		LeakProof:  leakProof,
	})
	b.Replace(&scpb.FunctionNullInputBehavior{
		FunctionID:        fnID,
		NullInputBehavior: catpb.FunctionNullInputBehavior{NullInputBehavior: nullInputBehavior},
	})
	b.Replace(&scpb.FunctionSecurity{
		FunctionID: fnID,
		Security:   catpb.FunctionSecurity{Security: security},
	})

	refProvider := b.BuildReferenceProvider(n)
	validateTypeReferences(b, refProvider, db.DatabaseID)
	validateFunctionRelationReferences(b, refProvider, db.DatabaseID)
	validateFunctionToFunctionReferences(b, refProvider, db.DatabaseID)

	// Build the FunctionBody element with the new body and references.
	fnBody := b.WrapFunctionBody(fnID, fnBodyStr, lang, typ, refProvider)
	b.Replace(fnBody)

	// Replace the FunctionParams element with the updated params.
	funcParams := &scpb.FunctionParams{
		FunctionID: fnID,
		Params:     newParams,
	}
	// When the return type is a UDT, its internal representation may have
	// changed (e.g., table type after ALTER TABLE ADD COLUMN). Carry the
	// refreshed type so the execution layer updates the descriptor.
	if isSameUDT {
		funcParams.ReturnType = &newReturnType
	}
	b.Replace(funcParams)

	// If this is a trigger function, propagate the new body to dependent
	// triggers so their inlined copies stay in sync.
	if newReturnType.Type.Identical(types.Trigger) {
		updateDependentTriggers(b, fnID, fnBodyStr)
	}

	b.LogEventForExistingTarget(existingFnElem)
}

// updateDependentTriggers finds all triggers that reference the given function
// and updates their inlined function body and dependency tracking to reflect the
// new function body.
func updateDependentTriggers(b BuildCtx, fnID descpb.ID, fnBodyStr string) {
	backRefs := b.BackReferences(fnID)
	backRefs.FilterTriggerFunctionCall().ForEach(
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.TriggerFunctionCall) {
			if target != scpb.ToPublic || e.FuncID != fnID {
				return
			}
			tableID := e.TableID
			triggerID := e.TriggerID

			// Build a reference provider by constructing a synthetic CREATE TRIGGER
			// statement with the new function body. This runs the optbuilder, which
			// analyzes the function body in the trigger's table context to capture
			// all dependencies (types, tables, routines referenced in SQL statements).
			syntheticCT := buildSyntheticCreateTrigger(b, tableID, fnID, fnBodyStr, e.FuncArgs)
			triggerRefProvider := b.BuildReferenceProvider(syntheticCT)

			// Replace TriggerFunctionCall with the qualified function body produced
			// by the optbuilder.
			b.Replace(&scpb.TriggerFunctionCall{
				TableID:   tableID,
				TriggerID: triggerID,
				FuncID:    fnID,
				FuncBody:  syntheticCT.FuncBody,
				FuncArgs:  e.FuncArgs,
			})

			// Replace TriggerDeps with new dependencies from the new function body.
			routineIDs := triggerRefProvider.ReferencedRoutines()
			routineIDs.Add(fnID)
			b.Replace(&scpb.TriggerDeps{
				TableID:        tableID,
				TriggerID:      triggerID,
				UsesRelations:  buildRelationDeps(tableID, triggerRefProvider),
				UsesTypeIDs:    triggerRefProvider.ReferencedTypes().Ordered(),
				UsesRoutineIDs: routineIDs.Ordered(),
			})
		},
	)
}

// buildSyntheticCreateTrigger constructs a minimal CREATE TRIGGER AST for use
// with BuildReferenceProvider. The FuncBodyOverride field is set so that the
// optbuilder analyzes the new function body instead of the old one from the
// catalog. Trigger metadata fields (ActionTime, Events, ForEach) use
// placeholder values since validation is skipped when FuncBodyOverride is set.
func buildSyntheticCreateTrigger(
	b BuildCtx, tableID descpb.ID, fnID descpb.ID, fnBodyStr string, funcArgs []string,
) *tree.CreateTrigger {
	// Look up table name parts.
	tableElts := b.QueryByID(tableID)
	ns := tableElts.FilterNamespace().MustGetOneElement()
	scNs := b.QueryByID(ns.SchemaID).FilterNamespace().MustGetOneElement()
	dbNs := b.QueryByID(ns.DatabaseID).FilterNamespace().MustGetOneElement()
	tableName, err := tree.NewUnresolvedObjectName(3,
		[3]string{ns.Name, scNs.Name, dbNs.Name}, tree.NoAnnotation)
	if err != nil {
		panic(err)
	}

	// Look up function name parts.
	fnElts := b.QueryByID(fnID)
	fnName := fnElts.FilterFunctionName().MustGetOneElement()
	fnParent := fnElts.FilterSchemaChild().MustGetOneElement()
	fnScNs := b.QueryByID(fnParent.SchemaID).FilterNamespace().MustGetOneElement()
	fnDbNs := b.QueryByID(fnScNs.DatabaseID).FilterNamespace().MustGetOneElement()
	funcName := tree.MakeUnresolvedName(fnDbNs.Name, fnScNs.Name, fnName.Name)

	return &tree.CreateTrigger{
		Replace:          true,
		Name:             "synthetic_trigger",
		ActionTime:       tree.TriggerActionTimeBefore,
		Events:           []*tree.TriggerEvent{{EventType: tree.TriggerEventInsert}},
		TableName:        tableName,
		ForEach:          tree.TriggerForEachRow,
		FuncName:         &funcName,
		FuncArgs:         funcArgs,
		FuncBodyOverride: fnBodyStr,
	}
}

// validateReplaceParams validates that parameter changes in CREATE OR REPLACE
// are allowed, mirroring the legacy validateParameters logic.
func validateReplaceParams(
	origParams []scpb.Function_Parameter,
	newParams []scpb.Function_Parameter,
	astParams tree.RoutineParams,
) {
	// Separate IN and OUT params from original.
	var origInParams []scpb.Function_Parameter
	var origOutParams []scpb.Function_Parameter
	for _, p := range origParams {
		class := funcdesc.ToTreeRoutineParamClass(p.Class.Class)
		if tree.IsInParamClass(class) {
			origInParams = append(origInParams, p)
		}
		if tree.IsOutParamClass(class) {
			origOutParams = append(origOutParams, p)
		}
	}

	// Separate IN and OUT params from new.
	var newInParams []scpb.Function_Parameter
	var newOutParams []scpb.Function_Parameter
	for _, p := range newParams {
		class := funcdesc.ToTreeRoutineParamClass(p.Class.Class)
		if tree.IsInParamClass(class) {
			newInParams = append(newInParams, p)
		}
		if tree.IsOutParamClass(class) {
			newOutParams = append(newOutParams, p)
		}
	}

	// IN parameter count must match (enforced by ResolveRoutine).
	if len(origInParams) != len(newInParams) {
		panic(errors.AssertionFailedf(
			"different number of IN parameters: old %d, new %d",
			len(origInParams), len(newInParams),
		))
	}

	// Verify IN parameter names are not changed (unless originally empty).
	for i := range origInParams {
		if origInParams[i].Name != newInParams[i].Name && origInParams[i].Name != "" {
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition,
				"cannot change name of input parameter %q",
				origInParams[i].Name,
			))
		}
	}

	// OUT parameter validation: if we have multiple on either side,
	// they must match exactly.
	if len(origOutParams) > 1 || len(newOutParams) > 1 {
		mismatch := len(origOutParams) != len(newOutParams)
		if !mismatch {
			for i := range origOutParams {
				if origOutParams[i].Name != newOutParams[i].Name {
					if origOutParams[i].Name != "" && newOutParams[i].Name != "" {
						mismatch = true
						break
					}
				}
			}
		}
		if mismatch {
			panic(errors.AssertionFailedf(
				"different return types should've been caught earlier: old %v, new %v",
				origOutParams, newOutParams,
			))
		}
	}

	// Verify no DEFAULT expressions have been removed.
	// Collect IN params from the AST to check their DefaultVal.
	var newInAstParams tree.RoutineParams
	for _, p := range astParams {
		if p.IsInParam() {
			newInAstParams = append(newInAstParams, p)
		}
	}
	for i := range origInParams {
		if origInParams[i].DefaultExpr != "" && newInAstParams[i].DefaultVal == nil {
			panic(pgerror.Newf(
				pgcode.InvalidFunctionDefinition,
				"cannot remove parameter defaults from existing function",
			))
		}
	}
}

func getOutputParameters(
	b BuildCtx, params tree.RoutineParams,
) (outParamTypes []*types.T, outParamNames []string) {
	// Note that this logic effectively copies what the optimizer does in
	// optbuilder.Builder.buildCreateFunction.
	for _, param := range params {
		if param.IsOutParam() {
			paramType := b.ResolveTypeRef(param.Type)
			outParamTypes = append(outParamTypes, paramType.Type)
			paramName := string(param.Name)
			if paramName == "" {
				paramName = fmt.Sprintf("column%d", len(outParamTypes))
			}
			outParamNames = append(outParamNames, paramName)
		}
	}
	return outParamTypes, outParamNames
}

func validateTypeReferences(b BuildCtx, refProvider ReferenceProvider, parentDBID descpb.ID) {
	for _, id := range refProvider.ReferencedTypes().Ordered() {
		maybeFailOnCrossDBTypeReference(b, id, parentDBID)
	}
}

func validateFunctionRelationReferences(
	b BuildCtx, refProvider ReferenceProvider, parentDBID descpb.ID,
) {
	for _, id := range refProvider.ReferencedRelationIDs().Ordered() {
		_, _, namespace := scpb.FindNamespace(b.QueryByID(id))
		if namespace == nil {
			// Relations should have Namespace elements. If not found, this could
			// indicate an internal error or an unexpected descriptor type.
			panic(errors.AssertionFailedf(
				"cannot find Namespace element for referenced relation ID %d", id))
		}
		if namespace.DatabaseID != parentDBID {
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"dependent relation %s cannot be from another database",
				namespace.Name))
		}
	}
}

// validateFunctionToFunctionReferences validates no function references are
// cross database.
func validateFunctionToFunctionReferences(
	b BuildCtx, refProvider ReferenceProvider, parentDBID descpb.ID,
) {
	err := refProvider.ForEachFunctionReference(func(id descpb.ID) error {
		funcElts := b.QueryByID(id)
		funcName := funcElts.FilterFunctionName().MustGetOneElement()
		schemaParent := funcElts.FilterSchemaChild().MustGetOneElement()
		schemaNamespace := b.QueryByID(schemaParent.SchemaID).FilterNamespace().MustGetOneElement()
		if schemaNamespace.DatabaseID != parentDBID {
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"dependent function %s cannot be from another database",
				funcName.Name)
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
}

func validateFunctionLeakProof(options tree.RoutineOptions, vp funcinfo.VolatilityProperties) {
	if err := vp.Apply(options); err != nil {
		panic(err)
	}
	if err := vp.Validate(); err != nil {
		panic(sqlerrors.NewInvalidVolatilityError(err))
	}
}
