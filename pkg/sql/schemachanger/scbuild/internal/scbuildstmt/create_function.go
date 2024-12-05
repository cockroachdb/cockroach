// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func CreateFunction(b BuildCtx, n *tree.CreateRoutine) {
	if n.Replace {
		panic(scerrors.NotImplementedError(n))
	}
	b.IncrementSchemaChangeCreateCounter("function")

	dbElts, scElts := b.ResolveTargetObject(n.Name.ToUnresolvedObjectName(), privilege.CREATE)
	_, _, sc := scpb.FindSchema(scElts)
	_, _, db := scpb.FindDatabase(dbElts)
	_, _, scName := scpb.FindNamespace(scElts)
	_, _, dbname := scpb.FindNamespace(dbElts)

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
	b.LogEventForExistingTarget(&fn)
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
