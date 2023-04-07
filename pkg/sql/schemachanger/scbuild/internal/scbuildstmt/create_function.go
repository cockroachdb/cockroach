// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
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
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func CreateFunction(b BuildCtx, n *tree.CreateFunction) {
	if n.Replace {
		panic(scerrors.NotImplementedError(n))
	}

	dbElts, scElts := b.ResolvePrefix(n.FuncName.ObjectNamePrefix, privilege.CREATE)
	_, _, sc := scpb.FindSchema(scElts)
	_, _, db := scpb.FindDatabase(dbElts)
	_, _, scName := scpb.FindNamespace(scElts)
	_, _, dbname := scpb.FindNamespace(dbElts)

	n.FuncName.SchemaName = tree.Name(scName.Name)
	n.FuncName.CatalogName = tree.Name(dbname.Name)

	validateParameters(n)

	existingFn := b.ResolveUDF(
		&tree.FuncObj{
			FuncName: n.FuncName,
			Params:   n.Params,
		},
		ResolveParams{
			IsExistenceOptional: true,
			RequireOwnership:    true,
		},
	)
	if existingFn != nil {
		panic(pgerror.Newf(
			pgcode.DuplicateFunction,
			"function %q already exists with same argument types",
			n.FuncName.Object(),
		))
	}

	fnID := b.GenerateUniqueDescID()
	fn := scpb.Function{
		FunctionID: fnID,
		ReturnSet:  n.ReturnType.IsSet,
		ReturnType: b.ResolveTypeRef(n.ReturnType.Type),
	}
	fn.Params = make([]scpb.Function_Parameter, len(n.Params))
	for i, param := range n.Params {
		// TODO(chengxiong): create `FunctionParamDefaultExpression` element when
		// default parameter default expression is enabled.
		if param.DefaultVal != nil {
			panic(unimplemented.NewWithIssue(100962, "default value"))
		}
		paramCls, err := funcinfo.ParamClassToProto(param.Class)
		if err != nil {
			panic(err)
		}
		fn.Params[i] = scpb.Function_Parameter{
			Name:  string(param.Name),
			Class: catpb.FunctionParamClass{Class: paramCls},
			Type:  b.ResolveTypeRef(param.Type),
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
		Name:       n.FuncName.Object(),
	})

	validateFunctionLeakProof(n.Options, funcinfo.MakeDefaultVolatilityProperties())
	var lang catpb.Function_Language
	var fnBodyStr string
	for _, option := range n.Options {
		switch t := option.(type) {
		case tree.FunctionVolatility:
			v, err := funcinfo.VolatilityToProto(t)
			if err != nil {
				panic(err)
			}
			b.Add(&scpb.FunctionVolatility{
				FunctionID: fnID,
				Volatility: catpb.FunctionVolatility{Volatility: v},
			})
		case tree.FunctionLeakproof:
			b.Add(&scpb.FunctionLeakProof{
				FunctionID: fnID,
				LeakProof:  bool(t),
			})
		case tree.FunctionNullInputBehavior:
			v, err := funcinfo.NullInputBehaviorToProto(t)
			if err != nil {
				panic(err)
			}
			b.Add(&scpb.FunctionNullInputBehavior{
				FunctionID:        fnID,
				NullInputBehavior: catpb.FunctionNullInputBehavior{NullInputBehavior: v},
			})
		case tree.FunctionLanguage:
			v, err := funcinfo.FunctionLangToProto(t)
			if err != nil {
				panic(err)
			}
			lang = v
		case tree.FunctionBodyStr:
			fnBodyStr = string(t)
		}
	}
	owner, ups := b.BuildUserPrivilegesFromDefaultPrivileges(
		db,
		sc,
		fnID,
		privilege.Functions,
	)
	b.Add(owner)
	for _, up := range ups {
		b.Add(up)
	}
	refProvider := b.BuildReferenceProvider(n)
	validateTypeReferences(b, refProvider, db.DatabaseID)
	validateFunctionRelationReferences(b, refProvider, db.DatabaseID)
	b.Add(b.WrapFunctionBody(fnID, fnBodyStr, lang, refProvider))
}

func validateParameters(n *tree.CreateFunction) {
	seen := make(map[tree.Name]struct{})
	for _, param := range n.Params {
		if param.Name != "" {
			if _, ok := seen[param.Name]; ok {
				// Argument names cannot be used more than once.
				panic(pgerror.Newf(
					pgcode.InvalidFunctionDefinition, "parameter name %q used more than once", param.Name,
				))
			}
			seen[param.Name] = struct{}{}
		}
	}
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
			name := tree.MakeTypeNameWithPrefix(b.NamePrefix(namespace), namespace.Name)
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"the function cannot refer to other databases",
				name.String()))
		}
	}
}

func validateFunctionLeakProof(options tree.FunctionOptions, vp funcinfo.VolatilityProperties) {
	if err := vp.Apply(options); err != nil {
		panic(err)
	}
	if err := vp.Validate(); err != nil {
		panic(sqlerrors.NewInvalidVolatilityError(err))
	}
}
