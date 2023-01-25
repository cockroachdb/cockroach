// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
)

func CreateFunction(b BuildCtx, n *tree.CreateFunction) {
	if n.Replace {
		panic(scerrors.NotImplementedError(n))
	}

	dbElts, scElts := b.ResolvePrefix(n.FuncName.ObjectNamePrefix, privilege.CREATE)
	_, _, sc := scpb.FindSchema(scElts)
	_, _, db := scpb.FindDatabase(dbElts)

	validateParameters(b, n, db.DatabaseID)

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
			panic(unimplemented.New("CREATE FUNCTION argument", "default value"))
		}
		paramCls, err := funcdesc.ParamClassToProto(param.Class)
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
	b.Add(&scpb.ObjectParent{
		ObjectID:       fnID,
		ParentSchemaID: sc.SchemaID,
	})
	b.Add(&scpb.FunctionName{
		FunctionID: fnID,
		Name:       n.FuncName.Object(),
	})

	var lang catpb.Function_Language
	var fnBodyStr string
	for _, option := range n.Options {
		switch t := option.(type) {
		case tree.FunctionVolatility:
			v, err := funcdesc.VolatilityToProto(t)
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
			v, err := funcdesc.NullInputBehaviorToProto(t)
			if err != nil {
				panic(err)
			}
			b.Add(&scpb.FunctionNullInputBehavior{
				FunctionID:        fnID,
				NullInputBehavior: catpb.FunctionNullInputBehavior{NullInputBehavior: v},
			})
		case tree.FunctionLanguage:
			v, err := funcdesc.FunctionLangToProto(t)
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

func validateParameters(b BuildCtx, n *tree.CreateFunction, parentDBID descpb.ID) {
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
	if err := refProvider.ForEachTypeReference(func(typeID descpb.ID) error {
		maybeFailOnCrossDBTypeReference(b, typeID, parentDBID)
		return nil
	}); err != nil {
		panic(err)
	}
}

func validateFunctionRelationReferences(
	b BuildCtx, refProvider ReferenceProvider, parentDBID descpb.ID,
) {
	if err := refProvider.ForEachTableReference(func(tblID descpb.ID, refs []descpb.TableDescriptor_Reference) error {
		_, _, namespace := scpb.FindNamespace(b.QueryByID(tblID))
		if namespace.DatabaseID != parentDBID {
			name := tree.MakeTypeNameWithPrefix(b.NamePrefix(namespace), namespace.Name)
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"the function cannot refer to other databases",
				name.String()))
		}
		return nil
	}); err != nil {
		panic(err)
	}
}
