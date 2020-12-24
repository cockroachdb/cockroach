// Copyright 2020 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type createFuncNode struct {
	n        *tree.CreateFunction
	dbDesc   catalog.DatabaseDescriptor
	funcName *tree.FuncName
	expr     tree.Expr
}

// Use to satisfy the linter.
var _ planNode = &createFuncNode{}

func (p *planner) CreateFunc(ctx context.Context, n *tree.CreateFunction) (planNode, error) {
	// TODO(jordan): remove this in the 21.2 release.
	if !p.ExecCfg().Settings.Version.IsActive(ctx, clusterversion.ScalarUDFs) {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported,
			"not all nodes are the correct version for FUNCTION creation")
	}
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"CREATE FUNCTION",
	); err != nil {
		return nil, err
	}

	if n.OrReplace {
		return nil, errors.New("unsupported OR REPLACE")
	}

	lang := strings.ToUpper(n.Language)
	if lang != "SQL" {
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "unsupported language %s", lang)
	}

	// Parse the function definition to see if it's even sensical.
	funcDef := n.FuncDef.RawString()
	// TODO(jordan): make sure that there are no unbound variables in this expr.
	funcExpr, err := funcdesc.ParseUserDefinedFuncDef(funcDef)
	if err != nil {
		return nil, err
	}

	// Resolve the desired new type name.
	funcName, db, err := resolveNewFuncName(p.RunParams(ctx), n.Name)
	if err != nil {
		return nil, err
	}

	return &createFuncNode{
		n:        n,
		expr:     funcExpr,
		funcName: funcName,
		dbDesc:   db,
	}, nil
}

func resolveNewFuncName(
	params runParams, name *tree.UnresolvedObjectName,
) (*tree.FuncName, catalog.DatabaseDescriptor, error) {
	// Resolve the target schema and database.
	db, _, prefix, err := params.p.ResolveTargetObject(params.ctx, name)
	if err != nil {
		return nil, nil, err
	}

	if err := params.p.CheckPrivilege(params.ctx, db, privilege.CREATE); err != nil {
		return nil, nil, err
	}

	// Disallow function creation in the system database.
	if db.GetID() == keys.SystemDatabaseID {
		return nil, nil, errors.New("cannot create a function in the system database")
	}

	funcname := tree.NewUnqualifiedFuncName(tree.Name(name.Object()))
	funcname.ObjectNamePrefix = prefix

	return funcname, db, nil
}

func (c *createFuncNode) startExec(params runParams) error {
	return params.p.createUserDefinedFunc(params, c.dbDesc, c.funcName, c.n, c.expr)
}

func (p *planner) createUserDefinedFunc(
	params runParams,
	db catalog.DatabaseDescriptor,
	funcName *tree.FuncName,
	n *tree.CreateFunction,
	expr tree.Expr,
) error {
	paramNames := make([]string, len(n.Params))
	paramTypes := make([]*types.T, len(n.Params))
	version := params.ExecCfg().Settings.Version.ActiveVersionOrEmpty(params.ctx)
	for i := range n.Params {
		param := &n.Params[i]
		toType, err := tree.ResolveType(params.ctx, param.Type, params.p.semaCtx.GetTypeResolver())
		if err != nil {
			return err
		}
		if supported, err := isTypeSupportedInVersion(version, toType); err != nil {
			return err
		} else if !supported {
			return pgerror.Newf(
				pgcode.FeatureNotSupported,
				"type %s is not supported until version upgrade is finalized",
				toType.SQLString(),
			)
		}
		paramNames[i] = param.Name
		paramTypes[i] = toType
	}
	retType, err := tree.ResolveType(params.ctx, n.ReturnType, params.p.semaCtx.GetTypeResolver())
	if err != nil {
		return err
	}
	if supported, err := isTypeSupportedInVersion(version, retType); err != nil {
		return err
	} else if !supported {
		return pgerror.Newf(
			pgcode.FeatureNotSupported,
			"type %s is not supported until version upgrade is finalized",
			retType.SQLString(),
		)
	}

	// Check we are not creating a function which conflicts with an alias available
	// as a built-in function in CockroachDB but an extension type on the public
	// schema for PostgreSQL.
	if funcName.Schema() == tree.PublicSchema {
		if _, ok := tree.FunDefs[funcName.Object()]; ok {
			return sqlerrors.NewFuncAlreadyExistsError(funcName.String())
		}
	}
	// Get the ID of the schema the type is being created in.
	dbID := db.GetID()
	schemaID, err := p.getSchemaIDForCreate(params.ctx, params.ExecCfg().Codec, dbID, funcName.Schema())
	if err != nil {
		return err
	}

	// Check permissions on the schema.
	if err := params.p.canCreateOnSchema(
		params.ctx, schemaID, dbID, params.p.User(), skipCheckPublicSchema); err != nil {
		return err
	}

	if schemaID != keys.PublicSchemaID {
		sqltelemetry.IncrementUserDefinedSchemaCounter(sqltelemetry.UserDefinedSchemaUsedByObject)
	}

	mangled := funcName.MangledName()
	funcKey := catalogkv.MakeObjectNameKey(params.ctx, params.ExecCfg().Settings, db.GetID(), schemaID, mangled)
	exists, collided, err := catalogkv.LookupObjectID(
		params.ctx, params.p.txn, params.ExecCfg().Codec, db.GetID(), schemaID, mangled)
	if err == nil && exists {
		// Try and see what kind of object we collided with.
		desc, err := catalogkv.GetAnyDescriptorByID(params.ctx, params.p.txn, params.ExecCfg().Codec, collided, catalogkv.Immutable)
		if err != nil {
			return sqlerrors.WrapErrorWhileConstructingObjectAlreadyExistsErr(err)
		}
		return sqlerrors.MakeObjectAlreadyExistsError(desc.DescriptorProto(), funcName.String())
	}
	if err != nil {
		return err
	}

	id, err := catalogkv.GenerateUniqueDescID(params.ctx, params.p.execCfg.DB, params.p.execCfg.Codec)
	if err != nil {
		return err
	}

	// Currently, we don't do anything with function privileges. In Postgres, by
	// default, functions are EXECUTE (aka usable) by PUBLIC, aka all roles.
	// Given that we have neither a PUBLIC role or EXECUTE privilege, for now
	// we'll just grant ALL to the function.
	// Also, Postgres only has EXECUTE as a privilege for functions.
	privs := descpb.NewDefaultPrivilegeDescriptor(params.p.User())

	newDesc := funcdesc.NewCreatedMutable(descpb.FuncDescriptor{
		ID:   id,
		Name: mangled,
		Overloads: []descpb.FuncDescriptor_Overload{
			{
				Def:        tree.AsStringWithFlags(expr, tree.FmtParsable),
				ParamNames: paramNames,
				ParamTypes: paramTypes,
				ReturnType: *retType,
			},
		},
		ParentSchemaID: schemaID,
		ParentID:       dbID,
		Privileges:     privs,
		Version:        1,
	})

	// Write descriptor to store.
	if err := p.createDescriptorWithID(
		params.ctx,
		funcKey.Key(params.ExecCfg().Codec),
		id,
		newDesc,
		params.EvalContext().Settings,
		"CREATE FUNCTION "+funcName.String(),
	); err != nil {
		return err
	}

	// Log the event.
	return p.logEvent(params.ctx,
		newDesc.GetID(),
		&eventpb.CreateFunc{
			FuncName: funcName.String(),
		})
}

func (c *createFuncNode) Next(_ runParams) (bool, error) { return false, nil }
func (c *createFuncNode) Values() tree.Datums            { return nil }
func (c *createFuncNode) Close(_ context.Context)        {}
func (n *createFuncNode) ReadingOwnWrites()              {}
