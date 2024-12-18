// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type alterFunctionOptionsNode struct {
	zeroInputPlanNode
	n *tree.AlterFunctionOptions
}

type alterFunctionRenameNode struct {
	zeroInputPlanNode
	n *tree.AlterRoutineRename
}

type alterFunctionSetOwnerNode struct {
	zeroInputPlanNode
	n *tree.AlterRoutineSetOwner
}

type alterFunctionSetSchemaNode struct {
	zeroInputPlanNode
	n *tree.AlterRoutineSetSchema
}

type alterFunctionDepExtensionNode struct {
	zeroInputPlanNode
	n *tree.AlterFunctionDepExtension
}

// AlterFunctionOptions alters a function's options.
func (p *planner) AlterFunctionOptions(
	ctx context.Context, n *tree.AlterFunctionOptions,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER FUNCTION",
	); err != nil {
		return nil, err
	}

	return &alterFunctionOptionsNode{n: n}, nil
}

func (n *alterFunctionOptionsNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("function"))

	fnDesc, err := params.p.mustGetMutableFunctionForAlter(params.ctx, &n.n.Function)
	if err != nil {
		return err
	}
	// TODO(chengxiong): add validation that a function can not be altered if it's
	// referenced by other objects. This is needed when want to allow function
	// references. Need to think about in what condition a function can be altered
	// or not.
	if err := tree.ValidateRoutineOptions(n.n.Options, fnDesc.IsProcedure()); err != nil {
		return err
	}

	if err := validateVolatilityInOptions(n.n.Options, fnDesc); err != nil {
		return err
	}

	for _, option := range n.n.Options {
		// Note that language and function body cannot be altered, and it's blocked
		// from parser level with "common_func_opt_item" syntax.
		if err := maybeValidateNewFuncVolatility(params, fnDesc, option); err != nil {
			return err
		}
	}

	if err := setFuncOptions(params, fnDesc, n.n.Options); err != nil {
		return err
	}

	if err := params.p.writeFuncSchemaChange(params.ctx, fnDesc); err != nil {
		return err
	}

	fnName, err := params.p.getQualifiedFunctionName(params.ctx, fnDesc)
	if err != nil {
		return err
	}
	event := eventpb.AlterFunctionOptions{
		FunctionName: fnName.FQString(),
	}
	return params.p.logEvent(params.ctx, fnDesc.GetID(), &event)
}

func maybeValidateNewFuncVolatility(
	params runParams, fnDesc catalog.FunctionDescriptor, option tree.RoutineOption,
) error {
	switch t := option.(type) {
	case tree.RoutineVolatility:
		f := NewReferenceProviderFactory(params.p)
		ast, err := fnDesc.ToCreateExpr()
		if err != nil {
			return err
		}
		for i, o := range ast.Options {
			if _, ok := o.(tree.RoutineVolatility); ok {
				ast.Options[i] = t
			}
		}
		if _, err := f.NewReferenceProvider(params.ctx, ast); err != nil {
			return err
		}
	}

	return nil
}

func (n *alterFunctionOptionsNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionOptionsNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionOptionsNode) Close(ctx context.Context)           {}

// AlterFunctionRename renames a function.
func (p *planner) AlterFunctionRename(
	ctx context.Context, n *tree.AlterRoutineRename,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER FUNCTION",
	); err != nil {
		return nil, err
	}

	return &alterFunctionRenameNode{n: n}, nil
}

func (n *alterFunctionRenameNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("function"))
	// TODO(chengxiong): add validation that a function can not be altered if it's
	// referenced by other objects. This is needed when want to allow function
	// references.
	fnDesc, err := params.p.mustGetMutableFunctionForAlter(params.ctx, &n.n.Function)
	if err != nil {
		return err
	}
	if !n.n.Procedure && fnDesc.IsProcedure() {
		return pgerror.Newf(
			pgcode.UndefinedFunction, "could not find a function named %q", &n.n.Function.FuncName,
		)
	}
	if n.n.Procedure && !fnDesc.IsProcedure() {
		return pgerror.Newf(
			pgcode.UndefinedFunction, "could not find a procedure named %q", &n.n.Function.FuncName,
		)
	}
	oldFnName, err := params.p.getQualifiedFunctionName(params.ctx, fnDesc)
	if err != nil {
		return err
	}

	scDesc, err := params.p.Descriptors().MutableByID(params.p.txn).Schema(params.ctx, fnDesc.GetParentSchemaID())
	if err != nil {
		return err
	}

	maybeExistingFuncObj, err := fnDesc.ToRoutineObj()
	if err != nil {
		return err
	}
	maybeExistingFuncObj.FuncName.ObjectName = n.n.NewName
	existing, err := params.p.matchRoutine(
		params.ctx, maybeExistingFuncObj, false, /* required */
		tree.UDFRoutine|tree.ProcedureRoutine, false, /* inDropContext */
	)
	if err != nil {
		return err
	}

	if existing != nil {
		if existing.Type == tree.ProcedureRoutine {
			return pgerror.Newf(
				pgcode.DuplicateFunction, "procedure %s already exists in schema %q",
				tree.AsString(maybeExistingFuncObj), scDesc.GetName(),
			)
		} else {
			return pgerror.Newf(
				pgcode.DuplicateFunction, "function %s already exists in schema %q",
				tree.AsString(maybeExistingFuncObj), scDesc.GetName(),
			)
		}
	}

	// Disallow renaming if this rename operation will break other UDF's invoking
	// this one.
	var dependentFuncs []string
	for _, dep := range fnDesc.GetDependedOnBy() {
		desc, err := params.p.Descriptors().ByIDWithoutLeased(params.p.Txn()).Get().Desc(params.ctx, dep.ID)
		if err != nil {
			return err
		}
		_, ok := desc.(catalog.FunctionDescriptor)
		if !ok {
			continue
		}
		fullyResolvedName, err := params.p.GetQualifiedFunctionNameByID(params.ctx, int64(dep.ID))
		if err != nil {
			return err
		}
		dependentFuncs = append(dependentFuncs, fullyResolvedName.FQString())
	}
	if len(dependentFuncs) > 0 {
		return errors.UnimplementedErrorf(
			errors.IssueLink{
				IssueURL: build.MakeIssueURL(83233),
				Detail:   "renames are disallowed because references are by name",
			},
			"cannot rename function %q because other functions ([%v]) still depend on it",
			fnDesc.Name, strings.Join(dependentFuncs, ", "))
	}

	scDesc.RemoveFunction(fnDesc.GetName(), fnDesc.GetID())
	fnDesc.SetName(string(n.n.NewName))
	scDesc.AddFunction(fnDesc.GetName(), toSchemaOverloadSignature(fnDesc))
	if err := params.p.writeFuncSchemaChange(params.ctx, fnDesc); err != nil {
		return err
	}

	if err := params.p.writeSchemaDescChange(params.ctx, scDesc, "alter function name"); err != nil {
		return err
	}

	newFnName, err := params.p.getQualifiedFunctionName(params.ctx, fnDesc)
	if err != nil {
		return err
	}
	event := eventpb.RenameFunction{
		FunctionName:    oldFnName.FQString(),
		NewFunctionName: newFnName.FQString(),
	}
	return params.p.logEvent(params.ctx, fnDesc.GetID(), &event)
}

func (n *alterFunctionRenameNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionRenameNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionRenameNode) Close(ctx context.Context)           {}

// AlterFunctionSetOwner sets a function's owner.
func (p *planner) AlterFunctionSetOwner(
	ctx context.Context, n *tree.AlterRoutineSetOwner,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER FUNCTION",
	); err != nil {
		return nil, err
	}

	return &alterFunctionSetOwnerNode{n: n}, nil
}

func (n *alterFunctionSetOwnerNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("function"))
	fnDesc, err := params.p.mustGetMutableFunctionForAlter(params.ctx, &n.n.Function)
	if err != nil {
		return err
	}
	if !n.n.Procedure && fnDesc.IsProcedure() {
		return pgerror.Newf(
			pgcode.UndefinedFunction, "could not find a function named %q", &n.n.Function.FuncName,
		)
	}
	if n.n.Procedure && !fnDesc.IsProcedure() {
		return pgerror.Newf(
			pgcode.UndefinedFunction, "could not find a procedure named %q", &n.n.Function.FuncName,
		)
	}
	newOwner, err := decodeusername.FromRoleSpec(
		params.p.SessionData(), username.PurposeValidation, n.n.NewOwner,
	)
	if err != nil {
		return err
	}

	// No-op if the new owner is the current owner.
	if newOwner == fnDesc.GetPrivileges().Owner() {
		return nil
	}
	if err := params.p.checkCanAlterToNewOwner(params.ctx, fnDesc, newOwner); err != nil {
		return err
	}
	if err := params.p.canCreateOnSchema(
		params.ctx, fnDesc.GetParentSchemaID(), fnDesc.GetParentID(), newOwner, checkPublicSchema,
	); err != nil {
		return err
	}

	fnDesc.GetPrivileges().SetOwner(newOwner)
	if err := params.p.writeFuncSchemaChange(params.ctx, fnDesc); err != nil {
		return err
	}

	fnName, err := params.p.getQualifiedFunctionName(params.ctx, fnDesc)
	if err != nil {
		return err
	}
	event := eventpb.AlterFunctionOwner{
		FunctionName: fnName.FQString(),
		Owner:        newOwner.Normalized(),
	}
	return params.p.logEvent(params.ctx, fnDesc.GetID(), &event)
}

func (n *alterFunctionSetOwnerNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionSetOwnerNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionSetOwnerNode) Close(ctx context.Context)           {}

// AlterFunctionSetSchema moves a function to another schema.
func (p *planner) AlterFunctionSetSchema(
	ctx context.Context, n *tree.AlterRoutineSetSchema,
) (planNode, error) {
	if err := checkSchemaChangeEnabled(
		ctx,
		p.ExecCfg(),
		"ALTER FUNCTION",
	); err != nil {
		return nil, err
	}

	return &alterFunctionSetSchemaNode{n: n}, nil
}

func (n *alterFunctionSetSchemaNode) startExec(params runParams) error {
	telemetry.Inc(sqltelemetry.SchemaChangeAlterCounter("function"))
	// TODO(chengxiong): add validation that a function can not be altered if it's
	// referenced by other objects. This is needed when want to allow function
	// references.
	fnDesc, err := params.p.mustGetMutableFunctionForAlter(params.ctx, &n.n.Function)
	if err != nil {
		return err
	}
	if !n.n.Procedure && fnDesc.IsProcedure() {
		return pgerror.Newf(
			pgcode.UndefinedFunction, "could not find a function named %q", &n.n.Function.FuncName,
		)
	}
	if n.n.Procedure && !fnDesc.IsProcedure() {
		return pgerror.Newf(
			pgcode.UndefinedFunction, "could not find a procedure named %q", &n.n.Function.FuncName,
		)
	}
	oldFnName, err := params.p.getQualifiedFunctionName(params.ctx, fnDesc)
	if err != nil {
		return err
	}
	// Functions cannot be resolved across db, so just use current db name to get
	// the descriptor.
	db, err := params.p.Descriptors().MutableByName(params.p.txn).Database(params.ctx, params.p.CurrentDatabase())
	if err != nil {
		return err
	}
	sc, err := params.p.Descriptors().ByName(params.p.txn).Get().Schema(params.ctx, db, string(n.n.NewSchemaName))
	if err != nil {
		return err
	}
	// Disallow renaming if this rename operation will break other UDF's invoking
	// this one.
	var dependentFuncs []string
	for _, dep := range fnDesc.GetDependedOnBy() {
		desc, err := params.p.Descriptors().ByIDWithoutLeased(params.p.Txn()).Get().Desc(params.ctx, dep.ID)
		if err != nil {
			return err
		}
		_, ok := desc.(catalog.FunctionDescriptor)
		if !ok {
			continue
		}
		fullyResolvedName, err := params.p.GetQualifiedFunctionNameByID(params.ctx, int64(dep.ID))
		if err != nil {
			return err
		}
		dependentFuncs = append(dependentFuncs, fullyResolvedName.FQString())
	}
	if len(dependentFuncs) > 0 {
		return errors.UnimplementedErrorf(
			errors.IssueLink{
				IssueURL: build.MakeIssueURL(83233),
				Detail: "set schema is disallowed because there are references from " +
					"other objects by name",
			},
			"cannot set schema for function %q because other functions ([%v]) still depend on it",
			fnDesc.Name, strings.Join(dependentFuncs, ", "))
	}

	switch sc.SchemaKind() {
	case catalog.SchemaTemporary:
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of temporary schemas")
	case catalog.SchemaVirtual:
		return pgerror.Newf(pgcode.FeatureNotSupported,
			"cannot move objects into or out of virtual schemas")
	case catalog.SchemaPublic:
		// We do not need to check for privileges on the public schema.
	default:
		err = params.p.CheckPrivilege(params.ctx, sc, privilege.CREATE)
		if err != nil {
			return err
		}
	}

	if sc.GetID() == fnDesc.GetParentSchemaID() {
		// No-op if moving to the same schema.
		return nil
	}
	targetSc, err := params.p.Descriptors().MutableByID(params.p.txn).Schema(params.ctx, sc.GetID())
	if err != nil {
		return err
	}

	// Check if there is a conflicting function exists.
	maybeExistingFuncObj, err := fnDesc.ToRoutineObj()
	if err != nil {
		return err
	}
	maybeExistingFuncObj.FuncName.SchemaName = tree.Name(targetSc.GetName())
	maybeExistingFuncObj.FuncName.ExplicitSchema = true
	existing, err := params.p.matchRoutine(
		params.ctx, maybeExistingFuncObj, false, /* required */
		tree.UDFRoutine|tree.ProcedureRoutine, false, /* inDropContext */
	)
	if err != nil {
		return err
	}
	if existing != nil {
		return pgerror.Newf(
			pgcode.DuplicateFunction, "function %s already exists in schema %q",
			tree.AsString(maybeExistingFuncObj), targetSc.GetName(),
		)
	}

	sourceSc, err := params.p.Descriptors().MutableByID(params.p.txn).Schema(params.ctx, fnDesc.GetParentSchemaID())
	if err != nil {
		return err
	}

	sourceSc.RemoveFunction(fnDesc.GetName(), fnDesc.GetID())
	if err := params.p.writeSchemaDesc(params.ctx, sourceSc); err != nil {
		return err
	}
	targetSc.AddFunction(fnDesc.GetName(), toSchemaOverloadSignature(fnDesc))
	if err := params.p.writeSchemaDesc(params.ctx, targetSc); err != nil {
		return err
	}
	fnDesc.SetParentSchemaID(targetSc.GetID())
	if err := params.p.writeFuncSchemaChange(params.ctx, fnDesc); err != nil {
		return err
	}

	newFnName, err := params.p.getQualifiedFunctionName(params.ctx, fnDesc)
	if err != nil {
		return err
	}
	event := eventpb.SetSchema{
		DescriptorName:    oldFnName.FQString(),
		NewDescriptorName: newFnName.FQString(),
		DescriptorType:    string(fnDesc.DescriptorType()),
	}
	return params.p.logEvent(params.ctx, fnDesc.GetID(), &event)
}

func (n *alterFunctionSetSchemaNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionSetSchemaNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionSetSchemaNode) Close(ctx context.Context)           {}

// AlterFunctionDepExtension alters a function dependency on an extension.
func (p *planner) AlterFunctionDepExtension(
	ctx context.Context, n *tree.AlterFunctionDepExtension,
) (planNode, error) {
	return &alterFunctionDepExtensionNode{n: n}, nil
}

func (n *alterFunctionDepExtensionNode) startExec(params runParams) error {
	return unimplemented.NewWithIssue(85532, "alter function depends on extension not supported")
}

func (n *alterFunctionDepExtensionNode) Next(params runParams) (bool, error) { return false, nil }
func (n *alterFunctionDepExtensionNode) Values() tree.Datums                 { return tree.Datums{} }
func (n *alterFunctionDepExtensionNode) Close(ctx context.Context)           {}

func (p *planner) mustGetMutableFunctionForAlter(
	ctx context.Context, routineObj *tree.RoutineObj,
) (*funcdesc.Mutable, error) {
	ol, err := p.matchRoutine(
		ctx, routineObj, true, /* required */
		tree.UDFRoutine|tree.ProcedureRoutine, false, /* inDropContext */
	)
	if err != nil {
		return nil, err
	}
	fnID := funcdesc.UserDefinedFunctionOIDToID(ol.Oid)
	mut, err := p.checkPrivilegesForDropFunction(ctx, fnID)
	if err != nil {
		return nil, err
	}
	return mut, nil
}

func toSchemaOverloadSignature(fnDesc *funcdesc.Mutable) descpb.SchemaDescriptor_FunctionSignature {
	ret := descpb.SchemaDescriptor_FunctionSignature{
		ID:          fnDesc.GetID(),
		ArgTypes:    make([]*types.T, 0, len(fnDesc.GetParams())),
		ReturnType:  fnDesc.ReturnType.Type,
		ReturnSet:   fnDesc.ReturnType.ReturnSet,
		IsProcedure: fnDesc.IsProcedure(),
	}
	for paramIdx, param := range fnDesc.Params {
		class := funcdesc.ToTreeRoutineParamClass(param.Class)
		if tree.IsInParamClass(class) {
			ret.ArgTypes = append(ret.ArgTypes, param.Type)
		}
		if class == tree.RoutineParamOut {
			ret.OutParamOrdinals = append(ret.OutParamOrdinals, int32(paramIdx))
			ret.OutParamTypes = append(ret.OutParamTypes, param.Type)
		}
		if param.DefaultExpr != nil {
			ret.DefaultExprs = append(ret.DefaultExprs, *param.DefaultExpr)
		}
	}
	return ret
}
