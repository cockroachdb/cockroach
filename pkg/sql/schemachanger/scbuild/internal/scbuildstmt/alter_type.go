// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

type supportedAlterTypeCommand = supportedStatement

// supportedAlterTypeStatements tracks alter type operations fully supported by
// the declarative schema changer. Operations marked as non-fully supported can
// only be used with the use_declarative_schema_changer session variable.
var supportedAlterTypeStatements = map[reflect.Type]supportedAlterTypeCommand{
	reflect.TypeOf((*tree.AlterTypeAddValue)(nil)):    {fn: alterTypeAddValue, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeRenameValue)(nil)): {fn: alterTypeRenameValue, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeRename)(nil)):      {fn: alterTypeRename, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeSetSchema)(nil)):   {fn: alterTypeSetSchema, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeOwner)(nil)):       {fn: alterTypeOwner, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeDropValue)(nil)):   {fn: alterTypeDropValue, on: false, checks: nil},
}

func init() {
	// Check function signatures inside the supportedAlterTypeStatements map.
	for statementType, statementEntry := range supportedAlterTypeStatements {
		callBackType := reflect.TypeOf(statementEntry.fn)
		if callBackType.Kind() != reflect.Func {
			panic(errors.AssertionFailedf("%v entry for statement is "+
				"not a function", statementType))
		}
		if callBackType.NumIn() != 4 ||
			!callBackType.In(0).Implements(reflect.TypeOf((*BuildCtx)(nil)).Elem()) ||
			callBackType.In(1) != reflect.TypeOf((*tree.TypeName)(nil)) ||
			callBackType.In(2) != reflect.TypeOf((*scpb.EnumType)(nil)) ||
			callBackType.In(3) != statementType {
			panic(errors.AssertionFailedf("%v entry for alter type statement "+
				"does not have a valid signature; got %v", statementType, callBackType))
		}
		if statementEntry.checks != nil {
			if _, ok := statementEntry.checks.(isVersionActiveFunc); !ok {
				panic(errors.AssertionFailedf(
					"%v checks is not an isVersionActiveFunc; got %T",
					statementType, statementEntry.checks))
			}
		}
	}
}

// alterTypeChecks determines if the alter type command is supported.
func alterTypeChecks(
	n *tree.AlterType,
	mode sessiondatapb.NewSchemaChangerMode,
	activeVersion clusterversion.ClusterVersion,
) bool {
	return isFullySupportedWithFalsePositiveInternal(
		supportedAlterTypeStatements,
		reflect.TypeOf(n.Cmd), reflect.ValueOf(n.Cmd), mode, activeVersion,
	)
}

// AlterType implements ALTER TYPE.
func AlterType(b BuildCtx, n *tree.AlterType) {
	elts := b.ResolveUserDefinedTypeType(n.Type, ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.USAGE,
	})

	// Check for each type kind. Only enum types are supported for ALTER TYPE
	// (with the exception of OWNER TO on multi-region enums). Alias types
	// (implicit array types) and table implicit record types are rejected.
	_, target, enumType := scpb.FindEnumType(elts)
	if enumType == nil {
		_, _, aliasType := scpb.FindAliasType(elts)
		if aliasType != nil {
			panic(pgerror.Newf(
				pgcode.WrongObjectType,
				"%q is an implicit array type and cannot be modified",
				n.Type.Object(),
			))
		}
		// For any other type kind (e.g. composite), fall back to the legacy
		// schema changer since ALTER TYPE subcommands are enum-specific.
		panic(scerrors.NotImplementedErrorf(n, "only enum types are supported"))
	}
	if target != scpb.ToPublic {
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"type %q is being dropped, try again later", n.Type.Object()))
	}

	// Multi-region enums can only be modified via OWNER TO.
	if enumType.IsMultiRegion {
		if _, ok := n.Cmd.(*tree.AlterTypeOwner); !ok {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.WrongObjectType,
					"%q is a multi-region enum and can't be modified using the alter type command",
					n.Type.Object()),
				"try adding/removing the region using ALTER DATABASE"))
		}
	}

	tn := n.Type.ToTypeName()
	tn.ObjectNamePrefix = b.NamePrefix(enumType)
	b.SetUnresolvedNameAnnotation(n.Type, &tn)
	b.IncrementSchemaChangeAlterCounter("type", n.Cmd.TelemetryName())
	b.IncrementEnumCounter(sqltelemetry.EnumAlter)

	// Invoke the callback function for the command.
	info := supportedAlterTypeStatements[reflect.TypeOf(n.Cmd)]
	fn := reflect.ValueOf(info.fn)
	fn.Call([]reflect.Value{
		reflect.ValueOf(b),
		reflect.ValueOf(&tn),
		reflect.ValueOf(enumType),
		reflect.ValueOf(n.Cmd),
	})
}

func alterTypeAddValue(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeAddValue,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE ADD VALUE is not yet supported"))
}

func alterTypeRenameValue(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeRenameValue,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE RENAME VALUE is not yet supported"))
}

func alterTypeRename(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeRename,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE RENAME is not yet supported"))
}

func alterTypeSetSchema(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeSetSchema,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE SET SCHEMA is not yet supported"))
}

func alterTypeOwner(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeOwner,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE OWNER TO is not yet supported"))
}

func alterTypeDropValue(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeDropValue,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE DROP VALUE is not yet supported"))
}
