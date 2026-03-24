// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"bytes"
	"reflect"
	"slices"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/enum"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/errors"
)

type supportedAlterTypeCommand = supportedStatement

// supportedAlterTypeStatements tracks alter type operations fully supported by
// the declarative schema changer. Operations marked as non-fully supported can
// only be used with the use_declarative_schema_changer session variable.
var supportedAlterTypeStatements = map[reflect.Type]supportedAlterTypeCommand{
	reflect.TypeOf((*tree.AlterTypeAddValue)(nil)):    {fn: alterTypeAddValue, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterTypeRenameValue)(nil)): {fn: alterTypeRenameValue, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeRename)(nil)):      {fn: alterTypeRename, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeSetSchema)(nil)):   {fn: alterTypeSetSchema, on: false, checks: nil},
	reflect.TypeOf((*tree.AlterTypeOwner)(nil)):       {fn: alterTypeOwner, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterTypeDropValue)(nil)):   {fn: alterTypeDropValue, on: true, checks: isV263Active},
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
	elts := b.ResolveUserDefinedTypeType(n.Type, ResolveParams{})

	if !elts.FilterCompositeType().IsEmpty() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "cannot modify composite type"))
	}

	enumType := elts.FilterEnumType().MustGetOneElement()
	elts.FilterEnumType().ForEach(func(_ scpb.Status, target scpb.TargetStatus, _ *scpb.EnumType) {
		if target != scpb.ToPublic {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"type %q is being dropped, try again later", n.Type.Object()))
		}
	})

	tn := n.Type.ToTypeName()
	tn.ObjectNamePrefix = b.NamePrefix(enumType)
	b.SetUnresolvedNameAnnotation(n.Type, &tn)

	if enumType.IsMultiRegion {
		if _, isAlterTypeOwner := n.Cmd.(*tree.AlterTypeOwner); !isAlterTypeOwner {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.WrongObjectType,
					"%q is a multi-region enum and can't be modified using the alter type command",
					tn.FQString()),
				"try adding/removing the region using ALTER DATABASE"))
		}
	}

	b.IncrementSchemaChangeAlterCounter("type", n.Cmd.TelemetryName())
	b.IncrementEnumCounter(sqltelemetry.EnumAlter)

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
	newVal := string(t.NewVal)

	type valueInfo struct {
		physicalRep []byte
		logicalRep  string
		target      scpb.TargetStatus
	}
	var existingValues []valueInfo
	b.QueryByID(enumType.TypeID).FilterEnumTypeValue().ForEach(
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.EnumTypeValue) {
			existingValues = append(existingValues, valueInfo{
				physicalRep: e.PhysicalRepresentation,
				logicalRep:  e.LogicalRepresentation,
				target:      target,
			})
		},
	)

	// Handle adding a duplicate.
	for _, v := range existingValues {
		if v.logicalRep != newVal {
			continue
		}
		if v.target == scpb.ToAbsent {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"enum value %q is being dropped, try again later", newVal))
		}
		if t.IfNotExists {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(
				b, pgnotice.Newf("enum value %q already exists, skipping", newVal),
			)
			return
		}
		panic(pgerror.Newf(pgcode.DuplicateObject, "enum value %q already exists", newVal))
	}

	var activeValues []valueInfo
	for _, v := range existingValues {
		if v.target != scpb.ToAbsent {
			activeValues = append(activeValues, v)
		}
	}
	sort.Slice(activeValues, func(i, j int) bool {
		return bytes.Compare(activeValues[i].physicalRep, activeValues[j].physicalRep) < 0
	})

	// Determine insertion position. By default the new value is appended.
	//
	// pos is the index of the value after which the new value will be inserted.
	pos := len(activeValues) - 1
	if t.Placement != nil {
		existing := string(t.Placement.ExistingVal)
		pos = slices.IndexFunc(activeValues, func(v valueInfo) bool {
			return v.logicalRep == existing
		})
		if pos == -1 {
			panic(pgerror.Newf(pgcode.InvalidParameterValue,
				"%q is not an existing enum value", existing))
		}
		if t.Placement.Before {
			pos--
		}
	}

	getPhysicalRep := func(idx int) []byte {
		if idx < 0 || idx >= len(activeValues) {
			return nil
		}
		return activeValues[idx].physicalRep
	}
	physicalRep := enum.GenByteStringBetween(
		getPhysicalRep(pos), getPhysicalRep(pos+1), enum.SpreadSpacing,
	)

	enumValue := &scpb.EnumTypeValue{
		TypeID:                 enumType.TypeID,
		PhysicalRepresentation: physicalRep,
		LogicalRepresentation:  newVal,
	}
	b.Add(enumValue)

	b.LogEventForExistingPayload(enumValue, &eventpb.AlterType{
		TypeName: tn.FQString(),
	})
}

func alterTypeRenameValue(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeRenameValue,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE RENAME VALUE is not supported"))
}

func alterTypeRename(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeRename,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE RENAME is not supported"))
}

func alterTypeSetSchema(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeSetSchema,
) {
	panic(scerrors.NotImplementedErrorf(t, "ALTER TYPE SET SCHEMA is not supported"))
}

func alterTypeOwner(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeOwner,
) {
	newOwner, err := decodeusername.FromRoleSpec(
		b.SessionData(), username.PurposeValidation, t.Owner,
	)
	if err != nil {
		panic(err)
	}

	typeElts := b.QueryByID(enumType.TypeID)
	oldOwner := typeElts.FilterOwner().MustGetOneElement()

	if newOwner.Normalized() == oldOwner.Owner {
		return
	}

	if !b.HasOwnership(enumType) {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be owner of type %s", tn.Object()))
	}

	if err := b.CheckRoleExists(b, newOwner); err != nil {
		panic(err)
	}
	if !b.CurrentUserHasAdminOrIsMemberOf(newOwner) {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"must be member of role %q", newOwner))
	}

	schemaChild := typeElts.FilterSchemaChild().MustGetOneElement()
	schemaElts := b.QueryByID(schemaChild.SchemaID)
	schema := schemaElts.FilterSchema().MustGetOneElement()
	if err := b.CheckPrivilegeForUser(schema, privilege.CREATE, newOwner); err != nil {
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"user %s does not have CREATE privilege on schema %s",
			newOwner, simpleName(b, schemaChild.SchemaID)))
	}

	b.Replace(&scpb.Owner{
		DescriptorID: enumType.TypeID,
		Owner:        newOwner.Normalized(),
	})

	b.Replace(&scpb.Owner{
		DescriptorID: enumType.ArrayTypeID,
		Owner:        newOwner.Normalized(),
	})

	arrayElts := b.QueryByID(enumType.ArrayTypeID)
	arrayNs := arrayElts.FilterNamespace().MustGetOneElement()
	arrayTn := tree.MakeTypeNameWithPrefix(b.NamePrefix(enumType), arrayNs.Name)

	b.LogEventForExistingPayload(enumType, &eventpb.AlterTypeOwner{
		TypeName: tn.FQString(),
		Owner:    newOwner.Normalized(),
	})
	b.LogEventForExistingPayload(enumType, &eventpb.AlterTypeOwner{
		TypeName: arrayTn.FQString(),
		Owner:    newOwner.Normalized(),
	})
}

func alterTypeDropValue(
	b BuildCtx, tn *tree.TypeName, enumType *scpb.EnumType, t *tree.AlterTypeDropValue,
) {
	dropVal := string(t.Val)

	var found bool
	b.QueryByID(enumType.TypeID).FilterEnumTypeValue().ForEach(
		func(_ scpb.Status, target scpb.TargetStatus, e *scpb.EnumTypeValue) {
			if e.LogicalRepresentation != dropVal {
				return
			}
			if target == scpb.ToAbsent {
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"enum value %q is already being dropped", dropVal))
			}
			if target != scpb.ToPublic {
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"enum value %q is being added, try again later", dropVal))
			}
			found = true
			b.Drop(e)
			b.LogEventForExistingPayload(e, &eventpb.AlterType{
				TypeName: tn.FQString(),
			})
		},
	)
	if !found {
		panic(pgerror.Newf(pgcode.UndefinedObject,
			"enum value %q does not exist", dropVal))
	}
}
