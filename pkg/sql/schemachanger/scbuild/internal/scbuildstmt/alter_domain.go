// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"fmt"
	"reflect"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/errors"
)

type supportedAlterDomainCommand = supportedStatement

// supportedAlterDomainStatements tracks alter domain operations fully supported
// by the declarative schema changer. Operations marked as non-fully supported
// can only be used with the use_declarative_schema_changer session variable.
var supportedAlterDomainStatements = map[reflect.Type]supportedAlterDomainCommand{
	reflect.TypeOf((*tree.AlterDomainSetDefault)(nil)):           {fn: alterDomainSetDefault, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainDropDefault)(nil)):          {fn: alterDomainDropDefault, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainSetNotNull)(nil)):           {fn: alterDomainSetNotNull, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainDropNotNull)(nil)):          {fn: alterDomainDropNotNull, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainAddCheckConstraint)(nil)):   {fn: alterDomainAddCheckConstraint, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainAddNotNullConstraint)(nil)): {fn: alterDomainAddNotNullConstraint, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainDropConstraint)(nil)):       {fn: alterDomainDropConstraint, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainRenameConstraint)(nil)):     {fn: alterDomainRenameConstraint, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainValidateConstraint)(nil)):   {fn: alterDomainValidateConstraint, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainOwner)(nil)):                {fn: alterDomainOwner, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainRename)(nil)):               {fn: alterDomainRename, on: true, checks: isV263Active},
	reflect.TypeOf((*tree.AlterDomainSetSchema)(nil)):            {fn: alterDomainSetSchema, on: true, checks: isV263Active},
}

func init() {
	// Check function signatures inside the supportedAlterDomainStatements map.
	for statementType, statementEntry := range supportedAlterDomainStatements {
		callBackType := reflect.TypeOf(statementEntry.fn)
		if callBackType.Kind() != reflect.Func {
			panic(errors.AssertionFailedf("%v entry for statement is "+
				"not a function", statementType))
		}
		if callBackType.NumIn() != 4 ||
			!callBackType.In(0).Implements(reflect.TypeOf((*BuildCtx)(nil)).Elem()) ||
			callBackType.In(1) != reflect.TypeOf((*tree.TypeName)(nil)) ||
			callBackType.In(2) != reflect.TypeOf((*scpb.DomainType)(nil)) ||
			callBackType.In(3) != statementType {
			panic(errors.AssertionFailedf("%v entry for alter domain statement "+
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

// alterDomainChecks determines if the alter domain command is supported.
func alterDomainChecks(
	n *tree.AlterDomain,
	mode sessiondatapb.NewSchemaChangerMode,
	activeVersion clusterversion.ClusterVersion,
) bool {
	return isFullySupportedWithFalsePositiveInternal(
		supportedAlterDomainStatements,
		reflect.TypeOf(n.Cmd), reflect.ValueOf(n.Cmd), mode, activeVersion,
	)
}

// AlterDomain implements ALTER DOMAIN.
func AlterDomain(b BuildCtx, n *tree.AlterDomain) {
	elts := b.ResolveUserDefinedTypeType(n.Domain, ResolveParams{})

	domainTypeElts := elts.FilterDomainType()
	if domainTypeElts.IsEmpty() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a domain", n.Domain.Object()))
	}
	domainType := domainTypeElts.MustGetOneElement()
	domainTypeElts.ForEach(func(_ scpb.Status, target scpb.TargetStatus, _ *scpb.DomainType) {
		if target != scpb.ToPublic {
			panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
				"domain %q is being dropped, try again later", n.Domain.Object()))
		}
	})

	tn := n.Domain.ToTypeName()
	tn.ObjectNamePrefix = b.NamePrefix(domainType)
	b.SetUnresolvedNameAnnotation(n.Domain, &tn)
	b.IncrementSchemaChangeAlterCounter("domain", n.Cmd.TelemetryName())

	info := supportedAlterDomainStatements[reflect.TypeOf(n.Cmd)]
	fn := reflect.ValueOf(info.fn)
	fn.Call([]reflect.Value{
		reflect.ValueOf(b),
		reflect.ValueOf(&tn),
		reflect.ValueOf(domainType),
		reflect.ValueOf(n.Cmd),
	})
}

func alterDomainSetDefault(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainSetDefault,
) {
	typeID := domainType.TypeID
	domainElts := b.QueryByID(typeID)

	oldDefault := domainElts.FilterDomainDefault().MustGetZeroOrOneElement()
	if oldDefault != nil {
		b.Drop(oldDefault)
	}

	if t.Default == nil || t.Default == tree.DNull {
		return
	}

	typedExpr, err := schemaexpr.SanitizeVarFreeExpr(
		b, t.Default, domainType.BaseTypeT.Type,
		tree.ColumnDefaultExprInSetDefault,
		b.SemaCtx(), volatility.Volatile, false, /* allowAssignmentCast */
	)
	if err != nil {
		panic(pgerror.WithCandidateCode(err, pgcode.DatatypeMismatch))
	}

	typedExpr, err = schemaexpr.MaybeReplaceUDFNameWithOIDReferenceInTypedExpr(typedExpr)
	if err != nil {
		panic(err)
	}

	b.Add(&scpb.DomainDefault{
		TypeID:     typeID,
		Expression: *b.WrapExpression(typeID, typedExpr),
	})
}

func alterDomainDropDefault(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainDropDefault,
) {
	typeID := domainType.TypeID
	domainElts := b.QueryByID(typeID)

	existingDefault := domainElts.FilterDomainDefault().MustGetZeroOrOneElement()
	if existingDefault == nil {
		return
	}
	b.Drop(existingDefault)
}

func alterDomainSetNotNull(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainSetNotNull,
) {
	typeID := domainType.TypeID
	domainElts := b.QueryByID(typeID)

	existingNotNull := domainElts.FilterDomainNotNull().MustGetZeroOrOneElement()
	if existingNotNull != nil {
		return
	}

	constraintID := nextDomainConstraintID(b, typeID)
	constraintName := chooseDomainNotNullConstraintName(b, tn, typeID)

	b.Add(&scpb.DomainNotNull{
		TypeID:       typeID,
		ConstraintID: constraintID,
	})
	b.Add(&scpb.DomainConstraintName{
		TypeID:       typeID,
		ConstraintID: constraintID,
		Name:         constraintName,
	})
}

func alterDomainDropNotNull(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainDropNotNull,
) {
	typeID := domainType.TypeID
	domainElts := b.QueryByID(typeID)

	existingNotNull := domainElts.FilterDomainNotNull().MustGetZeroOrOneElement()
	if existingNotNull == nil {
		return
	}
	b.Drop(existingNotNull)

	domainElts.FilterDomainConstraintName().Filter(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.DomainConstraintName) bool {
			return e.ConstraintID == existingNotNull.ConstraintID
		},
	).ForEach(func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.DomainConstraintName) {
		b.Drop(e)
	})
}

// nextDomainConstraintID returns the next available constraint ID for a domain
// type by checking the existing constraint elements.
func nextDomainConstraintID(b BuildCtx, typeID catid.DescID) catid.ConstraintID {
	var maxID catid.ConstraintID
	domainElts := b.QueryByID(typeID)
	domainElts.FilterDomainNotNull().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.DomainNotNull) {
			if e.ConstraintID > maxID {
				maxID = e.ConstraintID
			}
		},
	)
	domainElts.FilterDomainCheckConstraint().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.DomainCheckConstraint) {
			if e.ConstraintID > maxID {
				maxID = e.ConstraintID
			}
		},
	)
	domainElts.FilterDomainCheckConstraintUnvalidated().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.DomainCheckConstraintUnvalidated) {
			if e.ConstraintID > maxID {
				maxID = e.ConstraintID
			}
		},
	)
	return maxID + 1
}

// chooseDomainNotNullConstraintName generates a unique, unconflicted name for a
// domain NOT NULL constraint.
func chooseDomainNotNullConstraintName(b BuildCtx, tn *tree.TypeName, typeID catid.DescID) string {
	var usedNames []string
	b.QueryByID(typeID).FilterDomainConstraintName().ForEach(
		func(_ scpb.Status, _ scpb.TargetStatus, e *scpb.DomainConstraintName) {
			usedNames = append(usedNames, e.Name)
		},
	)
	domainName := tn.Object()
	candidate := fmt.Sprintf("%s_not_null", domainName)
	for pass := 0; ; pass++ {
		if pass > 0 {
			candidate = fmt.Sprintf("%s_not_null%d", domainName, pass)
		}
		if !slices.Contains(usedNames, candidate) {
			return candidate
		}
	}
}

func alterDomainAddCheckConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainAddCheckConstraint,
) {
	panic(pgerror.Newf(pgcode.FeatureNotSupported, "ALTER DOMAIN ADD CONSTRAINT ... CHECK is not supported"))
}

func alterDomainAddNotNullConstraint(
	b BuildCtx,
	tn *tree.TypeName,
	domainType *scpb.DomainType,
	t *tree.AlterDomainAddNotNullConstraint,
) {
	if t.ValidationBehavior == tree.ValidationSkip {
		panic(pgerror.Newf(pgcode.FeatureNotSupported,
			"NOT NULL constraints cannot be marked NOT VALID"))
	}
	panic(pgerror.Newf(pgcode.FeatureNotSupported, "ALTER DOMAIN ADD CONSTRAINT ... NOT NULL is not supported"))
}

func alterDomainDropConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainDropConstraint,
) {
	panic(pgerror.Newf(pgcode.FeatureNotSupported, "ALTER DOMAIN DROP CONSTRAINT is not supported"))
}

func alterDomainRenameConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainRenameConstraint,
) {
	panic(pgerror.Newf(pgcode.FeatureNotSupported, "ALTER DOMAIN RENAME CONSTRAINT is not supported"))
}

func alterDomainValidateConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainValidateConstraint,
) {
	panic(pgerror.Newf(pgcode.FeatureNotSupported, "ALTER DOMAIN VALIDATE CONSTRAINT is not supported"))
}

func alterDomainOwner(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainOwner,
) {
	setOwnerForTypeDesc(b, tn, domainType, domainType.TypeID, domainType.ArrayTypeID, t.Owner)
}

func alterDomainRename(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainRename,
) {
	renameForTypeDesc(b, tn, domainType, domainType.TypeID, domainType.ArrayTypeID, string(t.NewName))
}

func alterDomainSetSchema(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainSetSchema,
) {
	setSchemaForTypeDesc(b, domainType, domainType.TypeID, domainType.ArrayTypeID, t.Schema, "domain")
}
