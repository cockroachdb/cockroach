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
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/errors"
)

type supportedAlterDomainCommand = supportedStatement

// supportedAlterDomainStatements tracks alter domain operations fully supported
// by the declarative schema changer. Operations marked as non-fully supported
// can only be used with the use_declarative_schema_changer session variable.
var supportedAlterDomainStatements = map[reflect.Type]supportedAlterDomainCommand{
	// FIXME(bghal): Each delegation should use a isV263Active check when available.
	reflect.TypeOf((*tree.AlterDomainSetDefault)(nil)):         {fn: alterDomainSetDefault, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainDropDefault)(nil)):        {fn: alterDomainDropDefault, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainSetNotNull)(nil)):         {fn: alterDomainSetNotNull, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainDropNotNull)(nil)):        {fn: alterDomainDropNotNull, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainAddConstraint)(nil)):      {fn: alterDomainAddConstraint, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainDropConstraint)(nil)):     {fn: alterDomainDropConstraint, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainRenameConstraint)(nil)):   {fn: alterDomainRenameConstraint, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainValidateConstraint)(nil)): {fn: alterDomainValidateConstraint, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainOwner)(nil)):              {fn: alterDomainOwner, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainRename)(nil)):             {fn: alterDomainRename, on: true, checks: nil},
	reflect.TypeOf((*tree.AlterDomainSetSchema)(nil)):          {fn: alterDomainSetSchema, on: true, checks: nil},
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
	_, target, domainType := domainTypeElts.Get(0)
	if target != scpb.ToPublic {
		panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
			"domain %q is being dropped, try again later", n.Domain.Object()))
	}

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
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN SET DEFAULT is not supported"))
}

func alterDomainDropDefault(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainDropDefault,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN DROP DEFAULT is not supported"))
}

func alterDomainSetNotNull(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainSetNotNull,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN SET NOT NULL is not supported"))
}

func alterDomainDropNotNull(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainDropNotNull,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN DROP NOT NULL is not supported"))
}

func alterDomainAddConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainAddConstraint,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN ADD CONSTRAINT is not supported"))
}

func alterDomainDropConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainDropConstraint,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN DROP CONSTRAINT is not supported"))
}

func alterDomainRenameConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainRenameConstraint,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN RENAME CONSTRAINT is not supported"))
}

func alterDomainValidateConstraint(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainValidateConstraint,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN VALIDATE CONSTRAINT is not supported"))
}

func alterDomainOwner(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainOwner,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN OWNER TO is not supported"))
}

func alterDomainRename(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainRename,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN RENAME TO is not supported"))
}

func alterDomainSetSchema(
	b BuildCtx, tn *tree.TypeName, domainType *scpb.DomainType, t *tree.AlterDomainSetSchema,
) {
	panic(unimplemented.NewWithIssue(27796, "ALTER DOMAIN SET SCHEMA is not supported"))
}
