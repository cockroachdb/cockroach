// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scbuildstmt

import (
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/decodeusername"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// CreatePolicy implements CREATE POLICY.
func CreatePolicy(b BuildCtx, n *tree.CreatePolicy) {
	failIfRLSIsNotEnabled(b)
	b.IncrementSchemaChangeCreateCounter("policy")

	tableElems := b.ResolveTable(n.TableName, ResolveParams{
		RequiredPrivilege: privilege.CREATE,
	})
	panicIfSchemaChangeIsDisallowed(tableElems, n)
	tbl := tableElems.FilterTable().MustGetOneElement()

	// Resolve the policy name to make sure one doesn't already exist
	policyElems := b.ResolvePolicy(tbl.TableID, n.PolicyName, ResolveParams{
		IsExistenceOptional: true,
		RequiredPrivilege:   privilege.CREATE,
	})
	policyElems.FilterPolicyName().ForEachTarget(func(target scpb.TargetStatus, e *scpb.PolicyName) {
		if target == scpb.ToPublic {
			panic(pgerror.Newf(pgcode.DuplicateObject, "policy with name %q already exists on table %q",
				n.PolicyName, n.TableName.String()))
		}
	})

	// Depending on the command for the policy, some expressions are blocked.
	if n.Cmd == tree.PolicyCommandInsert && n.Exprs.Using != nil {
		panic(pgerror.Newf(pgcode.Syntax, "only WITH CHECK expression allowed for INSERT"))
	} else if (n.Cmd == tree.PolicyCommandDelete || n.Cmd == tree.PolicyCommandSelect) && n.Exprs.WithCheck != nil {
		panic(pgerror.Newf(pgcode.Syntax, "WITH CHECK cannot be applied to SELECT or DELETE"))
	}

	policyID := b.NextTablePolicyID(tbl.TableID)
	policy := &scpb.Policy{
		TableID:  tbl.TableID,
		PolicyID: policyID,
		Type:     convertPolicyType(n.Type),
		Command:  convertPolicyCommand(n.Cmd),
	}
	b.Add(policy)
	b.Add(&scpb.PolicyName{
		TableID:  tbl.TableID,
		PolicyID: policyID,
		Name:     string(n.PolicyName),
	})
	addRoleElements(b, n, tbl.TableID, policyID)
	addPolicyExpressions(b, n, tbl.TableID, policyID)
	b.LogEventForExistingTarget(policy)
}

// convertPolicyType will convert from a tree.PolicyType to a catpb.PolicyType
func convertPolicyType(in tree.PolicyType) catpb.PolicyType {
	switch in {
	case tree.PolicyTypeDefault, tree.PolicyTypePermissive:
		return catpb.PolicyType_PERMISSIVE
	case tree.PolicyTypeRestrictive:
		return catpb.PolicyType_RESTRICTIVE
	default:
		panic(errors.AssertionFailedf("cannot convert tree.PolicyType: %v", in))
	}
}

// convertPolicyCommand will convert from a tree.PolicyCommand to a catpb.PolicyCommand
func convertPolicyCommand(in tree.PolicyCommand) catpb.PolicyCommand {
	switch in {
	case tree.PolicyCommandDefault, tree.PolicyCommandAll:
		return catpb.PolicyCommand_ALL
	case tree.PolicyCommandSelect:
		return catpb.PolicyCommand_SELECT
	case tree.PolicyCommandInsert:
		return catpb.PolicyCommand_INSERT
	case tree.PolicyCommandUpdate:
		return catpb.PolicyCommand_UPDATE
	case tree.PolicyCommandDelete:
		return catpb.PolicyCommand_DELETE
	default:
		panic(errors.AssertionFailedf("cannot convert tree.PolicyCommand: %v", in))
	}
}

// addRoleElements will add an element for each role defined in the policy.
func addRoleElements(
	b BuildCtx, n *tree.CreatePolicy, tableID descpb.ID, policyID descpb.PolicyID,
) {
	// If there were no roles provided, then we will default to adding the public
	// role so that it applies to everyone.
	if len(n.Roles) == 0 {
		addRoleElement(b, tableID, policyID, username.PublicRoleName())
		return
	}
	for _, role := range n.Roles {
		authRole, err := decodeusername.FromRoleSpec(b.SessionData(), username.PurposeValidation, role)
		if err != nil {
			panic(err)
		}
		addRoleElement(b, tableID, policyID, authRole)
	}
}

// addRoleElement will add a single role element for the given role name
func addRoleElement(
	b BuildCtx, tableID descpb.ID, policyID descpb.PolicyID, username username.SQLUsername,
) {
	err := b.CheckRoleExists(b, username)
	if err != nil {
		panic(err)
	}
	b.Add(&scpb.PolicyRole{
		TableID:  tableID,
		PolicyID: policyID,
		RoleName: username.Normalized(),
	})
}

// addPolicyExpressions will add elements for the WITH CHECK and USING expressions.
func addPolicyExpressions(
	b BuildCtx, n *tree.CreatePolicy, tableID descpb.ID, policyID descpb.PolicyID,
) {
	tn := n.TableName.ToTableName()

	// We maintain the forward references for both expressions in a single
	// PolicyDeps elements. These vars are used to manage that.
	var usesTypeIDs catalog.DescriptorIDSet
	var usesRelationIDs catalog.DescriptorIDSet
	var usesFunctionIDs catalog.DescriptorIDSet

	if n.Exprs.Using != nil {
		expr := validateAndResolveTypesInExpr(b, &tn, tableID, n.Exprs.Using, tree.PolicyUsingExpr)
		b.Add(&scpb.PolicyUsingExpr{
			TableID:    tableID,
			PolicyID:   policyID,
			Expression: *expr,
		})
		usesTypeIDs = catalog.MakeDescriptorIDSet(expr.UsesTypeIDs...)
		usesRelationIDs = catalog.MakeDescriptorIDSet(expr.UsesSequenceIDs...)
		usesFunctionIDs = catalog.MakeDescriptorIDSet(expr.UsesFunctionIDs...)
	}
	if n.Exprs.WithCheck != nil {
		expr := validateAndResolveTypesInExpr(b, &tn, tableID, n.Exprs.WithCheck, tree.PolicyWithCheckExpr)
		b.Add(&scpb.PolicyWithCheckExpr{
			TableID:    tableID,
			PolicyID:   policyID,
			Expression: *expr,
		})
		usesTypeIDs = usesTypeIDs.Union(catalog.MakeDescriptorIDSet(expr.UsesTypeIDs...))
		usesRelationIDs = usesRelationIDs.Union(catalog.MakeDescriptorIDSet(expr.UsesSequenceIDs...))
		usesFunctionIDs = usesFunctionIDs.Union(catalog.MakeDescriptorIDSet(expr.UsesFunctionIDs...))
	}
	// If we had at least one expression then we need to add the policy deps.
	if n.Exprs.Using != nil || n.Exprs.WithCheck != nil {
		b.Add(&scpb.PolicyDeps{
			TableID:         tableID,
			PolicyID:        policyID,
			UsesTypeIDs:     usesTypeIDs.Ordered(),
			UsesRelationIDs: usesRelationIDs.Ordered(),
			UsesFunctionIDs: usesFunctionIDs.Ordered(),
		})
	}
}

// validateAndResolveTypesInExpr is a helper that will properly validate the
// expression and return a suitable scpb.Expression. This should be used in
// place of bare calls to WrapExpression since it properly handles setting
// types.
func validateAndResolveTypesInExpr(
	b BuildCtx,
	tn *tree.TableName,
	tableID descpb.ID,
	inExpr tree.Expr,
	context tree.SchemaExprContext,
) *scpb.Expression {
	validExpr, _, _, err := schemaexpr.DequalifyAndValidateExprImpl(b, inExpr, types.Bool,
		context, b.SemaCtx(), volatility.Volatile, tn, b.ClusterSettings().Version.ActiveVersion(b),
		func() colinfo.ResultColumns {
			return getNonDropResultColumns(b, tableID)
		},
		func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
			return columnLookupFn(b, tableID, columnName)
		},
	)
	if err != nil {
		panic(err)
	}
	typedExpr, err := parser.ParseExpr(validExpr)
	if err != nil {
		panic(err)
	}
	return b.WrapExpression(tableID, typedExpr)
}
