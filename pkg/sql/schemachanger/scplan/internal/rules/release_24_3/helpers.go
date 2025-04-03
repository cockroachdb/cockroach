// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_24_3

import (
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/rules"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/errors"
)

const (
	// rulesVersion version of elements that can be appended to rel rule names.
	rulesVersion = "-24.3"
)

// rulesVersionKey version of elements used by this rule set.
var rulesVersionKey = clusterversion.V24_3

// descriptorIsNotBeingDropped creates a clause which leads to the outer clause
// failing to unify if the passed element is part of a descriptor and
// that descriptor is being dropped.
var descriptorIsNotBeingDropped = screl.Schema.DefNotJoin1(
	"descriptorIsNotBeingDropped"+rulesVersion, "element", func(
		element rel.Var,
	) rel.Clauses {
		descriptor := rules.MkNodeVars("descriptor")
		return rel.Clauses{
			descriptor.TypeFilter(rulesVersionKey, isDescriptor),
			descriptor.JoinTarget(),
			rules.JoinOnDescIDUntyped(descriptor.El, element, "id"),
			descriptor.TargetStatus(scpb.ToAbsent),
		}
	},
)

// descriptorDataIsNotBeingAdded indicates if we are operating on a descriptor
// that already exists and was not created in the current transaction. This is
// determined by detecting if the data element is public, and not going from
// absent to public which newly created descriptors will.
var descriptorDataIsNotBeingAdded = screl.Schema.DefNotJoin1(
	"descriptorIsDataNotBeingAdded"+rulesVersion, "descID", func(
		descID rel.Var,
	) rel.Clauses {
		descriptorData := rules.MkNodeVars("descriptor-data")
		prevDescriptorData := rules.MkNodeVars("prev-descriptor-data")
		return rel.Clauses{
			descriptorData.Type((*scpb.TableData)(nil)),
			descriptorData.JoinTargetNode(),
			descriptorData.CurrentStatus(scpb.Status_PUBLIC),
			descriptorData.DescIDEq(descID),
			prevDescriptorData.Type((*scpb.TableData)(nil)),
			prevDescriptorData.JoinTargetNode(),
			prevDescriptorData.CurrentStatus(scpb.Status_ABSENT),
			prevDescriptorData.DescIDEq(descID),
			prevDescriptorData.El.AttrEqVar(rel.Self, descriptorData.El),
		}
	},
)

// isDescriptor returns true for a descriptor-element, i.e. an element which
// owns its corresponding descriptor.
func isDescriptor(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.Database, *scpb.Schema, *scpb.Table, *scpb.View, *scpb.Sequence,
		*scpb.AliasType, *scpb.EnumType, *scpb.CompositeType, *scpb.Function:
		return true
	}
	return false
}

// IsDescriptor returns true for a descriptor-element, i.e. an element which
// owns its corresponding descriptor. This is only used for exports
func IsDescriptor(e scpb.Element) bool {
	return isDescriptor(e)
}

func isSubjectTo2VersionInvariant(e scpb.Element) bool {
	// TODO(ajwerner): This should include constraints and enum values but it
	// currently does not because we do not support dropping them unless we're
	// dropping the descriptor and we do not support adding them.
	if isIndex(e) || isColumn(e) {
		return true
	}
	switch e.(type) {
	case *scpb.CheckConstraint, *scpb.UniqueWithoutIndexConstraint, *scpb.ForeignKeyConstraint,
		*scpb.ColumnNotNull:
		return true
	}
	return false
}

func isIndex(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	}
	return false
}

func isIndexColumn(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.IndexColumn:
		return true
	}
	return false
}

func isColumn(e scpb.Element) bool {
	_, ok := e.(*scpb.Column)
	return ok
}

func isSimpleDependent(e scpb.Element) bool {
	return !isDescriptor(e) && !isSubjectTo2VersionInvariant(e) && !isData(e)
}

func getTypeT(element scpb.Element) (*scpb.TypeT, error) {
	switch e := element.(type) {
	case *scpb.ColumnType:
		if e == nil {
			return nil, nil
		}
		return &e.TypeT, nil
	case *scpb.AliasType:
		if e == nil {
			return nil, nil
		}
		return &e.TypeT, nil
	}
	return nil, errors.AssertionFailedf("element %T does not have an embedded scpb.TypeT", element)
}

func isWithTypeT(element scpb.Element) bool {
	_, err := getTypeT(element)
	return err == nil
}

func getExpression(element scpb.Element) (*scpb.Expression, error) {
	switch e := element.(type) {
	case *scpb.ColumnType:
		if e == nil {
			return nil, nil
		}
		return e.ComputeExpr, nil
	case *scpb.ColumnComputeExpression:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.ColumnDefaultExpression:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.ColumnOnUpdateExpression:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.SecondaryIndex:
		if e == nil || e.EmbeddedExpr == nil {
			return nil, nil
		}
		return e.EmbeddedExpr, nil
	case *scpb.SecondaryIndexPartial:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.CheckConstraint:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.CheckConstraintUnvalidated:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.PolicyUsingExpr:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	case *scpb.PolicyWithCheckExpr:
		if e == nil {
			return nil, nil
		}
		return &e.Expression, nil
	}
	return nil, errors.AssertionFailedf("element %T does not have an embedded scpb.Expression", element)
}

func isWithExpression(element scpb.Element) bool {
	_, err := getExpression(element)
	return err == nil
}

func isTypeDescriptor(element scpb.Element) bool {
	switch element.(type) {
	case *scpb.EnumType, *scpb.AliasType, *scpb.CompositeType:
		return true
	default:
		return false
	}
}

func isColumnDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ColumnType, *scpb.ColumnNotNull:
		return true
	case *scpb.ColumnName, *scpb.ColumnComment, *scpb.IndexColumn:
		return true
	}
	return isColumnTypeDependent(e)
}

func isColumnNotNull(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ColumnNotNull:
		return true
	}
	return false
}
func isColumnTypeDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.SequenceOwner, *scpb.ColumnDefaultExpression, *scpb.ColumnOnUpdateExpression, *scpb.ColumnComputeExpression:
		return true
	}
	return false
}

func isIndexDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.IndexName, *scpb.IndexComment, *scpb.IndexColumn,
		*scpb.IndexZoneConfig:
		return true
	case *scpb.IndexPartitioning, *scpb.PartitionZoneConfig, *scpb.SecondaryIndexPartial:
		return true
	}
	return false
}

// CRDB supports five constraints of two categories:
// - PK, Unique (index-backed)
// - Check, UniqueWithoutIndex, FK (non-index-backed)
func isConstraint(e scpb.Element) bool {
	return isIndex(e) || isNonIndexBackedConstraint(e)
}

// isNonIndexBackedConstraint returns true if `e` is a non-index-backed constraint.
func isNonIndexBackedConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.CheckConstraint, *scpb.UniqueWithoutIndexConstraint, *scpb.ForeignKeyConstraint,
		*scpb.ColumnNotNull:
		return true
	case *scpb.CheckConstraintUnvalidated, *scpb.UniqueWithoutIndexConstraintUnvalidated,
		*scpb.ForeignKeyConstraintUnvalidated:
		return true
	}
	return false
}

// isNonIndexBackedCrossDescriptorConstraint returns true if `e` is a
// non-index-backed constraint and it can potentially reference another
// descriptor.
//
// This filter exists because in general we need to drop the constraint first
// before dropping referencing/referenced descriptor. Read rules that use
// this filter for more details.
//
// TODO (xiang): UniqueWithoutIndex and UniqueWithoutIndexNotValid should
// also be treated as cross-descriptor constraint because its partial predicate
// can references other descriptors.
func isNonIndexBackedCrossDescriptorConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.CheckConstraint, *scpb.UniqueWithoutIndexConstraint,
		*scpb.ForeignKeyConstraint:
		return true
	case *scpb.CheckConstraintUnvalidated, *scpb.UniqueWithoutIndexConstraintUnvalidated,
		*scpb.ForeignKeyConstraintUnvalidated:
		return true
	}
	return false
}

func isConstraintDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ConstraintWithoutIndexName:
		return true
	case *scpb.ConstraintComment:
		return true
	}
	return false
}

func isConstraintWithoutIndexName(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ConstraintWithoutIndexName:
		return true
	}
	return false
}

func isTriggerDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.TriggerName, *scpb.TriggerEnabled, *scpb.TriggerTiming,
		*scpb.TriggerEvents, *scpb.TriggerTransition, *scpb.TriggerWhen,
		*scpb.TriggerFunctionCall, *scpb.TriggerDeps:
		return true
	}
	return false
}

func isData(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.DatabaseData:
		return true
	case *scpb.TableData:
		return true
	case *scpb.IndexData:
		return true
	}
	return false
}

func isDescriptorParentReference(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.SchemaChild, *scpb.SchemaParent:
		return true
	}
	return false
}

func isOwner(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.Owner:
		return true
	}
	return false
}
