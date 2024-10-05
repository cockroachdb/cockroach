// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_22_2

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
	rulesVersion = "-22.2"
)

// rulesVersionKey version of elements used by this rule set.
var rulesVersionKey = clusterversion.V22_2

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

// isDescriptor returns true for a descriptor-element, i.e. an element which
// owns its corresponding descriptor.
func isDescriptor(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.Database, *scpb.Schema, *scpb.Table, *scpb.View, *scpb.Sequence,
		*scpb.AliasType, *scpb.EnumType:
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
	return IsIndex(e) || isColumn(e)
}

// IsIndex returns if an element is an index type.
func IsIndex(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	}
	return false
}

func isColumn(e scpb.Element) bool {
	_, ok := e.(*scpb.Column)
	return ok
}

func isSimpleDependent(e scpb.Element) bool {
	return !isDescriptor(e) && !isSubjectTo2VersionInvariant(e)
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
	}
	return nil, errors.AssertionFailedf("element %T does not have an embedded scpb.Expression", element)
}

func isWithExpression(element scpb.Element) bool {
	_, err := getExpression(element)
	return err == nil
}

func isTypeDescriptor(element scpb.Element) bool {
	switch element.(type) {
	case *scpb.EnumType, *scpb.AliasType:
		return true
	default:
		return false
	}
}

func isColumnDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.ColumnType:
		return true
	case *scpb.ColumnName, *scpb.ColumnComment, *scpb.IndexColumn:
		return true
	}
	return isColumnTypeDependent(e)
}

func isColumnTypeDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.SequenceOwner, *scpb.ColumnDefaultExpression, *scpb.ColumnOnUpdateExpression:
		return true
	}
	return false
}

func isIndexDependent(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.IndexName, *scpb.IndexComment, *scpb.IndexColumn:
		return true
	case *scpb.IndexPartitioning, *scpb.SecondaryIndexPartial:
		return true
	}
	return false
}

// isSupportedNonIndexBackedConstraint a non-index-backed constraint is one of {Check, FK, UniqueWithoutIndex}. We only
// support Check for now.
// TODO (xiang): Expand this predicate to include other non-index-backed constraints
// when we properly support adding/dropping them in the new schema changer.
func isSupportedNonIndexBackedConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.CheckConstraint, *scpb.ForeignKeyConstraint, *scpb.UniqueWithoutIndexConstraint:
		return true
	}
	return false
}

func isConstraint(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	case *scpb.CheckConstraint, *scpb.UniqueWithoutIndexConstraint, *scpb.ForeignKeyConstraint:
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
