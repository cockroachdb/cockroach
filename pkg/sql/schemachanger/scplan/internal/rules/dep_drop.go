// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rules

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/scgraph"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func isDescriptor(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.Database, *scpb.Schema, *scpb.Table, *scpb.View, *scpb.Sequence, *scpb.AliasType, *scpb.EnumType:
		return true
	}
	return false
}

func isSubjectTo2VersionInvariant(e scpb.Element) bool {
	switch e.(type) {
	case *scpb.Column, *scpb.PrimaryIndex, *scpb.SecondaryIndex, *scpb.TemporaryIndex:
		return true
	}
	return false
}

func isSimpleDependent(e scpb.Element) bool {
	return !isDescriptor(e) && !isSubjectTo2VersionInvariant(e)
}

// Assert that elements can be grouped into three categories when transitioning
// from PUBLIC to ABSENT:
// - go via DROPPED iff they're descriptor elements
// - go via a non-read status iff they're indexes or columns, which are
//   subject to the two-version invariant.
// - go direct to ABSENT in all other cases.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		s0 := opgen.InitialStatus(e, scpb.Status_ABSENT)
		s1 := opgen.NextStatus(e, scpb.Status_ABSENT, s0)
		switch s1 {
		case scpb.Status_DROPPED:
			if isDescriptor(e) {
				return nil
			}
		case scpb.Status_VALIDATED, scpb.Status_WRITE_ONLY, scpb.Status_DELETE_ONLY:
			if isSubjectTo2VersionInvariant(e) {
				return nil
			}
		case scpb.Status_ABSENT:
			if isSimpleDependent(e) {
				return nil
			}
		}
		panic(errors.AssertionFailedf(
			"unexpected transition %s -> %s in direction ABSENT for %T (descriptor=%v, 2VI=%v)",
			s0, s1, e, isDescriptor(e), isSubjectTo2VersionInvariant(e),
		))
	})
}

// These rules ensure that:
// - a descriptor element reaches the DROPPED state in the statement txn before
//   its dependent elements (namespace entry, comments, column names, etc) reach
//   the ABSENT state;
// - for those dependent elements which have to wait post-commit to reach the
//   ABSENT state, we tie them to the same stage as when the descriptor element
//   reaches the ABSENT state, but afterwards in the stage, so as to not
//   interfere with the event logging op which is tied to the descriptor element
//   removal.
func init() {

	registerDepRule(
		"descriptor drop right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "dependent",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, isDescriptor),
				elementTypes(to, isSimpleDependent),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				joinOnDescID(from.el, to.el, "desc-id"),
			}
		})

	registerDepRule(
		"descriptor removal right before dependent element removal",
		scgraph.SameStagePrecedence,
		"descriptor", "idx-or-col",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, isDescriptor),
				elementTypes(to, isSubjectTo2VersionInvariant),
				toAbsentInAbsent(from.target, from.node, to.target, to.node),
				joinOnDescID(from.el, to.el, "desc-id"),
			}
		},
	)
}

// Assert that only simple dependents (non-descriptor, non-index, non-column)
// have screl.ReferencedDescID attributes.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		if isSimpleDependent(e) {
			return nil
		}
		e = nonNilElement(e)
		if _, err := screl.Schema.GetAttribute(screl.ReferencedDescID, e); err == nil {
			panic(errors.AssertionFailedf("%T not expected to have screl.ReferencedDescID attr", e))
		}
		return nil
	})
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

// Assert that getTypeT covers all elements with embedded TypeTs.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		e = nonNilElement(e)
		return screl.WalkTypes(e, func(t *types.T) error {
			if _, err := getTypeT(e); err != nil {
				panic(errors.AssertionFailedf("getTypeT should support %T but doesn't", e))
			}
			return nil
		})
	})
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

// Assert that getExpression covers all elements with embedded expressions.
func init() {
	_ = forEachElement(func(e scpb.Element) error {
		return screl.WalkExpressions(e, func(t *catpb.Expression) error {
			if _, err := getExpression(e); err != nil {
				panic(errors.AssertionFailedf("getExpression should support %T but doesn't", e))
			}
			return nil
		})
	})
}

// These rules ensure that cross-referencing simple dependent elements reach
// ABSENT in the same stage right after the referenced descriptor element
// reaches DROPPED.
//
// References from simple dependent elements to other descriptors exist as
// follows:
// - simple dependent elements with a ReferencedDescID attribute,
// - those which embed a TypeT,
// - those which embed an Expression.
func init() {

	registerDepRule(
		"descriptor drop right before removing dependent with attr ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-attr",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, isDescriptor),
				elementTypes(to, isSimpleDependent),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				joinReferencedDescID(to.el, from.el, "desc-id"),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with type ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-type",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, isDescriptor),
				elementTypes(to, func(e scpb.Element) bool {
					_, err := getTypeT(e)
					return err == nil && isSimpleDependent(e)
				}),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				rel.Filter("RefByTypeT", from.el, to.el)(func(ref, e scpb.Element) bool {
					refID := screl.GetDescID(ref)
					typeT, err := getTypeT(e)
					if err != nil {
						panic(err)
					}
					return typeT != nil && idInIDs(typeT.ClosedTypeIDs, refID)
				}),
			}
		},
	)

	registerDepRule(
		"descriptor drop right before removing dependent with expr ref",
		scgraph.SameStagePrecedence,
		"referenced-descriptor", "referencing-via-expr",
		func(from, to nodeVars) rel.Clauses {
			return rel.Clauses{
				elementTypes(from, isDescriptor),
				elementTypes(to, func(e scpb.Element) bool {
					_, err := getExpression(e)
					return err == nil && isSimpleDependent(e)
				}),
				toAbsent(from.target, to.target),
				currentStatus(from.node, scpb.Status_DROPPED),
				currentStatus(to.node, scpb.Status_ABSENT),
				rel.Filter("RefByTypeT", from.el, to.el)(func(ref, e scpb.Element) bool {
					refID := screl.GetDescID(ref)
					expr, err := getExpression(e)
					if err != nil {
						panic(err)
					}
					return expr != nil && (idInIDs(expr.UsesTypeIDs, refID) || idInIDs(expr.UsesSequenceIDs, refID))
				}),
			}
		},
	)
}
