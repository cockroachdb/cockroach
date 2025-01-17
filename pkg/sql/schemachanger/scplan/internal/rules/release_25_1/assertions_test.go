// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package release_25_1

import (
	"reflect"
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scplan/internal/opgen"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// TestRuleAssertions verifies that important helper functions verify certain
// properties that the rule definitions rely on.
func TestRuleAssertions(t *testing.T) {
	for _, fn := range []func(e scpb.Element) error{
		checkSimpleDependentsReferenceDescID,
		checkToAbsentCategories,
		checkIsWithTypeT,
		checkIsWithExpression,
		checkIsColumnDependent,
		checkIsIndexDependent,
		checkIsConstraintDependent,
		checkConstraintPartitions,
		checkIsTriggerDependent,
	} {
		var fni interface{} = fn
		fullName := runtime.FuncForPC(reflect.ValueOf(fni).Pointer()).Name()
		nameParts := strings.Split(fullName, "rules.")
		shortName := nameParts[len(nameParts)-1]
		t.Run(shortName, func(t *testing.T) {
			_ = scpb.ForEachElementType(func(e scpb.Element) error {
				e = nonNilElement(e)
				if err := fn(e); err != nil {
					t.Errorf("%T: %+v", e, err)
				}
				return nil
			})
		})
	}
}

func nonNilElement(element scpb.Element) scpb.Element {
	return reflect.New(reflect.ValueOf(element).Type().Elem()).Interface().(scpb.Element)
}

// Assert that only simple dependents (non-descriptor, non-index, non-column)
// and data elements have screl.ReferencedDescID attributes.
// One exception is foreign key constraint, which is not simple dependent nor data
// element but it has a screl.ReferencedDescID attribute.
func checkSimpleDependentsReferenceDescID(e scpb.Element) error {
	if isSimpleDependent(e) || isData(e) {
		return nil
	}
	if _, ok := e.(*scpb.ForeignKeyConstraint); ok {
		return nil
	}
	if _, err := screl.Schema.GetAttribute(screl.ReferencedDescID, e); err == nil {
		return errors.New("unexpected screl.ReferencedDescID attr")
	}
	return nil
}

// Assert that elements can be grouped into three categories when transitioning
// from PUBLIC to ABSENT:
//   - go via DROPPED iff they're descriptor or data elements;
//   - go via a non-read status iff they're indexes or columns, which are
//     subject to the two-version invariant;
//   - go direct to ABSENT in all other cases.
func checkToAbsentCategories(e scpb.Element) error {
	s0 := opgen.InitialStatus(e, scpb.Status_ABSENT)
	s1 := opgen.NextStatus(e, scpb.Status_ABSENT, s0)
	switch s1 {
	case scpb.Status_DROPPED:
		if isDescriptor(e) || isData(e) {
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
	return errors.Newf("unexpected transition %s -> %s in direction ABSENT", s0, s1)
}

// Assert that isWithTypeT covers all elements with embedded TypeTs.
func checkIsWithTypeT(e scpb.Element) error {
	return screl.WalkTypes(e, func(t *types.T) error {
		if isWithTypeT(e) {
			return nil
		}
		return errors.New("should verify isWithTypeT but doesn't")
	})
}

// Assert that isWithExpression covers all elements with embedded
// expressions.
func checkIsWithExpression(e scpb.Element) error {
	return screl.WalkExpressions(e, func(t *catpb.Expression) error {
		switch e.(type) {
		// Ignore elements which have catpb.Expression fields but which don't
		// have them within an scpb.Expression for valid reasons.
		case *scpb.RowLevelTTL:
			return nil
		}
		if isWithExpression(e) {
			return nil
		}
		return errors.New("should verify isWithExpression but doesn't")
	})
}

// Assert that isColumnDependent covers all dependent elements of a column
// element.
func checkIsColumnDependent(e scpb.Element) error {
	// Exclude columns themselves.
	if isColumn(e) {
		return nil
	}
	// A column dependent should have a ColumnID attribute.
	_, err := screl.Schema.GetAttribute(screl.ColumnID, e)
	if isColumnDependent(e) {
		if err != nil {
			return errors.New("verifies isColumnDependent but doesn't have ColumnID attr")
		}
	} else if err == nil {
		return errors.New("has ColumnID attr but doesn't verify isColumnDependent")
	}
	return nil
}

// Assert that isIndexDependent covers all dependent elements of an index
// element.
func checkIsIndexDependent(e scpb.Element) error {
	// Exclude indexes themselves and their data.
	if isIndex(e) || isData(e) || isNonIndexBackedConstraint(e) {
		return nil
	}
	// An index dependent should have an IndexID attribute.
	_, err := screl.Schema.GetAttribute(screl.IndexID, e)
	if isIndexDependent(e) {
		if err != nil {
			return errors.New("verifies isIndexDependent but doesn't have IndexID attr")
		}
	} else if err == nil {
		return errors.New("has IndexID attr but doesn't verify isIndexDependent")
	}
	return nil
}

// Assert that checkIsConstraintDependent covers all elements of a constraint
// element.
func checkIsConstraintDependent(e scpb.Element) error {
	// Exclude constraints themselves.
	if isConstraint(e) {
		return nil
	}
	// A constraint dependent should have a ConstraintID attribute.
	_, err := screl.Schema.GetAttribute(screl.ConstraintID, e)
	if isConstraintDependent(e) {
		if err != nil {
			return errors.New("verifies isConstraintDependent but doesn't have ConstraintID attr")
		}
	} else if err == nil {
		return errors.New("has ConstraintID attr but doesn't verify isConstraintDependent")
	}
	return nil
}

// Assert the following partitions about constraints:
//  1. An element `e` with ConstraintID attr is either a constraint
//     or a constraint dependent.
//  2. A constraint is either index-backed or non-index-backed.
//
// TODO (xiang): Add test for cross-descriptor partition. We currently
// cannot have them until we added referenced.*ID attr for
// UniqueWithoutIndex[NotValid] element, which is required to support
// partial unique without index constraint with a predicate that references
// other descriptors.
func checkConstraintPartitions(e scpb.Element) error {
	_, err := screl.Schema.GetAttribute(screl.ConstraintID, e)
	if err != nil {
		return nil //nolint:returnerrcheck
	}
	if !isConstraint(e) && !isConstraintDependent(e) {
		return errors.New("has ConstraintID attr but is not a constraint nor a constraint dependent")
	}
	if isConstraintDependent(e) {
		return nil
	}
	if !isNonIndexBackedConstraint(e) && !isIndex(e) {
		return errors.New("verifies isConstraint but does not verify isNonIndexBackedConstraint nor isIndex")
	}
	return nil
}

// Assert that checkIsTriggerDependent covers all elements of a trigger element.
func checkIsTriggerDependent(e scpb.Element) error {
	// Exclude triggers themselves.
	switch e.(type) {
	case *scpb.Trigger:
		return nil
	}
	// A trigger dependent should have a TriggerID attribute.
	_, err := screl.Schema.GetAttribute(screl.TriggerID, e)
	if isTriggerDependent(e) {
		if err != nil {
			return errors.New("verifies isTriggerDependent but doesn't have TriggerID attr")
		}
	} else if err == nil {
		return errors.New("has TriggerID attr but doesn't verify isTriggerDependent")
	}
	return nil
}
