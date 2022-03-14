// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package props_test

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/constraint"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
)

func fieldsString(val *reflect.Value) string {
	var sb strings.Builder
	for i, n := 0, val.Type().NumField(); i < n; i++ {
		if i > 0 {
			sb.WriteRune(';')
		}
		sb.WriteString(fmt.Sprintf(
			"%s:%s",
			val.Type().Field(i).Name,
			val.Type().Field(i).Type,
		))
	}
	return sb.String() + ";"
}

var propsMatchString = "Populated:bool;HasSubquery:bool;HasCorrelatedSubquery:bool;" +
	"VolatilitySet:props.VolatilitySet;CanMutate:bool;HasPlaceholder:bool;OuterCols:opt.ColSet;" +
	"Rule:struct { WithUses props.WithUsesMap };Shared:props.Shared;Constraints:*constraint.Set;" +
	"FuncDeps:props.FuncDepSet;TightConstraints:bool;Rule:struct { Available props.AvailableRuleProps;" +
	" HasHoistableSubquery bool };"

// TestScalarPropsChanged acts as a reminder to update the Equals and
// CopyFrom functions when props.Shared or props.Scalar changes.
func TestScalarPropsChanged(t *testing.T) {
	shared := &props.Shared{}
	scalar := &props.Scalar{}
	sharedVal := reflect.Indirect(reflect.ValueOf(shared))
	scalarVal := reflect.Indirect(reflect.ValueOf(scalar))
	checkString := fieldsString(&sharedVal) + fieldsString(&scalarVal)
	if propsMatchString != checkString {
		t.Errorf(
			`
expected: %s
actual:   %s 
Shared or Scalar properties has changed. 
Please make sure to update the Equals and CopyFrom
functions of struct props.Shared and props.Scalar.`, propsMatchString, checkString)
	}
}

var constraintMatchString = "Columns:constraint.Columns;Spans:constraint.Spans;firstConstraint:" +
	"constraint.Constraint;otherConstraints:[]constraint.Constraint;length:int32;contradiction:bool;"

// TestConstraintChanged acts as a reminder to update the Equals functions in
// Constraint and constraint.Set if new fields are added.
func TestConstraintChanged(t *testing.T) {
	c := &constraint.Constraint{}
	cs := &constraint.Set{}
	constraintVal := reflect.Indirect(reflect.ValueOf(c))
	constraintSetVal := reflect.Indirect(reflect.ValueOf(cs))
	checkString := fieldsString(&constraintVal) + fieldsString(&constraintSetVal)
	if constraintMatchString != checkString {
		t.Errorf(
			`
expected: %s
actual:   %s 
Constraint or constraint.Set fields have changed. 
Please make sure to update the Equals and CopyAndMaybeRemapConstraintSetWithColMap
functions in struct constraint.Constraint and constraint.Set.`, constraintMatchString, checkString)
	}
}
