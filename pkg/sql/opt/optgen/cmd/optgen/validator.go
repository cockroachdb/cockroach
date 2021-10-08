// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
)

type validator struct {
	errors []error
}

// validate performs additional checks on the compiled Optgen expression. In
// particular, it checks the order and types of the fields in define
// expressions. The Optgen language itself allows any field order and types, so
// the compiler does not do these checks.
func (v *validator) validate(compiled *lang.CompiledExpr) []error {
	md := newMetadata(compiled, "")

	for _, rule := range compiled.Rules {
		if !rule.Tags.Contains("Normalize") && !rule.Tags.Contains("Explore") {
			v.addErrorf(rule.Source(), "%s rule is missing \"Normalize\" or \"Explore\" tag", rule.Name)
		}
	}

	for _, define := range compiled.Defines.WithoutTag("Private") {
		// 1. Ensure that fields have a non-nil type.
		// 2. Ensure that fields are defined in the following order:
		//      Expr*
		//      Private?
		//      unexported*
		// That is, there can be zero or more expression-typed fields, followed
		// by zero or one private field, followed by zero or more unexported fields.
		// The unexported fields are initialized separately
		var exprsDone, privateDone bool
		for _, field := range define.Fields {
			typ := md.typeOf(field)
			if typ == nil {
				format := "%s is not registered as a valid type in optgen/metadata.go"
				v.addErrorf(field.Source(), format, field.Type)
				continue
			}

			if typ.isExpr && exprsDone {
				format := "expression field '%s' cannot follow private or unexported fields in '%s'"
				v.addErrorf(field.Source(), format, field.Name, define.Name)
				break
			}

			if !typ.isExpr {
				exprsDone = true

				if isExportedField(field) || isEmbeddedField(field) {
					// Tolerate a Typ field for Scalars (even if there was a Private
					// field).
					if !(define.Tags.Contains("Scalar") && field.Name == "Typ" && field.Type == "Type") {
						// Private definition.
						if privateDone {
							format := "private field '%s' cannot follow private or unexported field in '%s'"
							v.addErrorf(field.Source(), format, field.Name, define.Name)
							break
						}
					}
				}
				// This is either a private definition, a Typ field, or an unexported
				// field. In either case, we can no longer accept a private definition.
				privateDone = true
			}
		}
	}

	var visitRules func(e lang.Expr) lang.Expr
	visitRules = func(e lang.Expr) lang.Expr {
		switch t := e.(type) {
		case *lang.ListExpr:
			// Ensure that data type references a List operator.
			extType := t.Typ.(*lang.ExternalDataType)
			if typ := md.lookupType(extType.Name); typ == nil || typ.listItemType == nil {
				v.addErrorf(t.Source(), "list match operator cannot match field of type %s", extType.Name)
			}
		}

		return e.Visit(visitRules)
	}

	visitRules(&compiled.Rules)

	return v.errors
}

// addErrorf adds a formatted error to the error collection if it's not already
// there.
func (v *validator) addErrorf(src *lang.SourceLoc, format string, args ...interface{}) {
	errText := fmt.Sprintf(format, args...)
	err := fmt.Errorf("%s: %s", src, errText)

	for _, existing := range v.errors {
		if err.Error() == existing.Error() {
			return
		}
	}
	v.errors = append(v.errors, err)
}
