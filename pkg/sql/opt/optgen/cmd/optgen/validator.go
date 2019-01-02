// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

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
		// That is, there can be zero or more expression-typed fields, followed
		// by zero or one private field.
		for i, field := range define.Fields {
			typ := md.typeOf(field)
			if typ == nil {
				format := "%s is not registered as a valid type in metadata.go"
				v.addErrorf(field.Source(), format, field.Type)
				continue
			}

			if !typ.isExpr {
				if i != len(define.Fields)-1 {
					format := "private field '%s' is not the last field in '%s'"
					v.addErrorf(field.Source(), format, field.Name, define.Name)
					break
				}
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
