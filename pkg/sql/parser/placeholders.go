// Copyright 2016 The Cockroach Authors.
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

package parser

import (
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// PlaceholderTypes relates placeholder names to their resolved type.
type PlaceholderTypes map[string]Type

// QueryArguments relates placeholder names to their provided query argument.
type QueryArguments map[string]TypedExpr

// PlaceholderInfo defines the interface to SQL placeholders.
type PlaceholderInfo struct {
	Values QueryArguments
	// TypeHints contains the initially set type hints for each placeholder if
	// present, and will be filled in completely by the end of type checking
	// Hints that were present before type checking will not change, and hints
	// that were not present before type checking will be set to their
	// placeholder's inferred type.
	TypeHints PlaceholderTypes
	// Types contains the final types set for each placeholder after type
	// checking.
	Types PlaceholderTypes
}

// MakePlaceholderInfo constructs an empty PlaceholderInfo.
func MakePlaceholderInfo() PlaceholderInfo {
	res := PlaceholderInfo{}
	res.Clear()
	return res
}

// Clear resets the placeholder info map.
func (p *PlaceholderInfo) Clear() {
	p.TypeHints = PlaceholderTypes{}
	p.Types = PlaceholderTypes{}
	p.Values = QueryArguments{}
}

// Assign resets the PlaceholderInfo to the contents of src.
// If src is nil, the map is cleared.
func (p *PlaceholderInfo) Assign(src *PlaceholderInfo) {
	if src != nil {
		*p = *src
	} else {
		p.Clear()
	}
}

// FillUnassigned sets all unsassigned placeholders to NULL.
func (p *PlaceholderInfo) FillUnassigned() {
	for pn := range p.Types {
		if _, ok := p.Values[pn]; !ok {
			p.Values[pn] = DNull
		}
	}
}

// AssertAllAssigned ensures that all placeholders that are used also
// have a value assigned.
func (p *PlaceholderInfo) AssertAllAssigned() error {
	var missing []string
	for pn := range p.Types {
		if _, ok := p.Values[pn]; !ok {
			missing = append(missing, "$"+pn)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return pgerror.NewErrorf(pgerror.CodeUndefinedParameterError,
			"no value provided for placeholder%s: %s",
			util.Pluralize(int64(len(missing))),
			strings.Join(missing, ", "),
		)
	}
	return nil
}

// Type returns the known type of a placeholder. If allowHints is true, will
// return a type hint if there's no known type yet but there is a type hint.
// Returns false in the 2nd value if the placeholder is not typed.
func (p *PlaceholderInfo) Type(name string, allowHints bool) (Type, bool) {
	if t, ok := p.Types[name]; ok {
		return t, true
	} else if t, ok := p.TypeHints[name]; ok {
		return t, true
	}
	return nil, false
}

// Value returns the known value of a placeholder.  Returns false in
// the 2nd value if the placeholder does not have a value.
func (p *PlaceholderInfo) Value(name string) (TypedExpr, bool) {
	if v, ok := p.Values[name]; ok {
		return v, true
	}
	return nil, false
}

// SetValue assigns a known value to a placeholder.
// If no type is known yet, it is inferred from the assigned value.
func (p *PlaceholderInfo) SetValue(name string, val Datum) {
	if _, ok := p.Values[name]; ok {
		panic(fmt.Sprintf("placeholder $%s already has a value", name))
	}
	p.Values[name] = val
	if _, ok := p.Types[name]; !ok {
		// No type yet, infer from value
		p.Types[name] = val.ResolvedType()
	}
}

// SetType assigns a known type to a placeholder.
// Reports an error if another type was previously assigned.
func (p *PlaceholderInfo) SetType(name string, typ Type) error {
	t, ok := p.Types[name]
	if ok && !typ.Equivalent(t) {
		return pgerror.NewErrorf(pgerror.CodeDatatypeMismatchError, "placeholder %s already has type %s, cannot assign %s", name, t, typ)
	}
	p.Types[name] = typ
	if _, ok := p.TypeHints[name]; !ok {
		// If the client didn't give us a type hint, we must communicate our
		// inferred type to pgwire so it can know how to parse incoming data.
		p.TypeHints[name] = typ
	}
	return nil
}

// SetTypeHints resets the type and values in the map and replaces the
// type hints map by an alias to src. If src is nil, the map is cleared.
// The type hints map is aliased because the invoking code from
// pgwire/v3.go for sql.Prepare needs to receive the updated type
// assignments after Prepare completes.
func (p *PlaceholderInfo) SetTypeHints(src PlaceholderTypes) {
	if src != nil {
		p.TypeHints = src
		p.Types = PlaceholderTypes{}
		p.Values = QueryArguments{}
	} else {
		p.Clear()
	}
}

// IsUnresolvedPlaceholder returns whether expr is an unresolved placeholder. In
// other words, it returns whether the provided expression is a placeholder
// expression or a placeholder expression within nested parentheses, and if so,
// whether the placeholder's type remains unset in the PlaceholderInfo.
func (p *PlaceholderInfo) IsUnresolvedPlaceholder(expr Expr) bool {
	if t, ok := StripParens(expr).(*Placeholder); ok {
		_, res := p.TypeHints[t.Name]
		return !res
	}
	return false
}

// placeholdersVisitor is a Visitor implementation used to
// replace placeholders with their supplied values.
type placeholdersVisitor struct {
	placeholders PlaceholderInfo
	err          error
}

var _ Visitor = &placeholdersVisitor{}

func (v *placeholdersVisitor) VisitPre(expr Expr) (recurse bool, newNode Expr) {
	switch t := expr.(type) {
	case *Placeholder:
		if e, ok := v.placeholders.Value(t.Name); ok {
			// Placeholder expressions cannot contain other placeholders, so we do
			// not need to recurse.
			typ, typed := v.placeholders.Type(t.Name, false)
			if !typed {
				// All placeholders should be typed at this point.
				v.err = pgerror.NewErrorf(pgerror.CodeInternalError, "missing type for placeholder %s", t.Name)
				return false, e
			}
			if !e.ResolvedType().Equivalent(typ) {
				// This happens when we overrode the placeholder's type during type
				// checking, since the placeholder's type hint didn't match the desired
				// type for the placeholder. In this case, we cast the expression to
				// the desired type.
				// TODO(jordan): introduce a restriction on what casts are allowed here.
				colType, err := DatumTypeToColumnType(typ)
				if err != nil {
					v.err = err
					return false, e
				}
				return false, &CastExpr{Expr: e, Type: colType}
			}
			return false, e
		}
	}
	return true, expr
}

func (*placeholdersVisitor) VisitPost(expr Expr) Expr { return expr }

// replacePlaceholders replaces all placeholders in the input expression with
// their supplied values in the SemaContext's Placeholders map. If there is no
// available value for a placeholder, it is left alone. A nil ctx makes
// this a no-op and is supported for tests only.
func replacePlaceholders(expr Expr, ctx *SemaContext) (Expr, error) {
	// We don't need to recurse through the input if there are no values to
	// replace, since the walk above will do no work in that case. This happens
	// during typechecking during the prepare phase. Missing placeholder values
	// during execute are detected before this step.
	if ctx == nil || len(ctx.Placeholders.Values) == 0 {
		return expr, nil
	}
	v := &placeholdersVisitor{placeholders: ctx.Placeholders}

	expr, _ = WalkExpr(v, expr)
	return expr, v.err
}
