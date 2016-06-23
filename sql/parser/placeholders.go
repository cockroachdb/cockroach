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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import "fmt"

// PlaceholderTypes relates placeholder names to their resolved type.
type PlaceholderTypes map[string]Datum

// QueryArguments relates placeholder names to their provided query argument.
type QueryArguments map[string]Datum

// PlaceholderInfo defines the interface to SQL placeholders.
type PlaceholderInfo struct {
	Values QueryArguments
	Types  PlaceholderTypes
}

// NewPlaceholderInfo constructs an empty PlaceholderInfo.
func NewPlaceholderInfo() *PlaceholderInfo {
	res := &PlaceholderInfo{}
	res.Clear()
	return res
}

// Clear resets the placeholder info map.
func (p *PlaceholderInfo) Clear() {
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

// Type returns the known type of a placeholder.
// Returns false in the 2nd value if the placeholder is not typed.
func (p *PlaceholderInfo) Type(name string) (Datum, bool) {
	if t, ok := p.Types[name]; ok {
		return t, true
	}
	return nil, false
}

// Value returns the known value of a placeholder.  Returns false in
// the 2nd value if the placeholder does not have a value.
func (p *PlaceholderInfo) Value(name string) (Datum, bool) {
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
		p.Types[name] = val.ReturnType()
	}
}

// SetType assignes a known type to a placeholder.
// Reports an error if another type was previously assigned.
func (p *PlaceholderInfo) SetType(name string, typ Datum) error {
	if t, ok := p.Types[name]; ok && !typ.TypeEqual(t) {
		return fmt.Errorf("placeholder %s already has type %s, cannot assign %s", name, t.Type(), typ.Type())
	}
	p.Types[name] = typ
	return nil
}

// SetTypes resets the type and values in the map and replaces the
// types map by an alias to src. If src is nil, the map is cleared.
// The types map is aliased because the invoking code from
// pgwire/v3.go for sql.Prepare needs to receive the updated type
// assignments after Prepare completes.
func (p *PlaceholderInfo) SetTypes(src PlaceholderTypes) {
	if src != nil {
		p.Types = src
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
	if t, ok := StripParens(expr).(Placeholder); ok {
		_, res := p.Types[t.Name]
		return !res
	}
	return false
}
