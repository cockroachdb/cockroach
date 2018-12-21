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

package tree

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util"
)

// PlaceholderTypes relates placeholder names to their resolved type.
type PlaceholderTypes map[types.PlaceholderIdx]types.T

// Equals returns true if two PlaceholderTypes contain the same types.
func (pt PlaceholderTypes) Equals(other PlaceholderTypes) bool {
	if len(pt) != len(other) {
		return false
	}
	for i, t := range pt {
		otherT, ok := other[i]
		if !ok || !t.Equivalent(otherT) {
			return false
		}
	}
	return true
}

// QueryArguments relates placeholder names to their provided query argument.
//
// A nil value represents a NULL argument.
type QueryArguments map[types.PlaceholderIdx]TypedExpr

var emptyQueryArgumentStr = "{}"

func (qa *QueryArguments) String() string {
	if len(*qa) == 0 {
		return emptyQueryArgumentStr
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	sep := ""
	for k, v := range *qa {
		fmt.Fprintf(&buf, "%s%s:%q", sep, k, v)
		sep = ", "
	}
	buf.WriteByte('}')
	return buf.String()
}

// PlaceholderTypesInfo encapsulates typing information for placeholders.
type PlaceholderTypesInfo struct {
	// TypeHints contains the initially set type hints for each placeholder if
	// present. It is not changed during query type checking.
	TypeHints PlaceholderTypes
	// Types contains the final types set for each placeholder after type
	// checking.
	Types PlaceholderTypes
}

// Type returns the known type of a placeholder. If there is no known type yet
// but there is a type hint, returns the type hint.
func (p *PlaceholderTypesInfo) Type(idx types.PlaceholderIdx) (_ types.T, ok bool) {
	if t, ok := p.Types[idx]; ok {
		return t, true
	} else if t, ok := p.TypeHints[idx]; ok {
		return t, true
	}
	return nil, false
}

// ValueType returns the type of the value that must be supplied for a placeholder.
// This is the type hint given by the client if there is one, or the placeholder
// type if there isn't one. This can differ from Type(idx) when a client hint is
// overridden (see Placeholder.Eval).
func (p *PlaceholderTypesInfo) ValueType(idx types.PlaceholderIdx) (_ types.T, ok bool) {
	if t, ok := p.TypeHints[idx]; ok {
		return t, true
	} else if t, ok := p.Types[idx]; ok {
		return t, true
	}
	return nil, false

}

// SetType assigns a known type to a placeholder.
// Reports an error if another type was previously assigned.
func (p *PlaceholderTypesInfo) SetType(idx types.PlaceholderIdx, typ types.T) error {
	if t, ok := p.Types[idx]; ok {
		if !typ.Equivalent(t) {
			return pgerror.NewErrorf(
				pgerror.CodeDatatypeMismatchError,
				"placeholder %s already has type %s, cannot assign %s", idx, t, typ)
		}
		return nil
	}
	p.Types[idx] = typ
	return nil
}

// PlaceholderInfo defines the interface to SQL placeholders.
type PlaceholderInfo struct {
	PlaceholderTypesInfo

	Values QueryArguments

	// permitUnassigned controls whether AssertAllAssigned returns an error when
	// there are unassigned placeholders. See PermitUnassigned().
	permitUnassigned bool
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
	p.permitUnassigned = false
}

// Reset resets the type and values in the map and replaces the type hints map
// by an alias to typeHints. If typeHints is nil, the map is cleared.
func (p *PlaceholderInfo) Reset(typeHints PlaceholderTypes) {
	if typeHints != nil {
		p.TypeHints = typeHints
		p.Types = PlaceholderTypes{}
		p.Values = QueryArguments{}
	} else {
		p.Clear()
	}
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

// PermitUnassigned permits unassigned placeholders during plan construction,
// so that EXPLAIN can work on statements with placeholders.
func (p *PlaceholderInfo) PermitUnassigned() {
	p.permitUnassigned = true
}

// AssertAllAssigned ensures that all placeholders that are used also have a
// value assigned, or that PermitUnassigned was called.
func (p *PlaceholderInfo) AssertAllAssigned() error {
	if p.permitUnassigned {
		return nil
	}
	var missing []string
	for pn := range p.Types {
		if _, ok := p.Values[pn]; !ok {
			missing = append(missing, pn.String())
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

// Value returns the known value of a placeholder.  Returns false in
// the 2nd value if the placeholder does not have a value.
func (p *PlaceholderInfo) Value(idx types.PlaceholderIdx) (TypedExpr, bool) {
	if v, ok := p.Values[idx]; ok {
		return v, true
	}
	return nil, false
}

// IsUnresolvedPlaceholder returns whether expr is an unresolved placeholder. In
// other words, it returns whether the provided expression is a placeholder
// expression or a placeholder expression within nested parentheses, and if so,
// whether the placeholder's type remains unset in the PlaceholderInfo.
func (p *PlaceholderInfo) IsUnresolvedPlaceholder(expr Expr) bool {
	if t, ok := StripParens(expr).(*Placeholder); ok {
		_, res := p.Type(t.Idx)
		return !res
	}
	return false
}
