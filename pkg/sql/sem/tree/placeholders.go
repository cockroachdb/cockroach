// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import (
	"bytes"
	"fmt"
	"math"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/redact"
)

// PlaceholderIdx is the 0-based index of a placeholder. Placeholder "$1"
// has PlaceholderIdx=0.
type PlaceholderIdx uint16

var _ redact.SafeValue = PlaceholderIdx(0)

// MaxPlaceholderIdx is the maximum allowed value of a PlaceholderIdx.
// The pgwire protocol is limited to 2^16 placeholders, so we limit the IDs to
// this range as well.
const MaxPlaceholderIdx = math.MaxUint16

// SafeValue implements the redact.SafeValue interface.
func (idx PlaceholderIdx) SafeValue() {}

// String returns the index as a placeholder string representation ($1, $2 etc).
func (idx PlaceholderIdx) String() string {
	return fmt.Sprintf("$%d", idx+1)
}

// PlaceholderTypes stores placeholder types (or type hints), one per
// PlaceholderIdx.  The slice is always pre-allocated to the number of
// placeholders in the statement. Entries that don't yet have a type are nil.
type PlaceholderTypes []*types.T

// Identical returns true if two PlaceholderTypes contain the same types.
func (pt PlaceholderTypes) Identical(other PlaceholderTypes) bool {
	if len(pt) != len(other) {
		return false
	}
	for i, t := range pt {
		switch {
		case t == nil && other[i] == nil:
		case t == nil || other[i] == nil:
			return false
		case !t.Identical(other[i]):
			return false
		}
	}
	return true
}

// AssertAllSet verifies that all types have been set and returns an error
// otherwise.
func (pt PlaceholderTypes) AssertAllSet() error {
	for i := range pt {
		if pt[i] == nil {
			return placeholderTypeAmbiguityError(PlaceholderIdx(i))
		}
	}
	return nil
}

// QueryArguments stores query arguments, one per PlaceholderIdx.
//
// A nil value represents a NULL argument.
type QueryArguments []TypedExpr

func (qa QueryArguments) String() string {
	if len(qa) == 0 {
		return "{}"
	}
	var buf bytes.Buffer
	buf.WriteByte('{')
	sep := ""
	for k, v := range qa {
		fmt.Fprintf(&buf, "%s%s:%q", sep, PlaceholderIdx(k), v)
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
func (p *PlaceholderTypesInfo) Type(idx PlaceholderIdx) (_ *types.T, ok bool, _ error) {
	if len(p.Types) <= int(idx) {
		return nil, false, NewNoValueProvidedForPlaceholderErr(idx)
	}
	t := p.Types[idx]
	if t == nil && len(p.TypeHints) > int(idx) {
		t = p.TypeHints[idx]
	}
	return t, t != nil, nil
}

// ValueType returns the type of the value that must be supplied for a placeholder.
// This is the type hint given by the client if there is one, or the placeholder
// type if there isn't one. This can differ from Type(idx) when a client hint is
// overridden (see Placeholder.Eval).
func (p *PlaceholderTypesInfo) ValueType(idx PlaceholderIdx) (_ *types.T, ok bool) {
	var t *types.T
	if len(p.TypeHints) >= int(idx) {
		t = p.TypeHints[idx]
	}
	if t == nil {
		t = p.Types[idx]
	}
	return t, (t != nil)
}

// SetType assigns a known type to a placeholder.
// Reports an error if another type was previously assigned.
func (p *PlaceholderTypesInfo) SetType(idx PlaceholderIdx, typ *types.T) error {
	if t := p.Types[idx]; t != nil {
		if !typ.Equivalent(t) {
			return pgerror.Newf(
				pgcode.DatatypeMismatch,
				"placeholder %s already has type %s, cannot assign %s", idx, t, typ)
		}
		// If `t` is not ambiguous or if `typ` is ambiguous, then we shouldn't
		// change the type that's already set. Otherwise, we can use `typ` since
		// it is more specific.
		if !t.IsAmbiguous() || typ.IsAmbiguous() {
			return nil
		}
	}
	p.Types[idx] = typ
	return nil
}

// PlaceholderInfo defines the interface to SQL placeholders.
type PlaceholderInfo struct {
	PlaceholderTypesInfo

	Values QueryArguments
}

// Init initializes a PlaceholderInfo structure appropriate for the given number
// of placeholders, and with the given (optional) type hints.
func (p *PlaceholderInfo) Init(numPlaceholders int, typeHints PlaceholderTypes) {
	if typeHints == nil {
		p.TypeHints = make(PlaceholderTypes, numPlaceholders)
		p.Types = make(PlaceholderTypes, numPlaceholders)
	} else {
		p.Types = make(PlaceholderTypes, len(typeHints))
		p.TypeHints = typeHints
	}
	p.Values = nil
}

// Assign resets the PlaceholderInfo to the contents of src.
// If src is nil, a new structure is initialized.
func (p *PlaceholderInfo) Assign(src *PlaceholderInfo, numPlaceholders int) {
	if src != nil {
		*p = *src
		return
	}
	p.Init(numPlaceholders, nil /* typeHints */)
}

// MaybeExtendTypes is to fill the nil types with the type hints, if exists.
func (p *PlaceholderInfo) MaybeExtendTypes() {
	if len(p.TypeHints) >= len(p.Types) {
		for i, t := range p.Types {
			if t == nil {
				p.Types[i] = p.TypeHints[i]
			}
		}
	}
}

// Value returns the known value of a placeholder.  Returns false in
// the 2nd value if the placeholder does not have a value.
func (p *PlaceholderInfo) Value(idx PlaceholderIdx) (TypedExpr, bool) {
	if len(p.Values) <= int(idx) || p.Values[idx] == nil {
		return nil, false
	}
	return p.Values[idx], true
}

// IsUnresolvedPlaceholder returns whether expr is an unresolved placeholder. In
// other words, it returns whether the provided expression is a placeholder
// expression or a placeholder expression within nested parentheses, and if so,
// whether the placeholder's type remains unset in the PlaceholderInfo.
func (p *PlaceholderInfo) IsUnresolvedPlaceholder(expr Expr) bool {
	if t, ok := StripParens(expr).(*Placeholder); ok {
		_, res, err := p.Type(t.Idx)
		return !(err == nil && res)
	}
	return false
}

// NewNoValueProvidedForPlaceholderErr constructs an error indicating a missing
// placeholder value.
func NewNoValueProvidedForPlaceholderErr(pIdx PlaceholderIdx) error {
	return pgerror.Newf(pgcode.UndefinedParameter,
		"no value provided for placeholder: $%d", pIdx+1,
	)
}
