package catenumpb

import (
	"strconv"

	"github.com/cockroachdb/redact"
)

// String implements the fmt.Stringer interface.
func (x ForeignKeyAction) String() string {
	switch x {
	case ForeignKeyAction_RESTRICT:
		return "RESTRICT"
	case ForeignKeyAction_SET_DEFAULT:
		return "SET DEFAULT"
	case ForeignKeyAction_SET_NULL:
		return "SET NULL"
	case ForeignKeyAction_CASCADE:
		return "CASCADE"
	default:
		return strconv.Itoa(int(x))
	}
}

var _ redact.SafeValue = ForeignKeyAction(0)

// SafeValue implements redact.SafeValue.
func (x ForeignKeyAction) SafeValue() {}

// String implements the fmt.Stringer interface.
func (x Match) String() string {
	switch x {
	case Match_SIMPLE:
		return "MATCH SIMPLE"
	case Match_FULL:
		return "MATCH FULL"
	case Match_PARTIAL:
		return "MATCH PARTIAL"
	default:
		return strconv.Itoa(int(x))
	}
}
