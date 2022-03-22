// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package catpb

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
