// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cast

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/lib/pq/oid"
)

// CastTypeName returns the name of the type used for casting.
func CastTypeName(t *types.T) string {
	// SQLString is wrong for these types.
	switch t.Oid() {
	case oid.T_numeric:
		// SQLString returns `decimal`
		return "numeric"
	case oid.T_char:
		// SQLString returns `"char"`
		return "char"
	case oid.T_bpchar:
		// SQLString returns `char`.
		return "bpchar"
	case oid.T_text:
		// SQLString returns `string`
		return "text"
	case oid.T_bit:
		// SQLString returns `decimal`
		return "bit"
	}
	return strings.ToLower(t.SQLString())
}
