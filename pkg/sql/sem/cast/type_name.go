// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	}
	return strings.ToLower(t.SQLString())
}
