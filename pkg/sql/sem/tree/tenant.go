// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tree

import "strconv"

// TenantID represents a tenant ID that can be pretty-printed.
type TenantID struct {
	ID uint64

	// Specified is set to true when the TENANT clause was specified in
	// the backup target input syntax. We need this, instead of relying
	// on ID != 0, because we need a special marker for
	// the case when the value was anonymized. In other places, the
	// value used for anonymized integer literals is 0, but we can't use
	// 0 for TenantID as this is refused during parsing, nor can we use
	// any other value since all non-zero integers are valid tenant IDs.
	Specified bool
}

var _ NodeFormatter = (*TenantID)(nil)

// IsSet returns whether the TenantID is set.
func (t *TenantID) IsSet() bool {
	return t.Specified && t.ID != 0
}

// Format implements the NodeFormatter interface.
func (t *TenantID) Format(ctx *FmtCtx) {
	if ctx.flags.HasFlags(FmtHideConstants) || !t.IsSet() {
		// The second part of the condition above is conceptually
		// redundant, but is sadly needed here because we need to be able
		// to re-print a TENANT clause after it has been anonymized,
		// and we can't emit the value zero which is unparseable.
		ctx.WriteByte('_')
	} else {
		ctx.WriteString(strconv.FormatUint(t.ID, 10))
	}
}
