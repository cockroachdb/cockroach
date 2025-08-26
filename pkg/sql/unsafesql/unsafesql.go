// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafesql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
)

var Limiter = log.Every(time.Millisecond * 1000)

// CheckInternalsAccess checks if the current session has permission to access
// unsafe internal tables and functionality. This includes system tables and
// virtual tables / builtins in the crdb_internal schema.
func CheckInternalsAccess(
	ctx context.Context, sd *sessiondata.SessionData, q redact.RedactableString,
) error {
	// If the querier is internal, we should allow it.
	if sd.Internal {
		return nil
	}

	// If an override is set, allow access to this virtual table.
	if sd.AllowUnsafeInternals {
		// Log this access to the SQL_EXEC channel since the override condition bypassed normal access controls.
		if Limiter.ShouldProcess(timeutil.Now()) {
			log.StructuredEvent(ctx, severity.WARNING, &eventpb.UnsafeInternalsAccess{Query: q})
		}
		return nil
	}

	return sqlerrors.ErrUnsafeTableAccess
}
