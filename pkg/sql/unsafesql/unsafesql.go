// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafesql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/crlib/crtime"
	"github.com/cockroachdb/redact"
)

// The accessedLogLimiter is used to limit the rate of logging unsafe internal access
// events. It is set to allow ten events per second.
var accessedLogLimiter = log.Every(time.Millisecond * 100)

// The deniedLogLimiter is used to limit the rate of logging unsafe internal access
// events. It is set to allow ten events per second.
var deniedLogLimiter = log.Every(time.Millisecond * 100)

// TestRemoveLimiter overrides the logLimiter so that all logs will
// be emitted, so that the tests can be run in short sequence with each other.
func TestRemoveLimiters() func() {
	accessedLogLimiter = log.Every(0)
	deniedLogLimiter = log.Every(0)
	return func() {
		accessedLogLimiter = log.Every(time.Millisecond * 100)
		deniedLogLimiter = log.Every(time.Millisecond * 100)
	}
}

// CheckInternalsAccess checks if the current session has permission to access
// unsafe internal tables and functionality. This includes system tables and
// virtual tables / builtins in the crdb_internal schema.
func CheckInternalsAccess(
	ctx context.Context,
	sd *sessiondata.SessionData,
	stmt tree.Statement,
	ann *tree.Annotations,
	sv *settings.Values,
	override func() *bool,
) error {
	allowUnsafe := sd.AllowUnsafeInternals
	if override != nil && override() != nil {
		allowUnsafe = *override()
	}

	// If the querier is internal, we should allow it.
	if sd.Internal || sd.IsInternalAppName() {
		return nil
	}

	q := SafeFormatQuery(stmt, ann, sv)
	// If an override is set, allow access to this virtual table.
	if allowUnsafe {
		// Log this access to the SENSITIVE_ACCESS channel since the override condition bypassed normal access controls.
		if accessedLogLimiter.ShouldProcess(crtime.NowMono()) {
			log.StructuredEvent(ctx, severity.WARNING, &eventpb.UnsafeInternalsAccessed{Query: q})
		}
		return nil
	}

	// Log this access to the SENSITIVE_ACCESS channel to show where failing internals accesses are happening.
	if deniedLogLimiter.ShouldProcess(crtime.NowMono()) {
		log.StructuredEvent(ctx, severity.WARNING, &eventpb.UnsafeInternalsDenied{Query: q})
	}

	return sqlerrors.ErrUnsafeTableAccess
}

// SafeFormatQuery attempts to format the query for logging, but recovers from
// any panics that may occur during formatting.
func SafeFormatQuery(
	stmt tree.Statement, ann *tree.Annotations, sv *settings.Values,
) (s redact.RedactableString) {
	if stmt == nil {
		return "<nil statement>"
	}
	defer func() {
		if r := recover(); r != nil {
			log.Dev.Errorf(context.TODO(), "panic in SafeFormatQuery: %v", r)
			s = "<panicked query format>"
		}
	}()
	return tree.FormatAstAsRedactableString(stmt, ann, sv)
}
