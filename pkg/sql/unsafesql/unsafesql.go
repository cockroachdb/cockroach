// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package unsafesql

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/eventpb"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

var TestOverrideAllowUnsafeInternals bool

// CheckInternalsAccess checks if the current session has permission to access
// unsafe internal tables and functionality. This includes system tables and
// virtual tables / builtins in the crdb_internal schema.
func CheckInternalsAccess(
	ctx context.Context,
	sd *sessiondata.SessionData,
	stmt tree.Statement,
	ann *tree.Annotations,
	sv *settings.Values,
) error {
	// If the querier is internal, we should allow it.
	if sd.Internal {
		return nil
	}

	if TestOverrideAllowUnsafeInternals {
		return nil
	}

	q := tree.FormatAstAsRedactableString(stmt, ann, sv)
	// If an override is set, allow access to this virtual table.
	if sd.AllowUnsafeInternals {
		// Log this access to the SENSITIVE_ACCESS channel since the override condition bypassed normal access controls.
		if accessedLogLimiter.ShouldProcess(timeutil.Now()) {
			log.StructuredEvent(ctx, severity.WARNING, &eventpb.UnsafeInternalsAccessed{Query: q})
		}
		return nil
	}

	qq(string(debug.Stack()))
	qq("failing query", q)
	// Log this access to the SENSITIVE_ACCESS channel to show where failing internals accesses are happening.
	if deniedLogLimiter.ShouldProcess(timeutil.Now()) {
		log.StructuredEvent(ctx, severity.WARNING, &eventpb.UnsafeInternalsDenied{Query: q})
	}
	return sqlerrors.ErrUnsafeTableAccess
}

func qq(args ...any) {
	tempDir := os.TempDir()
	file, err := os.OpenFile(tempDir+"/q", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	templateArr := []string{}
	for range args {
		templateArr = append(templateArr, "%v")
	}
	template := strings.Join(templateArr, " ") + "\n"
	text := fmt.Sprintf(template, args...)
	_, err = file.WriteString(text)
	if err != nil {
		panic(err)
	}
}
