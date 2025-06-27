// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scerrors

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// EventLogger is a convenience object used for logging schema changer events.
type EventLogger struct {
	msg   redact.SafeValue
	start time.Time
}

// StartEventf logs the start of a schema change event, and returns an object
// with a HandlePanicAndLogError method to handle panics and log errors at the
// end of the event.
//
// Typical usage is along the lines of:
// - defer StartEventf(...).HandlePanicAndLogError(...)
func StartEventf(
	ctx context.Context, level log.Level, format string, args ...interface{},
) EventLogger {
	msg := redact.Safe(fmt.Sprintf(format, args...))
	// Use depth=1 since we want to log as the caller of StartEventf.
	const depth = 1
	if log.VDepth(level, depth) {
		log.InfofDepth(ctx, depth, "%s", msg)
	}
	return EventLogger{
		msg:   msg,
		start: timeutil.Now(),
	}
}

// HandlePanicAndLogError handles panics by recovering them in an error,
// which it then also logs. See also StartEventf.
func (el EventLogger) HandlePanicAndLogError(ctx context.Context, err *error) {
	switch recErr := recover().(type) {
	case nil:
		// No panicked error.
	case runtime.Error:
		*err = errors.WithAssertionFailure(recErr)
	case error:
		*err = recErr
	default:
		*err = errors.AssertionFailedf("recovered from uncategorizable panic: %v", recErr)
	}
	if *err == nil {
		*err = ctx.Err()
	}
	if errors.Is(*err, context.Canceled) {
		return
	}
	// We use a depth of 2 because this function is generally called with defer;
	// using a depth of 1 would show that the caller was runtime/panic.go.
	const depth = 2
	switch {
	case *err == nil:
		if log.ExpensiveLogEnabled(ctx, 2) {
			log.InfofDepth(ctx, depth, "done %s in %s", el.msg, redact.Safe(timeutil.Since(el.start)))
		}
	case HasNotImplemented(*err):
		log.VEventfDepth(ctx, depth, 1, "declarative schema changer does not support %s: %v", el.msg, *err)
	case errors.HasAssertionFailure(*err):
		*err = errors.Wrapf(*err, "%s", el.msg)
		fallthrough
	default:
		log.WarningfDepth(ctx, depth, "failed %s with error: %v", el.msg, *err)
	}
}

type notImplementedError struct {
	n      tree.NodeFormatter
	detail string
}

// TODO(ajwerner): Deal with redaction.

var _ error = (*notImplementedError)(nil)

// HasNotImplemented returns true if the error indicates that the builder does
// not support the provided statement.
func HasNotImplemented(err error) bool {
	return errors.HasType(err, (*notImplementedError)(nil))
}

func (e *notImplementedError) Error() string {
	var buf strings.Builder
	fmt.Fprintf(&buf, "%T not implemented in the new schema changer", e.n)
	if e.detail != "" {
		fmt.Fprintf(&buf, ": %s", e.detail)
	}
	return buf.String()
}

// NotImplementedError returns an error for which HasNotImplemented would
// return true.
func NotImplementedError(n tree.NodeFormatter) error {
	return &notImplementedError{n: n}
}

// NotImplementedErrorf returns an error for which HasNotImplemented would
// return true.
func NotImplementedErrorf(n tree.NodeFormatter, fmtstr string, args ...interface{}) error {
	return &notImplementedError{n: n, detail: fmt.Sprintf(fmtstr, args...)}
}

// concurrentSchemaChangeError indicates that building the schema change plan
// is not currently possible because there are other concurrent schema changes
// on one of the descriptors.
type concurrentSchemaChangeError struct {
	// TODO(ajwerner): Instead of waiting for one descriptor at a time, we should
	// get all the IDs of the descriptors we might be waiting for and return them
	// from the builder.
	descID descpb.ID
}

var _ pgerror.ClientVisibleRetryError = (*concurrentSchemaChangeError)(nil)

// ClientVisibleRetryError is detected by the pgwire layer and will convert
// this error into a serialization error to be retried. See
// pgcode.ClientVisibleRetryError.
func (e *concurrentSchemaChangeError) ClientVisibleRetryError() {}

func (e *concurrentSchemaChangeError) Error() string {
	return fmt.Sprintf("descriptor %d is undergoing another schema change", e.descID)
}

// ConcurrentSchemaChangeDescID returns the ID of the descriptor which is
// undergoing a concurrent schema change, according to err, if applicable.
func ConcurrentSchemaChangeDescID(err error) descpb.ID {
	cscErr := (*concurrentSchemaChangeError)(nil)
	if !errors.As(err, &cscErr) {
		return descpb.InvalidID
	}
	return cscErr.descID
}

// ConcurrentSchemaChangeError returns a concurrent schema change error for the
// given table.
func ConcurrentSchemaChangeError(desc catalog.Descriptor) error {
	return &concurrentSchemaChangeError{descID: desc.GetID()}
}

type schemaChangerUserError struct {
	err error
}

var _ errors.SafeFormatter = &schemaChangerUserError{}

// SchemaChangerUserError wraps an error as user consumable, which will surface
// it from the declarative schema changer without any wrapping. Normally errors
// from the declarative schema changer get wrapped with plan details inside
// DecorateErrorWithPlanDetails, but if the user for example specifies an
// expression that will raise an error during backfill, then we need to surface
// the error from the backfiller directly without wrapping (for example a
// default expression with a division by zero).
func SchemaChangerUserError(err error) error {
	return &schemaChangerUserError{err: err}
}

// HasSchemaChangerUserError returns true if the error is meant to be surfaced.
func HasSchemaChangerUserError(err error) bool {
	return errors.HasType(err, (*schemaChangerUserError)(nil))
}

// UnwrapSchemaChangerUserError returns the cause of a schemaChangerUserError,
// or nil if the error is not a schemaChangerUserError.
func UnwrapSchemaChangerUserError(err error) error {
	if scUserError := (*schemaChangerUserError)(nil); errors.As(err, &scUserError) {
		return scUserError.err
	}
	return nil
}

// SafeFormatError is part of the errors.SafeFormatter interface.
func (e *schemaChangerUserError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("schema change operation encountered an error")
	return e.err
}

func (e *schemaChangerUserError) Error() string {
	// We don't want to print the schemaChangerUserError wrapper in the error,
	// this only serves as a marker to the declarative schema changer to surface.
	return fmt.Sprintf("%v", e.err)
}

func (e *schemaChangerUserError) Unwrap() error {
	return e.err
}
