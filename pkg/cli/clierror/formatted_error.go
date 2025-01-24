// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clierror

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/jackc/pgx/v5/pgconn"
)

// OutputError prints out an error object on the given writer.
//
// It has a somewhat inconvenient set of requirements: it must make
// the error both palatable to a human user, which mandates some
// beautification, and still retain a few guarantees for automatic
// parsers (and a modicum of care for cross-compatibility across
// versions), including that of keeping the output relatively stable.
//
// As a result, future changes should be careful to properly balance
// changes made in favor of one audience with the needs and
// requirements of the other audience.
func OutputError(w io.Writer, err error, showSeverity, verbose bool) {
	f := formattedError{err: err, showSeverity: showSeverity, verbose: verbose}
	fmt.Fprintln(w, f.Error())
}

// NewFormattedError wraps the error into another error object that displays
// the details of the error when the error is formatted.
// This constructor takes care of avoiding a wrap if the error is
// already a formattederror.
func NewFormattedError(err error, showSeverity, verbose bool) error {
	// We want to flatten the error to reveal the hints, details etc.
	// However we can't do it twice, so we need to detect first if
	// some code already added the formattedError{} wrapper.
	if f := (*formattedError)(nil); errors.As(err, &f) {
		return err
	}
	return &formattedError{err: err, showSeverity: showSeverity, verbose: verbose}
}

type formattedError struct {
	err                   error
	showSeverity, verbose bool
}

func (f *formattedError) Unwrap() error {
	return f.err
}

// Error implements the error interface.
func (f *formattedError) Error() string {
	// If we're applying recursively, ignore what's there and display the original error.
	// This happens when the shell reports an error for a second time.
	var other *formattedError
	if errors.As(f.err, &other) {
		return other.Error()
	}
	var buf strings.Builder

	// If the severity is missing, we're going to assume it's an error.
	severity := "ERROR"

	// Extract the fields.
	var message, hint, detail, location, constraintName string
	var code pgcode.Code
	if pgErr := (*pgconn.PgError)(nil); errors.As(f.err, &pgErr) {
		if pgErr.Severity != "" {
			severity = pgErr.Severity
		}
		constraintName = pgErr.ConstraintName
		message = pgErr.Message
		code = pgcode.MakeCode(pgErr.Code)
		hint, detail = pgErr.Hint, pgErr.Detail
		location = formatLocation(pgErr.File, int(pgErr.Line), pgErr.Routine)
	} else {
		message = f.err.Error()
		code = pgerror.GetPGCode(f.err)
		// Extract the standard hint and details.
		hint = errors.FlattenHints(f.err)
		detail = errors.FlattenDetails(f.err)
		if file, line, fn, ok := errors.GetOneLineSource(f.err); ok {
			location = formatLocation(file, line, fn)
		}
	}

	// The order of the printing goes from most to less important.

	if f.showSeverity && severity != "" {
		fmt.Fprintf(&buf, "%s: ", severity)
	}
	fmt.Fprintln(&buf, message)

	// Avoid printing the code for NOTICE, as the code is always 00000.
	if severity != "NOTICE" && code.String() != "" {
		// In contrast to `psql` we print the code even when printing
		// non-verbosely, because we want to promote users reporting codes
		// when interacting with support.
		if code == pgcode.Uncategorized && !f.verbose {
			// An exception is made for the "uncategorized" code, because we
			// also don't want users to get the idea they can rely on XXUUU
			// in their apps. That code is special, as we typically seek to
			// replace it over time by something more specific.
			//
			// So in this case, if not printing verbosely, we don't display
			// the code.
		} else {
			fmt.Fprintln(&buf, "SQLSTATE:", code)
		}
	}

	if detail != "" {
		fmt.Fprintln(&buf, "DETAIL:", detail)
	}
	if constraintName != "" {
		fmt.Fprintln(&buf, "CONSTRAINT:", constraintName)
	}
	if hint != "" {
		fmt.Fprintln(&buf, "HINT:", hint)
	}
	if f.verbose && location != "" {
		fmt.Fprintln(&buf, "LOCATION:", location)
	}

	// The code above is easier to read and write by stripping the
	// extraneous newline at the end, than ensuring it's not there in
	// the first place.
	return strings.TrimRight(buf.String(), "\n")
}

// formatLocation spells out the error's location in a format
// similar to psql: routine then file:num. The routine part is
// skipped if empty.
func formatLocation(file string, line int, fn string) string {
	var res strings.Builder
	res.WriteString(fn)
	if file != "" || line != 0 {
		if fn != "" {
			res.WriteString(", ")
		}
		if file == "" {
			res.WriteString("<unknown>")
		} else {
			res.WriteString(file)
		}
		if line != 0 {
			res.WriteByte(':')
			res.WriteString(strconv.Itoa(line))
		}
	}
	return res.String()
}
