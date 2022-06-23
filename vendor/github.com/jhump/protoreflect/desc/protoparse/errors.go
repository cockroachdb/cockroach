package protoparse

import (
	"errors"
	"fmt"
)

// ErrInvalidSource is a sentinel error that is returned by calls to
// Parser.ParseFiles and Parser.ParseFilesButDoNotLink in the event that syntax
// or link errors are encountered, but the parser's configured ErrorReporter
// always returns nil.
var ErrInvalidSource = errors.New("parse failed: invalid proto source")

// ErrNoSyntax is a sentinel error that may be passed to a warning reporter.
// The error the reporter receives will be wrapped with source position that
// indicates the file that had no syntax statement.
var ErrNoSyntax = errors.New("no syntax specified; defaulting to proto2 syntax")

// ErrLookupImportAndProtoSet is the error returned if both LookupImport and LookupImportProto are set.
var ErrLookupImportAndProtoSet = errors.New("both LookupImport and LookupImportProto set")

// ErrorReporter is responsible for reporting the given error. If the reporter
// returns a non-nil error, parsing/linking will abort with that error. If the
// reporter returns nil, parsing will continue, allowing the parser to try to
// report as many syntax and/or link errors as it can find.
type ErrorReporter func(err ErrorWithPos) error

// WarningReporter is responsible for reporting the given warning. This is used
// for indicating non-error messages to the calling program for things that do
// not cause the parse to fail but are considered bad practice. Though they are
// just warnings, the details are supplied to the reporter via an error type.
type WarningReporter func(ErrorWithPos)

func defaultErrorReporter(err ErrorWithPos) error {
	// abort parsing after first error encountered
	return err
}

type errorHandler struct {
	errReporter  ErrorReporter
	errsReported int
	err          error

	warnReporter WarningReporter
}

func newErrorHandler(errRep ErrorReporter, warnRep WarningReporter) *errorHandler {
	if errRep == nil {
		errRep = defaultErrorReporter
	}
	return &errorHandler{
		errReporter:  errRep,
		warnReporter: warnRep,
	}
}

func (h *errorHandler) handleErrorWithPos(pos *SourcePos, format string, args ...interface{}) error {
	if h.err != nil {
		return h.err
	}
	h.errsReported++
	err := h.errReporter(errorWithPos(pos, format, args...))
	h.err = err
	return err
}

func (h *errorHandler) handleError(err error) error {
	if h.err != nil {
		return h.err
	}
	if ewp, ok := err.(ErrorWithPos); ok {
		h.errsReported++
		err = h.errReporter(ewp)
	}
	h.err = err
	return err
}

func (h *errorHandler) warn(pos *SourcePos, err error) {
	if h.warnReporter != nil {
		h.warnReporter(ErrorWithSourcePos{Pos: pos, Underlying: err})
	}
}

func (h *errorHandler) getError() error {
	if h.errsReported > 0 && h.err == nil {
		return ErrInvalidSource
	}
	return h.err
}

// ErrorWithPos is an error about a proto source file that includes information
// about the location in the file that caused the error.
//
// The value of Error() will contain both the SourcePos and Underlying error.
// The value of Unwrap() will only be the Underlying error.
type ErrorWithPos interface {
	error
	GetPosition() SourcePos
	Unwrap() error
}

// ErrorWithSourcePos is an error about a proto source file that includes
// information about the location in the file that caused the error.
//
// Errors that include source location information *might* be of this type.
// However, calling code that is trying to examine errors with location info
// should instead look for instances of the ErrorWithPos interface, which
// will find other kinds of errors. This type is only exported for backwards
// compatibility.
//
// SourcePos should always be set and never nil.
type ErrorWithSourcePos struct {
	Underlying error
	Pos        *SourcePos
}

// Error implements the error interface
func (e ErrorWithSourcePos) Error() string {
	sourcePos := e.GetPosition()
	return fmt.Sprintf("%s: %v", sourcePos, e.Underlying)
}

// GetPosition implements the ErrorWithPos interface, supplying a location in
// proto source that caused the error.
func (e ErrorWithSourcePos) GetPosition() SourcePos {
	if e.Pos == nil {
		return SourcePos{Filename: "<input>"}
	}
	return *e.Pos
}

// Unwrap implements the ErrorWithPos interface, supplying the underlying
// error. This error will not include location information.
func (e ErrorWithSourcePos) Unwrap() error {
	return e.Underlying
}

var _ ErrorWithPos = ErrorWithSourcePos{}

func errorWithPos(pos *SourcePos, format string, args ...interface{}) ErrorWithPos {
	return ErrorWithSourcePos{Pos: pos, Underlying: fmt.Errorf(format, args...)}
}

type errorWithFilename struct {
	underlying error
	filename   string
}

func (e errorWithFilename) Error() string {
	return fmt.Sprintf("%s: %v", e.filename, e.underlying)
}

func (e errorWithFilename) Unwrap() error {
	return e.underlying
}

// ErrorUnusedImport may be passed to a warning reporter when an unused
// import is detected. The error the reporter receives will be wrapped
// with source position that indicates the file and line where the import
// statement appeared.
type ErrorUnusedImport interface {
	error
	UnusedImport() string
}

type errUnusedImport string

func (e errUnusedImport) Error() string {
	return fmt.Sprintf("import %q not used", string(e))
}

func (e errUnusedImport) UnusedImport() string {
	return string(e)
}
