// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pgerror

import (
	"context"
	"fmt"
	"io"
	"reflect"

	"github.com/pkg/errors"
)

// Marker errors are designed to solve a problem: recognize
// when a pgerror is derived from a particular error object.
//
// This can be used e.g. to recognized particular origins for errors.
//
// The rules are as follows:
//
// - undecorated errors are considered to have equivalent markers if
//   they have the same type (including package path for the type),
//   and for Error objects also if they have the same code.
//
// - use WithMarker(err) to decorate an error with a new marker that's
//   derived from its original marker but includes the file path of
//   the caller of WithMarker(). This can be used to produce separate
//   markers from that of undecorated errors, for example to separate
//   errors generated in different places with errors.New() using the
//   same message.
//
// - if an error is already marked, WithMarker() will panic.
//
// - Wrap() in this package, and errors.Wrap(), preserve the marker.
//
// - use pgerror.IsMarkedError(err1, err2) to test whether two
//   errors have equivalent markers.
//
// We use a string marker instead of the more common/traditional
// chaining through errors.Wrap() and the Cause() interface,
// because pgerror.Error objects must flow through the network
// via protobuf, and the errors' package Cause() mechanism
// is not protobuf-encodable.
//
// We use the file path specifically in WithMarker():
//
// - instead of the package path, so that different error objects in
//   the same package have different markers;
//
// - instead of including the line number or function name, so that
//   the marker persists across minor code refactorings; this ensures
//   comparability across mixed version clusters.

// WithMarker decorates an error with its own string marker.
// The marker is derived from the original marker to also include
// the file path of the caller of WithMarker.
func WithMarker(err error) error {
	// Generate a marker.
	marker := getMarker(err)

	// Decorate the marker with the caller information.
	src := makeSrcCtx(1)
	decoratedMarker := fmt.Sprintf("%s\n%s", marker, src.File)

	// Get the decorated pg error or build a fresh one.
	newErr := WrapWithDepthf(1, err, CodeUncategorizedError, "")
	// We're guaranteed an *Error object when the input is not nil.
	pgErr := newErr.(*Error)

	// If there's a mark already set and it's not the one we're adding,
	// refuse to create a new one.
	if pgErr.OriginMarker != "" && pgErr.OriginMarker != marker {
		panic(fmt.Sprintf("error already marked as %q", pgErr.OriginMarker))
	}

	// Override the original marker with the new one.
	pgErr.OriginMarker = decoratedMarker
	return pgErr
}

// WithSameMarker decorated an error with the same marker as another
// error.
func WithSameMarker(err error, refErr error) error {
	if err == nil || refErr == nil {
		panic("cannot mark nil error or using nil reference error")
	}

	var marker string
	pgRefErr, ok := GetPGCause(refErr)
	if ok && pgRefErr.OriginMarker != "" {
		marker = pgRefErr.OriginMarker
	} else {
		marker = getMarker(refErr)
	}

	// Get the decorated pg error or build a fresh one.
	newErr := WrapWithDepthf(1, err, CodeUncategorizedError, "")
	// We're guaranteed an *Error object when the input is not nil.
	pgErr := newErr.(*Error)

	// Assign the marker.
	pgErr.OriginMarker = marker
	return pgErr
}

// IsMarkedError tests whether a provided error has the same mark as
// the error in the second argument.
// The boolean return is the yes/no answer.
// If the errors are the same object (same reference), the result is
// true.
// If the errors are both derived from an Error object
// and they have the same, non-empty OriginMarker, the
// function returns true.
// Otherwise, it returns false.
func IsMarkedError(errToTest error, markedErr error) bool {
	if errToTest == markedErr {
		return true
	}
	if errToTest == nil || markedErr == nil {
		return false
	}

	lMarker := getMarker(errToTest)
	rMarker := getMarker(markedErr)

	return lMarker != "" && lMarker == rMarker
}

// getMarker returns a portable identification for the error. We
// assume that an error is identified by the full type identification
// (including package path) of the error object and its full error
// message.
func getMarker(err error) string {
	err = errors.Cause(err)

	pgErr, isPgError := err.(*Error)
	if isPgError && pgErr.OriginMarker != "" {
		return pgErr.OriginMarker
	}

	prefix := err.Error()
	if isPgError {
		prefix = fmt.Sprintf("(%s) %s", pgErr.Code, prefix)
	}

	// We'll need the type to get the type identification below.
	t := reflect.TypeOf(err)

	// We want the path of the type "inside".
	origT := t
	for {
		switch origT.Kind() {
		case reflect.Array, reflect.Chan, reflect.Map, reflect.Ptr, reflect.Slice:
			origT = origT.Elem()
			continue
		}
		break
	}

	// The "natural" representation of an error would put the package
	// name first. However, because we're interested in comparing these
	// markers and we want to reject different errors quickly, we want
	// to put the fields with the highest variability earlier in the
	// marker. Hence message first, then type, then package.
	return fmt.Sprintf("%s\n%s\n%s",
		prefix,
		t.String(),
		origT.PkgPath())
}

// ContextCanceledMarkerError can be used as parameter to
// IsMarkedError to recognize a pgerror that wrapped a
// context.Canceled error.
// This is provided for convenience only, to avoid recomputing
// the marker on every call to IsMarkerError().
var ContextCanceledMarkerError = Wrap(context.Canceled, "", "")

// ContextDeadlineExceededMarkerError can be used as parameter to
// IsMarkedError to recognize a pgerror that wrapped a
// context.DeadlineExceeded error.
// This is provided for convenience only, to avoid recomputing
// the marker on every call to IsMarkerError().
var ContextDeadlineExceededMarkerError = Wrap(context.DeadlineExceeded, "", "")

// IOEOFMarkerError can be used as parameter to
// IsMarkedError to recognize a pgerror that wrapped a
// io.EOF error.
// This is provided for convenience only, to avoid recomputing
// the marker on every call to IsMarkerError().
var IOEOFMarkerError = Wrap(io.EOF, "", "")
