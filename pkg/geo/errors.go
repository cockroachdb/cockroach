// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package geo

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/errors"
)

// NewMismatchingSRIDsError returns the error message for SRIDs of GeospatialTypes
// a and b being a mismatch.
func NewMismatchingSRIDsError(a geopb.SpatialObject, b geopb.SpatialObject) error {
	return errors.Newf(
		"operation on mixed SRIDs forbidden: (%s, %d) != (%s, %d)",
		a.ShapeType,
		a.SRID,
		b.ShapeType,
		b.SRID,
	)
}

// EmptyGeometryError is an error that is returned when the Geometry or any
// parts of its subgeometries are empty.
type EmptyGeometryError struct {
	cause error
}

var _ error = (*EmptyGeometryError)(nil)
var _ errors.SafeDetailer = (*EmptyGeometryError)(nil)
var _ fmt.Formatter = (*EmptyGeometryError)(nil)
var _ errors.Formatter = (*EmptyGeometryError)(nil)

// Error implements the error interface.
func (w *EmptyGeometryError) Error() string { return w.cause.Error() }

// Cause implements the errors.SafeDetailer interface.
func (w *EmptyGeometryError) Cause() error { return w.cause }

// Unwrap implements the SafeDetailer interface.
func (w *EmptyGeometryError) Unwrap() error { return w.cause }

// SafeDetails implements the SafeDetailer interface.
func (w *EmptyGeometryError) SafeDetails() []string { return []string{w.cause.Error()} }

// Format implements the errors.Formatter interface.
func (w *EmptyGeometryError) Format(s fmt.State, verb rune) { errors.FormatError(w, s, verb) }

// FormatError implements the errors.Formatter interface.
func (w *EmptyGeometryError) FormatError(p errors.Printer) (next error) { return w.cause }

// IsEmptyGeometryError returns true if the error is of type EmptyGeometryError.
func IsEmptyGeometryError(err error) bool {
	return errors.HasType(err, &EmptyGeometryError{})
}

// NewEmptyGeometryError returns an error indicating an empty geometry has been found.
func NewEmptyGeometryError() *EmptyGeometryError {
	return &EmptyGeometryError{cause: errors.Newf("empty shape found")}
}
