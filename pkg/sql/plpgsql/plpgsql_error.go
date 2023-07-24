// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package plpgsql contains logic related to parsing and executing PLpgSQL
// routines.
package plpgsql

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
)

// caughtRoutineException is used to wrap an error that is returned by the
// exception block of a routine so that the exception block does not execute
// more than once. This is necessary because tail-call optimization does not
// always succeed (e.g. for distributed execution), and so an error returned by
// the exception block of a nested routine can encounter the same exception
// block in the outer routine.
// TODO(drewk): If we unconditionally apply tail-call optimization for the
// routines that implement a single PLpgSQL function, we won't have to worry
// about encountering the same exception handler multiple times.
// TODO(drewk): If we add support for calling UDFs from other UDFs or nested
// PLpgSQL blocks before the previous TODO is addressed, we should store an
// ID for the exception handler that returned the error since handlers could
// be nested.
type caughtRoutineException struct {
	cause error
}

// NewCaughtRoutineException wraps the given error with a caughtRoutineException
// indicating that the error has already passed through a PLpgSQL exception
// handler.
func NewCaughtRoutineException(err error) error {
	return &caughtRoutineException{cause: err}
}

// IsCaughtRoutineException returns true if the given error has already passed
// through an exception handler.
func IsCaughtRoutineException(err error) bool {
	return errors.HasType(err, (*caughtRoutineException)(nil))
}

var _ errors.Wrapper = &caughtRoutineException{}

func (e *caughtRoutineException) Error() string { return e.cause.Error() }
func (e *caughtRoutineException) Cause() error  { return e.cause }
func (e *caughtRoutineException) Unwrap() error { return e.Cause() }

func decodeCaughtRoutineException(
	_ context.Context, cause error, _ string, _ []string, _ proto.Message,
) error {
	return NewCaughtRoutineException(cause)
}

func init() {
	errors.RegisterWrapperDecoder(errors.GetTypeKey((*caughtRoutineException)(nil)), decodeCaughtRoutineException)
}
