// Copyright 2020-2021 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rpc

import (
	"errors"
	"fmt"
	"strconv"
)

const (
	// ErrorCodeOK is a sentinel code for no error.
	//
	// No error should use this directly. This is returned by GetErrorCode if the error is nil.
	ErrorCodeOK ErrorCode = 0

	// ErrorCodeCanceled indicates the operation was canceled.
	//
	// HTTP equivalent: 408 REQUEST TIMEOUT
	ErrorCodeCanceled ErrorCode = 1

	// ErrorCodeUnknown indicates an unknown error.
	//
	// This is for errors that do not have specific error information.
	//
	// HTTP equivalent: 500 INTERNAL SERVER ERROR
	ErrorCodeUnknown ErrorCode = 2

	// ErrorCodeInvalidArgument indicates that the an invalid argument was specified, for
	// example missing required arguments and invalid arguments.
	//
	// HTTP equivalent: 400 BAD REQUEST
	ErrorCodeInvalidArgument ErrorCode = 3

	// ErrorCodeDeadlineExceeded indicates that a deadline was exceeded, for example a timeout.
	//
	// HTTP equivalent: 504 GATEWAY TIMEOUT
	// Note that Twirp treats this as a 408 REQUEST TIMEOUT, but grpc-gateway treats this
	// as 504 GATEWAY TIMEOUT.
	ErrorCodeDeadlineExceeded ErrorCode = 4

	// ErrorCodeNotFound indicates that an entity was not found.
	//
	// HTTP equivalent: 404 NOT FOUND
	ErrorCodeNotFound ErrorCode = 5

	// ErrorCodeAlreadyExists indicates that entity creation was unsuccessful as the entity
	// already exists.
	//
	// HTTP equivalent: 409 CONFLICT
	ErrorCodeAlreadyExists ErrorCode = 6

	// ErrorCodePermissionDenied indicates the caller does not have permission to perform
	// the requested operation.
	//
	// HTTP equivalent: 403 FORBIDDEN
	ErrorCodePermissionDenied ErrorCode = 7

	// ErrorCodeResourceExhausted indicates some resource has been exhausted, for example
	// throttling or out-of-space errors.
	//
	// HTTP equivalent: 429 TOO MANY REQUESTS
	ErrorCodeResourceExhausted ErrorCode = 8

	// ErrorCodeFailedPrecondition indicates operation was rejected because the system is not
	// in a state required for the operation's execution, for example a non-recursive
	// non-empty directory deletion.
	//
	// HTTP equivalent: 400 BAD REQUEST
	// Note that Twirp treats this as 412 PRECONDITION FAILED, but grpc-gateway treats this
	// as 400 BAD REQUEST, and has a note saying this is on purpose (and it makes sense).
	ErrorCodeFailedPrecondition ErrorCode = 9

	// ErrorCodeAborted indicates the operation was aborted, for example when a transaction
	// is aborted.
	//
	// HTTP equivalent: 409 CONFLICT
	ErrorCodeAborted ErrorCode = 10

	// ErrorCodeOutOfRange indicates an operation was attempted past the valid range, for example
	// seeking or reading past the end of a paginated collection.
	//
	// Unlike InvalidArgument, this error indicates a problem that may be fixed if
	// the system state changes (i.e. adding more items to the collection).
	//
	// There is a fair bit of overlap between FailedPrecondition and OutOfRange.
	// We recommend using OutOfRange (the more specific error) when it applies so
	// that callers who are iterating through a space can easily look for an
	// OutOfRange error to detect when they are done.
	//
	// HTTP equivant: 400 BAD REQUEST
	ErrorCodeOutOfRange ErrorCode = 11

	// ErrorCodeUnimplemented indicates operation is not implemented or not
	// supported/enabled in this service.
	//
	// HTTP equivalent: 501 NOT IMPLEMENTED
	ErrorCodeUnimplemented ErrorCode = 12

	// ErrorCodeInternal indicates an internal system error.
	//
	// HTTP equivalent: 500 INTERNAL SERVER ERROR
	ErrorCodeInternal ErrorCode = 13

	// ErrorCodeUnavailable indicates the service is currently unavailable.
	// This is a most likely a transient condition and may be corrected
	// by retrying with a backoff.
	//
	// HTTP equivalent: 503 SERVICE UNAVAILABLE
	ErrorCodeUnavailable ErrorCode = 14

	// ErrorCodeDataLoss indicates unrecoverable data loss or corruption.
	//
	// HTTP equivalent: 500 INTERNAL SERVER ERROR
	ErrorCodeDataLoss ErrorCode = 15

	// ErrorCodeUnauthenticated indicates the request does not have valid
	// authentication credentials for the operation. This is different than
	// PermissionDenied, which deals with authorization.
	//
	// HTTP equivalent: 401 UNAUTHORIZED
	ErrorCodeUnauthenticated ErrorCode = 16
)

var (
	errorCodeToString = map[ErrorCode]string{
		ErrorCodeCanceled:           "CANCELED",
		ErrorCodeUnknown:            "UNKNOWN",
		ErrorCodeInvalidArgument:    "INVALID_ARGUMENT",
		ErrorCodeDeadlineExceeded:   "DEADLINE_EXCEEDED",
		ErrorCodeNotFound:           "NOT_FOUND",
		ErrorCodeAlreadyExists:      "ALREADY_EXISTS",
		ErrorCodePermissionDenied:   "PERMISSION_DENIED",
		ErrorCodeResourceExhausted:  "RESOURCE_EXHAUSTED",
		ErrorCodeFailedPrecondition: "FAILED_PRECONDITION",
		ErrorCodeAborted:            "ABORTED",
		ErrorCodeOutOfRange:         "OUT_OF_RANGE",
		ErrorCodeUnimplemented:      "UNIMPLEMENTED",
		ErrorCodeInternal:           "INTERNAL",
		ErrorCodeUnavailable:        "UNAVAILABLE",
		ErrorCodeDataLoss:           "DATA_LOSS",
		ErrorCodeUnauthenticated:    "UNAUTHENTICATED",
	}
)

// ErrorCode is an error code.
//
// Unlike gRPC and Twirp, there is no zero code for success.
//
// All errors must have a valid error code. If an error does not have a valid
// error code when performing error operations, a new error with ErrorCodeInternal
// will be returned.
type ErrorCode int

// String returns the string value of e.
func (e ErrorCode) String() string {
	s, ok := errorCodeToString[e]
	if !ok {
		return strconv.Itoa(int(e))
	}
	return s
}

// NewError returns a new error with a code.
//
// The value of Error() will only contain the message. If you would like to
// also print the error code, you must do this manually.
//
// If the code is invalid, an error with ErrorCodeInternal will be returned.
func NewError(errorCode ErrorCode, message string) error {
	return newRPCError(errorCode, message)
}

// NewErrorf returns a new error.
//
// The value of Error() will only contain the message. If you would like to
// also print the code, you must do this manually.
//
// If the code is invalid, an error with ErrorCodeInternal will be returned.
func NewErrorf(errorCode ErrorCode, format string, args ...interface{}) error {
	return newRPCError(errorCode, fmt.Sprintf(format, args...))
}

// NewCanceledError is a convenience function for errors with ErrorCodeCanceled.
func NewCanceledError(message string) error {
	return NewError(ErrorCodeCanceled, message)
}

// NewCanceledErrorf is a convenience function for errors with ErrorCodeCanceled.
func NewCanceledErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeCanceled, format, args...)
}

// NewUnknownError is a convenience function for errors with ErrorCodeUnknown.
func NewUnknownError(message string) error {
	return NewError(ErrorCodeUnknown, message)
}

// NewUnknownErrorf is a convenience function for errors with ErrorCodeUnknown.
func NewUnknownErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeUnknown, format, args...)
}

// NewInvalidArgumentError is a convenience function for errors with ErrorCodeInvalidArgument.
func NewInvalidArgumentError(message string) error {
	return NewError(ErrorCodeInvalidArgument, message)
}

// NewInvalidArgumentErrorf is a convenience function for errors with ErrorCodeInvalidArgument.
func NewInvalidArgumentErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeInvalidArgument, format, args...)
}

// NewDeadlineExceededError is a convenience function for errors with ErrorCodeDeadlineExceeded.
func NewDeadlineExceededError(message string) error {
	return NewError(ErrorCodeDeadlineExceeded, message)
}

// NewDeadlineExceededErrorf is a convenience function for errors with ErrorCodeDeadlineExceeded.
func NewDeadlineExceededErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeDeadlineExceeded, format, args...)
}

// NewNotFoundError is a convenience function for errors with ErrorCodeNotFound.
func NewNotFoundError(message string) error {
	return NewError(ErrorCodeNotFound, message)
}

// NewNotFoundErrorf is a convenience function for errors with ErrorCodeNotFound.
func NewNotFoundErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeNotFound, format, args...)
}

// NewAlreadyExistsError is a convenience function for errors with ErrorCodeAlreadyExists.
func NewAlreadyExistsError(message string) error {
	return NewError(ErrorCodeAlreadyExists, message)
}

// NewAlreadyExistsErrorf is a convenience function for errors with ErrorCodeAlreadyExists.
func NewAlreadyExistsErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeAlreadyExists, format, args...)
}

// NewPermissionDeniedError is a convenience function for errors with ErrorCodePermissionDenied.
func NewPermissionDeniedError(message string) error {
	return NewError(ErrorCodePermissionDenied, message)
}

// NewPermissionDeniedErrorf is a convenience function for errors with ErrorCodePermissionDenied.
func NewPermissionDeniedErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodePermissionDenied, format, args...)
}

// NewResourceExhaustedError is a convenience function for errors with ErrorCodeResourceExhausted.
func NewResourceExhaustedError(message string) error {
	return NewError(ErrorCodeResourceExhausted, message)
}

// NewResourceExhaustedErrorf is a convenience function for errors with ErrorCodeResourceExhausted.
func NewResourceExhaustedErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeResourceExhausted, format, args...)
}

// NewFailedPreconditionError is a convenience function for errors with ErrorCodeFailedPrecondition.
func NewFailedPreconditionError(message string) error {
	return NewError(ErrorCodeFailedPrecondition, message)
}

// NewFailedPreconditionErrorf is a convenience function for errors with ErrorCodeFailedPrecondition.
func NewFailedPreconditionErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeFailedPrecondition, format, args...)
}

// NewAbortedError is a convenience function for errors with ErrorCodeAborted.
func NewAbortedError(message string) error {
	return NewError(ErrorCodeAborted, message)
}

// NewAbortedErrorf is a convenience function for errors with ErrorCodeAborted.
func NewAbortedErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeAborted, format, args...)
}

// NewOutOfRangeError is a convenience function for errors with ErrorCodeOutOfRange.
func NewOutOfRangeError(message string) error {
	return NewError(ErrorCodeOutOfRange, message)
}

// NewOutOfRangeErrorf is a convenience function for errors with ErrorCodeOutOfRange.
func NewOutOfRangeErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeOutOfRange, format, args...)
}

// NewUnimplementedError is a convenience function for errors with ErrorCodeUnimplemented.
func NewUnimplementedError(message string) error {
	return NewError(ErrorCodeUnimplemented, message)
}

// NewUnimplementedErrorf is a convenience function for errors with ErrorCodeUnimplemented.
func NewUnimplementedErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeUnimplemented, format, args...)
}

// NewInternalError is a convenience function for errors with ErrorCodeInternal.
func NewInternalError(message string) error {
	return NewError(ErrorCodeInternal, message)
}

// NewInternalErrorf is a convenience function for errors with ErrorCodeInternal.
func NewInternalErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeInternal, format, args...)
}

// NewUnavailableError is a convenience function for errors with ErrorCodeUnavailable.
func NewUnavailableError(message string) error {
	return NewError(ErrorCodeUnavailable, message)
}

// NewUnavailableErrorf is a convenience function for errors with ErrorCodeUnavailable.
func NewUnavailableErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeUnavailable, format, args...)
}

// NewDataLossError is a convenience function for errors with ErrorCodeDataLoss.
func NewDataLossError(message string) error {
	return NewError(ErrorCodeDataLoss, message)
}

// NewDataLossErrorf is a convenience function for errors with ErrorCodeDataLoss.
func NewDataLossErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeDataLoss, format, args...)
}

// NewUnauthenticatedError is a convenience function for errors with ErrorCodeUnauthenticated.
func NewUnauthenticatedError(message string) error {
	return NewError(ErrorCodeUnauthenticated, message)
}

// NewUnauthenticatedErrorf is a convenience function for errors with ErrorCodeUnauthenticated.
func NewUnauthenticatedErrorf(format string, args ...interface{}) error {
	return NewErrorf(ErrorCodeUnauthenticated, format, args...)
}

// GetErrorCode gets the error code.
//
// If the error is nil, this returns ErrorCodeOK.
// If the error is not created by this package, it will return ErrorCodeInternal.
//
// Note that gRPC maps to ErrorCodeUnknown in the same scenario.
func GetErrorCode(err error) ErrorCode {
	if err == nil {
		return 0
	}
	if rpcErr := (&rpcError{}); errors.As(err, &rpcErr) {
		return rpcErr.errorCode
	}
	return ErrorCodeInternal
}

// IsError returns true if err is an error created by this package.
//
// If the error is nil, this returns false.
func IsError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, &rpcError{})
}

type rpcError struct {
	errorCode ErrorCode
	message   string
}

func newRPCError(errorCode ErrorCode, message string) *rpcError {
	if _, ok := errorCodeToString[errorCode]; !ok {
		message = fmt.Sprintf(
			"got invalid error code %q when constructing error (original message was %q)",
			errorCode.String(),
			message,
		)
		errorCode = ErrorCodeInternal
	}
	return &rpcError{
		errorCode: errorCode,
		message:   message,
	}
}

func (r *rpcError) Error() string {
	return r.message
}

func (r *rpcError) Is(err error) bool {
	_, ok := err.(*rpcError)
	return ok
}
