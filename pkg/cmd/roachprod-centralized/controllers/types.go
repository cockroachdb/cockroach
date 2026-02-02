// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/errors"
)

// GetGenericStatusCode maps common/generic errors to HTTP status codes.
// This is a utility function that can be used by component-specific error handlers.
func GetGenericStatusCode(err error) int {
	if err == nil {
		return http.StatusOK
	}

	// Handle PublicError wrapping
	if errors.HasType(err, &utils.PublicError{}) {
		unwrapped := errors.Unwrap(err)
		if unwrapped != nil {
			return GetGenericStatusCode(unwrapped)
		}
		return http.StatusBadRequest
	}

	// Handle truly generic error patterns
	switch {
	case errors.Is(err, ErrUnauthorized):
		return http.StatusUnauthorized
	case errors.Is(err, ErrInternalServerError):
		return http.StatusInternalServerError
	default:
		return http.StatusInternalServerError
	}
}

// BadRequestResult is a result that represents the result for a bad request.
type BadRequestResult struct {
	Error error
}

// GetData returns nil for error responses.
func (r *BadRequestResult) GetData() any {
	return nil
}

// GetError returns the error as a public error.
// BadRequestResult errors are always public.
func (r *BadRequestResult) GetError() error {
	if r.Error == nil {
		return nil
	}
	return utils.NewPublicError(r.Error)
}

// GetAssociatedStatusCode returns 400 Bad Request for all BadRequestResult instances.
func (r *BadRequestResult) GetAssociatedStatusCode() int {
	return http.StatusBadRequest
}

// SuccessResult is a generic result for successful operations.
// Use this for endpoints that return data without domain-specific error handling.
type SuccessResult struct {
	Data any
}

// GetData returns the data.
func (r *SuccessResult) GetData() any {
	return r.Data
}

// GetError returns nil (no error).
func (r *SuccessResult) GetError() error {
	return nil
}

// GetAssociatedStatusCode returns 200 OK for successful operations.
func (r *SuccessResult) GetAssociatedStatusCode() int {
	return http.StatusOK
}

// ErrorResult is a generic result for error responses.
// The error can be either public (shown to user) or internal (logged only).
type ErrorResult struct {
	Error      error
	StatusCode int
	IsPublic   bool
}

// GetData returns nil for error responses.
func (r *ErrorResult) GetData() any {
	return nil
}

// GetError returns the error, wrapped as PublicError if IsPublic is true.
func (r *ErrorResult) GetError() error {
	if r.Error == nil {
		return nil
	}
	if r.IsPublic {
		return utils.NewPublicError(r.Error)
	}
	return r.Error
}

// GetAssociatedStatusCode returns the HTTP status code for this error.
func (r *ErrorResult) GetAssociatedStatusCode() int {
	if r.StatusCode == 0 {
		return http.StatusInternalServerError
	}
	return r.StatusCode
}
