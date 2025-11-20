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

// AuthenticationType is the type of authentication required for a controller.
type AuthenticationType string

const (
	// AuthenticationTypeNone is the authentication type that does not
	// check authentication.
	AuthenticationTypeNone AuthenticationType = "none"
	// AuthenticationTypeRequired is the type of authentication that requires
	// authentication.
	AuthenticationTypeRequired AuthenticationType = "required"
)
