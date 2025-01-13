// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package controllers

import (
	"net/http"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
)

// BadRequestResult is a result that represents the result for a bad request.
type BadRequestResult struct {
	Error error
}

// GetData returns the data.
// BadRequestResult does not have data.
func (r *BadRequestResult) GetData() any {
	return nil
}

// GetError returns the error.
// BadRequestResult is a public error.
func (r *BadRequestResult) GetError() error {
	return utils.NewPublicError(r.Error)
}

// GetAssociatedStatusCode returns the status code associated.
// BadRequestResult is always 400.
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
