// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/utils"
	"github.com/cockroachdb/errors"
)

// Authentication errors (PUBLIC - shown to users)
var (
	// ErrNotAuthenticated is returned when no principal is found in the request context.
	ErrNotAuthenticated = utils.NewPublicError(errors.New("not authenticated"))
)

// Input validation errors (PUBLIC - shown to users)
var (
	ErrInvalidEmail  = utils.NewPublicError(errors.New("invalid email format"))
	ErrRequiredField = utils.NewPublicError(errors.New("required field missing"))
	ErrInvalidUUID   = utils.NewPublicError(errors.New("invalid UUID format"))
)

// Resource not found errors (PUBLIC - shown to users)
var (
	ErrUserNotFound           = utils.NewPublicError(errors.New("user not found"))
	ErrServiceAccountNotFound = utils.NewPublicError(errors.New("service account not found"))
	ErrTokenNotFound          = utils.NewPublicError(errors.New("token not found"))
	ErrGroupNotFound          = utils.NewPublicError(errors.New("group not found"))
)

// Authorization errors (PUBLIC - shown to users)
var (
	ErrUnauthorized = utils.NewPublicError(errors.New("unauthorized"))
	ErrForbidden    = utils.NewPublicError(errors.New("forbidden"))
)

// State/permission errors (PUBLIC - shown to users)
var (
	ErrUserNotProvisioned     = utils.NewPublicError(errors.New("user not provisioned - please contact your administrator"))
	ErrUserDeactivated        = utils.NewPublicError(errors.New("user is deactivated"))
	ErrServiceAccountDisabled = utils.NewPublicError(errors.New("service account is disabled"))
	ErrInvalidToken           = utils.NewPublicError(errors.New("invalid token"))
	ErrTokenExpired           = utils.NewPublicError(errors.New("token expired"))
	ErrIPNotAllowed           = utils.NewPublicError(errors.New("client IP not allowed for this service account"))
)

// Business rule violations (PUBLIC - shown to users)
var (
	ErrDuplicateUser   = utils.NewPublicError(errors.New("user with this email already exists"))
	ErrDuplicateOktaID = utils.NewPublicError(errors.New("user with this Okta ID already exists"))
	ErrDuplicateGroup  = utils.NewPublicError(errors.New("group with this external ID already exists"))
)

var (
	ErrShutdownTimeout = errors.New("service shutdown timeout")
)
