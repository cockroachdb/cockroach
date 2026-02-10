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
	ErrInvalidEmail      = utils.NewPublicError(errors.New("invalid email format"))
	ErrRequiredField     = utils.NewPublicError(errors.New("required field missing"))
	ErrInvalidUUID       = utils.NewPublicError(errors.New("invalid UUID format"))
	ErrInvalidTTL        = utils.NewPublicError(errors.New("invalid TTL"))
	ErrInvalidOriginCIDR = utils.NewPublicError(errors.New("invalid CIDR format for service account origin"))
	ErrInvalidTokenType  = utils.NewPublicError(errors.New("invalid token type"))
)

// Resource not found errors (PUBLIC - shown to users)
var (
	ErrUserNotFound            = utils.NewPublicError(errors.New("user not found"))
	ErrServiceAccountNotFound  = utils.NewPublicError(errors.New("service account not found"))
	ErrTokenNotFound           = utils.NewPublicError(errors.New("token not found"))
	ErrGroupPermissionNotFound = utils.NewPublicError(errors.New("group permission not found"))
	ErrGroupNotFound           = utils.NewPublicError(errors.New("group not found"))
	ErrOriginNotFound          = utils.NewPublicError(errors.New("service account origin not found"))
	ErrPermissionNotFound      = utils.NewPublicError(errors.New("permission not found"))
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
	ErrDuplicateUser                     = utils.NewPublicError(errors.New("user with this email already exists"))
	ErrDuplicateOktaID                   = utils.NewPublicError(errors.New("user with this Okta ID already exists"))
	ErrDuplicateGroup                    = utils.NewPublicError(errors.New("group with this external ID already exists"))
	ErrTokenLimitExceeded                = utils.NewPublicError(errors.New("token limit exceeded for this principal"))
	ErrPermissionAlreadyExists           = utils.NewPublicError(errors.New("permission already exists"))
	ErrPermissionEscalation              = utils.NewPublicError(errors.New("cannot grant permission you do not have"))
	ErrSACreationNotAllowedFromOrphanSA  = utils.NewPublicError(errors.New("creation only allowed from user principals or non-orphan service accounts"))
	ErrNonOrphanSAPermissionModification = utils.NewPublicError(errors.New("cannot modify permissions for service accounts that inherit permissions from a user principal"))
)

var (
	ErrShutdownTimeout = errors.New("service shutdown timeout")
)
