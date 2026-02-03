// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"net/mail"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/models/auth"
	authtypes "github.com/cockroachdb/cockroach/pkg/cmd/roachprod-centralized/services/auth/types"
	"github.com/cockroachdb/errors"
)

// validateEmail checks if the email is in valid format.
func validateEmail(email string) error {
	if email == "" {
		return errors.Wrap(authtypes.ErrRequiredField, "email")
	}
	_, err := mail.ParseAddress(email)
	if err != nil {
		return authtypes.ErrInvalidEmail
	}
	return nil
}

// validateUser validates a user object for creation/update.
// isUpdate: true if validating for update, false for creation.
func validateUser(user *auth.User, isUpdate bool) error {
	if err := validateEmail(user.Email); err != nil {
		return err
	}

	// OktaUserID required on creation
	if !isUpdate && user.OktaUserID == "" {
		return errors.Wrap(authtypes.ErrRequiredField, "okta_user_id")
	}

	return nil
}
