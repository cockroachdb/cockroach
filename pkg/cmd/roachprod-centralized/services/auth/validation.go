// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package auth

import (
	"net"
	"net/mail"
	"time"

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

// validateServiceAccount validates a service account for creation/update.
func validateServiceAccount(sa *auth.ServiceAccount, isUpdate bool) error {
	if sa.Name == "" {
		return errors.Wrap(authtypes.ErrRequiredField, "name")
	}
	return nil
}

// validateTTL validates a token TTL duration.
func validateTTL(ttl time.Duration) error {
	if ttl <= 0 {
		return errors.Wrap(authtypes.ErrInvalidTTL, "TTL must be positive")
	}
	if ttl > authtypes.MaxTokenTTL {
		return errors.Wrapf(authtypes.ErrInvalidTTL, "TTL cannot exceed %s", authtypes.MaxTokenTTL)
	}
	return nil
}

// validateCIDR validates a CIDR string.
func validateCIDR(cidr string) error {
	_, _, err := net.ParseCIDR(cidr)
	if err != nil {
		return authtypes.ErrInvalidOriginCIDR
	}
	return nil
}

// validateOktaGroups validates Okta group names.
func validateOktaGroups(groups []string) error {
	if len(groups) == 0 {
		return errors.Wrap(authtypes.ErrRequiredField, "okta_groups")
	}
	for _, group := range groups {
		if group == "" {
			return errors.New("empty group name in okta_groups")
		}
	}
	return nil
}
