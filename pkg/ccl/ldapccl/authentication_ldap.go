// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package ldapccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/go-ldap/ldap/v3"
)

const (
	beginAuthNCounterName   = counterPrefix + "begin_authentication"
	loginSuccessCounterName = counterPrefix + "login_success"
)

var (
	beginAuthNUseCounter   = telemetry.GetCounterOnce(beginAuthNCounterName)
	loginSuccessUseCounter = telemetry.GetCounterOnce(loginSuccessCounterName)
)

// validateLDAPAuthNOptions checks the ldap authentication config values.
func (authManager *ldapAuthManager) validateLDAPAuthNOptions() error {
	const ldapOptionsErrorMsg = "ldap authentication params in HBA conf missing"
	if authManager.mu.conf.ldapSearchFilter == "" {
		return errors.New(ldapOptionsErrorMsg + " search filter")
	}
	if authManager.mu.conf.ldapSearchAttribute == "" {
		return errors.New(ldapOptionsErrorMsg + " search attribute")
	}
	return nil
}

// ValidateLDAPLogin validates an attempt to bind to an LDAP server.
// In particular, it checks that:
// * The cluster has an enterprise license.
// * The active cluster version is 24.2 for this feature.
// * LDAP authManager is enabled after settings were reloaded.
// * The auth attempt is not for a reserved user.
// * The hba conf entry options could be parsed to obtain ldap server params.
// * All ldap server params are valid.
// * LDAPs connection can be established with configured server.
// * Configured bind DN and password can be used to search for the sql user DN on ldap server.
// * The obtained user DN could be used to bind with the password from sql connection string.
// It returns the retrievedUserDN which is the DN associated with the user in
// LDAP server, authError (which is the error sql clients will see in case of
// failures) and detailedError (which is the internal error from ldap clients
// that might contain sensitive information we do not want to send to sql
// clients but still want to log it). We do not want to send any information
// back to client which was not provided by the client.
func (authManager *ldapAuthManager) ValidateLDAPLogin(
	ctx context.Context,
	st *cluster.Settings,
	user username.SQLUsername,
	ldapPwd string,
	entry *hba.Entry,
	_ *identmap.Conf,
) (retrievedUserDN *ldap.DN, detailedErrorMsg redact.RedactableString, authError error) {
	if err := utilccl.CheckEnterpriseEnabled(st, "LDAP authentication"); err != nil {
		return nil, "", err
	}
	if !st.Version.IsActive(ctx, clusterversion.V24_2) {
		return nil, "", pgerror.Newf(pgcode.FeatureNotSupported, "LDAP authentication is only supported after v24.2 upgrade is finalized")
	}

	authManager.mu.Lock()
	defer authManager.mu.Unlock()

	if !authManager.mu.enabled {
		return nil, "", errors.Newf("LDAP authentication: not enabled")
	}
	telemetry.Inc(beginAuthNUseCounter)

	if user.IsRootUser() || user.IsReserved() {
		return nil, "", errors.WithDetailf(
			errors.Newf("LDAP authentication: invalid identity"),
			"cannot use LDAP auth to login to a reserved user %s", user.Normalized())
	}

	if err := authManager.setLDAPConfigOptions(entry); err != nil {
		return nil, redact.Sprintf("error parsing hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authentication: unable to parse hba conf options")
	}

	if err := authManager.validateLDAPBaseOptions(); err != nil {
		return nil, redact.Sprintf("error validating base hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authentication: unable to validate authManager base options")
	}

	if err := authManager.validateLDAPAuthNOptions(); err != nil {
		return nil, redact.Sprintf("error validating authentication hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authentication: unable to validate authManager authentication options")
	}

	// Establish a LDAPs connection with the set LDAP server and port
	err := authManager.mu.util.MaybeInitLDAPsConn(ctx, authManager.mu.conf)
	if err != nil {
		return nil, redact.Sprintf("error when trying to create LDAP connection: %v", err),
			errors.Newf("LDAP authentication: unable to establish LDAP connection")
	}

	// Fetch the ldap server Distinguished Name using sql username as search value
	// for  ldap search attribute
	userDN, err := authManager.mu.util.Search(ctx, authManager.mu.conf, user.Normalized())
	if err != nil {
		return nil, redact.Sprintf("error when searching for user in LDAP server: %v", err),
			errors.WithDetailf(
				errors.Newf("LDAP authentication: unable to find LDAP user distinguished name"),
				"cannot find provided user %s on LDAP server", user.Normalized())
	}

	retrievedUserDN, err = distinguishedname.ParseDN(userDN)
	if err != nil {
		return nil, redact.Sprintf("error parsing user DN %s obtained from LDAP server: %v", userDN, err),
			errors.WithDetailf(
				errors.Newf("LDAP authentication: unable to parse LDAP user distinguished name"),
				"cannot find provided user %s on LDAP server", user.Normalized())
	}

	// Bind as the user to verify their password
	err = authManager.mu.util.Bind(ctx, userDN, ldapPwd)
	if err != nil {
		return retrievedUserDN, redact.Sprintf("error when binding as user %s with DN(%s) in LDAP server: %v",
				user.Normalized(), userDN, err,
			),
			errors.WithDetailf(
				errors.Newf("LDAP authentication: unable to bind as LDAP user"),
				"credentials invalid for LDAP server user %s", user.Normalized())
	}

	telemetry.Inc(loginSuccessUseCounter)
	return retrievedUserDN, "", nil
}
