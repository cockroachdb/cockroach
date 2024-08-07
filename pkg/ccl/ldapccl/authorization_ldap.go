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
)

const (
	beginAuthZCounterName   = counterPrefix + "begin_authorization"
	authZSuccessCounterName = counterPrefix + "authorization_success"
)

var (
	beginAuthZUseCounter = telemetry.GetCounterOnce(beginAuthZCounterName)
	authZSuccessCounter  = telemetry.GetCounterOnce(authZSuccessCounterName)
)

// validateLDAPAuthZOptions checks the ldap authorization config values.
func (authManager *ldapAuthManager) validateLDAPAuthZOptions() error {
	const ldapOptionsErrorMsg = "ldap authorization params in HBA conf missing"
	if authManager.mu.conf.ldapGroupListFilter == "" {
		return errors.New(ldapOptionsErrorMsg + " group list attribute")
	}
	return nil
}

// FetchLDAPGroups retrieves ldap groups for supplied ldap user DN.
// In particular, it checks that:
// * The cluster has an enterprise license.
// * The active cluster version is 24.2 for this feature.
// * The provided LDAP user distinguished name is a valid DN.
// * LDAP authManager is enabled after settings were reloaded.
// * The hba conf entry options could be parsed to obtain ldap server params.
// * All ldap server params are valid.
// * LDAPs connection can be established with configured server.
// * Configured bind DN and password can be used to fetch ldap groups for provided user DN.
// It returns the ldap groups DN list for which  the user is a member, authError
// (which is the error sql clients will see in case of failures) and
// detailedError (which is the internal error from ldap clients that might
// contain sensitive information we do not want to send to sql clients but still
// want to log it). We do not want to send any information back to client which
// was not provided by the client.
func (authManager *ldapAuthManager) FetchLDAPGroups(
	ctx context.Context,
	st *cluster.Settings,
	userDN username.SQLUsername,
	entry *hba.Entry,
	_ *identmap.Conf,
) (ldapGroups []string, detailedErrorMsg redact.RedactableString, authError error) {
	if err := utilccl.CheckEnterpriseEnabled(st, "LDAP authorization"); err != nil {
		return nil, "", err
	}
	if !st.Version.IsActive(ctx, clusterversion.V24_2) {
		return nil, "", pgerror.Newf(pgcode.FeatureNotSupported, "LDAP authorization is only supported after v24.2 upgrade is finalized")
	}

	authManager.mu.Lock()
	defer authManager.mu.Unlock()

	if !authManager.mu.enabled {
		return nil, "", errors.Newf("LDAP authentication: not enabled")
	}
	telemetry.Inc(beginAuthZUseCounter)

	if err := distinguishedname.ValidateDN(userDN.Normalized()); err != nil {
		return nil,
			redact.Sprintf("error validating provided ldap DN %q for LDAP: %v", userDN.Normalized(), err),
			errors.Newf("LDAP authorization: unable to validate provided ldap DN")
	}

	if err := authManager.setLDAPConfigOptions(entry); err != nil {
		return nil, redact.Sprintf("error parsing hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authorization: unable to parse hba conf options")
	}

	if err := authManager.validateLDAPBaseOptions(); err != nil {
		return nil, redact.Sprintf("error validating base hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authorization: unable to validate authManager base options")
	}

	if err := authManager.validateLDAPAuthZOptions(); err != nil {
		return nil, redact.Sprintf("error validating authZ hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authorization: unable to validate authManager authZ options")
	}

	// Establish a LDAPs connection with the set LDAP server and port
	err := authManager.mu.util.MaybeInitLDAPsConn(ctx, authManager.mu.conf)
	if err != nil {
		return nil, redact.Sprintf("error when trying to create LDAP connection: %v", err),
			errors.Newf("LDAP authorization: unable to establish LDAP connection")
	}

	// Fetch the ldap server Distinguished Name using sql username as search value
	// for  ldap search attribute
	ldapGroups, err = authManager.mu.util.ListGroups(ctx, authManager.mu.conf, userDN.Normalized())
	if err != nil {
		return nil, redact.Sprintf("error when searching for user dn %q in LDAP server: %v", userDN.Normalized(), err),
			errors.WithDetailf(
				errors.Newf("LDAP authorization: unable to fetch groups for user"),
				"cannot find groups for which user is a member")
	}

	telemetry.Inc(authZSuccessCounter)
	return ldapGroups, "", nil
}
