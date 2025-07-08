// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/go-ldap/ldap/v3"
)

const (
	beginAuthZCounterName   = counterPrefix + "begin_authorization"
	authZSuccessCounterName = counterPrefix + "authorization_success"
)

var (
	beginAuthZUseCounter = telemetry.GetCounterOnce(beginAuthZCounterName)
	authZSuccessCounter  = telemetry.GetCounterOnce(authZSuccessCounterName)
)

// FetchLDAPGroups retrieves ldap groups for supplied ldap user DN.
// In particular, it checks that:
// * The cluster has an enterprise license.
// * The active cluster version is 24.3 for this feature.
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
	userDN *ldap.DN,
	user username.SQLUsername,
	entry *hba.Entry,
	_ *identmap.Conf,
) (_ []*ldap.DN, detailedErrorMsg redact.RedactableString, authError error) {
	if err := utilccl.CheckEnterpriseEnabled(st, "LDAP authorization"); err != nil {
		return nil, "", err
	}

	authManager.mu.Lock()
	defer authManager.mu.Unlock()

	if !authManager.mu.enabled {
		return nil, "", errors.Newf("LDAP authentication: not enabled")
	}
	telemetry.Inc(beginAuthZUseCounter)

	if err := authManager.setLDAPConfigOptions(entry); err != nil {
		return nil, redact.Sprintf("error parsing hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authorization: unable to parse hba conf options")
	}

	// Establish a LDAPs connection with the set LDAP server and port
	err := authManager.mu.util.MaybeInitLDAPsConn(ctx, authManager.mu.conf)
	if err != nil {
		return nil, redact.Sprintf("error when trying to create LDAP connection: %v", err),
			errors.Newf("LDAP authorization: unable to establish LDAP connection")
	}

	// Bind with ldap service user DN and passwd for performing the groups listing for ldap user.
	if err := authManager.mu.util.Bind(ctx, authManager.mu.conf.ldapBindDN, authManager.mu.conf.ldapBindPassword); err != nil {
		return nil, redact.Sprintf("error binding ldap service account: %v", err),
			errors.Newf("LDAP authorization: error binding as LDAP service user with configured credentials")
	}

	// Fetch the ldap server Distinguished Name using sql username as search value
	// for  ldap search attribute
	fetchedGroups, err := authManager.mu.util.ListGroups(ctx, authManager.mu.conf, userDN.String())
	if err != nil {
		return nil, redact.Sprintf("error when fetching groups for user dn %q in LDAP server: %v", userDN.String(), err),
			errors.WithDetailf(
				errors.Newf("LDAP authorization: unable to fetch groups for user"),
				"cannot find groups for which user is a member")
	}

	ldapGroups := make([]*ldap.DN, len(fetchedGroups))
	for idx := range fetchedGroups {
		ldapGroups[idx], err = distinguishedname.ParseDN(fetchedGroups[idx])
		if err != nil {
			return nil, redact.Sprintf("error parsing member group DN %s obtained from LDAP server: %v", ldapGroups[idx], err),
				errors.WithDetailf(
					errors.Newf("LDAP authentication: unable to parse member LDAP group distinguished name"),
					"cannot find provided user %s on LDAP server", user.Normalized())
		}
	}

	telemetry.Inc(authZSuccessCounter)
	return ldapGroups, "", nil
}

// Authorize synchronizes a user's roles from their LDAP group memberships.
// It returns a generic, client-safe error and a detailed error for internal
// logging.
func (authManager *ldapAuthManager) Authorize(
	ctx context.Context,
	execCfg *sql.ExecutorConfig,
	ldapUserDN *ldap.DN,
	user username.SQLUsername,
	entry *hba.Entry,
	identMap *identmap.Conf,
) (redact.RedactableString, error) {
	ldapGroups, detailedErrors, authError := authManager.FetchLDAPGroups(
		ctx, execCfg.Settings, ldapUserDN, user, entry, identMap,
	)
	if authError != nil {
		return detailedErrors, authError
	}

	// Parse and apply transformation to LDAP group DNs for roles granter.
	sqlRoles := make([]username.SQLUsername, 0, len(ldapGroups))
	for _, ldapGroup := range ldapGroups {
		// Extract the CN from the LDAP group DN to use as the SQL role.
		sqlRole, found, err := distinguishedname.ExtractCNAsSQLUsername(ldapGroup)
		if err != nil {
			detailedErr := errors.Wrapf(err, "LDAP authorization: error finding matching SQL role for group %s", ldapGroup.String())
			clientErr := errors.New("LDAP authorization: could not map LDAP group to a valid role")
			return redact.Sprint(detailedErr), clientErr
		}
		if !found {
			continue
		}
		sqlRoles = append(sqlRoles, sqlRole)
	}

	// Assign roles to the user.
	if err := sql.EnsureUserOnlyBelongsToRoles(ctx, execCfg, user, sqlRoles); err != nil {
		detailedErr := errors.Wrapf(err, "LDAP authorization: error assigning roles to user %s", user)
		clientErr := errors.New("LDAP authorization: failed to synchronize roles")
		return redact.Sprint(detailedErr), clientErr
	}
	return "", nil
}
