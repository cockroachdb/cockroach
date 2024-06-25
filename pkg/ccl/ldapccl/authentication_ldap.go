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
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

const (
	counterPrefix           = "auth.ldap."
	beginAuthCounterName    = counterPrefix + "begin_auth"
	loginSuccessCounterName = counterPrefix + "login_success"
	enableCounterName       = counterPrefix + "enable"
)

var (
	beginAuthUseCounter    = telemetry.GetCounterOnce(beginAuthCounterName)
	loginSuccessUseCounter = telemetry.GetCounterOnce(loginSuccessCounterName)
	enableUseCounter       = telemetry.GetCounterOnce(enableCounterName)
)

// ldapAuthenticator is an object that is used to enable ldap connection
// validation that are used as part of the CRDB client auth flow.
//
// The implementation uses the `go-ldap/ldap/` client package and is supported
// through a number of cluster settings defined in `ldapccl/settings.go`. These
// settings specify how the ldap auth attempt should be executed and if this
// feature is enabled.
type ldapAuthenticator struct {
	mu struct {
		syncutil.RWMutex
		// conf contains all the values that come from cluster settings.
		conf ldapAuthenticatorConf
		// util contains connection object required for interfacing with ldap server.
		util ILDAPUtil
		// enabled represents the present state of if this feature is enabled. It
		// is set to true once ldap util is initialized.
		enabled bool
	}
	// clusterUUID is used to check the validity of the enterprise license. It is
	// set once at initialization.
	clusterUUID uuid.UUID
}

// ldapAuthenticatorConf contains all the values to configure LDAP
// authentication. These values are copied from the matching cluster settings or
// from hba conf options for LDAP entry.
type ldapAuthenticatorConf struct {
	domainCACert        string
	clientTLSCert       string
	clientTLSKey        string
	ldapServer          string
	ldapPort            string
	ldapBaseDN          string
	ldapBindDN          string
	ldapBindPassword    string
	ldapSearchFilter    string
	ldapSearchAttribute string
}

// reloadConfig locks mutex and then refreshes the values in conf from the cluster settings.
func (authenticator *ldapAuthenticator) reloadConfig(ctx context.Context, st *cluster.Settings) {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	authenticator.reloadConfigLocked(ctx, st)
}

// reloadConfig refreshes the values in conf from the cluster settings without locking the mutex.
func (authenticator *ldapAuthenticator) reloadConfigLocked(
	ctx context.Context, st *cluster.Settings,
) {
	conf := ldapAuthenticatorConf{
		domainCACert:  LDAPDomainCACertificate.Get(&st.SV),
		clientTLSCert: LDAPClientTLSCertSetting.Get(&st.SV),
		clientTLSKey:  LDAPClientTLSKeySetting.Get(&st.SV),
	}
	authenticator.mu.conf = conf

	var err error
	authenticator.mu.util, err = NewLDAPUtil(ctx, authenticator.mu.conf)
	if err != nil {
		log.Warningf(ctx, "LDAP authentication: unable to initialize LDAP connection: %v", err)
		return
	}

	if !authenticator.mu.enabled {
		telemetry.Inc(enableUseCounter)
	}
	authenticator.mu.enabled = true
	log.Infof(ctx, "initialized LDAP authenticator")
}

// setLDAPConfigOptions extracts hba conf parameters required for connecting and
// querying LDAP server from hba conf entry and sets them for LDAP authenticator.
func (authenticator *ldapAuthenticator) setLDAPConfigOptions(entry *hba.Entry) error {
	conf := ldapAuthenticatorConf{
		domainCACert: authenticator.mu.conf.domainCACert,
	}
	for _, opt := range entry.Options {
		switch opt[0] {
		case "ldapserver":
			conf.ldapServer = opt[1]
		case "ldapport":
			conf.ldapPort = opt[1]
		case "ldapbasedn":
			conf.ldapBaseDN = opt[1]
		case "ldapbinddn":
			conf.ldapBindDN = opt[1]
		case "ldapbindpasswd":
			conf.ldapBindPassword = opt[1]
		case "ldapsearchfilter":
			conf.ldapSearchFilter = opt[1]
		case "ldapsearchattribute":
			conf.ldapSearchAttribute = opt[1]
		default:
			return errors.Newf("invalid LDAP option provided in hba conf: %s", opt[0])
		}
	}
	authenticator.mu.conf = conf
	return nil
}

// validateLDAPOptions checks the ldap authenticator config values for validity.
func (authenticator *ldapAuthenticator) validateLDAPOptions() error {
	const ldapOptionsErrorMsg = "ldap params in HBA conf missing"
	if authenticator.mu.conf.ldapServer == "" {
		return errors.New(ldapOptionsErrorMsg + " ldap server")
	}
	if authenticator.mu.conf.ldapPort == "" {
		return errors.New(ldapOptionsErrorMsg + " ldap port")
	}
	if authenticator.mu.conf.ldapBaseDN == "" {
		return errors.New(ldapOptionsErrorMsg + " base DN")
	}
	if authenticator.mu.conf.ldapBindDN == "" {
		return errors.New(ldapOptionsErrorMsg + " bind DN")
	}
	if authenticator.mu.conf.ldapBindPassword == "" {
		return errors.New(ldapOptionsErrorMsg + " bind password")
	}
	if authenticator.mu.conf.ldapSearchFilter == "" {
		return errors.New(ldapOptionsErrorMsg + " search filter")
	}
	if authenticator.mu.conf.ldapSearchAttribute == "" {
		return errors.New(ldapOptionsErrorMsg + " search attribute")
	}
	return nil
}

// ValidateLDAPLogin validates an attempt to bind to an LDAP server.
// In particular, it checks that:
// * The cluster has an enterprise license.
// * The active cluster version is 24.2 for this feature.
// * LDAP authentication is enabled after settings were reloaded.
// * The auth attempt is not for a reserved user.
// * The hba conf entry options could be parsed to obtain ldap server params.
// * All ldap server params are valid.
// * LDAPs connection can be established with configured server.
// * Configured bind DN and password can be used to search for the sql user DN on ldap server.
// * The obtained user DN could be used to bind with the password from sql connection string.
// It returns authError (which is the error sql clients will see in case of
// failures) and detailedError (which is the internal error from ldap clients
// that might contain sensitive information we do not want to send to sql
// clients but still want to log it). We do not want to send any information
// back to client which was not provided by the client.
func (authenticator *ldapAuthenticator) ValidateLDAPLogin(
	ctx context.Context,
	st *cluster.Settings,
	user username.SQLUsername,
	ldapPwd string,
	entry *hba.Entry,
	_ *identmap.Conf,
) (detailedErrorMsg redact.RedactableString, authError error) {
	if err := utilccl.CheckEnterpriseEnabled(st, "LDAP authentication"); err != nil {
		return "", err
	}
	if !st.Version.IsActive(ctx, clusterversion.V24_2) {
		return "", pgerror.Newf(pgcode.FeatureNotSupported, "LDAP authentication is only supported after v24.2 upgrade is finalized")
	}

	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()

	if !authenticator.mu.enabled {
		return "", errors.Newf("LDAP authentication: not enabled")
	}
	telemetry.Inc(beginAuthUseCounter)

	if user.IsRootUser() || user.IsReserved() {
		return "", errors.WithDetailf(
			errors.Newf("LDAP authentication: invalid identity"),
			"cannot use LDAP auth to login to a reserved user %s", user.Normalized())
	}

	if err := authenticator.setLDAPConfigOptions(entry); err != nil {
		return redact.Sprintf("error parsing hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authentication: unable to parse hba conf options")
	}

	if err := authenticator.validateLDAPOptions(); err != nil {
		return redact.Sprintf("error validating hba conf options for LDAP: %v", err),
			errors.Newf("LDAP authentication: unable to validate authenticator options")
	}

	// Establish a LDAPs connection with the set LDAP server and port
	err := authenticator.mu.util.InitLDAPsConn(ctx, authenticator.mu.conf)
	if err != nil {
		return redact.Sprintf("error when trying to create LDAP connection: %v", err),
			errors.Newf("LDAP authentication: unable to establish LDAP connection")
	}

	// Fetch the ldap server Distinguished Name using sql username as search value
	// for  ldap search attribute
	userDN, err := authenticator.mu.util.Search(ctx, authenticator.mu.conf, user.Normalized())
	if err != nil {
		return redact.Sprintf("error when searching for user in LDAP server: %v", err),
			errors.WithDetailf(
				errors.Newf("LDAP authentication: unable to find LDAP user distinguished name"),
				"cannot find provided user %s on LDAP server", user.Normalized())
	}

	// Bind as the user to verify their password
	err = authenticator.mu.util.Bind(ctx, userDN, ldapPwd)
	if err != nil {
		return redact.Sprintf("error when binding as user %s with DN(%s) in LDAP server: %v",
				user.Normalized(), userDN, err,
			),
			errors.WithDetailf(
				errors.Newf("LDAP authentication: unable to bind as LDAP user"),
				"credentials invalid for LDAP server user %s", user.Normalized())
	}

	telemetry.Inc(loginSuccessUseCounter)
	return "", nil
}

// ConfigureLDAPAuth initializes and returns a ldapAuthenticator. It also sets up listeners so
// that the ldapAuthenticator's config is updated when the cluster settings values change.
var ConfigureLDAPAuth = func(
	serverCtx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	clusterUUID uuid.UUID,
) pgwire.LDAPVerifier {
	authenticator := ldapAuthenticator{}
	authenticator.clusterUUID = clusterUUID
	authenticator.reloadConfig(serverCtx, st)
	LDAPDomainCACertificate.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	return &authenticator
}

func init() {
	pgwire.ConfigureLDAPAuth = ConfigureLDAPAuth
}
