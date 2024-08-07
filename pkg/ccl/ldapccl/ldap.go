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

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/hba"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

const (
	counterPrefix     = "auth.ldap."
	enableCounterName = counterPrefix + "enable"
)

var enableUseCounter = telemetry.GetCounterOnce(enableCounterName)

// ldapAuthManager is an object that is used for both:
// 1. enabling ldap connection validation that are used as part of the CRDB
// client auth flow.
// 2. facilitating authorization by fetch parent groups as part of CRDB role
// privilege resolution.
//
// The implementation uses the `go-ldap/ldap/` client package and is supported
// through a number of cluster settings defined in `ldapccl/settings.go`. These
// settings specify how the ldap auth attempt should be executed and if this
// feature is enabled. A common ldapAuthManager object is used for both authN
// and authZ to reduce redundancy of cluster settings listeners and auth
// parameter configurations.
type ldapAuthManager struct {
	mu struct {
		syncutil.RWMutex
		// conf contains all the values that come from cluster settings.
		conf ldapConfig
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

// ldapConfig contains all the values to configure LDAP authN and authZ. These
// values are set using matching cluster settings or from hba conf options for
// LDAP entry.
type ldapConfig struct {
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
	ldapGroupListFilter string
}

// reloadConfig locks mutex and then refreshes the values in conf from the cluster settings.
func (authManager *ldapAuthManager) reloadConfig(ctx context.Context, st *cluster.Settings) {
	authManager.mu.Lock()
	defer authManager.mu.Unlock()
	authManager.reloadConfigLocked(ctx, st)
}

// reloadConfig refreshes the values in conf from the cluster settings without locking the mutex.
func (authManager *ldapAuthManager) reloadConfigLocked(ctx context.Context, st *cluster.Settings) {
	conf := ldapConfig{
		domainCACert:  LDAPDomainCACertificate.Get(&st.SV),
		clientTLSCert: LDAPClientTLSCertSetting.Get(&st.SV),
		clientTLSKey:  LDAPClientTLSKeySetting.Get(&st.SV),
	}
	authManager.mu.conf = conf

	var err error
	authManager.mu.util, err = NewLDAPUtil(ctx, authManager.mu.conf)
	if err != nil {
		log.Warningf(ctx, "LDAP auth manager: unable to initialize LDAP connection: %v", err)
		return
	}

	if !authManager.mu.enabled {
		telemetry.Inc(enableUseCounter)
	}
	authManager.mu.enabled = true
	log.Infof(ctx, "initialized LDAP authManager")
}

// setLDAPConfigOptions extracts hba conf parameters required for connecting and
// querying LDAP server from hba conf entry and sets them for LDAP auth.
func (authManager *ldapAuthManager) setLDAPConfigOptions(entry *hba.Entry) error {
	conf := ldapConfig{
		domainCACert: authManager.mu.conf.domainCACert,
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
		case "ldapgrouplistfilter":
			conf.ldapGroupListFilter = opt[1]
		default:
			return errors.Newf("invalid LDAP option provided in hba conf: %s", opt[0])
		}
	}
	authManager.mu.conf = conf
	return nil
}

// validateLDAPBaseOptions checks the mandatory ldap auth config values for validity.
func (authManager *ldapAuthManager) validateLDAPBaseOptions() error {
	const ldapOptionsErrorMsg = "ldap params in HBA conf missing"
	if authManager.mu.conf.ldapServer == "" {
		return errors.New(ldapOptionsErrorMsg + " ldap server")
	}
	if authManager.mu.conf.ldapPort == "" {
		return errors.New(ldapOptionsErrorMsg + " ldap port")
	}
	if authManager.mu.conf.ldapBaseDN == "" {
		return errors.New(ldapOptionsErrorMsg + " base DN")
	}
	if authManager.mu.conf.ldapBindDN == "" {
		return errors.New(ldapOptionsErrorMsg + " bind DN")
	}
	if authManager.mu.conf.ldapBindPassword == "" {
		return errors.New(ldapOptionsErrorMsg + " bind password")
	}
	return nil
}

// ConfigureLDAPAuth initializes and returns a ldapAuthManager. It also sets up listeners so
// that the ldapAuthManager's config is updated when the cluster settings values change.
var ConfigureLDAPAuth = func(
	serverCtx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	clusterUUID uuid.UUID,
) pgwire.LDAPManager {
	authManager := ldapAuthManager{}
	authManager.clusterUUID = clusterUUID
	authManager.reloadConfig(serverCtx, st)
	LDAPDomainCACertificate.SetOnChange(&st.SV, func(ctx context.Context) {
		authManager.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	return &authManager
}

func init() {
	pgwire.ConfigureLDAPAuth = ConfigureLDAPAuth
}
