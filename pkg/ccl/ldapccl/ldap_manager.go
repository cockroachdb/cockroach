// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ldapccl

import (
	"context"
	"net/url"
	"regexp"

	"github.com/cockroachdb/cockroach/pkg/security/distinguishedname"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
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

var (
	enableUseCounter = telemetry.GetCounterOnce(enableCounterName)
	// ldapSearchRe performs a regex match for ldap search options provided in HBA
	// configuration. This generally adheres to the format "(key=value)" with
	// interleaved spaces but could be more flexible as value field could be
	// provided as a wildcard match string(mail=*@example.com), a key-value
	// distinguished name("memberOf=CN=test") or a combination of
	// both("memberOf=CN=Sh*").
	//
	// The regex string is kept generic as search options could also contain
	// multiple search entries like "(key1=value1)(key2=value2)".
	ldapSearchRe = regexp.MustCompile(`\(\s*\S+\s*=\s*\S+.*\)`)
)

// ldapAuthManager is an object that is used for both:
// 1. enabling ldap connection validation that are used as part of the CRDB
// client auth flow.
// 2. facilitating authorization to fetch parent groups as part of CRDB role
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
	conf := authManager.mu.conf
	conf.domainCACert = LDAPDomainCACertificate.Get(&st.SV)
	conf.clientTLSCert = LDAPClientTLSCertSetting.Get(&st.SV)
	conf.clientTLSKey = LDAPClientTLSKeySetting.Get(&st.SV)
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
	conf := authManager.mu.conf
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

// checkHBAEntryLDAP validates that the HBA entry for ldap has all the options
// set to acceptable values and mandatory options are all set.
func checkHBAEntryLDAP(_ *settings.Values, entry hba.Entry) error {
	var parseErr error
	entryOptions := map[string]bool{}
	for _, opt := range entry.Options {
		switch opt[0] {
		case "ldapserver":
			_, parseErr = url.Parse(opt[1])
		case "ldapport":
			if opt[1] != "389" && opt[1] != "636" {
				parseErr = errors.Newf("%q is not set to either 389 or 636", opt[0])
			}
		case "ldapbasedn":
			fallthrough
		case "ldapbinddn":
			_, parseErr = distinguishedname.ParseDN(opt[1])
		case "ldapbindpasswd":
			fallthrough
		case "ldapsearchattribute":
			if opt[1] == "" {
				parseErr = errors.Newf("%q is set to empty", opt[0])
			}
		case "ldapsearchfilter":
			fallthrough
		case "ldapgrouplistfilter":
			if !ldapSearchRe.MatchString(opt[1]) {
				parseErr = errors.Newf("%q is not of the format \"(key = value)\"", opt[0])
			}
		default:
			return errors.Newf("unknown ldap option provided in hba conf: %q", opt[0])
		}
		if parseErr != nil {
			return errors.Wrapf(parseErr, "LDAP option %q is set to invalid value: %q", opt[0], opt[1])
		}
		entryOptions[opt[0]] = true
	}
	// check for missing ldap options
	for _, opt := range []string{"ldapserver", "ldapport", "ldapbasedn", "ldapbinddn", "ldapbindpasswd", "ldapsearchattribute", "ldapsearchfilter"} {
		if _, ok := entryOptions[opt]; !ok {
			return errors.Newf("ldap option not found in hba entry: %q", opt)
		}
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
	LDAPClientTLSCertSetting.SetOnChange(&st.SV, func(ctx context.Context) {
		authManager.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	LDAPClientTLSKeySetting.SetOnChange(&st.SV, func(ctx context.Context) {
		authManager.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	return &authManager
}

func init() {
	pgwire.ConfigureLDAPAuth = ConfigureLDAPAuth
	pgwire.RegisterAuthMethod("ldap", pgwire.AuthLDAP, hba.ConnAny, checkHBAEntryLDAP)
}
