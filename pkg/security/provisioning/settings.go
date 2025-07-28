// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package provisioning

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// All cluster settings necessary for the provisioning feature.
const (
	supportedAuthMethodLDAP             = "ldap"
	testSupportedAuthMethodCertPassword = "cert-password"
	supportedAuthMethodJWT              = "jwt_token"
	baseProvisioningSettingName         = "security.provisioning."
	ldapProvisioningEnableSettingName   = baseProvisioningSettingName + "ldap.enabled"
	jwtProvisioningEnableSettingName    = baseProvisioningSettingName + "jwt.enabled"

	baseCounterPrefix = "auth.provisioning."
	ldapCounterPrefix = baseCounterPrefix + "ldap."
	jwtCounterPrefix  = baseCounterPrefix + "jwt."

	beginLDAPProvisionCounterName   = ldapCounterPrefix + "begin"
	provisionLDAPSuccessCounterName = ldapCounterPrefix + "success"
	enableLDAPProvisionCounterName  = ldapCounterPrefix + "enable"

	beginJWTProvisionCounterName   = jwtCounterPrefix + "begin"
	provisionJWTSuccessCounterName = jwtCounterPrefix + "success"
	enableJWTProvisionCounterName  = jwtCounterPrefix + "enable"

	provisionedUserLoginSuccessCounterName = baseCounterPrefix + "login_success"
)

var (
	BeginLDAPProvisionUseCounter = telemetry.GetCounterOnce(beginLDAPProvisionCounterName)
	ProvisionLDAPSuccessCounter  = telemetry.GetCounterOnce(provisionLDAPSuccessCounterName)
	enableLDAPProvisionCounter   = telemetry.GetCounterOnce(enableLDAPProvisionCounterName)

	BeginJWTProvisionUseCounter = telemetry.GetCounterOnce(beginJWTProvisionCounterName)
	ProvisionJWTSuccessCounter  = telemetry.GetCounterOnce(provisionJWTSuccessCounterName)
	enableJWTProvisionCounter   = telemetry.GetCounterOnce(enableJWTProvisionCounterName)

	ProvisionedUserLoginSuccessCounter = telemetry.GetCounterOnce(provisionedUserLoginSuccessCounterName)
)

// UserProvisioningConfig allows for customization of automatic user
// provisioning behavior. It is backed by cluster settings in a running node,
// but may be overridden differently in CLI tools.
type UserProvisioningConfig interface {
	Enabled(authMethod string) bool
}

// ldapProvisioningEnabled enables automatic user provisioning for ldap
// authentication method.
var ldapProvisioningEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	ldapProvisioningEnableSettingName,
	"enables automatic creation of SQL users upon successful LDAP login",
	false,
	settings.WithReportable(true),
	settings.WithPublic,
)

// jwtProvisioningEnabled enables automatic user provisioning for jwt
// authentication method.
var jwtProvisioningEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	jwtProvisioningEnableSettingName,
	"enables or disables automatic user provisioning for jwt authentication method",
	false,
)

type clusterProvisioningConfig struct {
	settings *cluster.Settings
}

var _ UserProvisioningConfig = clusterProvisioningConfig{}
var Testing = struct {
	Supported bool
}{}

// Enabled validates if automatic user provisioning is enabled for the provided
// authentication method via cluster settings.
func (c clusterProvisioningConfig) Enabled(authMethod string) bool {
	switch authMethod {
	case supportedAuthMethodLDAP:
		return ldapProvisioningEnabled.Get(&c.settings.SV)
	case testSupportedAuthMethodCertPassword:
		return Testing.Supported
	case supportedAuthMethodJWT:
		return jwtProvisioningEnabled.Get(&c.settings.SV)
	default:
		return false
	}
}

// ClusterProvisioningConfig creates a UserProvisioningConfig backed by the
// given cluster settings. It also installs a callback for changes to cluster
// setting related to enablement of provisioning for different auth methods.
func ClusterProvisioningConfig(settings *cluster.Settings) UserProvisioningConfig {
	ldapProvisioningEnabled.SetOnChange(&settings.SV, func(_ context.Context) {
		if ldapProvisioningEnabled.Get(&settings.SV) {
			telemetry.Inc(enableLDAPProvisionCounter)
		}
	})
	jwtProvisioningEnabled.SetOnChange(&settings.SV, func(_ context.Context) {
		if jwtProvisioningEnabled.Get(&settings.SV) {
			telemetry.Inc(enableJWTProvisionCounter)
		}
	})
	return clusterProvisioningConfig{settings: settings}
}
