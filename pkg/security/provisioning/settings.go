package provisioning

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
)

// All cluster settings necessary for the provisioning feature.
const (
	supportedAuthMethodLDAP           = "ldap"
	baseProvisioningSettingName       = "security.provisioning."
	ldapProvisioningEnableSettingName = baseProvisioningSettingName + "ldap.enabled"
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
	"enables or disables automatic user provisioning for ldap authentication method",
	false,
	settings.WithReportable(true),
	settings.WithPublic,
)

type clusterProvisioningConfig struct {
	settings *cluster.Settings
}

var _ UserProvisioningConfig = clusterProvisioningConfig{}

// Enabled validates if automatic user provisioning is enabled for the provided
// authentication method via cluster settings.
func (c clusterProvisioningConfig) Enabled(authMethod string) bool {
	switch authMethod {
	case supportedAuthMethodLDAP:
		return ldapProvisioningEnabled.Get(&c.settings.SV)
	default:
		return false
	}
}

// ClusterProvisioningConfig creates a UserProvisioningConfig backed by the
// given cluster settings.
func ClusterProvisioningConfig(settings *cluster.Settings) UserProvisioningConfig {
	return clusterProvisioningConfig{settings}
}

// noProvisioningConfig defines the base user provisioning settings.
type noProvisioningConfig struct{}

var _ UserProvisioningConfig = noProvisioningConfig{}

// Enabled validates if automatic user provisioning is enabled for the provided
// authentication method via command-line tools.
func (noProvisioningConfig) Enabled(_ string) bool {
	return false
}
