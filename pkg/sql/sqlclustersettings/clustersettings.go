// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlclustersettings

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/errors"
)

// DefaultPrimaryRegionClusterSettingName is the name of the cluster setting
// that returns the default primary region.
const DefaultPrimaryRegionClusterSettingName = "sql.defaults.primary_region"

// DefaultPrimaryRegion is a cluster setting that contains the default primary region.
var DefaultPrimaryRegion = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	DefaultPrimaryRegionClusterSettingName,
	`if not empty, all databases created without a PRIMARY REGION will `+
		`implicitly have the given PRIMARY REGION`,
	"",
	settings.WithPublic)

// PublicSchemaCreatePrivilegeEnabled is the cluster setting that determines
// whether the CREATE privilege is given to the `public` role on the `public`
// schema at the time the schema is created.
var PublicSchemaCreatePrivilegeEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	"sql.auth.public_schema_create_privilege.enabled",
	"determines whether to grant all users the CREATE privileges on the public "+
		"schema when it is created",
	true,
	settings.WithPublic)

// RestrictAccessToSystemInterface restricts access to certain SQL
// features from the system tenant/interface. This restriction exists
// to prevent the following UX surprise:
//
//   - end-user desires to achieve a certain outcome in a virtual cluster;
//   - however, they mess up their connection string and connect to the
//     system tenant instead;
//   - without this setting, the resulting SQL would succeed in the
//     system tenant and the user would not realize they were not
//     connected to the right place.
var RestrictAccessToSystemInterface = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"sql.restrict_system_interface.enabled",
	"if enabled, certain statements produce errors or warnings when run from the system interface to encourage use of a virtual cluster",
	false)

// SecondaryTenantZoneConfigsEnabled controls if secondary tenants are allowed
// to set zone configurations. It has no effect for the system tenant.
//
// This setting has no effect on zone configurations that have already been set.
var SecondaryTenantZoneConfigsEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"sql.zone_configs.allow_for_secondary_tenant.enabled",
	"enable the use of ALTER CONFIGURE ZONE in virtual clusters",
	true,
	settings.WithName("sql.virtual_cluster.feature_access.zone_configs.enabled"),
)

// SecondaryTenantsAllZoneConfigsEnabled is an extension of
// SecondaryTenantZoneConfigsEnabled that allows virtual clusters to modify all
// type of constraints in zone configs (i.e. not only zones and regions).
var SecondaryTenantsAllZoneConfigsEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"sql.virtual_cluster.feature_access.zone_configs_unrestricted.enabled",
	"enable unrestricted usage of ALTER CONFIGURE ZONE in virtual clusters",
	true,
)

// MultiRegionSystemDatabaseEnabled controls if system tenants are allowed
// to be set up to be multi-region.
var MultiRegionSystemDatabaseEnabled = settings.RegisterBoolSetting(
	settings.SystemVisible,
	"sql.multiregion.system_database_multiregion.enabled",
	"enable option to set up system database as multi-region",
	false,
)

// RequireSystemTenantOrClusterSetting returns a setting disabled error if
// executed from inside a secondary tenant that does not have the specified
// cluster setting.
func RequireSystemTenantOrClusterSetting(
	codec keys.SQLCodec, settings *cluster.Settings, setting *settings.BoolSetting,
) error {
	if codec.ForSystemTenant() || setting.Get(&settings.SV) {
		return nil
	}
	return errors.WithDetailf(errors.WithHint(
		errors.New("operation is disabled within a virtual cluster"),
		"Feature was disabled by the system operator."),
		"Feature flag: %s", setting.Name())
}

// CachedSequencesCacheSizeSetting is the default cache size used when
// SessionNormalizationMode is SerialUsesCachedSQLSequences or
// SerialUsesCachedNodeSQLSequences.
var CachedSequencesCacheSizeSetting = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"sql.defaults.serial_sequences_cache_size",
	"the default cache size when the session's serial normalization mode is set to cached sequences"+
		"A cache size of 1 means no caching. Any cache size less than 1 is invalid.",
	256,
	settings.PositiveInt,
)
