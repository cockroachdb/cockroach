// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlclustersettings

import "github.com/cockroachdb/cockroach/pkg/settings"

// DefaultPrimaryRegionClusterSettingName is the name of the cluster setting that returns
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
