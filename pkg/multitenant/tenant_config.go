// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package multitenant

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
)

// DefaultClusterSelectSettingName is the name of the setting that
// configures the default tenant to use when a client does not specify
// a specific tenant.
const DefaultClusterSelectSettingName = "server.controller.default_target_cluster"

// DefaultTenantSelect determines which tenant serves requests from
// clients that do not specify explicitly the tenant they want to use.
var DefaultTenantSelect = settings.RegisterStringSetting(
	settings.SystemOnly,
	"server.controller.default_tenant",
	"name of the virtual cluster to use when SQL or HTTP clients don't specify a target cluster",
	catconstants.SystemTenantName,
	settings.WithName(DefaultClusterSelectSettingName),
)

// WaitForClusterStartTimeout is the amount of time the tenant
// controller will wait for the default virtual cluster to have an
// active SQL server.
var WaitForClusterStartTimeout = settings.RegisterDurationSetting(
	settings.SystemOnly,
	"server.controller.mux_virtual_cluster_wait.timeout",
	"amount of time to wait for a default virtual cluster to become available when serving SQL connections (0 to disable)",
	10*time.Second,
)
