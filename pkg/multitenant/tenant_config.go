// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package multitenant

import (
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
)

// DefaultTenantSelectSettingName is the name of the setting that
// configures the default tenant to use when a client does not specify
// a specific tenant.
const DefaultTenantSelectSettingName = "server.controller.default_tenant"

// DefaultTenantSelect determines which tenant serves requests from
// clients that do not specify explicitly the tenant they want to use.
var DefaultTenantSelect = settings.RegisterStringSetting(
	settings.SystemOnly,
	DefaultTenantSelectSettingName,
	"name of the tenant to use to serve requests when clients don't specify a tenant",
	catconstants.SystemTenantName,
).WithPublic()

// VerifyTenantService determines whether there should be an advisory
// interlock between changes to the tenant service and changes to the
// above cluster setting.
var VerifyTenantService = settings.RegisterBoolSetting(
	settings.SystemOnly,
	"server.controller.default_tenant.check_service.enabled",
	"verify that a tenant's service is coherently set with the value of "+DefaultTenantSelectSettingName,
	true,
)
