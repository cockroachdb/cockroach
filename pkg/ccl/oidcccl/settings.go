// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
	"github.com/coreos/go-oidc"
)

// All cluster settings necessary for the OIDC feature.
const (
	baseOIDCSettingName           = "server.oidc_authentication."
	OIDCEnabledSettingName        = baseOIDCSettingName + "enabled"
	OIDCClientIDSettingName       = baseOIDCSettingName + "client_id"
	OIDCClientSecretSettingName   = baseOIDCSettingName + "client_secret"
	OIDCRedirectURLSettingName    = baseOIDCSettingName + "redirect_url"
	OIDCProviderURLSettingName    = baseOIDCSettingName + "provider_url"
	OIDCScopesSettingName         = baseOIDCSettingName + "scopes"
	OIDCClaimJSONKeySettingName   = baseOIDCSettingName + "claim_json_key"
	OIDCPrincipalRegexSettingName = baseOIDCSettingName + "principal_regex"
	OIDCButtonTextSettingName     = baseOIDCSettingName + "button_text"
)

// OIDCEnabledCounter counts how many times the OIDC feature is enabled in cluster settings
var OIDCEnabledCounter = telemetry.GetCounterOnce("server.oidc_authentication.enabled")

// OIDCDisabledCounter counts how many times the OIDC feature is disabled in cluster settings
var OIDCDisabledCounter = telemetry.GetCounterOnce("server.oidc_authentication.disabled")

// OIDCEnabled enables or disabled OIDC login for the Admin UI
var OIDCEnabled = func() *settings.BoolSetting {
	s := settings.RegisterPublicBoolSetting(
		OIDCEnabledSettingName,
		"enables or disabled OIDC login for the Admin UI",
		false,
	)
	s.SetReportable(true)
	return s
}()

// OIDCClientID is the OIDC client id
var OIDCClientID = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCClientIDSettingName,
		"experimental: OIDC client id",
		"",
	)
	s.SetReportable(true)
	return s
}()

// OIDCClientSecret is the OIDC client secret
var OIDCClientSecret = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCClientSecretSettingName,
		"experimental: OIDC client secret",
		"",
	)
	s.SetReportable(false)
	return s
}()

// OIDCRedirectURL is the cluster URL to redirect to after OIDC auth completes
var OIDCRedirectURL = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCRedirectURLSettingName,
		"experimental: OIDC redirect URL (base HTTP URL, likely your load balancer, "+
			"CRDB will automatically append `/oidc/callback` to the value)",
		"https://localhost:8080/oidc/callback",
	)
	s.SetReportable(true)
	return s
}()

// OIDCProviderURL is the location of the OIDC discovery document for the auth provider
var OIDCProviderURL = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCProviderURLSettingName,
		"experimental: OIDC provider URL ({provider_url}/.well-known/openid-configuration must resolve)",
		"",
	)
	s.SetReportable(true)
	return s
}()

// OIDCScopes contains the list of scopes to request from the auth provider
var OIDCScopes = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		OIDCScopesSettingName,
		"experimental: OIDC scopes to include with authentication request "+
			"(space delimited list of strings, required to start with `openid`)",
		"openid",
		func(values *settings.Values, s string) error {
			if !strings.HasPrefix(s, oidc.ScopeOpenID) {
				return errors.New("Missing `openid` scope which is required for OIDC")
			}
			return nil
		},
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// OIDCClaimJSONKey is the key of the claim to extract from the OIDC id_token
var OIDCClaimJSONKey = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCClaimJSONKeySettingName,
		"experimental: JSON key of principal to extract from payload after OIDC authentication completes "+
			"(usually email or sid)",
		"",
	)
	return s
}()

// OIDCPrincipalRegex is a regular expression to apply to the OIDC id_token claim value to conver
// it to a DB principal
var OIDCPrincipalRegex = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		OIDCPrincipalRegexSettingName,
		"experimental: regular expression to apply to extracted principal (see claim_json_key setting) to "+
			"translate to SQL user (golang regex format, must include 1 grouping to extract)",
		"(.+)",
		func(values *settings.Values, s string) error {
			_, err := regexp.Compile(s)
			if err != nil {
				return errors.Wrapf(err, "unable to initialize %s setting, regex does not compile",
					OIDCPrincipalRegexSettingName)
			}
			return nil
		},
	)
	s.SetVisibility(settings.Public)
	return s
}()

// OIDCButtonText is a string to display on the button in the Admin UI to login with OIDC
var OIDCButtonText = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCButtonTextSettingName,
		"experimental: text to show on button on admin ui login page to login with your OIDC provider "+
			"(only shown if OIDC is enabled)",
		"Login with your OIDC provider",
	)
	return s
}()
