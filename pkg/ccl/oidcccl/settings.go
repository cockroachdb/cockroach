// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"

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
	OIDCAutoLoginSettingName      = baseOIDCSettingName + "autologin"
)

// OIDCEnabled enables or disabled OIDC login for the DB Console
var OIDCEnabled = func() *settings.BoolSetting {
	s := settings.RegisterPublicBoolSetting(
		OIDCEnabledSettingName,
		"enables or disabled OIDC login for the DB Console (this feature is experimental)",
		false,
	)
	s.SetReportable(true)
	return s
}()

// OIDCClientID is the OIDC client id
var OIDCClientID = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCClientIDSettingName,
		"sets OIDC client id (this feature is experimental)",
		"",
	)
	s.SetReportable(true)
	return s
}()

// OIDCClientSecret is the OIDC client secret
var OIDCClientSecret = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCClientSecretSettingName,
		"sets OIDC client secret (this feature is experimental)",
		"",
	)
	s.SetReportable(false)
	return s
}()

// multiRegionRedirectURLs is a struct that defines a valid JSON body for the OIDCRedirectURL
// cluster setting in multi-region environments.
type multiRegionRedirectURLs struct {
	RedirectURLs map[string]string `json:"redirect_urls"`
	DefaultURL   string            `json:"default_url"`
}

func mustParseOIDCRedirectURL(s string) multiRegionRedirectURLs {
	var mrru = multiRegionRedirectURLs{}
	err := json.Unmarshal([]byte(s), &mrru)
	if err != nil {
		mrru.DefaultURL = s
	}
	return mrru
}

func validateOIDCRedirectURL(values *settings.Values, s string) error {
	var mrru = multiRegionRedirectURLs{}
	err := json.Unmarshal([]byte(s), &mrru)
	if err != nil {
		// Fall back to assuming basic URL string for backwards compatibility and to keep the simple
		// configuration option
		_, err = url.Parse(s)
		if err != nil {
			return err
		}
		return nil
	}
	if mrru.DefaultURL == "" {
		return errors.New("oidc: empty default_url in redirect_url setting JSON")
	}
	if _, err := url.Parse(mrru.DefaultURL); err != nil {
		return err
	}
	return nil
}

// OIDCRedirectURL is the cluster URL to redirect to after OIDC auth completes. This can be set
// to a simple string that Go can parse as a valid URL (although it's incredibly permissive) or as
// a string that can be parsed as valid JSON and deserialized into an instance of
// multiRegionRedirectURLs defined above. In the latter case, it is expected that each node will
// use a callback URL that matches its own `region` locality tag.
//
// Example valid values:
// - 'https://cluster.example.com:8080/oidc/v1/callback'
// - '{
//    "default_url": "https://localhost:8080/oidc/v1/callback",
//    "redirect_urls": {
//      "us-east-1": "https://localhost:8080/oidc/v1/callback",
//      "eu-west-1": "example.com"
//    }
//   }'
// - '{"default_url": "https://localhost:8080/oidc/v1/callback"}'
//
// In a multi-region cluster where this setting is set to a URL string, we will use the same
// callback URL on all auth requests. In a multi-region setting where the cluster's region is not
// listed in the `redirect_urls` object, we will use the required `default_url` callback URL.
var OIDCRedirectURL = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		OIDCRedirectURLSettingName,
		"sets OIDC redirect URL via a URL string or a JSON string containing a required "+
			"`default_url` key and a `redirect_urls` key containing an object that maps from region keys "+
			"to URL strings "+
			"(URLs should point to your load balancer and must route to the path /oidc/v1/callback) "+
			"(this feature is experimental)",
		"https://localhost:8080/oidc/v1/callback",
		validateOIDCRedirectURL,
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// OIDCProviderURL is the location of the OIDC discovery document for the auth provider
var OIDCProviderURL = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		OIDCProviderURLSettingName,
		"sets OIDC provider URL ({provider_url}/.well-known/openid-configuration must resolve) (this feature is experimental)",
		"",
		func(values *settings.Values, s string) error {
			_, err := url.Parse(s)
			if err != nil {
				return err
			}
			return nil
		},
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// OIDCScopes contains the list of scopes to request from the auth provider
var OIDCScopes = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		OIDCScopesSettingName,
		"sets OIDC scopes to include with authentication request "+
			"(space delimited list of strings, required to start with `openid`) (this feature is experimental)",
		"openid",
		func(values *settings.Values, s string) error {
			if s != oidc.ScopeOpenID && !strings.HasPrefix(s, oidc.ScopeOpenID+" ") {
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
		"sets JSON key of principal to extract from payload after OIDC authentication completes "+
			"(usually email or sid) (this feature is experimental)",
		"",
	)
	return s
}()

// OIDCPrincipalRegex is a regular expression to apply to the OIDC id_token claim value to conver
// it to a DB principal
var OIDCPrincipalRegex = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		OIDCPrincipalRegexSettingName,
		"regular expression to apply to extracted principal (see claim_json_key setting) to "+
			"translate to SQL user (golang regex format, must include 1 grouping to extract) (this feature is experimental)",
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

// OIDCButtonText is a string to display on the button in the DB Console to login with OIDC
var OIDCButtonText = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		OIDCButtonTextSettingName,
		"text to show on button on DB Console login page to login with your OIDC provider "+
			"(only shown if OIDC is enabled) (this feature is experimental)",
		"Login with your OIDC provider",
	)
	return s
}()

// OIDCAutoLogin is a boolean that enables automatic redirection to OIDC auth in the DB Console
var OIDCAutoLogin = func() *settings.BoolSetting {
	s := settings.RegisterPublicBoolSetting(
		OIDCAutoLoginSettingName,
		"if true, logged-out visitors to the DB Console will be "+
			"automatically redirected to the OIDC login endpoint (this feature is experimental)",
		false,
	)
	return s
}()
