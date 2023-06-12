// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"bytes"
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

	OIDCGenerateJWTAuthTokenEnabledSettingName  = baseOIDCSettingName + "generate_jwt_auth_token.enabled"
	OIDCGenerateJWTAuthTokenUseTokenSettingName = baseOIDCSettingName + "generate_jwt_auth_token.use_token"
	OIDCGenerateJWTAuthTokenSQLHostSettingName  = baseOIDCSettingName + "generate_jwt_auth_token.sql_host"
	OIDCGenerateJWTAuthTokenSQLPortSettingName  = baseOIDCSettingName + "generate_jwt_auth_token.sql_port"
)

// OIDCEnabled enables or disabled OIDC login for the DB Console.
var OIDCEnabled = func() *settings.BoolSetting {
	s := settings.RegisterBoolSetting(
		settings.TenantWritable,
		OIDCEnabledSettingName,
		"enables or disabled OIDC login for the DB Console",
		false,
	).WithPublic()
	s.SetReportable(true)
	return s
}()

// OIDCClientID is the OIDC client id.
var OIDCClientID = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		settings.TenantWritable,
		OIDCClientIDSettingName,
		"sets OIDC client id",
		"",
	).WithPublic()
	s.SetReportable(true)
	return s
}()

// OIDCClientSecret is the OIDC client secret.
var OIDCClientSecret = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		settings.TenantWritable,
		OIDCClientSecretSettingName,
		"sets OIDC client secret",
		"",
	).WithPublic()
	s.SetReportable(false)
	return s
}()

type redirectURLConf struct {
	mrru *multiRegionRedirectURLs
	sru  *singleRedirectURL
}

// getForRegion is used when we have a cluster with regions configured.
// Both configuration types can return valid responses here.
func (conf *redirectURLConf) getForRegion(region string) (string, bool) {
	if conf.mrru != nil {
		s, ok := conf.mrru.RedirectURLs[region]
		return s, ok
	}
	if conf.sru != nil {
		return conf.sru.RedirectURL, true
	}
	return "", false
}

// get is used in the case where regions are not configured on the cluster.
// Only a singleRedirectURL configuration can return a valid result here.
func (conf *redirectURLConf) get() (string, bool) {
	if conf.sru != nil {
		return conf.sru.RedirectURL, true
	}
	return "", false
}

// multiRegionRedirectURLs is a struct that defines a valid JSON body for the
// OIDCRedirectURL cluster setting in multi-region environments.
type multiRegionRedirectURLs struct {
	RedirectURLs map[string]string `json:"redirect_urls"`
}

// singleRedirectURL is a struct containing a string that stores a single
// redirect URL in the case where the configuration only has a single one.
type singleRedirectURL struct {
	RedirectURL string
}

// mustParseOIDCRedirectURL will read in a string that's from the
// `OIDCRedirectURL` setting. We know from the validation that runs on that
// setting that any value that's not valid JSON that deserializes into the
// `multiRegionRedirectURLs` struct will be a URL.
func mustParseOIDCRedirectURL(s string) redirectURLConf {
	var mrru = multiRegionRedirectURLs{}
	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	err := decoder.Decode(&mrru)
	if err != nil {
		return redirectURLConf{sru: &singleRedirectURL{RedirectURL: s}}
	}
	return redirectURLConf{mrru: &mrru}
}

func validateOIDCRedirectURL(values *settings.Values, s string) error {
	var mrru = multiRegionRedirectURLs{}

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(s), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic URL
		// string to use the simple redirect URL configuration option
		if _, err := url.Parse(s); err != nil {
			return errors.Wrap(err, "OIDC redirect URL not valid")
		}
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&mrru); err != nil {
		return errors.Wrap(err, "OIDC redirect JSON not valid")
	}
	for _, route := range mrru.RedirectURLs {
		if _, err := url.Parse(route); err != nil {
			return errors.Wrapf(err, "OIDC redirect JSON contains invalid URL: %s", route)
		}
	}
	return nil
}

// OIDCRedirectURL is the cluster URL to redirect to after OIDC auth completes.
// This can be set to a simple string that Go can parse as a valid URL (although
// it's incredibly permissive) or as a string that can be parsed as valid JSON
// and deserialized into an instance of multiRegionRedirectURLs or
// singleRedirectURL defined above which implement the `callbackRedirecter`
// interface. In the latter case, it is expected that each node will use a
// callback URL that matches its own `region` locality tag.
//
// Example valid values:
//   - 'https://cluster.example.com:8080/oidc/v1/callback'
//   - '{
//     "redirect_urls": {
//     "us-east-1": "https://localhost:8080/oidc/v1/callback",
//     "eu-west-1": "example.com"
//     }
//     }'
//
// In a multi-region cluster where this setting is set to a URL string, we will
// use the same callback URL on all auth requests. In a multi-region setting
// where the cluster's region is not listed in the `redirect_urls` object, we
// will use the required `default_url` callback URL.
var OIDCRedirectURL = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		settings.TenantWritable,
		OIDCRedirectURLSettingName,
		"sets OIDC redirect URL via a URL string or a JSON string containing a required "+
			"`redirect_urls` key with an object that maps from region keys to URL strings "+
			"(URLs should point to your load balancer and must route to the path /oidc/v1/callback) ",
		"https://localhost:8080/oidc/v1/callback",
		validateOIDCRedirectURL,
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// OIDCProviderURL is the location of the OIDC discovery document for the auth
// provider.
var OIDCProviderURL = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		settings.TenantWritable,
		OIDCProviderURLSettingName,
		"sets OIDC provider URL ({provider_url}/.well-known/openid-configuration must resolve)",
		"",
		func(values *settings.Values, s string) error {
			if _, err := url.Parse(s); err != nil {
				return err
			}
			return nil
		},
	)
	s.SetReportable(true)
	s.SetVisibility(settings.Public)
	return s
}()

// OIDCScopes contains the list of scopes to request from the auth provider.
var OIDCScopes = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		settings.TenantWritable,
		OIDCScopesSettingName,
		"sets OIDC scopes to include with authentication request "+
			"(space delimited list of strings, required to start with `openid`)",
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

// OIDCClaimJSONKey is the key of the claim to extract from the OIDC id_token.
var OIDCClaimJSONKey = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		settings.TenantWritable,
		OIDCClaimJSONKeySettingName,
		"sets JSON key of principal to extract from payload after OIDC authentication completes "+
			"(usually email or sid)",
		"",
	).WithPublic()
	return s
}()

// OIDCPrincipalRegex is a regular expression to apply to the OIDC id_token
// claim value to conver it to a DB principal.
var OIDCPrincipalRegex = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		settings.TenantWritable,
		OIDCPrincipalRegexSettingName,
		"regular expression to apply to extracted principal (see claim_json_key setting) to "+
			"translate to SQL user (golang regex format, must include 1 grouping to extract)",
		"(.+)",
		func(values *settings.Values, s string) error {
			if _, err := regexp.Compile(s); err != nil {
				return errors.Wrapf(err, "unable to initialize %s setting, regex does not compile",
					OIDCPrincipalRegexSettingName)
			}
			return nil
		},
	)
	s.SetVisibility(settings.Public)
	return s
}()

// OIDCButtonText is a string to display on the button in the DB Console to
// login with OIDC.
var OIDCButtonText = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		settings.TenantWritable,
		OIDCButtonTextSettingName,
		"text to show on button on DB Console login page to login with your OIDC provider "+
			"(only shown if OIDC is enabled)",
		"Log in with your OIDC provider",
	).WithPublic()
	return s
}()

// OIDCAutoLogin is a boolean that enables automatic redirection to OIDC auth in
// the DB Console.
var OIDCAutoLogin = func() *settings.BoolSetting {
	s := settings.RegisterBoolSetting(
		settings.TenantWritable,
		OIDCAutoLoginSettingName,
		"if true, logged-out visitors to the DB Console will be "+
			"automatically redirected to the OIDC login endpoint",
		false,
	).WithPublic()
	return s
}()

// OIDCGenerateJWTAuthTokenEnabled enables or disables generating JWT auth
// tokens for cluster SSO with OIDC.
var OIDCGenerateJWTAuthTokenEnabled = func() *settings.BoolSetting {
	s := settings.RegisterBoolSetting(
		settings.TenantWritable,
		OIDCGenerateJWTAuthTokenEnabledSettingName,
		"enables or disables using OIDC to generate JWT auth tokens for cluster SSO",
		false,
	)
	return s
}()

type tokenToUse int64

const (
	useIdToken tokenToUse = iota
	useAccessToken
)

// OIDCGenerateJWTAuthTokenUseToken selects which OIDC callback token to use
// for cluster SSO.
var OIDCGenerateJWTAuthTokenUseToken = func() *settings.EnumSetting {
	s := settings.RegisterEnumSetting(
		settings.TenantWritable,
		OIDCGenerateJWTAuthTokenUseTokenSettingName,
		"selects which OIDC callback token to use for cluster SSO",
		"id_token",
		map[int64]string{
			int64(useIdToken):     "id_token",
			int64(useAccessToken): "access_token",
		},
	)
	return s
}()

// OIDCGenerateJWTAuthTokenSQLHost stores the host name or address to be used
// for making SQL connections to the cluster, for display purposes only.
var OIDCGenerateJWTAuthTokenSQLHost = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		settings.TenantWritable,
		OIDCGenerateJWTAuthTokenSQLHostSettingName,
		"stores the host name or address to be used for making SQL connections to the cluster, for display purposes only",
		"localhost",
	)
	return s
}()

// OIDCGenerateJWTAuthTokenSQLPort stores the port number to be used for making
// SQL connections to the cluster, for display purposes only.
var OIDCGenerateJWTAuthTokenSQLPort = func() *settings.IntSetting {
	s := settings.RegisterIntSetting(
		settings.TenantWritable,
		OIDCGenerateJWTAuthTokenSQLPortSettingName,
		"stores the port number to be used for making SQL connections to the cluster, for display purposes only",
		26257,
		settings.NonNegativeIntWithMaximum(65535),
	)
	return s
}()
