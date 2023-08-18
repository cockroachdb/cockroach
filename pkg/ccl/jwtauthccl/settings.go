// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package jwtauthccl

import (
	"bytes"
	"encoding/json"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/jwk"
)

// All cluster settings necessary for the JWT authentication feature.
const (
	baseJWTAuthSettingName     = "server.jwt_authentication."
	JWTAuthAudienceSettingName = baseJWTAuthSettingName + "audience"
	JWTAuthEnabledSettingName  = baseJWTAuthSettingName + "enabled"
	JWTAuthIssuersSettingName  = baseJWTAuthSettingName + "issuers"
	JWTAuthJWKSSettingName     = baseJWTAuthSettingName + "jwks"
	JWTAuthClaimSettingName    = baseJWTAuthSettingName + "claim"
)

// JWTAuthClaim sets the JWT claim that is parsed to get the username.
var JWTAuthClaim = settings.RegisterStringSetting(
	settings.TenantWritable,
	JWTAuthClaimSettingName,
	"sets the JWT claim that is parsed to get the username",
	"",
	settings.WithReportable(true),
)

// JWTAuthAudience sets accepted audience values for JWT logins over the SQL interface.
var JWTAuthAudience = settings.RegisterStringSetting(
	settings.TenantWritable,
	JWTAuthAudienceSettingName,
	"sets accepted audience values for JWT logins over the SQL interface",
	"",
	settings.WithValidateString(validateJWTAuthAudiences),
)

// JWTAuthEnabled enables or disabled JWT login over the SQL interface.
var JWTAuthEnabled = settings.RegisterBoolSetting(
	settings.TenantWritable,
	JWTAuthEnabledSettingName,
	"enables or disabled JWT login for the SQL interface",
	false,
	settings.WithReportable(true),
)

// JWTAuthJWKS is the public key set for JWT logins over the SQL interface.
var JWTAuthJWKS = settings.RegisterStringSetting(
	settings.TenantWritable,
	JWTAuthJWKSSettingName,
	"sets the public key set for JWT logins over the SQL interface (JWKS format)",
	"{\"keys\":[]}",
	settings.WithValidateString(validateJWTAuthJWKS),
)

// JWTAuthIssuers is the list of "issuer" values that are accepted for JWT logins over the SQL interface.
var JWTAuthIssuers = settings.RegisterStringSetting(
	settings.TenantWritable,
	JWTAuthIssuersSettingName,
	"sets accepted issuer values for JWT logins over the SQL interface either as a string or as a JSON "+
		"string with an array of issuer strings in it",
	"",
	settings.WithValidateString(validateJWTAuthIssuers),
)

func validateJWTAuthIssuers(values *settings.Values, s string) error {
	var issuers []string

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(s), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic
		// string to use a single valid issuer
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&issuers); err != nil {
		return errors.Wrap(err, "JWT authentication issuers JSON not valid")
	}
	return nil
}

func validateJWTAuthAudiences(values *settings.Values, s string) error {
	var audiences []string

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(s), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic
		// string to use a single valid issuer
		return nil
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&audiences); err != nil {
		return errors.Wrap(err, "JWT authentication audiences JSON not valid")
	}
	return nil
}

func validateJWTAuthJWKS(values *settings.Values, s string) error {
	if _, err := jwk.Parse([]byte(s)); err != nil {
		return errors.Wrap(err, "JWT authentication JWKS not a valid JWKS")
	}
	return nil
}

func mustParseValueOrArray(rawString string) []string {
	var array []string

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(rawString), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic
		// string to use a single valid.
		return []string{rawString}
	}

	decoder := json.NewDecoder(bytes.NewReader([]byte(rawString)))
	if err := decoder.Decode(&array); err != nil {
		return []string{rawString}
	}
	return array
}

func mustParseJWKS(jwks string) jwk.Set {
	keySet, err := jwk.Parse([]byte(jwks))
	if err != nil {
		return jwk.NewSet()
	}
	return keySet
}
