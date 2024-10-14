// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauthccl

import (
	"bytes"
	"crypto/x509"
	"encoding/json"
	"slices"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/v2/jwk"
	"golang.org/x/exp/maps"
)

// All cluster settings necessary for the JWT authentication feature.
const (
	baseJWTAuthSettingName           = "server.jwt_authentication."
	JWTAuthAudienceSettingName       = baseJWTAuthSettingName + "audience"
	JWTAuthEnabledSettingName        = baseJWTAuthSettingName + "enabled"
	JWTAuthIssuersSettingName        = baseJWTAuthSettingName + "issuers"
	JWTAuthJWKSSettingName           = baseJWTAuthSettingName + "jwks"
	JWTAuthClaimSettingName          = baseJWTAuthSettingName + "claim"
	JWKSAutoFetchEnabledSettingName  = baseJWTAuthSettingName + "jwks_auto_fetch.enabled"
	jwtAuthIssuerCustomCASettingName = baseJWTAuthSettingName + "issuers.custom_ca"
	jwtAuthClientTimeoutSettingName  = baseJWTAuthSettingName + "client.timeout"
	jwtAuthIssuersConfigSettingName  = JWTAuthIssuersSettingName + ".configuration"
)

// JWTAuthClaim sets the JWT claim that is parsed to get the username.
var JWTAuthClaim = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthClaimSettingName,
	"sets the JWT claim that is parsed to get the username",
	"",
	settings.WithReportable(true),
	settings.WithPublic,
)

// JWTAuthAudience sets accepted audience values for JWT logins over the SQL interface.
var JWTAuthAudience = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthAudienceSettingName,
	"sets accepted audience values for JWT logins over the SQL interface",
	"",
	settings.WithValidateString(validateJWTAuthAudiences),
	settings.WithPublic,
)

// JWTAuthEnabled enables or disabled JWT login over the SQL interface.
var JWTAuthEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	JWTAuthEnabledSettingName,
	"enables or disables JWT login for the SQL interface",
	false,
	settings.WithReportable(true),
	settings.WithPublic,
)

// JWTAuthJWKS is the public key set for JWT logins over the SQL interface.
var JWTAuthJWKS = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthJWKSSettingName,
	"sets the public key set for JWT logins over the SQL interface (JWKS format)",
	"{\"keys\":[]}",
	settings.WithValidateString(validateJWTAuthJWKS),
	settings.WithPublic,
)

// JWTAuthIssuersConfig contains the configuration of all JWT issuers  whose
// tokens are allowed for JWT logins over the SQL interface. This can be set to
// one of the following values:
// 1. Simple string that Go can parse as a valid issuer URL.
// 2. String that can be parsed as valid JSON array of issuer URLs list.
// 3. String that can be parsed as valid JSON and deserialized into a map of
// issuer URLs to corresponding JWKS URIs.
// In the third case we will be overriding the JWKS URI present in the issuer's
// well-known endpoint.
// Example valid values:
//   - 'https://accounts.google.com'
//   - ['example.com/adfs','https://accounts.google.com']
//   - '{
//     "issuer_jwks_map": {
//     "https://accounts.google.com": "https://www.googleapis.com/oauth2/v3/certs",
//     "example.com/adfs": "https://example.com/adfs/discovery/keys"
//     }
//     }'
//
// When issuer_jwks_map is set, we directly use the JWKS URI to get the key set.
// In all other cases where JWKSAutoFetchEnabled is set we obtain the JWKS URI
// first from issuer's well-known endpoint and the use this endpoint.
var JWTAuthIssuersConfig = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	JWTAuthIssuersSettingName,
	"sets accepted issuer values for JWT logins over the SQL interface which can "+
		"be a single issuer URL string or a JSON string containing an array of "+
		"issuer URLs or a JSON object containing map of issuer URLS to JWKS URIs",
	"",
	settings.WithValidateString(validateJWTAuthIssuersConf),
	settings.WithName(jwtAuthIssuersConfigSettingName),
	settings.WithPublic,
)

// JWTAuthIssuerCustomCA is the custom root CA for verifying certificates while
// fetching JWKS from the JWT issuers.
var JWTAuthIssuerCustomCA = settings.RegisterStringSetting(
	settings.ApplicationLevel,
	jwtAuthIssuerCustomCASettingName,
	"sets the PEM encoded custom root CA for verifying certificates while fetching JWKS",
	"",
	settings.WithReportable(false),
	settings.Sensitive,
	settings.WithValidateString(validateJWTAuthIssuerCACert),
	settings.WithPublic,
)

// JWKSAutoFetchEnabled enables or disables automatic fetching of JWKS either
// from JWKS URI present in the issuer's well-known endpoint  or value set in
// JWTAuthIssuersConfig for JWKS URI corresponding to the issuer from presented
// JWT token for JWT login over SQL interface.
var JWKSAutoFetchEnabled = settings.RegisterBoolSetting(
	settings.ApplicationLevel,
	JWKSAutoFetchEnabledSettingName,
	"enables or disables automatic fetching of JWKS from the issuer's well-known "+
		"endpoint or JWKS URI set in JWTAuthIssuersConfig. If this is enabled, the "+
		"server.jwt_authentication.jwks will be ignored.",
	false,
	settings.WithReportable(true),
	settings.WithPublic,
)

// JWTAuthClientTimeout is the client timeout for all the external calls made
// during JWT authentication (e.g. fetching JWKS, etc.).
var JWTAuthClientTimeout = settings.RegisterDurationSetting(
	settings.ApplicationLevel,
	jwtAuthClientTimeoutSettingName,
	"sets the client timeout for external calls made during JWT authentication "+
		"(e.g. fetching JWKS, etc.)",
	15*time.Second,
	settings.NonNegativeDuration,
	settings.WithPublic,
)

// getJSONDecoder generates a new decoder from provided json string. This is
// necessary as currently the offset for decoder can't be reset after Decode().
func getJSONDecoder(s string) *json.Decoder {
	decoder := json.NewDecoder(bytes.NewReader([]byte(s)))
	decoder.DisallowUnknownFields()
	return decoder
}

type issuerURLConf struct {
	ijMap   *issuerJWKSMap
	issuers []string
}

func (conf *issuerURLConf) checkIssuerConfigured(issuer string) error {
	if !slices.Contains(conf.issuers, issuer) {
		return errors.Newf("JWT authentication: invalid issuer")
	}
	return nil
}

func (conf *issuerURLConf) checkJWKSConfigured() error {
	if conf.ijMap == nil || len(conf.ijMap.Mappings) == 0 {
		return errors.Newf("JWT authentication: no jwks mappings configured")
	}
	return nil
}

func (conf *issuerURLConf) getJWKSURI(issuer string) (jwksURI string, err error) {
	var ok bool
	if jwksURI, ok = conf.ijMap.Mappings[issuer]; !ok {
		return "", errors.Newf("JWT authentication: no jwks uri set for issuer")
	}
	return jwksURI, nil
}

// issuerJWKSMap is a struct that defines a valid JSON body for the
// OIDCRedirectURL cluster setting in multi-region environments.
type issuerJWKSMap struct {
	Mappings map[string]string `json:"issuer_jwks_map"`
}

// mustParseJWTIssuersConf will read in a string that's from the
// JWTAuthIssuersConfig setting. We know from the validation that runs on that
// setting that any value that's not valid JSON that deserializes into the
// issuerJWKSMap struct will be either a list of issuer URLs or a single issuer
// URL which will populate and return issuerURLConf.
func mustParseJWTIssuersConf(s string) issuerURLConf {
	var ijMap = issuerJWKSMap{}
	var issuers []string
	decoder := getJSONDecoder(s)
	err := decoder.Decode(&ijMap)
	if err == nil {
		issuers = append(issuers, maps.Keys(ijMap.Mappings)...)
		return issuerURLConf{ijMap: &ijMap, issuers: issuers}
	}

	decoder = getJSONDecoder(s)
	err = decoder.Decode(&issuers)
	if err == nil {
		return issuerURLConf{issuers: issuers}
	}
	return issuerURLConf{issuers: []string{s}}
}

func validateJWTAuthIssuersConf(values *settings.Values, s string) error {
	var issuers []string
	var ijMap = issuerJWKSMap{}

	var jsonCheck json.RawMessage
	if json.Unmarshal([]byte(s), &jsonCheck) != nil {
		// If we know the string is *not* valid JSON, fall back to assuming basic
		// string to use a single valid issuer.
		return nil
	}

	decoder := getJSONDecoder(s)
	issuerListErr := decoder.Decode(&issuers)
	decoder = getJSONDecoder(s)
	issuerJWKSMapErr := decoder.Decode(&ijMap)
	if issuerListErr != nil && issuerJWKSMapErr != nil {
		return errors.Wrap(
			errors.Join(issuerListErr, issuerJWKSMapErr),
			"JWT authentication: issuers JSON not valid",
		)
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
		// string to use a single valid issuer.
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

func validateJWTAuthIssuerCACert(values *settings.Values, s string) error {
	if len(s) != 0 {
		if ok := x509.NewCertPool().AppendCertsFromPEM([]byte(s)); !ok {
			return errors.Newf("JWT authentication issuer custom CA certificate not valid")
		}
	}
	return nil
}
