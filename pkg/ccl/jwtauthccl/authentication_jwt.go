// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package jwtauthccl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/lestrrat-go/jwx/jwk"
	"github.com/lestrrat-go/jwx/jwt"
)

const (
	counterPrefix           = "auth.jwt."
	beginAuthCounterName    = counterPrefix + "begin_auth"
	loginSuccessCounterName = counterPrefix + "login_success"
	enableCounterName       = counterPrefix + "enable"
)

var (
	beginAuthUseCounter    = telemetry.GetCounterOnce(beginAuthCounterName)
	loginSuccessUseCounter = telemetry.GetCounterOnce(loginSuccessCounterName)
	enableUseCounter       = telemetry.GetCounterOnce(enableCounterName)
)

// jwtAuthenticator is an object that is used to validate JWTs that are used as part of
// the CRDB SSO login flow.
//
// The implementation uses the `lestrrat-go` JWK and JWT packages and is supported through a number of
// cluster settings defined in `jwtauthccl/settings.go`. These settings specify how the JWTs should be
// validated and if this feature is enabled.
type jwtAuthenticator struct {
	mu struct {
		syncutil.RWMutex
		// conf contains all the values that come from cluster settings.
		conf jwtAuthenticatorConf
		// enabled represents the present state of if this feature is enabled. When combined with the enabled value
		// of conf, it allows us to detect when this feature becomes enabled.
		enabled bool
	}
	// clusterUUID is used to check the validity of the enterprise license. It is set once at initialization.
	clusterUUID uuid.UUID
}

// jwtAuthenticatorConf contains all the values to configure JWT authentication. These values are copied from
// the matching cluster settings.
type jwtAuthenticatorConf struct {
	audience             []string
	enabled              bool
	issuers              []string
	jwks                 jwk.Set
	claim                string
	jwksAutoFetchEnabled bool
}

// reloadConfig locks mutex and then refreshes the values in conf from the cluster settings.
func (authenticator *jwtAuthenticator) reloadConfig(ctx context.Context, st *cluster.Settings) {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()
	authenticator.reloadConfigLocked(ctx, st)
}

// reloadConfig refreshes the values in conf from the cluster settings without locking the mutex.
func (authenticator *jwtAuthenticator) reloadConfigLocked(
	ctx context.Context, st *cluster.Settings,
) {
	conf := jwtAuthenticatorConf{
		audience:             mustParseValueOrArray(JWTAuthAudience.Get(&st.SV)),
		enabled:              JWTAuthEnabled.Get(&st.SV),
		issuers:              mustParseValueOrArray(JWTAuthIssuers.Get(&st.SV)),
		jwks:                 mustParseJWKS(JWTAuthJWKS.Get(&st.SV)),
		claim:                JWTAuthClaim.Get(&st.SV),
		jwksAutoFetchEnabled: JWKSAutoFetchEnabled.Get(&st.SV),
	}

	if !authenticator.mu.conf.enabled && conf.enabled {
		telemetry.Inc(enableUseCounter)
	}

	authenticator.mu.conf = conf
	authenticator.mu.enabled = authenticator.mu.conf.enabled

	log.Infof(ctx, "initialized JWT authenticator")
}

// mapUsername takes maps the tokenUsername using the identMap corresponding to the issuer.
func (authenticator *jwtAuthenticator) mapUsername(
	tokenUsername string, issuer string, identMap *identmap.Conf,
) ([]username.SQLUsername, error) {
	users, mapFound, err := identMap.Map(issuer, tokenUsername)
	if !mapFound {
		// Despite the purpose being set to validation, it does no validation that the user string is a valid username.
		u, err := username.MakeSQLUsernameFromUserInput(tokenUsername, username.PurposeValidation)
		return []username.SQLUsername{u}, err
	}
	return users, err
}

// ValidateJWTLogin checks that a given token is a valid credential for the given user.
// In particular, it checks that:
// * JWT authentication is enabled.
// * the token is signed by one of the keys in the JWKS cluster setting.
// * the token has not expired.
// * the token was not issued in the future.
// * the subject field matches the username.
// * the audience field matches the audience cluster setting.
// * the issuer field is one of the values in the issuer cluster setting.
// * the cluster has an enterprise license.
// It returns authError (which is the error sql clients will see in case of
// failures) and detailedError (which is the internal error from http clients
// that might contain sensitive information we do not want to send to sql
// clients but still want to log it). We do not want to send any information
// back to client which was not provided by the client.
func (authenticator *jwtAuthenticator) ValidateJWTLogin(
	ctx context.Context,
	st *cluster.Settings,
	user username.SQLUsername,
	tokenBytes []byte,
	identMap *identmap.Conf,
) (detailedErrorMsg string, authError error) {
	authenticator.mu.Lock()
	defer authenticator.mu.Unlock()

	if !authenticator.mu.enabled {
		return "", errors.Newf("JWT authentication: not enabled")
	}

	telemetry.Inc(beginAuthUseCounter)

	// Just parse the token to check the format is valid and issuer is present.
	// The token will be parsed again later to actually verify the signature.
	unverifiedToken, err := jwt.Parse(tokenBytes)
	if err != nil {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid token"),
			"token parsing failed: %v", err)
	}

	// Check for issuer match against configured issuers.
	issuerUrl := ""
	issuerMatch := false
	for _, issuer := range authenticator.mu.conf.issuers {
		if issuer == unverifiedToken.Issuer() {
			issuerMatch = true
			issuerUrl = issuer
			break
		}
	}
	if !issuerMatch {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid issuer"),
			"token issued by %s", unverifiedToken.Issuer())
	}

	var jwkSet jwk.Set
	// If auto-fetch is enabled, fetch the JWKS remotely from the issuer's well known jwks url.
	if authenticator.mu.conf.jwksAutoFetchEnabled {
		jwkSet, err = authenticator.remoteFetchJWKS(ctx, issuerUrl)
		if err != nil {
			return fmt.Sprintf("unable to fetch jwks: %v", err),
				errors.Newf("JWT authentication: unable to validate token")
		}
	} else {
		jwkSet = authenticator.mu.conf.jwks
	}

	// Now that both the issuer and key-id are matched, parse the token again to validate the signature.
	parsedToken, err := jwt.Parse(tokenBytes, jwt.WithKeySet(jwkSet), jwt.WithValidate(true), jwt.InferAlgorithmFromKey(true))
	if err != nil {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid token"),
			"unable to parse token: %v", err)
	}

	// Extract all requested principals from the token. By default, we take it from the subject unless they specify
	// an alternate claim to pull from.
	var tokenPrincipals []string
	if authenticator.mu.conf.claim == "" || authenticator.mu.conf.claim == "sub" {
		tokenPrincipals = []string{parsedToken.Subject()}
	} else {
		claimValue, ok := parsedToken.Get(authenticator.mu.conf.claim)
		if !ok {
			return "", errors.WithDetailf(
				errors.Newf("JWT authentication: missing claim"),
				"token does not contain a claim for %s", authenticator.mu.conf.claim)
		}
		switch castClaimValue := claimValue.(type) {
		case string:
			// Accept a single string value.
			tokenPrincipals = []string{castClaimValue}
		case []interface{}:
			// Iterate over the slice and add all string values to the tokenPrincipals.
			for _, maybePrincipal := range castClaimValue {
				tokenPrincipals = append(tokenPrincipals, fmt.Sprint(maybePrincipal))
			}
		case []string:
			// This case never seems to happen but is included in case an implementation detail changes in the library.
			tokenPrincipals = castClaimValue
		default:
			tokenPrincipals = []string{fmt.Sprint(castClaimValue)}
		}
	}

	// Take the principals from the token and send each of them through the identity map to generate the
	// list of usernames that this token is valid authentication for.
	var acceptedUsernames []username.SQLUsername
	for _, tokenPrincipal := range tokenPrincipals {
		mappedUsernames, err := authenticator.mapUsername(tokenPrincipal, parsedToken.Issuer(), identMap)
		if err != nil {
			return "", errors.WithDetailf(
				errors.Newf("JWT authentication: invalid claim value"),
				"the value %s for the issuer %s is invalid", tokenPrincipal, parsedToken.Issuer())
		}
		acceptedUsernames = append(acceptedUsernames, mappedUsernames...)
	}
	if len(acceptedUsernames) == 0 {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid principal"),
			"the value %s for the issuer %s is invalid", tokenPrincipals, parsedToken.Issuer())
	}
	principalMatch := false
	for _, username := range acceptedUsernames {
		if username.Normalized() == user.Normalized() {
			principalMatch = true
			break
		}
	}
	if !principalMatch {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid principal"),
			"token issued for %s and login was for %s", tokenPrincipals, user.Normalized())
	}
	if user.IsRootUser() || user.IsReserved() {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid identity"),
			"cannot use JWT auth to login to a reserved user %s", user.Normalized())
	}
	audienceMatch := false
	for _, tokenAudience := range parsedToken.Audience() {
		for _, crdbAudience := range authenticator.mu.conf.audience {
			if crdbAudience == tokenAudience {
				audienceMatch = true
				break
			}
		}
	}
	if !audienceMatch {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid audience"),
			"token issued with an audience of %s", parsedToken.Audience())
	}

	if err = utilccl.CheckEnterpriseEnabled(st, "JWT authentication"); err != nil {
		return "", err
	}

	telemetry.Inc(loginSuccessUseCounter)
	return "", nil
}

// remoteFetchJWKS fetches the JWKS from the provided URI.
func (authenticator *jwtAuthenticator) remoteFetchJWKS(
	ctx context.Context, issuerUrl string,
) (jwk.Set, error) {
	jwksUrl, err := authenticator.getJWKSUrl(ctx, issuerUrl)
	if err != nil {
		return nil, err
	}

	body, err := getHttpResponse(ctx, jwksUrl, authenticator)
	if err != nil {
		return nil, err
	}
	jwkSet, err := jwk.Parse(body)
	if err != nil {
		return nil, err
	}
	return jwkSet, nil
}

// getJWKSUrl returns the JWKS URI from the OpenID configuration endpoint.
func (authenticator *jwtAuthenticator) getJWKSUrl(
	ctx context.Context, issuerUrl string,
) (string, error) {
	type OIDCConfigResponse struct {
		JWKSUri string `json:"jwks_uri"`
	}
	openIdConfigEndpoint := getOpenIdConfigEndpoint(issuerUrl)
	body, err := getHttpResponse(ctx, openIdConfigEndpoint, authenticator)
	if err != nil {
		return "", err
	}
	var config OIDCConfigResponse
	if err = json.Unmarshal(body, &config); err != nil {
		return "", err
	}
	if config.JWKSUri == "" {
		return "", errors.Newf("no JWKS URI found in OpenID configuration")
	}
	return config.JWKSUri, nil
}

// getOpenIdConfigEndpoint returns the OpenID configuration endpoint by appending standard open-id url.
func getOpenIdConfigEndpoint(issuerUrl string) string {
	openIdConfigEndpoint := strings.TrimSuffix(issuerUrl, "/") + "/.well-known/openid-configuration"
	return openIdConfigEndpoint
}

var getHttpResponse = func(ctx context.Context, url string, authenticator *jwtAuthenticator) ([]byte, error) {
	// TODO(souravcrl): cache the http client in a callback attached to customCA
	// and other http client cluster settings as re parsing the custom CA every
	// time is expensive
	httpClient := httputil.NewClientWithTimeout(httputil.StandardHTTPTimeout)
	resp, err := httpClient.Get(context.Background(), url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

// ConfigureJWTAuth initializes and returns a jwtAuthenticator. It also sets up listeners so
// that the jwtAuthenticator's config is updated when the cluster settings values change.
var ConfigureJWTAuth = func(
	serverCtx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	clusterUUID uuid.UUID,
) pgwire.JWTVerifier {
	authenticator := jwtAuthenticator{}
	authenticator.clusterUUID = clusterUUID
	authenticator.reloadConfig(serverCtx, st)
	JWTAuthAudience.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthEnabled.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthIssuers.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthJWKS.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthClaim.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWKSAutoFetchEnabled.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	return &authenticator
}

func init() {
	pgwire.ConfigureJWTAuth = ConfigureJWTAuth
}
