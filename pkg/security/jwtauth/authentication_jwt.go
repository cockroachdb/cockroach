// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jwtauth

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/lestrrat-go/jwx/v3/jwk"
	"github.com/lestrrat-go/jwx/v3/jws"
	"github.com/lestrrat-go/jwx/v3/jwt"
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

// JWTVerifier is an interface for JWT login support. It validates whether a
// given JWT token is a proper credential for a given user to log in.
type JWTVerifier interface {
	ValidateJWTLogin(_ context.Context, _ *cluster.Settings,
		_ username.SQLUsername,
		_ []byte,
		_ *identmap.Conf,
	) (detailedErrorMsg redact.RedactableString, authError error)

	// RetrieveIdentity retrieves the user identity from the JWT.
	//
	// If a user identity is provided as input, it matches it against the token
	// principals. In case of a match, it returns the matched user and no
	// error. Otherwise, it returns the input user along with the error.
	//
	// If a user identity is not provided as input, and there is a single token
	// principal to match against, it returns this user identity and no error.
	// If there are multiple matches, then it returns an error.
	RetrieveIdentity(
		_ context.Context, st *cluster.Settings, _ username.SQLUsername, _ []byte, _ *identmap.Conf,
	) (retrievedUser username.SQLUsername, authError error)

	ExtractGroups(ctx context.Context, st *cluster.Settings, token []byte) ([]string, error)

	// VerifyAndExtractIssuer is a combined minimal check used by the
	// provisioner:
	//   - verifies signature with the configured key set
	//   - confirms the `iss` claim is configured
	//   - returns the verified issuer string
	//
	//   issuer       – verified `iss` string (empty on failure)
	//   detailedErr  – redactable diagnostics for LogAuthFailed
	//   authErr      – high-level error shown to the client
	VerifyAndExtractIssuer(
		_ context.Context,
		_ *cluster.Settings,
		_ []byte,
	) (issuer string, detailedErr redact.RedactableString, authErr error)
}

// jwtAuthenticator is an object that is used to validate JWTs that are used as part of
// the CRDB SSO login flow.
//
// The implementation uses the `lestrrat-go` JWK and JWT packages and is supported through a number of
// cluster settings defined in `jwtauth/settings.go`. These settings specify how the JWTs should be
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
	// jwksCache caches auto-fetched JWK sets per issuer for this node so that
	// steady-state logins do not touch the network. It is nil until
	// ConfigureJWTAuth installs one via RebuildJWKSCache. It is swapped atomically
	// so a configuration change can replace it without racing a login that already
	// loaded the previous cache.
	jwksCache atomic.Pointer[JWKSCache]
}

// jwtAuthenticatorConf contains all the values to configure JWT authentication. These values are copied from
// the matching cluster settings.
type jwtAuthenticatorConf struct {
	audience             []string
	enabled              bool
	issuersConf          issuerURLConf
	issuerCA             string
	jwks                 jwk.Set
	claim                string
	jwksAutoFetchEnabled bool
	httpClient           *httputil.Client
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
	clientTimeout := JWTAuthClientTimeout.Get(&st.SV)
	conf := jwtAuthenticatorConf{
		audience:             mustParseValueOrArray(JWTAuthAudience.Get(&st.SV)),
		enabled:              JWTAuthEnabled.Get(&st.SV),
		issuersConf:          mustParseJWTIssuersConf(JWTAuthIssuersConfig.Get(&st.SV)),
		issuerCA:             JWTAuthIssuerCustomCA.Get(&st.SV),
		jwks:                 mustParseJWKS(JWTAuthJWKS.Get(&st.SV)),
		claim:                JWTAuthClaim.Get(&st.SV),
		jwksAutoFetchEnabled: JWKSAutoFetchEnabled.Get(&st.SV),
		httpClient: httputil.NewClient(
			httputil.WithClientTimeout(clientTimeout),
			httputil.WithDialerTimeout(clientTimeout),
			httputil.WithCustomCAPEM(JWTAuthIssuerCustomCA.Get(&st.SV)),
		),
	}

	if !authenticator.mu.conf.enabled && conf.enabled {
		telemetry.Inc(enableUseCounter)
	}

	authenticator.mu.conf = conf
	authenticator.mu.enabled = authenticator.mu.conf.enabled

	log.Dev.Infof(ctx, "initialized JWT authenticator")
}

// snapshot returns a consistent copy of the current configuration and enabled
// flag taken under the read lock. reloadConfig replaces mu.conf wholesale and
// never mutates a published jwtAuthenticatorConf in place, so the shallow copy
// returned here can be read without holding mu. Authentication snapshots the
// configuration and then performs the slow remote JWKS/OIDC fetch without the
// lock, so one connection's in-flight fetch does not block others.
func (authenticator *jwtAuthenticator) snapshot() (conf jwtAuthenticatorConf, enabled bool) {
	authenticator.mu.RLock()
	defer authenticator.mu.RUnlock()
	return authenticator.mu.conf, authenticator.mu.enabled
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
) (detailedErrorMsg redact.RedactableString, authError error) {
	authenticator.reloadConfig(ctx, st)
	conf, enabled := authenticator.snapshot()

	if !enabled {
		return "", errors.Newf("JWT authentication: not enabled")
	}

	telemetry.Inc(beginAuthUseCounter)

	// Validate the token as below:
	// 1. Check the token format and extract issuer
	// The jwx/v3 library mandates signature verification with Parse,
	// so use ParseInsecure instead
	// 2. Fetch JWKS corresponding to the issuer
	// 3. Use Parse for signature verification
	unverifiedToken, err := jwt.ParseInsecure(tokenBytes)
	if err != nil {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid token"),
			"token parsing failed: %v", err)
	}

	// Check for issuer match against configured issuers.
	tokenIssuer, _ := unverifiedToken.Issuer()
	if err = conf.issuersConf.checkIssuerConfigured(tokenIssuer); err != nil {
		return "", errors.WithDetailf(err, "token issued by %s", tokenIssuer)
	}

	var jwkSet jwk.Set
	// If auto-fetch is enabled, fetch the JWKS remotely from the issuer's well known jwks URI.
	if conf.jwksAutoFetchEnabled {
		jwkSet, err = authenticator.remoteFetchJWKS(ctx, tokenIssuer, keyIDFromToken(tokenBytes), conf)
		if err != nil {
			return redact.Sprintf("unable to fetch jwks: %v", err),
				errors.Newf("JWT authentication: unable to validate token")
		}
	} else {
		jwkSet = conf.jwks
	}

	// Now that both the issuer and key-id are matched, parse the token again to validate the signature.
	parsedToken, err := jwt.Parse(tokenBytes, jwt.WithKeySet(jwkSet, jws.WithInferAlgorithmFromKey(true)), jwt.WithValidate(true))
	if err != nil {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid token"),
			"unable to parse token: %v", err)
	}

	// Match the input user identity against the user identities mapped within the JWT.
	user, authError = authenticator.retrieveIdentity(user, tokenBytes, identMap, conf)
	if authError != nil {
		return
	}

	if user.IsRootUser() || user.IsReserved() {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid identity"),
			"cannot use JWT auth to login to a reserved user %s", user.Normalized())
	}

	audienceMatch := false
	tokenAudiences, _ := parsedToken.Audience()
	for _, tokenAudience := range tokenAudiences {
		for _, crdbAudience := range conf.audience {
			if crdbAudience == tokenAudience {
				audienceMatch = true
				break
			}
		}
	}
	if !audienceMatch {
		return "", errors.WithDetailf(
			errors.Newf("JWT authentication: invalid audience"),
			"token issued with an audience of %s", tokenAudiences)
	}

	telemetry.Inc(loginSuccessUseCounter)
	return "", nil
}

// RetrieveIdentity implements JWTVerifier.
//
// It reloads the authenticator’s configuration from the passed
// *cluster.Settings, snapshots it, then delegates to retrieveIdentity
// without holding a.mu.
//
// The method parses `tokenBytes`, applies the ident-map, and returns
// the SQL user that this JWT is allowed to act as. On success the
// returned username is either `user` (when it matches a token
// principal) or the single mapped identity. on failure it returns an
// error context'd with "JWT authentication:".
func (authenticator *jwtAuthenticator) RetrieveIdentity(
	ctx context.Context,
	st *cluster.Settings,
	user username.SQLUsername,
	tokenBytes []byte,
	identMap *identmap.Conf,
) (retrievedUser username.SQLUsername, authError error) {
	authenticator.reloadConfig(ctx, st)
	conf, _ := authenticator.snapshot()
	return authenticator.retrieveIdentity(user, tokenBytes, identMap, conf)
}

// retrieveIdentity contains the core principal-to-user mapping logic. It reads
// configuration from the supplied snapshot rather than mu.conf, so callers run
// it without holding a.mu.
func (authenticator *jwtAuthenticator) retrieveIdentity(
	user username.SQLUsername, tokenBytes []byte, identMap *identmap.Conf, conf jwtAuthenticatorConf,
) (retrievedUser username.SQLUsername, authError error) {
	if !conf.enabled {
		return user, errors.New("JWT authentication: not enabled")
	}
	unverifiedToken, err := jwt.ParseInsecure(tokenBytes)
	if err != nil {
		return user, errors.WithDetailf(
			errors.Newf("JWT authentication: invalid token"),
			"token parsing failed: %v", err)
	}

	// Extract all requested principals from the token. By default, we take it
	// from the subject unless they specify an alternate claim to pull from.
	var tokenPrincipals []string
	if conf.claim == "" || conf.claim == "sub" {
		sub, _ := unverifiedToken.Subject()
		tokenPrincipals = []string{sub}
	} else {
		if !unverifiedToken.Has(conf.claim) {
			return user, errors.WithDetailf(
				errors.Newf("JWT authentication: missing claim"),
				"token does not contain a claim for %s", conf.claim)
		}
		var claimValue any
		if err := unverifiedToken.Get(conf.claim, &claimValue); err != nil {
			return user, errors.WithDetailf(
				errors.Newf("JWT authentication: missing claim"),
				"token does not contain a claim for %s", conf.claim)
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
			// This case never seems to happen but is included in case an
			// implementation detail changes in the library.
			tokenPrincipals = castClaimValue
		default:
			tokenPrincipals = []string{fmt.Sprint(castClaimValue)}
		}
	}

	// Take the principals from the token and send each of them through the
	// identity map to generate the list of usernames that this token is valid
	// authentication for.
	issuer, _ := unverifiedToken.Issuer()
	var acceptedUsernames []username.SQLUsername
	for _, tokenPrincipal := range tokenPrincipals {
		mappedUsernames, err := authenticator.mapUsername(tokenPrincipal, issuer, identMap)
		if err != nil {
			return user, errors.WithDetailf(
				errors.Newf("JWT authentication: invalid claim value"),
				"the value %s for the issuer %s is invalid", tokenPrincipal, issuer)
		}
		acceptedUsernames = append(acceptedUsernames, mappedUsernames...)
	}
	if len(acceptedUsernames) == 0 {
		return user, errors.WithDetailf(
			errors.Newf("JWT authentication: invalid principal"),
			"the value %s for the issuer %s is invalid", tokenPrincipals, issuer)
	}

	principalMatch := false
	for _, userName := range acceptedUsernames {
		if userName.Normalized() == user.Normalized() {
			principalMatch = true
			break
		}
	}

	if !principalMatch {
		// If the username is not provided, and we match it to a single user,
		// then use that user identity.
		if user.IsEmptyRole() && len(acceptedUsernames) == 1 {
			return acceptedUsernames[0], nil
		}
		return user, errors.WithDetailf(
			errors.Newf("JWT authentication: invalid principal"),
			"token issued for %s and login was for %s", tokenPrincipals, user.Normalized())

	}

	return user, nil
}

// VerifyAndExtractIssuer checks a JWT’s JWS signature with the configured
// key set and confirms the `iss` claim matches a configured issuer.
//
// It is called from the provisioning path in authJwtToken, AFTER
// RetrieveIdentity has set the replacement identity. Audience, expiry and
// other claim checks happen later in Authenticate(); this helper should not
// modify state.
//
// Like ValidateJWTLogin, it returns two error values:
//
//	issuer       – the verified `iss` claim to build "jwt_token:<issuer>"
//	detailedErr  – redactable detail for LogAuthFailed
//	authErr      – high-level error shown to the client
func (a *jwtAuthenticator) VerifyAndExtractIssuer(
	ctx context.Context, st *cluster.Settings, tokenBytes []byte,
) (issuer string, detailedErr redact.RedactableString, authErr error) {
	a.reloadConfig(ctx, st)
	conf, enabled := a.snapshot()

	if !enabled {
		return "", "", errors.New("JWT authentication: not enabled")
	}

	// Parse token without verifying signature to pull out issuer & key id.
	unverified, err := jwt.ParseInsecure(tokenBytes)
	if err != nil {
		return "", redact.Sprintf("JWT authentication: invalid token format: %v", err),
			errors.New("JWT authentication: invalid token")
	}

	issuer, _ = unverified.Issuer()
	if err := conf.issuersConf.checkIssuerConfigured(issuer); err != nil {
		return "", "", errors.WithDetail(
			errors.New("JWT authentication: invalid issuer"),
			fmt.Sprintf("token issued by %s", issuer),
		)
	}

	// Fetch the JWKS (auto-fetch or static) for that issuer.
	var set jwk.Set
	if conf.jwksAutoFetchEnabled {
		set, err = a.remoteFetchJWKS(ctx, issuer, keyIDFromToken(tokenBytes), conf)
		if err != nil {
			return "", redact.Sprintf("JWT authentication: unable to fetch jwks: %v", err),
				errors.New("JWT authentication: unable to validate token")
		}
	} else {
		set = conf.jwks
	}

	// JWS verification
	if _, err := jwt.Parse(
		tokenBytes,
		jwt.WithKeySet(set, jws.WithInferAlgorithmFromKey(true)),
		jwt.WithValidate(false),
	); err != nil {
		return "", "", errors.WithDetailf(
			errors.New("JWT authentication: invalid token"),
			"unable to parse token: %v", err,
		)
	}

	return issuer, "", nil
}

// remoteFetchJWKS returns the JWKS for the provided issuer. When a per-node
// JWKS cache is installed, it serves the set from memory (refreshed in the
// background), passing kid so the cache can force an early refresh if the token
// presents a rotated key. Otherwise it falls back to fetching the JWKS on every
// call. kid may be empty. Issuer configuration and the HTTP client are read from
// the supplied snapshot so the call runs without holding the authenticator lock.
func (authenticator *jwtAuthenticator) remoteFetchJWKS(
	ctx context.Context, issuerURL string, kid string, conf jwtAuthenticatorConf,
) (jwk.Set, error) {
	if cache := authenticator.jwksCache.Load(); cache != nil {
		return cache.KeySet(ctx, &conf.issuersConf, issuerURL, kid)
	}

	var jwksURI string
	// if JWKS URI is configured in JWTAuthIssuersConfig use that instead of URL
	// from issuer's well-known endpoint
	err := conf.issuersConf.checkJWKSConfigured()
	if err != nil {
		jwksURI, err = authenticator.getJWKSURI(ctx, issuerURL, conf)
		if err != nil {
			return nil, err
		}
	} else {
		jwksURI, err = conf.issuersConf.getJWKSURI(issuerURL)
		if err != nil {
			return nil, err
		}
	}

	body, err := getHttpResponse(ctx, jwksURI, conf.httpClient)
	if err != nil {
		return nil, err
	}
	jwkSet, err := jwk.Parse(body)
	if err != nil {
		return nil, err
	}
	return jwkSet, nil
}

// getJWKSURI returns the JWKS URI from the OpenID configuration endpoint.
func (authenticator *jwtAuthenticator) getJWKSURI(
	ctx context.Context, issuerUrl string, conf jwtAuthenticatorConf,
) (string, error) {
	type OIDCConfigResponse struct {
		JWKSUri string `json:"jwks_uri"`
	}
	openIdConfigEndpoint := getOpenIdConfigEndpoint(issuerUrl)
	body, err := getHttpResponse(ctx, openIdConfigEndpoint, conf.httpClient)
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

// getHTTPResponse issues a GET request using the supplied HTTP client,
// optionally setting the supplied headers.
//
//	ctx    – caller’s context (for cancellation / deadlines)
//	url    – absolute URL to fetch
//	client – the HTTP client to use (carries the configured custom CA and
//	         timeout); taken from a configuration snapshot so the call needs
//	         no lock
//	hdr    – optional: pass one http.Header with any extra headers, or omit entirely
//
// The function returns the response body (fully read) so that callers can
// inspect the payload without worrying about closing the body.
//
// Callers should wrap the returned error with errors.WithDetailf to tag the
// operation they’re performing.
var getHttpResponse = func(
	ctx context.Context,
	url string,
	client *httputil.Client,
	header ...http.Header, // optional variadic param for extra headers
) ([]byte, error) {

	// Reject misuse: only zero or one header may be supplied.
	if len(header) > 1 {
		return nil, errors.New("getHttpResponse: provide at most one extra header")
	}

	// Build the request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	if len(header) == 1 && header[0] != nil {
		req.Header = header[0].Clone()
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		bodySnippet := string(body)
		if len(bodySnippet) > 200 {
			bodySnippet = bodySnippet[:200] + "..."
		}
		return nil, errors.WithDetailf(
			errors.Newf("JWT authentication: HTTP request failed"),
			"GET %s returned %s: %s", url, resp.Status, bodySnippet,
		)
	}

	return body, nil
}

// jwtCacheHTTPClient builds the stable HTTP client that the per-node JWKS cache
// uses for OIDC discovery and JWKS fetches. Unlike the per-login client in
// reloadConfig, this one lives for the life of the cache and is rebuilt only
// when the custom CA or client timeout changes. It is a package-level variable
// so that tests can substitute a client which serves fixtures without real
// network access.
var jwtCacheHTTPClient = func(st *cluster.Settings) *httputil.Client {
	clientTimeout := JWTAuthClientTimeout.Get(&st.SV)
	return httputil.NewClient(
		httputil.WithClientTimeout(clientTimeout),
		httputil.WithDialerTimeout(clientTimeout),
		httputil.WithCustomCAPEM(JWTAuthIssuerCustomCA.Get(&st.SV)),
	)
}

// RebuildJWKSCache installs a fresh per-node JWKS cache that uses httpClient for
// discovery and JWKS fetches, replacing any previous cache (whose background
// refresh goroutine is stopped). ConfigureJWTAuth calls this once at
// construction and again whenever a setting that affects fetching changes
// (issuer config, custom CA, client timeout), so keys are never served under
// stale configuration. serverCtx bounds the cache's lifetime.
func (authenticator *jwtAuthenticator) RebuildJWKSCache(
	serverCtx context.Context, httpClient *httputil.Client,
) error {
	nc, err := NewJWKSCache(serverCtx, httpClient)
	if err != nil {
		return err
	}
	if old := authenticator.jwksCache.Swap(nc); old != nil {
		old.Close()
	}
	return nil
}

// getJWKSURIFromWellKnown fetches the issuer's OpenID Connect discovery document
// using the provided client and returns its jwks_uri. It is the client-explicit
// form of getJWKSURI used by the per-node JWKS cache, which owns a long-lived
// HTTP client distinct from the per-login client bound to the authenticator.
func getJWKSURIFromWellKnown(
	ctx context.Context, client *httputil.Client, issuerURL string,
) (string, error) {
	endpoint := getOpenIdConfigEndpoint(issuerURL)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return "", err
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", errors.Newf("JWT authentication: HTTP request failed: GET %s returned %s", endpoint, resp.Status)
	}
	var config struct {
		JWKSUri string `json:"jwks_uri"`
	}
	if err := json.Unmarshal(body, &config); err != nil {
		return "", err
	}
	if config.JWKSUri == "" {
		return "", errors.Newf("no JWKS URI found in OpenID configuration")
	}
	return config.JWKSUri, nil
}

// ConfigureJWTAuth initializes and returns a jwtAuthenticator. It also sets up
// listeners so that the jwtAuthenticator's config is updated when the cluster
// settings values change.
func ConfigureJWTAuth(
	serverCtx context.Context,
	ambientCtx log.AmbientContext,
	st *cluster.Settings,
	clusterUUID uuid.UUID,
	stopper *stop.Stopper,
) JWTVerifier {
	authenticator := jwtAuthenticator{}
	authenticator.clusterUUID = clusterUUID
	authenticator.reloadConfig(serverCtx, st)

	// Build the per-node JWKS cache so steady-state logins are served from memory
	// instead of fetching on every login. The cache's background refresh goroutine
	// is bound to the stopper (not serverCtx, which may be a per-connection context
	// via the pgwire lazy initialization), so it lives for the node's lifetime and
	// stops on shutdown.
	cacheCtx, _ := stopper.WithCancelOnQuiesce(context.Background()) // nolint:quiesce
	rebuildCache := func(ctx context.Context) {
		if err := authenticator.RebuildJWKSCache(cacheCtx, jwtCacheHTTPClient(st)); err != nil {
			log.Ops.Warningf(ctx,
				"failed to build JWKS cache; existing cache, if any, remains active: %v", err)
		}
	}
	rebuildCache(serverCtx)

	JWTAuthAudience.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthEnabled.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthIssuersConfig.SetOnChange(&st.SV, func(ctx context.Context) {
		authenticator.reloadConfig(ambientCtx.AnnotateCtx(ctx), st)
	})
	JWTAuthIssuerCustomCA.SetOnChange(&st.SV, func(ctx context.Context) {
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

	// Rebuild the cache when a setting that affects fetching changes: the custom
	// CA and client timeout are baked into the cache's HTTP client, and an issuer
	// config change may point at different JWKS endpoints. SetOnChange appends, so
	// these run in addition to the reload callbacks above.
	cacheOnChange := func(ctx context.Context) { rebuildCache(ambientCtx.AnnotateCtx(ctx)) }
	JWTAuthIssuerCustomCA.SetOnChange(&st.SV, cacheOnChange)
	JWTAuthClientTimeout.SetOnChange(&st.SV, cacheOnChange)
	JWTAuthIssuersConfig.SetOnChange(&st.SV, cacheOnChange)
	return &authenticator
}
