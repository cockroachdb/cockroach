// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package oidcccl

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/jwtauthccl"
	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/identmap"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	oidc "github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
)

const (
	idTokenKey               = "id_token"
	codeKey                  = "code"
	stateKey                 = "state"
	secretCookieName         = "oidc_secret"
	oidcLoginPath            = "/oidc/v1/login"
	oidcCallbackPath         = "/oidc/v1/callback"
	oidcJWTPath              = "/oidc/v1/jwt"
	genericCallbackHTTPError = "OIDC: unable to complete authentication"
	genericLoginHTTPError    = "OIDC: unable to initiate authentication"
	counterPrefix            = "auth.oidc."
	beginAuthCounterName     = counterPrefix + "begin_auth"
	beginCallbackCounterName = counterPrefix + "begin_callback"
	beginJWTCounterName      = counterPrefix + "begin_jwt"
	loginSuccessCounterName  = counterPrefix + "login_success"
	enableCounterName        = counterPrefix + "enable"
	hmacKeySize              = 32
	stateTokenSize           = 32
)

var (
	beginAuthUseCounter     = telemetry.GetCounterOnce(beginAuthCounterName)
	beginCallbackUseCounter = telemetry.GetCounterOnce(beginCallbackCounterName)
	beginJWTUseCounter      = telemetry.GetCounterOnce(beginJWTCounterName)
	loginSuccessUseCounter  = telemetry.GetCounterOnce(loginSuccessCounterName)
	enableUseCounter        = telemetry.GetCounterOnce(enableCounterName)
)

// oidcAuthenticationServer is an implementation of the OpenID Connect authentication code flow
// to support single-sign-on to the Admin UI via an external identity provider.
//
// The implementation uses the `go-oidc` implementation and is supported through a number of
// cluster settings defined in `oidc/settings.go`. These configure the CRDB cluster to redirect
// to an auth provider for logins, and to accept a callback once authentication completes, where
// CRDB translates the provided login principal to a SQL principal and creates a web session.
//
// The implementation adds two new HTTP handlers to the server at
// `/oidc/v1/login/` and `/oidc/v1/callback` the functions of which are described below.
//
// A successful configuration and login flow looks like the following (logout logic is unchanged
// with OIDC):
//
//  0. The cluster operator configures the cluster to use OIDC. Once the cluster setting
//     `server.oidc_authentication.enabled` is set to true, the OIDC client will make a request to
//     retrieve the discovery document using the `server.oidc_authentication.provider_url` setting.
//     This attempt will be retried automatically with every call to the `login` or `callback` or
//     any change to any OIDC settings as long as `enabled` is still true. That behavior is meant to
//     support easy recovery from any downtime or HTTP errors on the provider side.
//
//  1. A CRDB user opens the Admin UI and clicks on the `Login with OIDC` button (text is
//     configurable using the `server.oidc_authentication.button_text` setting.
//
//  2. The browser loads `/oidc/v1/login` from the cluster, which triggers a redirect to the auth
//     provider. A number of parameters are sent along with this request: (these are all defined in
//     the OIDC spec available at: https://openid.net/specs/openid-connect-core-1_0.html)
//     - client_id and client_secret: these are set using their correspondingly named cluster
//     settings and are values that the auth provider will create.
//     - redirect_uri: set using the `server.oidc_authentication.redirect_url` cluster setting. This
//     will point to `/oidc/v1/callback` at the appropriate host or load balancer
//     that the cluster is deployed to.
//     - scopes: set using the `server.oidc_authentication.scopes` cluster setting. This defines what
//     information about the user we expect to receive in the callback.
//     - state: this is a base64 encoded protobuf value that contains the NodeID of the node that
//     originated the login request and the state variable that was recorded as being in the
//     caller's cookie at the time. This value wil be returned back as a parameter to the
//     callback URL by the authentication provider. We check to make sure it matches the
//     cookie and our stored state to ensure we're processing a response to the request
//     that we triggered.
//
// 3. The user authenticates at the auth provider
//
//  4. The auth provider redirects to the `redirect_uri` we provided, which is handled at
//     `/oidc/v1/callback`. We validate that the `state` parameter matches the user's browser cookie,
//     then we exchange the `authentication_code` that was provided for an OAuth token from the
//     auth provider via an HTTP request. This handled by the `go-oidc` library. Once we have the
//     id token, we validate and decode it, extract a field from the JSON set via the
//     `server.oidc_authentication.claim_json_key`. The key is then passed through a regular
//     expression to transform its value to a DB principal (this is to support the typical workflow
//     of stripping a realm or domain name from an email address principal). The regular expression
//     is set using the `server.oidc_authentication.principal_regex` cluster setting.
//
//     If the username we compute exists in the DB, we create a web session for them in the usual
//     manner, bypassing any password validation requirements, and redirect them to `/` so they can
//     enjoy a logged-in experience in the Admin UI.
type oidcAuthenticationServer struct {
	mutex   syncutil.RWMutex
	conf    oidcAuthenticationConf
	manager IOIDCManager
	// enabled is used to store whether the user has flipped the enabled flag in the cluster settings
	// if enabled is true and initialized is false, the code will continue to attempt to re-initialize
	// the OIDC server every time a handler is invoked for the login or callback endpoints. This is
	// to help us gracefully recover from auth provider downtime without operator intervention.
	enabled     bool
	initialized bool
}

type oidcAuthenticationConf struct {
	clientID        string
	clientSecret    string
	redirectURLConf redirectURLConf
	providerURL     string
	scopes          string
	enabled         bool
	claimJSONKey    string
	principalRegex  *regexp.Regexp
	buttonText      string
	autoLogin       bool
	successPath     string

	generateJWTAuthTokenEnabled  bool
	generateJWTAuthTokenUseToken tokenToUse
	generateJWTAuthTokenSQLHost  string
	generateJWTAuthTokenSQLPort  int64
	providerCustomCA             string
	httpClient                   *httputil.Client
}

// GetOIDCConf is used to extract certain parts of the OIDC
// configuration at run-time for embedding into the DB Console in order
// to manage the login experience the UI provides.
func (s *oidcAuthenticationServer) GetOIDCConf() ui.OIDCUIConf {
	return ui.OIDCUIConf{
		ButtonText: s.conf.buttonText,
		Enabled:    s.enabled,
		AutoLogin:  s.conf.autoLogin,

		GenerateJWTAuthTokenEnabled: s.conf.generateJWTAuthTokenEnabled,
	}
}

// maybeInitializeLocked intializes the OIDC authentication server
// if not already initialized using the parameters passed in the arguments.
// It assumes oidcAuthenticationServer struct to be in locked state.
func (s *oidcAuthenticationServer) maybeInitializeLocked(
	ctx context.Context, locality roachpb.Locality, st *cluster.Settings,
) error {
	if s.enabled && !s.initialized {
		reloadConfigLocked(ctx, s, locality, st)
		if !s.initialized {
			return errors.New("OIDC: auth server could not be initialized")
		}
	}

	return nil
}

type oidcManager struct {
	oauth2Config *oauth2.Config
	verifier     *oidc.IDTokenVerifier
	httpClient   *httputil.Client
}

func (o *oidcManager) ExchangeVerifyGetClaims(
	ctx context.Context, code string, idTokenKey string,
) (map[string]json.RawMessage, error) {
	credentials, err := o.Exchange(ctx, code)
	if err != nil {
		log.Errorf(ctx, "OIDC: failed to exchange code for token: %v", err)
		return nil, err
	}

	rawIDToken, ok := credentials.Extra(idTokenKey).(string)
	if !ok {
		err := errors.New("OIDC: failed to extract ID token from the token credentials")
		log.Error(ctx, "OIDC: failed to extract ID token from the token credentials")
		return nil, err
	}

	idToken, err := o.Verify(ctx, rawIDToken)
	if err != nil {
		log.Errorf(ctx, "OIDC: unable to verify ID token: %v", err)
		return nil, err
	}

	var claims map[string]json.RawMessage
	if err := idToken.Claims(&claims); err != nil {
		log.Errorf(ctx, "OIDC: unable to deserialize token claims: %v", err)
		return nil, err
	}

	return claims, nil
}

func (o *oidcManager) Verify(ctx context.Context, s string) (*oidc.IDToken, error) {
	// Set the HTTP client in the context, as required by the `go-oidc` module.
	//
	// Note that the `Verify` method in the current version (v2.2.1) of the
	// `go-oidc` module does not use the HTTP client from the ctx provided here.
	//
	// It uses the HTTP client from the ctx passed to `NewProvider` at the time
	// of initialization instead. However, this behavior has been fixed in the
	// latest version of the module.
	//
	// This change is being made for forward-compatibility.
	octx := oidc.ClientContext(ctx, o.httpClient.Client)
	return o.verifier.Verify(octx, s)
}

func (o *oidcManager) Exchange(
	ctx context.Context, s string, option ...oauth2.AuthCodeOption,
) (*oauth2.Token, error) {
	// Set the HTTP client in the context, as required by the `oauth2` module.
	octx := oidc.ClientContext(ctx, o.httpClient.Client)
	return o.oauth2Config.Exchange(octx, s, option...)
}

func (o oidcManager) AuthCodeURL(s string, option ...oauth2.AuthCodeOption) string {
	return o.oauth2Config.AuthCodeURL(s, option...)
}

type IOIDCManager interface {
	Verify(context.Context, string) (*oidc.IDToken, error)
	Exchange(context.Context, string, ...oauth2.AuthCodeOption) (*oauth2.Token, error)
	AuthCodeURL(string, ...oauth2.AuthCodeOption) string
	ExchangeVerifyGetClaims(context.Context, string, string) (map[string]json.RawMessage, error)
}

var _ IOIDCManager = &oidcManager{}

var NewOIDCManager func(context.Context, oidcAuthenticationConf, string, []string) (IOIDCManager, error) = func(
	ctx context.Context,
	conf oidcAuthenticationConf,
	redirectURL string,
	scopes []string,
) (IOIDCManager, error) {
	// We need to provide a context which cannot be cancelled because of a specific implementation
	// which prohibits context to be cancelled if we are to reuse the provider object
	// https://github.com/coreos/go-oidc/issues/339
	// TODO(souravcrl): Update go-oidc version - to control the context, in the current version of
	// go-oidc, verifier instance can be created with VerifierContext
	// https://github.com/coreos/go-oidc/blob/6d6be43e852de391805e5a5bc14146ba3cdd4195/oidc/verify.go#L125
	ctx = context.WithoutCancel(ctx)
	octx := oidc.ClientContext(ctx, conf.httpClient.Client)

	provider, err := oidc.NewProvider(octx, conf.providerURL)
	if err != nil {
		return nil, err
	}

	oauth2Config := &oauth2.Config{
		ClientID:     conf.clientID,
		ClientSecret: conf.clientSecret,
		RedirectURL:  redirectURL,

		Endpoint: provider.Endpoint(),
		Scopes:   scopes,
	}

	verifier := provider.Verifier(&oidc.Config{ClientID: conf.clientID})

	return &oidcManager{
		verifier:     verifier,
		oauth2Config: oauth2Config,
		httpClient:   conf.httpClient,
	}, nil
}

func reloadConfig(
	ctx context.Context,
	server *oidcAuthenticationServer,
	locality roachpb.Locality,
	st *cluster.Settings,
) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	reloadConfigLocked(ctx, server, locality, st)
}

func reloadConfigLocked(
	ctx context.Context,
	oidcAuthServer *oidcAuthenticationServer,
	locality roachpb.Locality,
	st *cluster.Settings,
) {
	clientTimeout := OIDCAuthClientTimeout.Get(&st.SV)

	conf := oidcAuthenticationConf{
		clientID:        OIDCClientID.Get(&st.SV),
		clientSecret:    OIDCClientSecret.Get(&st.SV),
		redirectURLConf: mustParseOIDCRedirectURL(OIDCRedirectURL.Get(&st.SV)),
		providerURL:     OIDCProviderURL.Get(&st.SV),
		scopes:          OIDCScopes.Get(&st.SV),
		claimJSONKey:    OIDCClaimJSONKey.Get(&st.SV),
		enabled:         OIDCEnabled.Get(&st.SV),
		// The success of this line is guaranteed by the validation of the setting
		principalRegex: regexp.MustCompile(OIDCPrincipalRegex.Get(&st.SV)),
		buttonText:     OIDCButtonText.Get(&st.SV),
		autoLogin:      OIDCAutoLogin.Get(&st.SV),
		successPath:    server.ServerHTTPBasePath.Get(&st.SV),

		generateJWTAuthTokenEnabled:  OIDCGenerateClusterSSOTokenEnabled.Get(&st.SV),
		generateJWTAuthTokenUseToken: OIDCGenerateClusterSSOTokenUseToken.Get(&st.SV),
		generateJWTAuthTokenSQLHost:  OIDCGenerateClusterSSOTokenSQLHost.Get(&st.SV),
		generateJWTAuthTokenSQLPort:  OIDCGenerateClusterSSOTokenSQLPort.Get(&st.SV),
		providerCustomCA:             OIDCProviderCustomCA.Get(&st.SV),
		httpClient: httputil.NewClient(
			httputil.WithClientTimeout(clientTimeout),
			httputil.WithDialerTimeout(clientTimeout),
			httputil.WithCustomCAPEM(OIDCProviderCustomCA.Get(&st.SV)),
		),
	}

	if !oidcAuthServer.conf.enabled && conf.enabled {
		telemetry.Inc(enableUseCounter)
	}

	oidcAuthServer.initialized = false
	oidcAuthServer.conf = conf
	if oidcAuthServer.conf.enabled {
		// `enabled` stores the configuration state and records the operator's _intent_ that the feature
		// be enabled. Since the call to `NewProvider` below makes an HTTP request and could fail for
		// many reasons, we record the successful configuration of a provider using the `initialized`
		// flag which is set at the bottom of this function.
		// If `enabled` is true and `initialized` is false, the HTTP handlers for OIDC will attempt
		// to initialize the OIDC provider.
		oidcAuthServer.enabled = true
	} else {
		oidcAuthServer.enabled = false
		return
	}

	// Validation of the scope setting will require that we have the `openid` scope.
	scopesForOauth := strings.Split(oidcAuthServer.conf.scopes, " ")

	redirectURL, err := getRegionSpecificRedirectURL(locality, oidcAuthServer.conf.redirectURLConf)
	if err != nil {
		log.Warningf(ctx, "unable to initialize OIDC server, disabling OIDC: %v", err)
		if log.V(1) {
			log.Infof(ctx, "check redirect URL OIDC cluster setting: "+OIDCRedirectURLSettingName)
		}
		return
	}

	manager, err := NewOIDCManager(ctx, oidcAuthServer.conf, redirectURL, scopesForOauth)
	if err != nil {
		log.Warningf(ctx, "unable to initialize OIDC server, disabling OIDC: %v", err)
		if log.V(1) {
			log.Infof(ctx, "check provider URL OIDC cluster setting: "+OIDCProviderURLSettingName)
		}
		return
	}

	oidcAuthServer.manager = manager
	oidcAuthServer.initialized = true
	log.Infof(ctx, "initialized OIDC server")
}

// getRegionSpecificRedirectURL will query the localities and see if we have
// regions configured. If we do, it will ask the configuration for a
// region-specific redirect, otherwise it will query for one without a region.
func getRegionSpecificRedirectURL(locality roachpb.Locality, conf redirectURLConf) (string, error) {
	if len(locality.Tiers) > 0 {
		region, containsRegion := locality.Find("region")
		if containsRegion {
			if redirectURL, ok := conf.getForRegion(region); ok {
				return redirectURL, nil
			}
			return "", errors.Newf("OIDC: no matching redirect URL found for region %s", region)
		}
	}
	s, ok := conf.get()
	if !ok {
		return "", errors.New("OIDC: redirect URL config expects region setting, which is unset")
	}
	return s, nil
}

// ConfigureOIDC attaches handlers to the server `mux` that
// can initiate and complete an OIDC authentication flow.
// This flow consists of an initial login request that triggers
// an HTTP redirect to the auth provider, and a callback endpoint
// that the auth provider redirects the user back to with
// parameters containing authenticated user info.
// The login and callback handlers also support an alternative
// flow that, rather than logging the user in, produces a JWT
// auth token that may be used for cluster SSO.
var ConfigureOIDC = func(
	serverCtx context.Context,
	st *cluster.Settings,
	locality roachpb.Locality,
	handleHTTP func(pattern string, handler http.Handler),
	userLoginFromSSO func(ctx context.Context, username string) (*http.Cookie, error),
	ambientCtx log.AmbientContext,
	cluster uuid.UUID,
) (authserver.OIDC, error) {
	oidcAuthentication := &oidcAuthenticationServer{}

	// Don't want to use GRPC here since these endpoints require HTTP-Redirect behaviors and the
	// callback endpoint will be receiving specialized parameters that grpc-gateway will only get
	// in the way of processing.
	handleHTTP(oidcCallbackPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Verify state and errors.
		oidcAuthentication.mutex.Lock()
		defer oidcAuthentication.mutex.Unlock()

		if err := oidcAuthentication.maybeInitializeLocked(ctx, locality, st); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !oidcAuthentication.enabled {
			http.Error(w, "OIDC: disabled", http.StatusBadRequest)
			return
		}

		// We trigger telemetry on this endpoint only when we pass through the enabled gate to maintain
		// a useful signal.
		telemetry.Inc(beginCallbackUseCounter)

		state := r.URL.Query().Get(stateKey)

		secretCookie, err := r.Cookie(secretCookieName)
		if err != nil {
			log.Errorf(ctx, "OIDC: missing client side cookie: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		kast := keyAndSignedToken{
			secretCookie,
			state,
		}

		valid, mode, err := kast.validate()
		if err != nil {
			log.Errorf(ctx, "OIDC: validating client cookie and state token pair: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}
		if !valid {
			log.Error(ctx, "OIDC: invalid client cookie and state token pair")
			http.Error(w, genericCallbackHTTPError, http.StatusBadRequest)
			return
		}
		// If the user wanted to generate a JWT auth token instead of logging
		// in, we redirect to a web UI that handles the rest of the work.
		if mode == serverpb.OIDCState_MODE_GENERATE_JWT_AUTH_TOKEN {
			telemetry.Inc(loginSuccessUseCounter)

			payload, err := json.Marshal(struct{ State, Code string }{
				State: r.URL.Query().Get(stateKey),
				Code:  r.URL.Query().Get(codeKey),
			})
			if err != nil {
				log.Error(ctx, "OIDC: failed to marshal state and code (can this happen?)")
				http.Error(w, genericCallbackHTTPError, http.StatusBadRequest)
			}

			encoded := base64.StdEncoding.EncodeToString(payload)
			u := url.URL{Path: "/", Fragment: fmt.Sprintf("/jwt/%s", encoded)}
			http.Redirect(w, r, u.String(), http.StatusTemporaryRedirect)
			return
		}

		claims, err := oidcAuthentication.manager.ExchangeVerifyGetClaims(ctx, r.URL.Query().Get(codeKey), idTokenKey)
		if err != nil {
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		if log.V(1) {
			log.Infof(
				ctx,
				"attempting to extract SQL username from the payload using the claim key %s and regex %s",
				oidcAuthentication.conf.claimJSONKey,
				oidcAuthentication.conf.principalRegex,
			)
		}

		username, err := extractUsernameFromClaims(
			ctx, claims, oidcAuthentication.conf.claimJSONKey, oidcAuthentication.conf.principalRegex,
		)
		if err != nil {
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		cookie, err := userLoginFromSSO(ctx, username)
		if err != nil {
			log.Errorf(ctx, "OIDC: failed to complete authentication: unable to create session for %s: %v", username, err)
			http.Error(w, genericCallbackHTTPError, http.StatusForbidden)
			return
		}

		if err := utilccl.CheckEnterpriseEnabled(st, "OIDC"); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		http.SetCookie(w, cookie)
		http.Redirect(w, r, oidcAuthentication.conf.successPath, http.StatusTemporaryRedirect)

		telemetry.Inc(loginSuccessUseCounter)
	}))

	handleHTTP(oidcJWTPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Verify state and errors.
		oidcAuthentication.mutex.Lock()
		defer oidcAuthentication.mutex.Unlock()

		if err := oidcAuthentication.maybeInitializeLocked(ctx, locality, st); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !oidcAuthentication.enabled {
			http.Error(w, "OIDC: disabled", http.StatusBadRequest)
			return
		}

		if !oidcAuthentication.conf.generateJWTAuthTokenEnabled {
			http.Error(w, "OIDC: generate JWT auth token disabled", http.StatusBadRequest)
			return
		}

		// We trigger telemetry on this endpoint only when we pass through the enabled gate to maintain
		// a useful signal.
		telemetry.Inc(beginJWTUseCounter)

		state := r.URL.Query().Get(stateKey)

		secretCookie, err := r.Cookie(secretCookieName)
		if err != nil {
			log.Errorf(ctx, "OIDC: missing client side cookie: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		kast := keyAndSignedToken{
			secretCookie,
			state,
		}

		// There's no need to check mode because we're only handling the JWT mode here.
		valid, _, err := kast.validate()
		if err != nil {
			log.Errorf(ctx, "OIDC: validating client cookie and state token pair: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}
		if !valid {
			log.Error(ctx, "OIDC: invalid client cookie and state token pair")
			http.Error(w, genericCallbackHTTPError, http.StatusBadRequest)
			return
		}

		credentials, err := oidcAuthentication.manager.Exchange(ctx, r.URL.Query().Get(codeKey))
		if err != nil {
			log.Errorf(ctx, "OIDC: failed to exchange code for token: %v", err)
			log.Errorf(ctx, "%v", r.URL.Query().Get(codeKey))
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		rawToken := credentials.AccessToken
		if oidcAuthentication.conf.generateJWTAuthTokenUseToken == useIdToken {
			rawIDToken, ok := credentials.Extra(idTokenKey).(string)
			if !ok {
				log.Error(ctx, "OIDC: failed to extract ID token from the token credentials")
				http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
				return
			}
			rawToken = rawIDToken
		}

		token, err := oidcAuthentication.manager.Verify(ctx, rawToken)
		if err != nil {
			log.Errorf(ctx, "OIDC: unable to verify ID token: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		var claims map[string]json.RawMessage
		if err := token.Claims(&claims); err != nil {
			log.Errorf(ctx, "OIDC: unable to deserialize token claims: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		claim := jwtauthccl.JWTAuthClaim.Get(&st.SV)

		if log.V(1) {
			log.Infof(
				ctx,
				"attempting to extract SQL username from the payload using the claim key %s, issuer %s, and %s",
				claim,
				token.Issuer,
				pgwire.ConnIdentityMapConf.Name(),
			)
		}

		// TODO(todd): Consider removing the duplication here with
		//   jwtAuthenticator.ValidateJWTLogin(). That may be slightly tricky,
		//   because that code works with a jwt.Token instead of an
		//   *oidc.IDToken. (Though the two should contain the same
		//   information.)

		// 1. Extract principals from claims, in the style of
		//    extractUsernameFromClaims.
		var tokenPrincipals []string
		{
			var principal string

			if claim == "" || claim == "sub" {
				principal = token.Subject
			} else {
				claimKeys := make([]string, len(claims))
				i := 0
				for k := range claims {
					claimKeys[i] = k
					i++
				}

				targetClaim, ok := claims[claim]
				if !ok {
					log.Errorf(ctx, "OIDC: failed to complete authentication: invalid JSON claim key: %s", claim)
					log.Infof(ctx, "token payload includes the following claims: %s", strings.Join(claimKeys, ", "))
					http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
					return
				}
				if err := json.Unmarshal(targetClaim, &principal); err != nil {
					if log.V(1) {
						log.Infof(ctx, "failed parsing claim as string; attempting to parse as a list")
					}
					if err := json.Unmarshal(targetClaim, &tokenPrincipals); err != nil {
						log.Errorf(ctx, "OIDC: failed to complete authentication: failed to parse value for the claim %s: %v", claim, err)
						http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
						return
					}
				}
			}

			if len(tokenPrincipals) == 0 {
				tokenPrincipals = []string{principal}
			}
		}

		// 2. Load the identity map.
		var idMap *identmap.Conf
		{
			// TODO(todd): Get the identity map from someplace that's already caching it.
			val := pgwire.ConnIdentityMapConf.Get(&st.SV)
			idMap, err = identmap.From(strings.NewReader(val))
			if err != nil {
				log.Ops.Warningf(ctx, "invalid %s: %v", val, err)
				idMap = identmap.Empty()
			}
		}

		// 3. Translate principals to SQL usernames, in the style of ValidateJWTLogin:
		var acceptedUsernames []string
		{
			for _, tokenPrincipal := range tokenPrincipals {
				if usernames, mapFound, err := idMap.Map(token.Issuer, tokenPrincipal); mapFound {
					if err != nil {
						log.Errorf(ctx, "OIDC: failed to map %s, issuer %s, to SQL usernames: %v", tokenPrincipal, token.Issuer, err)
						http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
						return
					}
					for _, username := range usernames {
						acceptedUsernames = append(acceptedUsernames, username.Normalized())
					}
				} else {
					log.Infof(ctx, "OIDC: no identity map found for issuer %s; using %s without mapping", token.Issuer, tokenPrincipal)
					if username, err := username.MakeSQLUsernameFromUserInput(tokenPrincipal, username.PurposeValidation); err != nil {
						acceptedUsernames = append(acceptedUsernames, username.Normalized())
					}
				}
			}
		}

		if len(acceptedUsernames) == 0 {
			log.Errorf(ctx, "OIDC: failed to extract usernames from principals %v; check %s", tokenPrincipals, pgwire.ConnIdentityMapConf.Name())
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		// TODO(todd): Rework this into something more CRDB-native as we reconsider these handlers.
		body, err := json.MarshalIndent(struct {
			Usernames []string
			Password  string
			Host      string
			Port      int64
			Expiry    time.Time
		}{
			Usernames: acceptedUsernames,
			Password:  rawToken,
			Host:      oidcAuthentication.conf.generateJWTAuthTokenSQLHost,
			Port:      oidcAuthentication.conf.generateJWTAuthTokenSQLPort,
			Expiry:    token.Expiry,
		}, "", "  ")

		if err != nil {
			log.Error(ctx, "OIDC: failed to marshal connection parameters (can this happen?)")
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
		}

		w.Header().Add("Content-Security-Policy", "sandbox")
		w.Header().Add("Content-Type", "application/json")
		// Explicitly ignore any errors from writing our body as there's
		// nothing to be done if the write fails.
		_, _ = w.Write(body)
	}))

	handleHTTP(oidcLoginPath, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		oidcAuthentication.mutex.Lock()
		defer oidcAuthentication.mutex.Unlock()

		if err := oidcAuthentication.maybeInitializeLocked(ctx, locality, st); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if !oidcAuthentication.enabled {
			http.Error(w, "OIDC: disabled", http.StatusBadRequest)
			return
		}

		telemetry.Inc(beginAuthUseCounter)

		mode := serverpb.OIDCState_MODE_LOG_IN
		if r.URL.Query().Has("jwt") {
			mode = serverpb.OIDCState_MODE_GENERATE_JWT_AUTH_TOKEN
		}

		kast, err := newKeyAndSignedToken(hmacKeySize, stateTokenSize, mode)
		if err != nil {
			log.Errorf(ctx, "OIDC: unable to generate key and signed message: %v", err)
			http.Error(w, genericLoginHTTPError, http.StatusInternalServerError)
			return
		}

		http.SetCookie(w, kast.secretKeyCookie)
		http.Redirect(w, r, oidcAuthentication.manager.AuthCodeURL(kast.signedTokenEncoded), http.StatusFound)
	}))

	reloadConfig(serverCtx, oidcAuthentication, locality, st)

	OIDCEnabled.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCClientID.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCClientSecret.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCRedirectURL.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCProviderURL.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCScopes.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCClaimJSONKey.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCPrincipalRegex.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCButtonText.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCAutoLogin.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	server.ServerHTTPBasePath.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCGenerateClusterSSOTokenEnabled.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCGenerateClusterSSOTokenUseToken.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCGenerateClusterSSOTokenSQLHost.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCGenerateClusterSSOTokenSQLPort.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})
	OIDCAuthClientTimeout.SetOnChange(&st.SV, func(ctx context.Context) {
		reloadConfig(ambientCtx.AnnotateCtx(ctx), oidcAuthentication, locality, st)
	})

	return oidcAuthentication, nil
}

func init() {
	authserver.ConfigureOIDC = ConfigureOIDC
}
