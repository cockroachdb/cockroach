// Copyright 2020 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package oidcccl

import (
	"context"
	crypto_rand "crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/utilccl"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
)

const (
	idTokenKey               = "id_token"
	codeKey                  = "code"
	stateKey                 = "state"
	stateCookieName          = "oidc_state"
	oidcLoginPath            = "/oidc/v1/login"
	oidcCallbackPath         = "/oidc/v1/callback"
	genericCallbackHTTPError = "OIDC: unable to complete authentication"
	genericLoginHTTPError    = "OIDC: unable to initiate authentication"
	counterPrefix            = "auth.oidc."
	beginAuthCounterName     = counterPrefix + "begin_auth"
	beginCallbackCounterName = counterPrefix + "begin_callback"
	loginSuccessCounterName  = counterPrefix + "login_success"
	enableCounterName        = counterPrefix + "enable"
)

var (
	beginAuthUseCounter     = telemetry.GetCounterOnce(beginAuthCounterName)
	beginCallbackUseCounter = telemetry.GetCounterOnce(beginCallbackCounterName)
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
// 0. The cluster operator configures the cluster to use OIDC. Once the cluster setting
//    `server.oidc_authentication.enabled` is set to true, the OIDC client will make a request to
//    retrieve the discovery document using the `server.oidc_authentication.provider_url` setting.
//    This attempt will be retried automatically with every call to the `login` or `callback` or
//    any change to any OIDC settings as long as `enabled` is still true. That behavior is meant to
//    support easy recovery from any downtime or HTTP errors on the provider side.
//
// 1. A CRDB user opens the Admin UI and clicks on the `Login with OIDC` button (text is
//    configurable using the `server.oidc_authentication.button_text` setting.
//
// 2. The browser loads `/oidc/v1/login` from the cluster, which triggers a redirect to the auth
//    provider. A number of parameters are sent along with this request: (these are all defined in
//    the OIDC spec available at: https://openid.net/specs/openid-connect-core-1_0.html)
//    - client_id and client_secret: these are set using their correspondingly named cluster
//                                   settings and are values that the auth provider will create.
//    - redirect_uri: set using the `server.oidc_authentication.redirect_url` cluster setting. This
//                    will point to `/oidc/v1/callback` at the appropriate host or load balancer
//                    that the cluster is deployed to.
//    - scopes: set using the `server.oidc_authentication.scopes` cluster setting. This defines what
//              information about the user we expect to receive in the callback.
//    - state: this is a base64 encoded protobuf value that contains the NodeID of the node that
//             originated the login request and the state variable that was recorded as being in the
//             caller's cookie at the time. This value wil be returned back as a parameter to the
//             callback URL by the authentication provider. We check to make sure it matches the
//             cookie and our stored state to ensure we're processing a response to the request
//             that we triggered.
//
// 3. The user authenticates at the auth provider
//
// 4. The auth provider redirects to the `redirect_uri` we provided, which is handled at
//    `/oidc/v1/callback`. We validate that the `state` parameter matches the user's browser cookie,
//    then we exchange the `authentication_code` that was provided for an OAuth token from the
//    auth provider via an HTTP request. This handled by the `go-oidc` library. Once we have the
//    id token, we validate and decode it, extract a field from the JSON set via the
//    `server.oidc_authentication.claim_json_key`. The key is then passed through a regular
//    expression to transform its value to a DB principal (this is to support the typical workflow
//    of stripping a realm or domain name from an email address principal). The regular expression
//    is set using the `server.oidc_authentication.principal_regex` cluster setting.
//
//    If the username we compute exists in the DB, we create a web session for them in the usual
//    manner, bypassing any password validation requirements, and redirect them to `/` so they can
//    enjoy a logged-in experience in the Admin UI.
type oidcAuthenticationServer struct {
	mutex          syncutil.RWMutex
	conf           oidcAuthenticationConf
	oauth2Config   oauth2.Config
	verifier       *oidc.IDTokenVerifier
	stateValidator *stateValidator
	// enabled is used to store whether the user has flipped the enabled flag in the cluster settings
	// if enabled is true and initialized is false, the code will continue to attempt to re-initialize
	// the OIDC server every time a handler is invoked for the login or callback endpoints. This is
	// to help us gracefully recover from auth provider downtime without operator intervention.
	enabled     bool
	initialized bool
}

type oidcAuthenticationConf struct {
	clientID       string
	clientSecret   string
	redirectURL    string
	providerURL    string
	scopes         string
	enabled        bool
	claimJSONKey   string
	principalRegex *regexp.Regexp
	buttonText     string
}

// GetUIConf is used to extract certain parts of the OIDC
// configuration at run-time for embedding into the
// Admin UI HTML in order to manage the login experience
// the UI provides.
func (s *oidcAuthenticationServer) GetOIDCConf() ui.OIDCUIConf {
	return ui.OIDCUIConf{
		ButtonText: s.conf.buttonText,
		Enabled:    s.enabled,
	}
}

func reloadConfig(ctx context.Context, server *oidcAuthenticationServer, st *cluster.Settings) {
	server.mutex.Lock()
	defer server.mutex.Unlock()
	reloadConfigLocked(ctx, server, st)
}

func reloadConfigLocked(
	ctx context.Context, server *oidcAuthenticationServer, st *cluster.Settings,
) {
	conf := oidcAuthenticationConf{
		clientID:     OIDCClientID.Get(&st.SV),
		clientSecret: OIDCClientSecret.Get(&st.SV),
		redirectURL:  OIDCRedirectURL.Get(&st.SV),
		providerURL:  OIDCProviderURL.Get(&st.SV),
		scopes:       OIDCScopes.Get(&st.SV),
		claimJSONKey: OIDCClaimJSONKey.Get(&st.SV),
		enabled:      OIDCEnabled.Get(&st.SV),
		// The success of this line is guaranteed by the validation of the setting
		principalRegex: regexp.MustCompile(OIDCPrincipalRegex.Get(&st.SV)),
		buttonText:     OIDCButtonText.Get(&st.SV),
	}

	if !server.conf.enabled && conf.enabled {
		telemetry.Inc(enableUseCounter)
	}

	server.initialized = false
	server.conf = conf
	server.stateValidator = newStateValidator()
	if server.conf.enabled {
		// `enabled` stores the configuration state and records the operator's _intent_ that the feature
		// be enabled. Since the call to `NewProvider` below makes an HTTP request and could fail for
		// many reasons, we record the successful configuration of a provider using the `initialized`
		// flag which is set at the bottom of this function.
		// If `enabled` is true and `initialized` is false, the HTTP handlers for OIDC will attempt
		// to initialize the OIDC provider.
		server.enabled = true
	} else {
		server.enabled = false
		return
	}

	provider, err := oidc.NewProvider(ctx, server.conf.providerURL)
	if err != nil {
		log.Warningf(ctx, "unable to initialize OIDC provider, disabling OIDC: %v", err)
		return
	}

	// Validation of the scope setting will require that we have the `openid` scope
	scopesForOauth := strings.Split(server.conf.scopes, " ")

	server.oauth2Config = oauth2.Config{
		ClientID:     server.conf.clientID,
		ClientSecret: server.conf.clientSecret,
		RedirectURL:  server.conf.redirectURL,

		Endpoint: provider.Endpoint(),
		Scopes:   scopesForOauth,
	}

	server.verifier = provider.Verifier(&oidc.Config{ClientID: server.conf.clientID})
	server.initialized = true
	log.Infof(ctx, "initialized oidc server")
}

// ConfigureOIDC attaches handlers to the server `mux` that
// can initiate and complete an OIDC authentication flow.
// This flow consists of an initial login request that triggers
// an HTTP redirect to the auth provider, and a callback endpoint
// that the auth provider redirects the user back to with
// parameters containing authenticated user info.
var ConfigureOIDC = func(
	serverCtx context.Context,
	st *cluster.Settings,
	mux *http.ServeMux,
	userLoginFromSSO func(ctx context.Context, username string) (*http.Cookie, error),
	ambientCtx log.AmbientContext,
	cluster uuid.UUID,
	nodeDialer *nodedialer.Dialer,
	nodeID roachpb.NodeID,
) (server.OIDC, error) {
	oidcAuthentication := &oidcAuthenticationServer{}

	// Don't want to use GRPC here since these endpoints require HTTP-Redirect behaviors and the
	// callback endpoint will be receiving specialized parameters that grpc-gateway will only get
	// in the way of processing.
	mux.HandleFunc(oidcCallbackPath, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		// Verify state and errors.
		oidcAuthentication.mutex.Lock()
		defer oidcAuthentication.mutex.Unlock()

		if oidcAuthentication.enabled && !oidcAuthentication.initialized {
			reloadConfigLocked(ctx, oidcAuthentication, st)
		}

		if !oidcAuthentication.enabled {
			http.Error(w, "OIDC: disabled", http.StatusBadRequest)
			return
		}

		// We trigger telemetry on this endpoint only when we pass through the enabled gate to maintain
		// a useful signal.
		telemetry.Inc(beginCallbackUseCounter)

		state := r.URL.Query().Get(stateKey)

		// First we check to see that the state matches the cookie, then make sure it matches one we
		// stored server-side. Since we stored the entire encoded proto in the cookie, we can compare
		// without deserialization.
		stateCookie, err := r.Cookie(stateCookieName)
		if err != nil {
			log.Errorf(ctx, "OIDC: missing client side cookie: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}
		if stateCookie.Value != state {
			log.Errorf(ctx, "OIDC: client side cookie and callback param do not match: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusBadRequest)
			return
		}

		statePb, err := decodeOIDCState(state)
		if err != nil {
			log.Errorf(ctx, "OIDC: failed to decode state proto: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		if statePb.NodeID == nodeID {
			// Validate locally if same node.
			err = oidcAuthentication.stateValidator.validateAndClear(string(statePb.Secret))
			if err != nil {
				log.Errorf(ctx, "OIDC: this node reported invalid state: %v", err)
				http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
				return
			}
		} else {
			//Ask another node if necessary.
			conn, err := nodeDialer.Dial(ctx, statePb.NodeID, rpc.DefaultClass)
			if err != nil {
				log.Errorf(ctx, "OIDC: failed to dial node %d to validate state: %v", statePb.NodeID, err)
				http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
				return
			}
			client := serverpb.NewLogInClient(conn)
			_, err = client.ValidateOIDCState(ctx, &serverpb.ValidateOIDCStateRequest{State: statePb})
			if err != nil {
				log.Errorf(ctx, "OIDC: node %d reported invalid state: %v", statePb.NodeID, err)
				http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
				return
			}
		}

		oauth2Token, err := oidcAuthentication.oauth2Config.Exchange(ctx, r.URL.Query().Get(codeKey))
		if err != nil {
			log.Errorf(ctx, "OIDC: failed to exchange code for token: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		rawIDToken, ok := oauth2Token.Extra(idTokenKey).(string)
		if !ok {
			log.Error(ctx, "OIDC: failed to extract ID token from OAuth2 token")
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		idToken, err := oidcAuthentication.verifier.Verify(ctx, rawIDToken)
		if err != nil {
			log.Errorf(ctx, "OIDC: unable to verify token: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		var claims map[string]json.RawMessage
		if err := idToken.Claims(&claims); err != nil {
			log.Errorf(ctx, "OIDC: unable to deserialize token claims: %v", err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		var principal string
		claim := claims[oidcAuthentication.conf.claimJSONKey]
		if err := json.Unmarshal(claim, &principal); err != nil {
			log.Errorf(ctx, "OIDC: failed to complete authentication: failed to extract claim key %s: %v", oidcAuthentication.conf.claimJSONKey, err)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		match := oidcAuthentication.conf.principalRegex.FindStringSubmatch(principal)
		numGroups := len(match)
		if numGroups != 2 {
			log.Errorf(ctx, "OIDC: failed to complete authentication: expected one group in regexp, got %d", numGroups)
			http.Error(w, genericCallbackHTTPError, http.StatusInternalServerError)
			return
		}

		username := match[1]
		cookie, err := userLoginFromSSO(ctx, username)
		if err != nil {
			log.Errorf(ctx, "OIDC: failed to complete authentication: unable to create session for %s: %v", username, err)
			http.Error(w, genericCallbackHTTPError, http.StatusForbidden)
			return
		}

		org := sql.ClusterOrganization.Get(&st.SV)
		if err := utilccl.CheckEnterpriseEnabled(st, cluster, org, "OIDC"); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		http.SetCookie(w, cookie)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)

		telemetry.Inc(loginSuccessUseCounter)
	})

	mux.HandleFunc(oidcLoginPath, func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		oidcAuthentication.mutex.Lock()
		defer oidcAuthentication.mutex.Unlock()

		if oidcAuthentication.enabled && !oidcAuthentication.initialized {
			reloadConfigLocked(ctx, oidcAuthentication, st)
		}

		if !oidcAuthentication.enabled {
			http.Error(w, "OIDC: disabled", http.StatusBadRequest)
			return
		}

		telemetry.Inc(beginAuthUseCounter)

		size := 16
		state := make([]byte, size)
		if _, err := crypto_rand.Read(state); err != nil {
			log.Errorf(ctx, "OIDC: unable to generate oidc state cookie: %v", err)
			http.Error(w, genericLoginHTTPError, http.StatusInternalServerError)
			return
		}
		base64State := base64.URLEncoding.EncodeToString(state)
		oidcAuthentication.stateValidator.add(base64State)

		encodedStateProto, err := encodeOIDCState(serverpb.OIDCState{
			NodeID: nodeID,
			Secret: []byte(base64State),
		})
		if err != nil {
			log.Errorf(ctx, "OIDC: no state encoded: %v", err)
			http.Error(w, genericLoginHTTPError, http.StatusInternalServerError)
			return
		}

		oidcStateCookie := http.Cookie{
			Name:     stateCookieName,
			Value:    encodedStateProto,
			Secure:   true,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		}

		http.SetCookie(w, &oidcStateCookie)
		http.Redirect(w, r, oidcAuthentication.oauth2Config.AuthCodeURL(encodedStateProto), http.StatusFound)
	})

	reloadConfig(serverCtx, oidcAuthentication, st)

	OIDCEnabled.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCClientID.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCClientSecret.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCRedirectURL.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCProviderURL.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCScopes.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCClaimJSONKey.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCPrincipalRegex.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCButtonText.SetOnChange(&st.SV, func() {
		reloadConfig(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})

	return oidcAuthentication, nil
}

func (s *oidcAuthenticationServer) ValidateOIDCState(state *serverpb.OIDCState) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	return s.stateValidator.validateAndClear(string(state.Secret))
}

func init() {
	server.ConfigureOIDC = ConfigureOIDC
}
