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
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
)

const (
	idTokenKey = "id_token"
	codeKey    = "code"
	stateKey   = "state"
)

// oidcAuthenticationServer is an implementation of the OpenID Connect authentication code flow
// to support single-sign-on to the Admin UI via an external identity provider.
//
// The implementation uses the `go-oidc` implementation and is supported through a number of
// cluster settings defined in `oidc/settings.go`. These configure the CRDB cluster to redirect
// to an auth provider for logins, and to accept a callback once authentication completes, where
// CRDB translates the provided login principal to a SQL principal and creates a web session.
//
// The implementation adds two new HTTP handlers to the server at `/oidc/login/` and `oidc/callback`
// the functions of which are described below.
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
// 2. The browser loads `/oidc/login` from the cluster, which triggers a redirect to the auth
//    provider. A number of parameters are sent along with this request: (these are all defined in
//    the OIDC spec available at: https://openid.net/specs/openid-connect-core-1_0.html)
//    - client_id and client_secret: these are set using their correspondingly named cluster
//                                   settings and are values that the auth provider will create.
//    - redirect_uri: set using the `server.oidc_authentication.redirect_url` cluster setting. This
//                    should point to `/oidc/callback` at the appropriate host or load balancer that
//                    the cluster is deployed to.
//    - scopes: set using the `server.oidc_authentication.scopes` cluster setting. This defines what
//              information about the user we expect to receive in the callback.
//    - state: this is a random string set in a cookie when the user first loads the Admin UI. It's
//             used to prevent CSRF attacks in the authentication code flow and will be returned
//             back as a parameter to the callback URL by the authentication provider. We check to
//             make sure it matches the cookie to ensure we're processing a response to the request
//             that we triggered.
//
// 3. The user authenticates at the auth provider
//
// 4. The auth provider redirects to the `redirect_uri` we provided, which is handled at
//    `/oidc/callback`. We validate that the `state` parameter matches the user's browser cookie,
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
	mutex        syncutil.RWMutex
	conf         oidcAuthenticationConf
	oauth2Config oauth2.Config
	verifier     *oidc.IDTokenVerifier
	enabled      bool
	initialized  bool
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

func loadLocalOIDCConfigUponRemoteSettingsChange(
	ctx context.Context, server *oidcAuthenticationServer, st *cluster.Settings,
) {
	oidcAuthenticationConf := oidcAuthenticationConf{
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

	server.mutex.Lock()
	defer server.mutex.Unlock()

	server.initialized = false
	server.conf = oidcAuthenticationConf
	if server.conf.enabled {
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
	ctx context.Context,
	st *cluster.Settings,
	mux *http.ServeMux,
	userLoginFromSSO func(ctx context.Context, username string) (*http.Cookie, error),
	ambientCtx log.AmbientContext,
	cluster uuid.UUID,
) (ui.OIDCUIConfGetter, error) {
	oidcAuthentication := &oidcAuthenticationServer{}

	// Don't want to use GRPC here since these endpoints require HTTP-Redirect behaviors and the
	// callback endpoint will be receiving specialized parameters that grpc-gateway will only get
	// in the way of processing.
	mux.HandleFunc("/oidc/callback", func(w http.ResponseWriter, r *http.Request) {
		ctx = r.Context()

		if oidcAuthentication.enabled && !oidcAuthentication.initialized {
			loadLocalOIDCConfigUponRemoteSettingsChange(ctx, oidcAuthentication, st)
		}

		// Verify state and errors.
		oidcAuthentication.mutex.Lock()
		defer oidcAuthentication.mutex.Unlock()

		if !oidcAuthentication.enabled {
			http.Error(w, "SSO via OIDC is disabled", http.StatusBadRequest)
			return
		}

		authProviderState := r.URL.Query().Get(stateKey)
		clientStateCookie, err := r.Cookie("oidc_state")
		if err != nil {
			log.Errorf(ctx, "missing state in OIDC callback: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		if authProviderState != clientStateCookie.Value {
			// We're choosing not to delete the cookie here in order to allow for some retry ability.
			// If the user re-visits the login page after a failed login attempt, they will get a new
			// state cookie set anyway.
			err := errors.New("OIDC callback state does not match auth provider state")
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		oauth2Token, err := oidcAuthentication.oauth2Config.Exchange(ctx, r.URL.Query().Get(codeKey))
		if err != nil {
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		rawIDToken, ok := oauth2Token.Extra(idTokenKey).(string)
		if !ok {
			err := errors.New("failed to extract ID token from OAuth2 token")
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		idToken, err := oidcAuthentication.verifier.Verify(ctx, rawIDToken)
		if err != nil {
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var claims map[string]json.RawMessage
		if err := idToken.Claims(&claims); err != nil {
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var principal string
		claim := claims[oidcAuthentication.conf.claimJSONKey]
		if err := json.Unmarshal(claim, &principal); err != nil {
			log.Errorf(ctx, "failed to complete OIDC authentication: failed to extract claim key %s: %v", oidcAuthentication.conf.claimJSONKey, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		match := oidcAuthentication.conf.principalRegex.FindStringSubmatch(principal)
		numGroups := len(match)
		if numGroups != 2 {
			err := errors.Newf("failed to complete OIDC authentication: expected two groups in regexp, got %d", numGroups)
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		username := match[1]
		cookie, err := userLoginFromSSO(ctx, username)
		if err != nil {
			log.Errorf(ctx, "failed to complete OIDC authentication: unable to create session for %s: %v", username, err)
			http.Error(w, err.Error(), http.StatusForbidden)
			return
		}

		org := sql.ClusterOrganization.Get(&st.SV)
		if err := utilccl.CheckEnterpriseEnabled(st, cluster, org, "OIDC"); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		http.SetCookie(w, cookie)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
	})

	mux.HandleFunc("/oidc/login", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		if oidcAuthentication.enabled && !oidcAuthentication.initialized {
			loadLocalOIDCConfigUponRemoteSettingsChange(ctx, oidcAuthentication, st)
		}

		oidcAuthentication.mutex.Lock()
		defer oidcAuthentication.mutex.Unlock()

		if !oidcAuthentication.enabled {
			http.Error(w, "SSO via OIDC is Disabled", http.StatusBadRequest)
			return
		}

		state, err := r.Cookie("oidc_state")
		if err != nil {
			log.Errorf(ctx, "unable to get OIDC state cookie: %v", err)
			http.Error(w, "OIDC is unable to proceed: unable to get OIDC state cookie", http.StatusBadRequest)
			return
		}
		if len(state.Value) == 0 {
			log.Error(ctx, "OIDC is unable to proceed: state is empty")
			http.Error(w, "OIDC is unable to proceed: state is empty", http.StatusBadRequest)
			return
		}

		http.Redirect(w, r, oidcAuthentication.oauth2Config.AuthCodeURL(state.Value), http.StatusFound)
	})

	loadLocalOIDCConfigUponRemoteSettingsChange(ctx, oidcAuthentication, st)

	OIDCEnabled.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCClientID.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCClientSecret.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCRedirectURL.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCProviderURL.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCScopes.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCClaimJSONKey.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCPrincipalRegex.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})
	OIDCButtonText.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(
			ambientCtx.AnnotateCtx(context.Background()),
			oidcAuthentication,
			st,
		)
	})

	return oidcAuthentication, nil
}

// SetOIDCStateCookie is a function that is used by the `ui` module as middleware to set a state
// cookie on unauthenticated requests for the Admin UI. This cookie will be picked up in the
// handlers defined above and used to verify that callbacks we receive from the auth provider match
// requests that we initiated.
var SetOIDCStateCookie = func(w http.ResponseWriter, r *http.Request) {
	size := 16
	state := make([]byte, size)
	if _, err := crypto_rand.Read(state); err != nil {
		log.Errorf(r.Context(), "unable to generate oidc state cookie: %v", err)
	}
	encodedState := base64.StdEncoding.EncodeToString(state)
	oidcStateCookie := http.Cookie{
		Name:  "oidc_state",
		Value: encodedState,
	}
	http.SetCookie(w, &oidcStateCookie)
}

func init() {
	server.ConfigureOIDC = ConfigureOIDC
	ui.SetOIDCStateCookie = SetOIDCStateCookie
}
