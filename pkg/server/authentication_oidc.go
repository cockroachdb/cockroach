// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"regexp"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/rpc"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/ui"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
)

const (
	idTokenKey = "id_token"
	codeKey    = "code"
	stateKey   = "state"
)

type oidcAuthenticationServer struct {
	syncutil.RWMutex
	conf         oidcAuthenticationConf
	oauth2Config oauth2.Config
	verifier     *oidc.IDTokenVerifier
	enabled      bool
	//TODO(davidh): Consider whether a more specialized "cache" data structure should be used here
	// maybe something with a time-based eviction policy since these states are short-lived.
	states map[string]struct{}
}

type oidcAuthenticationConf struct {
	clientID       string
	clientSecret   string
	redirectURL    string
	providerURL    string
	scopes         string
	enabled        bool
	claimJSONKey   string
	principalRegex string
	buttonText     string
}

// GetUIConf is used to extract certain parts of the OIDC
// configuration at run-time for embedding into the
// Admin UI HTML in order to manage the login experience
// the UI provides.
func (s *oidcAuthenticationServer) GetUIConf() ui.OidcUIConf {
	return ui.OidcUIConf{
		ButtonText: s.conf.buttonText,
		Enabled:    s.enabled,
	}
}

var oidcEnabled = func() *settings.BoolSetting {
	s := settings.RegisterPublicBoolSetting(
		"server.oidc_authentication.enabled",
		"enables or disabled oidc login for the admin ui",
		false,
	)
	return s
}()

var oidcClientIDConf = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		"server.oidc_authentication.client_id",
		"oidc client id",
		"",
	)
	return s
}()

var oidcClientSecretConf = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		"server.oidc_authentication.client_secret",
		"oidc client secret",
		"",
	)
	return s
}()

var oidcRedirectURL = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		"server.oidc_authentication.redirect_url",
		"oidc redirect url (base http url, likely your load balancer, with /oidc/callback appended)",
		"https://localhost:8080/oidc/callback",
	)
	return s
}()

var oidcProviderURLConf = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		"server.oidc_authentication.provider_url",
		"oidc provider url ({provider_url}/.well-known/openid-configuration must resolve)",
		"",
	)
	return s
}()

var oidcScopesConf = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		"server.oidc_authentication.scopes",
		"oidc scopes to include with authentication request (space delimited list of strings)",
		"",
	)
	return s
}()

var oidcClaimJSONKeyConf = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		"server.oidc_authentication.claim_json_key",
		"json key of principal to extract from payload after oidc authentication completes (usually email or sid)",
		"",
	)
	return s
}()

var oidcPrincipalRegex = func() *settings.StringSetting {
	s := settings.RegisterValidatedStringSetting(
		"server.oidc_authentication.principal_regex",
		"regular expression to apply to extracted principal (see claim_json_key setting) to translate to SQL user (golang regex format, must include 1 grouping to extract)",
		"(.+)",
		func(values *settings.Values, s string) error {
			_, err := regexp.Compile(s)
			if err != nil {
				return err
			}
			return nil
		},
	)
	s.SetVisibility(settings.Public)
	return s
}()

var oidcButtonText = func() *settings.StringSetting {
	s := settings.RegisterPublicStringSetting(
		"server.oidc_authentication.button_text",
		"text to show on button on admin ui login page to login with your oidc provider (only shown if oidc is enabled)",
		"Login with your OIDC provider",
	)
	return s
}()

func loadLocalOIDCConfigUponRemoteSettingsChange(
	ctx context.Context, server *oidcAuthenticationServer, st *cluster.Settings,
) {
	oidcEnabled := oidcEnabled.Get(&st.SV)
	clientID := oidcClientIDConf.Get(&st.SV)
	clientSecret := oidcClientSecretConf.Get(&st.SV)
	redirectURL := oidcRedirectURL.Get(&st.SV)
	providerURL := oidcProviderURLConf.Get(&st.SV)
	scopes := oidcScopesConf.Get(&st.SV)
	claimJSONKey := oidcClaimJSONKeyConf.Get(&st.SV)
	regex := oidcPrincipalRegex.Get(&st.SV)
	buttonText := oidcButtonText.Get(&st.SV)

	oidcAuthenticationConf := oidcAuthenticationConf{
		clientID:       clientID,
		clientSecret:   clientSecret,
		redirectURL:    redirectURL,
		providerURL:    providerURL,
		scopes:         scopes,
		claimJSONKey:   claimJSONKey,
		enabled:        oidcEnabled,
		principalRegex: regex,
		buttonText:     buttonText,
	}

	server.Lock()
	defer server.Unlock()

	server.conf = oidcAuthenticationConf
	if server.conf.enabled {
		server.enabled = true
	} else {
		server.enabled = false
		return
	}

	provider, err := oidc.NewProvider(ctx, server.conf.providerURL)
	if err != nil {
		log.Warningf(ctx, "unable to initialize oidc provider, disabling oidc: %v", err)
		server.enabled = false
		return
	}

	scopesForOauth := []string{oidc.ScopeOpenID}
	scopesForOauth = append(scopesForOauth, strings.Split(server.conf.scopes, " ")...)

	server.oauth2Config = oauth2.Config{
		ClientID:     server.conf.clientID,
		ClientSecret: server.conf.clientSecret,
		RedirectURL:  server.conf.redirectURL,

		Endpoint: provider.Endpoint(),
		Scopes:   scopesForOauth,
	}

	server.verifier = provider.Verifier(&oidc.Config{ClientID: server.conf.clientID})
	log.Infof(ctx, "initialized oidc server with params: %v", server.oauth2Config)
}

func (s *oidcAuthenticationServer) Validate(state *serverpb.OIDCState) (bool, error) {
	if _, ok := s.states[string(state.Secret)]; !ok {
		return false, nil
	}

	delete(s.states, string(state.Secret))
	return true, nil
}

// ConfigureOIDC attaches handlers to the server `mux` that
// can initiate and complete an OIDC authentication flow.
// This flow consists of an initial login request that triggers
// an HTTP redirect to the auth provider, and a callback endpoint
// that the auth provider redirects the user back to with
// parameters containing authenticated user info.
func (s *Server) ConfigureOIDC(ctx context.Context, st *cluster.Settings) error {

	s.oidcAuthentication = &oidcAuthenticationServer{
		states: make(map[string]struct{}),
	}

	// Don't want to use GRPC here since these endpoints require HTTP-Redirect behaviors and the callback
	// endpoint will be receiving specialized parameters that grpc-gateway will only get in the way
	// of processing.
	s.mux.HandleFunc("/oidc/callback", func(w http.ResponseWriter, r *http.Request) {
		ctx = r.Context()

		// Verify state and errors.
		s.oidcAuthentication.Lock()
		defer s.oidcAuthentication.Unlock()

		if !s.oidcAuthentication.enabled {
			http.Error(w, "sso via oidc is disabled", http.StatusBadRequest)
			return
		}

		state := r.URL.Query().Get(stateKey)
		statePb, err := decodeOIDCState(state)
		if err != nil {
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		var valid bool
		if statePb.NodeID == s.NodeID() {
			//Validate locally if same node.
			valid, err = s.oidcAuthentication.Validate(statePb)
			if err != nil {
				log.Errorf(ctx, "%v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		} else {
			//Ask another node if necessary.
			conn, err := s.nodeDialer.Dial(ctx, statePb.NodeID, rpc.DefaultClass)
			if err != nil {
				log.Errorf(ctx, "%v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			client := serverpb.NewLogInClient(conn)
			resp, err := client.ValidateOIDCState(ctx, &serverpb.ValidateOIDCStateRequest{State: statePb})
			if err != nil {
				log.Errorf(ctx, "%v", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			valid = resp.Valid
		}

		if !valid {
			err := errors.New("invalid state in oidc callback")
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		oauth2Token, err := s.oidcAuthentication.oauth2Config.Exchange(ctx, r.URL.Query().Get(codeKey))
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

		idToken, err := s.oidcAuthentication.verifier.Verify(ctx, rawIDToken)
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
		if err := json.Unmarshal(claims[s.oidcAuthentication.conf.claimJSONKey], &principal); err != nil {
			log.Errorf(ctx, "failed to extract claim key %s: %v", s.oidcAuthentication.conf.claimJSONKey, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// The success of this line is guaranteed by the validation of the setting defined above.
		principalExtractor := regexp.MustCompile(s.oidcAuthentication.conf.principalRegex)
		match := principalExtractor.FindStringSubmatch(principal)

		if len(match) <= 1 {
			err := errors.Newf("regular expression %s did not produce any groups", s.oidcAuthentication.conf.principalRegex)
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		username := match[1]
		cookie, err := s.authentication.createSessionFor(ctx, username)
		if err != nil {
			log.Errorf(ctx, "failed to create session for %s: %v", username, err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		http.SetCookie(w, cookie)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
	})

	s.mux.HandleFunc("/oidc/login", func(w http.ResponseWriter, r *http.Request) {
		s.oidcAuthentication.Lock()
		defer s.oidcAuthentication.Unlock()

		if !s.oidcAuthentication.enabled {
			http.Error(w, "sso via oidc is Disabled", http.StatusBadRequest)
			return
		}

		//TODO(davidh): should this be re-created on every request?
		rand, _ := randutil.NewPseudoRand()
		state := randutil.RandBytes(rand, 10)

		s.oidcAuthentication.states[string(state)] = struct{}{}

		statePb := serverpb.OIDCState{
			NodeID: s.NodeID(),
			Secret: state,
		}

		value, err := encodeOIDCState(statePb)
		if err != nil {
			log.Errorf(ctx, "%v", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		http.Redirect(w, r, s.oidcAuthentication.oauth2Config.AuthCodeURL(value), http.StatusFound)
	})

	loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)

	oidcEnabled.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcClientIDConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcClientSecretConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcRedirectURL.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcProviderURLConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcScopesConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcClaimJSONKeyConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcPrincipalRegex.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})
	oidcButtonText.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, s.oidcAuthentication, st)
	})

	return nil
}

func encodeOIDCState(statePb serverpb.OIDCState) (string, error) {
	stateBytes, err := protoutil.Marshal(&statePb)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(stateBytes), nil
}

func decodeOIDCState(encodedState string) (*serverpb.OIDCState, error) {
	// Cookie value should be a base64 encoded protobuf.
	stateBytes, err := base64.StdEncoding.DecodeString(encodedState)
	if err != nil {
		return nil, errors.Wrap(err, "state could not be decoded")
	}
	var stateValue serverpb.OIDCState
	if err := protoutil.Unmarshal(stateBytes, &stateValue); err != nil {
		return nil, errors.Wrap(err, "state could not be unmarshaled")
	}
	return &stateValue, nil
}
