package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
)

const (
	someStateForNow = "my_special_state"
	IDTokenKey      = "id_token"
)

type oidcAuthenticationServer struct {
	syncutil.RWMutex
	conf         oidcAuthenticationConf
	oauth2Config oauth2.Config
	verifier     *oidc.IDTokenVerifier
	enabled      bool
}

type nodeURL string

type oidcAuthenticationConf struct {
	clientID     string
	clientSecret string
	nodeURLs     map[roachpb.NodeID]nodeURL
	providerURL  string
	scopes       string
	enabled      bool
	claimJSONKey string
}

var oidcEnabled = func() *settings.BoolSetting {
	s := settings.RegisterBoolSetting(
		"server.oidc_authentication.enabled",
		"todo",
		false,
	)
	s.SetVisibility(settings.Public)
	return s
}()

var oidcClientIDConf = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"server.oidc_authentication.client_id",
		"todo",
		"",
	)
	s.SetVisibility(settings.Public)
	return s
}()

var oidcClientSecretConf = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"server.oidc_authentication.client_secret",
		"todo",
		"",
	)
	s.SetVisibility(settings.Public)
	return s
}()

var oidcNodeURLsConf = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"server.oidc_authentication.node_urls",
		"todo",
		"http://localhost/oidc/callback",
	)
	s.SetVisibility(settings.Public)
	return s
}()

var oidcProviderURLConf = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"server.oidc_authentication.provider_url",
		"todo",
		"",
	)
	s.SetVisibility(settings.Public)
	return s
}()

var oidcScopesConf = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"server.oidc_authentication.scopes",
		"todo",
		"",
	)
	s.SetVisibility(settings.Public)
	return s
}()

var oidcClaimJSONKeyConf = func() *settings.StringSetting {
	s := settings.RegisterStringSetting(
		"server.oidc_authentication.claim_json_key",
		"todo",
		"",
	)
	s.SetVisibility(settings.Public)
	return s
}()

func loadLocalOIDCConfigUponRemoteSettingsChange(
	ctx context.Context, server *oidcAuthenticationServer, st *cluster.Settings, nodeID roachpb.NodeID,
) {
	oidcEnabled := oidcEnabled.Get(&st.SV)
	clientID := oidcClientIDConf.Get(&st.SV)
	clientSecret := oidcClientSecretConf.Get(&st.SV)
	nodeURLsString := oidcNodeURLsConf.Get(&st.SV)
	providerURL := oidcProviderURLConf.Get(&st.SV)
	scopes := oidcScopesConf.Get(&st.SV)
	claimJSONKey := oidcClaimJSONKeyConf.Get(&st.SV)

	var nodeURLs map[roachpb.NodeID]nodeURL
	err := json.Unmarshal([]byte(nodeURLsString), &nodeURLs)
	if err != nil {
		log.Warningf(ctx, "unable to deserialize node urls from settings, disabling oidc: %v", err)
		oidcEnabled = false
	}

	oidcAuthenticationConf := oidcAuthenticationConf{
		clientID:     clientID,
		clientSecret: clientSecret,
		nodeURLs:     nodeURLs,
		providerURL:  providerURL,
		scopes:       scopes,
		claimJSONKey: claimJSONKey,
		enabled:      oidcEnabled,
	}

	server.Lock()
	defer server.Unlock()
	server.conf = oidcAuthenticationConf
	setUpOidcServer(ctx, server, nodeID)
}

func (s *Server) ConfigureOIDC(ctx context.Context, st *cluster.Settings) error {

	oidcServer := oidcAuthenticationServer{}

	/**
	Don't want to use GRPC here since these endpoints require HTTP-Redirect behaviors and the callback
	endpoint will be receiving specialized parameters that grpc-gateway will only get in the way
	of processing.
	*/
	s.mux.HandleFunc("/oidc/callback", func(w http.ResponseWriter, r *http.Request) {
		// Verify state and errors.
		oidcServer.Lock()
		defer oidcServer.Unlock()

		if !oidcServer.enabled {
			http.Error(w, "SSO via OIDC is Disabled", http.StatusForbidden)
			return
		}

		oauth2Token, err := oidcServer.oauth2Config.Exchange(ctx, r.URL.Query().Get("code"))
		if err != nil {
			http.Error(w, fmt.Sprintf("exchange failed, %v", err), http.StatusInternalServerError)
			return
		}

		// Extract the ID Token from OAuth2 token.
		rawIDToken, ok := oauth2Token.Extra(IDTokenKey).(string)
		if !ok {
			http.Error(w, "couldn't extract id token", http.StatusInternalServerError)
			return
		}

		// Parse and verify ID Token payload.
		idToken, err := oidcServer.verifier.Verify(ctx, rawIDToken)
		if err != nil {
			http.Error(w, fmt.Sprintf("verifier failed, %v", err.Error()), http.StatusInternalServerError)
			return
		}

		// Extract custom claims
		var claims map[string]json.RawMessage
		if err := idToken.Claims(&claims); err != nil {
			http.Error(w, fmt.Sprintf("deserialization %v", err.Error()), http.StatusInternalServerError)
			return
		}

		var username string
		if err := json.Unmarshal(claims[oidcServer.conf.claimJSONKey], &username); err != nil {
			http.Error(w, fmt.Sprintf("unable to decode claim key %s from payload %v", oidcServer.conf.claimJSONKey, claims), http.StatusBadRequest)
			return
		}

		cookie, err := s.authentication.createSessionFor(ctx, username)
		if err != nil {
			http.Error(w, fmt.Sprintf("couldnt create session %v", err.Error()), http.StatusInternalServerError)
			return
		}

		http.SetCookie(w, cookie)
		http.Redirect(w, r, "/", http.StatusTemporaryRedirect)
	})

	s.mux.HandleFunc("/oidc/login", func(w http.ResponseWriter, r *http.Request) {
		oidcServer.Lock()
		defer oidcServer.Unlock()

		if !oidcServer.enabled {
			http.Error(w, "SSO via OIDC is Disabled", http.StatusForbidden)
			return
		}

		http.Redirect(w, r, oidcServer.oauth2Config.AuthCodeURL(someStateForNow), http.StatusFound)
	})

	oidcServer.Lock()
	defer oidcServer.Unlock()
	nodeID := s.NodeID()

	setUpOidcServer(ctx, &oidcServer, nodeID)

	oidcEnabled.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, &oidcServer, st, nodeID)
	})
	oidcClientIDConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, &oidcServer, st, nodeID)
	})
	oidcClientSecretConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, &oidcServer, st, nodeID)
	})
	oidcNodeURLsConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, &oidcServer, st, nodeID)
	})
	oidcProviderURLConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, &oidcServer, st, nodeID)
	})
	oidcScopesConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, &oidcServer, st, nodeID)
	})
	oidcClaimJSONKeyConf.SetOnChange(&st.SV, func() {
		loadLocalOIDCConfigUponRemoteSettingsChange(ctx, &oidcServer, st, nodeID)
	})

	return nil
}

func setUpOidcServer(ctx context.Context, oidcServer *oidcAuthenticationServer, nodeID roachpb.NodeID) {
	if oidcServer.conf.enabled {
		oidcServer.enabled = true
	} else {
		oidcServer.enabled = false
		return
	}

	provider, err := oidc.NewProvider(ctx, oidcServer.conf.providerURL)
	if err != nil {
		log.Warningf(ctx, "unable to initialize oidc provider, disabling oidc: %v", err)
		oidcServer.enabled = false
		return
	}

	scopesForOauth := []string{oidc.ScopeOpenID}
	scopesForOauth = append(scopesForOauth, strings.Split(oidcServer.conf.scopes, " ")...)

	oidcServer.oauth2Config = oauth2.Config{
		ClientID:     oidcServer.conf.clientID,
		ClientSecret: oidcServer.conf.clientSecret,
		//TODO(davidh): append urls the smart way
		RedirectURL: fmt.Sprintf("%s/oidc/callback", oidcServer.conf.nodeURLs[nodeID]),

		Endpoint: provider.Endpoint(),

		Scopes: scopesForOauth,
	}

	oidcServer.verifier = provider.Verifier(&oidc.Config{ClientID: oidcServer.conf.clientID})
	log.Infof(ctx, "initialized oidc server with params: %v", oidcServer.oauth2Config)
}
