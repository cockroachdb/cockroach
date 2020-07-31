package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/coreos/go-oidc"
	"golang.org/x/oauth2"
)


const (
	clientID = "TODO_CLIENT_ID"
	clientSecret = "TODO_CLIENT_SECRET"
	redirectURL = "http://localhost:8080/oidc/callback"
	someStateForNow = "my_special_state"
	providerURL = "https://accounts.google.com"
	IDTokenKey = "id_token"
)

var scopes = []string{"profile", "email"}

func (s *Server) ConfigureOIDC(ctx context.Context) error {
	provider, err := oidc.NewProvider(ctx, providerURL)
	if err != nil {
		panic(err)
	}

	scopesForOauth := []string{oidc.ScopeOpenID}
	for _, scope := range scopes {
		scopesForOauth = append(scopesForOauth, scope)
	}

	oauth2Config := oauth2.Config{
		ClientID: clientID,
		ClientSecret: clientSecret,
		RedirectURL: redirectURL,

		Endpoint: provider.Endpoint(),

		Scopes: scopesForOauth,
	}

	verifier := provider.Verifier(&oidc.Config{ClientID: clientID})

	/**
	Don't want to use GRPC here since these endpoints require HTTP-Redirect behaviors and the callback
	endpoint will be receiving specialized parameters that grpc-gateway will only get in the way
	of processing.
	*/
	s.mux.HandleFunc("/oidc/callback", func(w http.ResponseWriter, r *http.Request) {
		// Verify state and errors.

		oauth2Token, err := oauth2Config.Exchange(ctx, r.URL.Query().Get("code"))
		if err != nil {
			http.Error(w, fmt.Sprintf("exchange failed, %v", err), http.StatusInternalServerError)
		}

		// Extract the ID Token from OAuth2 token.
		rawIDToken, ok := oauth2Token.Extra(IDTokenKey).(string)
		if !ok {
			http.Error(w, "couldn't extract id token", http.StatusInternalServerError)
		}

		// Parse and verify ID Token payload.
		idToken, err := verifier.Verify(ctx, rawIDToken)
		if err != nil {
			http.Error(w, fmt.Sprintf("verifier failed, %v", err.Error()), http.StatusInternalServerError)
		}



		// Extract custom claims
		var claims struct {
			Email    string `json:"email"`
			Verified bool   `json:"email_verified"`
		}
		if err := idToken.Claims(&claims); err != nil {
			http.Error(w, fmt.Sprintf("deserialization %v", err.Error()), http.StatusInternalServerError)
		}

		if claims.Verified {
			email := strings.Split(claims.Email, "@")
			if email[1] == "cockroachlabs.com" {
				/**
				Create session for username to the left of the `@` in the email address.

				This actually works regardless whether the user exists at the moment. :/ They gain
				non-admin privileges it seems.
				 */
				cookie, err := s.authentication.createSessionFor(ctx, email[0])
				if err != nil {
					http.Error(w, fmt.Sprintf("couldnt create session %v", err.Error()), http.StatusInternalServerError)
				}

				http.SetCookie(w, cookie)
				http.Redirect(w,r,"/", http.StatusTemporaryRedirect)
			} else {
				http.Error(w, "not roacher", http.StatusForbidden)
			}
		} else {
			http.Error(w, "email not verified", http.StatusForbidden)
		}

	})

	s.mux.HandleFunc("/oidc/login", func (w http.ResponseWriter, r *http.Request) {
		http.Redirect(w,r,oauth2Config.AuthCodeURL(someStateForNow), http.StatusFound)
	})

	return nil
}
