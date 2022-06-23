package slack

import (
	"context"
	"net/url"
)

// AuthRevokeResponse contains our Auth response from the auth.revoke endpoint
type AuthRevokeResponse struct {
	SlackResponse      // Contains the "ok", and "Error", if any
	Revoked       bool `json:"revoked,omitempty"`
}

// authRequest sends the actual request, and unmarshals the response
func (api *Client) authRequest(ctx context.Context, path string, values url.Values) (*AuthRevokeResponse, error) {
	response := &AuthRevokeResponse{}
	err := api.postMethod(ctx, path, values, response)
	if err != nil {
		return nil, err
	}

	return response, response.Err()
}

// SendAuthRevoke will send a revocation for our token
func (api *Client) SendAuthRevoke(token string) (*AuthRevokeResponse, error) {
	return api.SendAuthRevokeContext(context.Background(), token)
}

// SendAuthRevokeContext will send a revocation request for our token to api.revoke with context
func (api *Client) SendAuthRevokeContext(ctx context.Context, token string) (*AuthRevokeResponse, error) {
	if token == "" {
		token = api.token
	}
	values := url.Values{
		"token": {token},
	}

	return api.authRequest(ctx, "auth.revoke", values)
}
