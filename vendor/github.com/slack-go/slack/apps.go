package slack

import (
	"context"
	"encoding/json"
	"net/url"
)

type listEventAuthorizationsResponse struct {
	SlackResponse
	Authorizations []EventAuthorization `json:"authorizations"`
}

type EventAuthorization struct {
	EnterpriseID        string `json:"enterprise_id"`
	TeamID              string `json:"team_id"`
	UserID              string `json:"user_id"`
	IsBot               bool   `json:"is_bot"`
	IsEnterpriseInstall bool   `json:"is_enterprise_install"`
}

func (api *Client) ListEventAuthorizations(eventContext string) ([]EventAuthorization, error) {
	return api.ListEventAuthorizationsContext(context.Background(), eventContext)
}

// ListEventAuthorizationsContext lists authed users and teams for the given event_context. You must provide an app-level token to the client using OptionAppLevelToken. More info: https://api.slack.com/methods/apps.event.authorizations.list
func (api *Client) ListEventAuthorizationsContext(ctx context.Context, eventContext string) ([]EventAuthorization, error) {
	resp := &listEventAuthorizationsResponse{}

	request, _ := json.Marshal(map[string]string{
		"event_context": eventContext,
	})

	err := postJSON(ctx, api.httpclient, api.endpoint+"apps.event.authorizations.list", api.appLevelToken, request, &resp, api)

	if err != nil {
		return nil, err
	}
	if !resp.Ok {
		return nil, resp.Err()
	}

	return resp.Authorizations, nil
}

func (api *Client) UninstallApp(clientID, clientSecret string) error {
	values := url.Values{
		"client_id":     {clientID},
		"client_secret": {clientSecret},
	}

	response := SlackResponse{}

	err := api.getMethod(context.Background(), "apps.uninstall", api.token, values, &response)
	if err != nil {
		return err
	}

	return response.Err()
}
