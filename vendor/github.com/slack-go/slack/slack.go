package slack

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
)

const (
	// APIURL of the slack api.
	APIURL = "https://slack.com/api/"
	// WEBAPIURLFormat ...
	WEBAPIURLFormat = "https://%s.slack.com/api/users.admin.%s?t=%d"
)

// httpClient defines the minimal interface needed for an http.Client to be implemented.
type httpClient interface {
	Do(*http.Request) (*http.Response, error)
}

// ResponseMetadata holds pagination metadata
type ResponseMetadata struct {
	Cursor   string   `json:"next_cursor"`
	Messages []string `json:"messages"`
	Warnings []string `json:"warnings"`
}

func (t *ResponseMetadata) initialize() *ResponseMetadata {
	if t != nil {
		return t
	}

	return &ResponseMetadata{}
}

// AuthTestResponse ...
type AuthTestResponse struct {
	URL    string `json:"url"`
	Team   string `json:"team"`
	User   string `json:"user"`
	TeamID string `json:"team_id"`
	UserID string `json:"user_id"`
	// EnterpriseID is only returned when an enterprise id present
	EnterpriseID string `json:"enterprise_id,omitempty"`
	BotID        string `json:"bot_id"`
}

type authTestResponseFull struct {
	SlackResponse
	AuthTestResponse
}

// Client for the slack api.
type ParamOption func(*url.Values)

type Client struct {
	token         string
	appLevelToken string
	endpoint      string
	debug         bool
	log           ilogger
	httpclient    httpClient
}

// Option defines an option for a Client
type Option func(*Client)

// OptionHTTPClient - provide a custom http client to the slack client.
func OptionHTTPClient(client httpClient) func(*Client) {
	return func(c *Client) {
		c.httpclient = client
	}
}

// OptionDebug enable debugging for the client
func OptionDebug(b bool) func(*Client) {
	return func(c *Client) {
		c.debug = b
	}
}

// OptionLog set logging for client.
func OptionLog(l logger) func(*Client) {
	return func(c *Client) {
		c.log = internalLog{logger: l}
	}
}

// OptionAPIURL set the url for the client. only useful for testing.
func OptionAPIURL(u string) func(*Client) {
	return func(c *Client) { c.endpoint = u }
}

// OptionAppLevelToken sets an app-level token for the client.
func OptionAppLevelToken(token string) func(*Client) {
	return func(c *Client) { c.appLevelToken = token }
}

// New builds a slack client from the provided token and options.
func New(token string, options ...Option) *Client {
	s := &Client{
		token:      token,
		endpoint:   APIURL,
		httpclient: &http.Client{},
		log:        log.New(os.Stderr, "slack-go/slack", log.LstdFlags|log.Lshortfile),
	}

	for _, opt := range options {
		opt(s)
	}

	return s
}

// AuthTest tests if the user is able to do authenticated requests or not
func (api *Client) AuthTest() (response *AuthTestResponse, error error) {
	return api.AuthTestContext(context.Background())
}

// AuthTestContext tests if the user is able to do authenticated requests or not with a custom context
func (api *Client) AuthTestContext(ctx context.Context) (response *AuthTestResponse, err error) {
	api.Debugf("Challenging auth...")
	responseFull := &authTestResponseFull{}
	err = api.postMethod(ctx, "auth.test", url.Values{"token": {api.token}}, responseFull)
	if err != nil {
		return nil, err
	}

	return &responseFull.AuthTestResponse, responseFull.Err()
}

// Debugf print a formatted debug line.
func (api *Client) Debugf(format string, v ...interface{}) {
	if api.debug {
		api.log.Output(2, fmt.Sprintf(format, v...))
	}
}

// Debugln print a debug line.
func (api *Client) Debugln(v ...interface{}) {
	if api.debug {
		api.log.Output(2, fmt.Sprintln(v...))
	}
}

// Debug returns if debug is enabled.
func (api *Client) Debug() bool {
	return api.debug
}

// post to a slack web method.
func (api *Client) postMethod(ctx context.Context, path string, values url.Values, intf interface{}) error {
	return postForm(ctx, api.httpclient, api.endpoint+path, values, intf, api)
}

// get a slack web method.
func (api *Client) getMethod(ctx context.Context, path string, token string, values url.Values, intf interface{}) error {
	return getResource(ctx, api.httpclient, api.endpoint+path, token, values, intf, api)
}
