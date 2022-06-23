package slack

import (
	"context"
	"net/url"
)

type channelResponseFull struct {
	Channel      Channel   `json:"channel"`
	Channels     []Channel `json:"channels"`
	Purpose      string    `json:"purpose"`
	Topic        string    `json:"topic"`
	NotInChannel bool      `json:"not_in_channel"`
	History
	SlackResponse
	Metadata ResponseMetadata `json:"response_metadata"`
}

// Channel contains information about the channel
type Channel struct {
	GroupConversation
	IsChannel bool   `json:"is_channel"`
	IsGeneral bool   `json:"is_general"`
	IsMember  bool   `json:"is_member"`
	Locale    string `json:"locale"`
}

func (api *Client) channelRequest(ctx context.Context, path string, values url.Values) (*channelResponseFull, error) {
	response := &channelResponseFull{}
	err := postForm(ctx, api.httpclient, api.endpoint+path, values, response, api)
	if err != nil {
		return nil, err
	}

	return response, response.Err()
}
