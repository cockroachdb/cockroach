package slack

import (
	"context"
	"net/url"
)

type emojiResponseFull struct {
	Emoji map[string]string `json:"emoji"`
	SlackResponse
}

// GetEmoji retrieves all the emojis
func (api *Client) GetEmoji() (map[string]string, error) {
	return api.GetEmojiContext(context.Background())
}

// GetEmojiContext retrieves all the emojis with a custom context
func (api *Client) GetEmojiContext(ctx context.Context) (map[string]string, error) {
	values := url.Values{
		"token": {api.token},
	}
	response := &emojiResponseFull{}

	err := api.postMethod(ctx, "emoji.list", values, response)
	if err != nil {
		return nil, err
	}

	if response.Err() != nil {
		return nil, response.Err()
	}

	return response.Emoji, nil
}
