package slack

import (
	"context"
	"net/http"
)

type WebhookMessage struct {
	Username        string       `json:"username,omitempty"`
	IconEmoji       string       `json:"icon_emoji,omitempty"`
	IconURL         string       `json:"icon_url,omitempty"`
	Channel         string       `json:"channel,omitempty"`
	ThreadTimestamp string       `json:"thread_ts,omitempty"`
	Text            string       `json:"text,omitempty"`
	Attachments     []Attachment `json:"attachments,omitempty"`
	Parse           string       `json:"parse,omitempty"`
	Blocks          *Blocks      `json:"blocks,omitempty"`
}

func PostWebhook(url string, msg *WebhookMessage) error {
	return PostWebhookCustomHTTPContext(context.Background(), url, http.DefaultClient, msg)
}

func PostWebhookContext(ctx context.Context, url string, msg *WebhookMessage) error {
	return PostWebhookCustomHTTPContext(ctx, url, http.DefaultClient, msg)
}

func PostWebhookCustomHTTP(url string, httpClient *http.Client, msg *WebhookMessage) error {
	return PostWebhookCustomHTTPContext(context.Background(), url, httpClient, msg)
}
