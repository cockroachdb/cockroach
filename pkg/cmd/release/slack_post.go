// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"log"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/httputil"
	"github.com/cockroachdb/errors"
	"github.com/slack-go/slack"
)

// slackClient posts a single message to a Slack channel and returns a
// permalink so we can embed it in the Jira comment that mirrors the
// notification.
type slackClient struct {
	api *slack.Client
}

// slackHTTPTimeout bounds each Slack API call. The slack-go library defaults
// to http.DefaultClient (no timeout); without this option a wedged Slack
// endpoint would hang the run.
const slackHTTPTimeout = 15 * time.Second

func newSlackClient(token string) *slackClient {
	return &slackClient{
		api: slack.New(token,
			slack.OptionHTTPClient(httputil.NewClientWithTimeout(slackHTTPTimeout).Client),
		),
	}
}

// PostMessage posts text to the named channel and returns the message
// permalink (or "" if the link can't be retrieved).
func (s *slackClient) PostMessage(channel, text string) (string, error) {
	channelID, ts, err := s.api.PostMessage(channel,
		slack.MsgOptionText(text, false),
		slack.MsgOptionDisableLinkUnfurl(),
	)
	if err != nil {
		return "", errors.Wrapf(err, "posting to %s", channel)
	}
	link, err := s.api.GetPermalink(&slack.PermalinkParameters{
		Channel: channelID,
		Ts:      ts,
	})
	if err != nil {
		// A missing permalink isn't fatal — the message was delivered, and
		// callers fall back to embedding the bare message body in the Jira
		// comment when the link is empty.
		log.Printf("warning: get permalink for %s/%s: %v", channelID, ts, err)
		return "", nil
	}
	return link, nil
}
