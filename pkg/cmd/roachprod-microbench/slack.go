// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/slack-go/slack"
)

type slackClient struct {
	*slack.Client
	slackUser    string
	slackChannel string
}

func newSlackClient(slackUser, slackChannel, slackToken string) *slackClient {
	return &slackClient{Client: slack.New(slackToken), slackUser: slackUser, slackChannel: slackChannel}
}

// findChannel finds a slack channel by name. It will recursively search
// through all channels if necessary.
func (c *slackClient) findChannel(name, nextCursor string) (string, error) {
	channels, cursor, err := c.GetConversationsForUser(
		&slack.GetConversationsForUserParameters{Cursor: nextCursor},
	)
	if err != nil {
		return "", errors.Wrapf(err, "slack error")
	}
	for _, channel := range channels {
		if channel.Name == name {
			return channel.ID, nil
		}
	}
	if cursor != "" {
		return c.findChannel(name, cursor)
	}
	return "", fmt.Errorf("unable to find slack channel %s", name)
}

// Post posts a message to slack using the configured slack user and channel for
// roachprod-microbench.
func (c *slackClient) Post(options ...slack.MsgOption) error {
	options = append(options, slack.MsgOptionUsername(c.slackUser))
	channelID, err := c.findChannel(c.slackChannel, "")
	if err != nil {
		return err
	}
	if _, _, err := c.PostMessage(channelID, options...); err != nil {
		return errors.Wrapf(err, "unable to post slack report")
	}
	return nil
}
