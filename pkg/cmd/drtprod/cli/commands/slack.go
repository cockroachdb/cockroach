// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package commands

import (
	"fmt"
	"os"

	"github.com/cockroachdb/cockroach/pkg/roachprod/config"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/slack-go/slack"
)

type Status string

const (
	StatusStarting  Status = "Starting"
	StatusCompleted Status = "Completed"
	StatusFailed    Status = "Failed"

	envSlackToken   = "SLACK_BOT_TOKEN"
	envSlackChannel = "SLACK_CHANNEL"
)

// Notifier is an interface for sending notifications for target and step status updates
type Notifier interface {
	// SendNotification sends a notification to the defined endpoint.
	SendNotification(targetName string, message string) error
}

// SlackNotifier implements the Notifier interface for Slack
type SlackNotifier struct {
	channel     string // Slack channel to post messages to
	enabled     bool   // Whether Slack integration is enabled
	slackClient *slack.Client
	// Maps each target to its Slack thread timestamp (`threadTS`), ensuring all
	// messages for the same target are posted in a single Slack thread.
	threadTimestamps     map[string]string
	threadTimestampsLock syncutil.Mutex
}

// NewSlackNotifier creates a new SlackNotifier
func NewSlackNotifier() Notifier {
	sn := &SlackNotifier{
		threadTimestamps: make(map[string]string),
	}
	sn.initSlackIntegration(os.Getenv(envSlackToken), os.Getenv(envSlackChannel))
	return sn
}

// InitSlackIntegration initializes the Slack integration
func (sn *SlackNotifier) initSlackIntegration(botToken, channel string) {
	// Check if Slack integration is enabled
	if botToken == "" || channel == "" {
		return
	}

	// Create the Slack client
	sn.slackClient = slack.New(botToken)
	sn.channel = channel
	sn.enabled = true
	config.Logger.Printf("Slack integration initialized successfully for slack channel '%s'\n", channel)
}

// SendNotification sends a notification to the defined Slack endpoint.
func (sn *SlackNotifier) SendNotification(targetName, message string) error {
	if !sn.enabled || sn.slackClient == nil {
		return nil
	}
	return sn.postMessage(targetName, message)
}

// postMessage sends a message to Slack with the given blocks, handling thread tracking
func (sn *SlackNotifier) postMessage(targetName string, messageText string) error {
	// Check if we have a thread timestamp for this target
	threadTS := sn.getThreadTimestamp(targetName)

	var options []slack.MsgOption
	blocks := []slack.Block{
		slack.NewSectionBlock(
			slack.NewTextBlockObject("mrkdwn", messageText, false, false),
			nil,
			nil,
		),
	}

	options = append(options, slack.MsgOptionBlocks(blocks...))

	// `threadTS` is the timestamp of the parent message.
	// Including `threadTS` makes the new message a reply to that parent.
	if threadTS != "" {
		options = append(options, slack.MsgOptionTS(threadTS))
	}

	// Send the message to Slack
	_, timestamp, err := sn.slackClient.PostMessage(
		sn.channel,
		options...,
	)

	// If this is the first message for this target, store the timestamp
	if threadTS == "" && err == nil {
		sn.setThreadTimestamp(targetName, timestamp)
	}

	return err
}

// getThreadTimestamp returns the thread timestamp for a workflow+target combination
// If no thread timestamp exists, it returns an empty string
func (sn *SlackNotifier) getThreadTimestamp(targetName string) string {
	sn.threadTimestampsLock.Lock()
	defer sn.threadTimestampsLock.Unlock()
	return sn.threadTimestamps[targetName]
}

// setThreadTimestamp sets the thread timestamp for a workflow+target combination
func (sn *SlackNotifier) setThreadTimestamp(targetName, timestamp string) {
	sn.threadTimestampsLock.Lock()
	defer sn.threadTimestampsLock.Unlock()
	sn.threadTimestamps[targetName] = timestamp
}

func buildTargetUpdateMessage(targetName string, status Status) string {
	return fmt.Sprintf("%v Target *%s*.", status, targetName)
}

func buildStepUpdateMessage(command string, status Status) string {
	return fmt.Sprintf("%v Command: `%s`", status, command)
}
